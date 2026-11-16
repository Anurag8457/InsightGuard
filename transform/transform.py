"""Transform landed transactions into a star schema and load into Postgres."""

import argparse
import hashlib
import logging
import os
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

LOGGER = logging.getLogger("insightguard.transform")

STAR_TABLES = (
    "fact_sales",
    "dim_product",
    "dim_customer",
    "dim_date",
    "dim_region",
)


def _stable_key(prefix, value):
    """Hash-based key so dimension IDs are deterministic across runs."""
    digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def country_to_region(country):
    """Bucket countries into a handful of reporting regions.
    Nothing fancy — just enough granularity for the dashboard."""
    c = country.strip().lower()
    if c in {"united kingdom", "uk", "great britain"}:
        return "United Kingdom"
    if c in {
        "austria", "belgium", "cyprus", "denmark", "eire", "finland", "france",
        "germany", "greece", "iceland", "italy", "malta", "netherlands", "norway",
        "poland", "portugal", "spain", "sweden", "switzerland",
    }:
        return "Europe"
    return "Other"


def clean_transactions(frame, ingestion_date):
    """Normalize types, drop dupes, remove zero-qty rows, compute derived columns."""
    required = {
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
        "UnitPrice", "CustomerID", "Country",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    df = frame.copy()
    df = df.drop_duplicates().reset_index(drop=True)

    # normalize string columns
    df["InvoiceNo"] = df["InvoiceNo"].astype("string").str.strip()
    df["StockCode"] = df["StockCode"].astype("string").str.strip()
    df["Description"] = df["Description"].fillna("Unknown product").astype("string").str.strip()
    df["Country"] = df["Country"].fillna("Unknown").astype("string").str.strip()

    # coerce numerics
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce", dayfirst=True)
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce").astype("Int64")

    # filter out rows we can't use
    keep = (
        df["InvoiceNo"].notna()
        & df["StockCode"].notna()
        & df["InvoiceDate"].notna()
        & df["Quantity"].notna()
        & df["UnitPrice"].notna()
        & df["UnitPrice"].ge(0)
        & df["Quantity"].ne(0)
    )
    df = df.loc[keep].copy()

    df["InvoiceDate"] = df["InvoiceDate"].dt.tz_localize(None)
    df["ingestion_date"] = pd.Timestamp(ingestion_date)
    df["currency_code"] = "GBP"
    df["region_name"] = df["Country"].map(country_to_region)

    # revenue calculations
    df["gross_sales"] = (df["Quantity"] * df["UnitPrice"]).round(2)
    df["return_amount"] = df["gross_sales"].where(df["Quantity"].lt(0), 0).abs()
    df["net_sales"] = df["gross_sales"].where(df["Quantity"].ge(0), 0)
    df["is_return"] = df["Quantity"].lt(0) | df["InvoiceNo"].str.upper().str.startswith("C")

    return df.reset_index(drop=True)


def build_star_schema(cleaned):
    """Take the cleaned dataframe and split it into dims + fact table."""
    if cleaned.empty:
        raise ValueError("No valid transactions remain after cleaning")

    # dim_product
    products = (
        cleaned[["StockCode", "Description"]]
        .drop_duplicates("StockCode")
        .sort_values("StockCode")
        .rename(columns={"StockCode": "stock_code", "Description": "description"})
        .reset_index(drop=True)
    )
    products.insert(0, "product_key", products["stock_code"].map(lambda x: _stable_key("prod", x)))

    # dim_customer — UNKNOWN for missing customer IDs
    customers = (
        cleaned[["CustomerID", "Country"]]
        .assign(customer_id=lambda d: d["CustomerID"].astype("string").fillna("UNKNOWN"))
        .drop(columns="CustomerID")
        .drop_duplicates("customer_id")
        .sort_values("customer_id")
        .rename(columns={"Country": "country"})
        .reset_index(drop=True)
    )
    customers.insert(0, "customer_key", customers["customer_id"].map(lambda x: _stable_key("cust", x)))

    # dim_region
    regions = (
        cleaned[["region_name"]]
        .drop_duplicates()
        .sort_values("region_name")
        .reset_index(drop=True)
    )
    regions.insert(0, "region_key", regions["region_name"].map(lambda x: _stable_key("region", x)))

    # dim_date
    dates = pd.DataFrame({"full_date": cleaned["InvoiceDate"].dt.normalize().drop_duplicates()})
    dates = dates.sort_values("full_date").reset_index(drop=True)
    dates.insert(0, "date_key", dates["full_date"].dt.strftime("%Y%m%d").astype(int))
    dates["year"] = dates["full_date"].dt.year
    dates["quarter"] = dates["full_date"].dt.quarter
    dates["month"] = dates["full_date"].dt.month
    dates["month_name"] = dates["full_date"].dt.month_name()
    dates["week_of_year"] = dates["full_date"].dt.isocalendar().week.astype(int)
    dates["day_of_month"] = dates["full_date"].dt.day
    dates["full_date"] = dates["full_date"].dt.date

    # fact_sales
    facts = cleaned.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "CustomerID": "customer_id_raw",
        "InvoiceDate": "invoice_datetime",
        "UnitPrice": "unit_price",
        "Quantity": "quantity",
    }).copy()

    facts["product_key"] = facts["stock_code"].map(lambda x: _stable_key("prod", x))
    facts["customer_key"] = (
        facts["customer_id_raw"].astype("string").fillna("UNKNOWN")
        .map(lambda x: _stable_key("cust", x))
    )
    facts["region_key"] = facts["region_name"].map(lambda x: _stable_key("region", x))
    facts["date_key"] = facts["invoice_datetime"].dt.strftime("%Y%m%d").astype(int)

    # build a composite transaction ID for dedup
    facts["transaction_id"] = facts.apply(
        lambda row: _stable_key(
            "txn", f"{row['invoice_no']}|{row['stock_code']}|{row['invoice_datetime']}|{row['quantity']}|{row['unit_price']}"
        ),
        axis=1,
    )

    fact_cols = [
        "transaction_id", "invoice_no", "product_key", "customer_key", "date_key", "region_key",
        "quantity", "unit_price", "currency_code", "gross_sales", "return_amount", "net_sales",
        "is_return", "ingestion_date",
    ]
    facts = facts[fact_cols]
    facts["ingestion_date"] = pd.to_datetime(facts["ingestion_date"]).dt.date

    return {
        "dim_product": products,
        "dim_customer": customers,
        "dim_date": dates,
        "dim_region": regions,
        "fact_sales": facts,
    }


def _make_s3_client(endpoint_url=None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


def read_landing_transactions(bucket, ingestion_date, *, endpoint_url=None):
    """Pull the validated CSV from the landing partition in S3."""
    client = _make_s3_client(endpoint_url)
    key = f"landing/ingestion_date={ingestion_date.isoformat()}/validated/transactions.csv"
    resp = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(resp["Body"])


def write_star_schema_to_s3(star_schema, bucket, ingestion_date, *, endpoint_url=None):
    """Dump transformed tables as CSVs into the curated partition."""
    client = _make_s3_client(endpoint_url)
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)

    keys = []
    prefix = f"curated/ingestion_date={ingestion_date.isoformat()}"
    for name, df in star_schema.items():
        key = f"{prefix}/{name}.csv"
        client.put_object(
            Bucket=bucket, Key=key,
            Body=df.to_csv(index=False).encode("utf-8"),
            ContentType="text/csv",
        )
        keys.append(key)
    return keys


def read_star_schema_from_s3(bucket, ingestion_date, *, endpoint_url=None):
    """Read back the curated CSVs written by write_star_schema_to_s3."""
    client = _make_s3_client(endpoint_url)
    prefix = f"curated/ingestion_date={ingestion_date.isoformat()}"
    result = {}
    for tbl in ("dim_product", "dim_customer", "dim_date", "dim_region", "fact_sales"):
        resp = client.get_object(Bucket=bucket, Key=f"{prefix}/{tbl}.csv")
        df = pd.read_csv(resp["Body"])
        if tbl == "dim_date":
            df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
        if tbl == "fact_sales":
            df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date
        result[tbl] = df
    return result


# ---- Postgres DDL + loading ----

def create_star_schema(engine):
    """Set up the warehouse tables if they don't exist yet."""
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key VARCHAR(64) PRIMARY KEY,
        stock_code VARCHAR(64) NOT NULL UNIQUE,
        description TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key VARCHAR(64) PRIMARY KEY,
        customer_id VARCHAR(64) NOT NULL UNIQUE,
        country VARCHAR(128) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE NOT NULL UNIQUE,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        month INTEGER NOT NULL,
        month_name VARCHAR(20) NOT NULL,
        week_of_year INTEGER NOT NULL,
        day_of_month INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dim_region (
        region_key VARCHAR(64) PRIMARY KEY,
        region_name VARCHAR(64) NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS fact_sales (
        sales_id BIGSERIAL PRIMARY KEY,
        transaction_id VARCHAR(64) NOT NULL UNIQUE,
        invoice_no VARCHAR(64) NOT NULL,
        product_key VARCHAR(64) NOT NULL REFERENCES dim_product(product_key),
        customer_key VARCHAR(64) NOT NULL REFERENCES dim_customer(customer_key),
        date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
        region_key VARCHAR(64) NOT NULL REFERENCES dim_region(region_key),
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12, 2) NOT NULL,
        currency_code CHAR(3) NOT NULL,
        gross_sales NUMERIC(14, 2) NOT NULL,
        return_amount NUMERIC(14, 2) NOT NULL,
        net_sales NUMERIC(14, 2) NOT NULL,
        is_return BOOLEAN NOT NULL,
        ingestion_date DATE NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_fact_sales_ingestion_date ON fact_sales(ingestion_date);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_date_region ON fact_sales(date_key, region_key);
    """
    with engine.begin() as conn:
        for stmt in ddl.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def load_star_schema(engine, star_schema, ingestion_date):
    """Upsert dims (ON CONFLICT DO NOTHING) and replace the fact partition for this date."""
    create_star_schema(engine)

    with engine.begin() as conn:
        # wipe facts for this partition so re-runs are clean
        conn.execute(
            text("DELETE FROM fact_sales WHERE ingestion_date = :dt"),
            {"dt": ingestion_date},
        )

        # load dimensions
        pk_map = {
            "dim_product": "product_key",
            "dim_customer": "customer_key",
            "dim_date": "date_key",
            "dim_region": "region_key",
        }
        for tbl_name in ("dim_product", "dim_customer", "dim_date", "dim_region"):
            df = star_schema[tbl_name]
            records = df.where(pd.notna(df), None).to_dict(orient="records")
            if not records:
                continue
            cols = list(df.columns)
            col_sql = ", ".join(cols)
            val_sql = ", ".join(f":{c}" for c in cols)
            pk = pk_map[tbl_name]
            conn.execute(
                text(f"INSERT INTO {tbl_name} ({col_sql}) VALUES ({val_sql}) ON CONFLICT ({pk}) DO NOTHING"),
                records,
            )

        # load facts
        fact_df = star_schema["fact_sales"].where(pd.notna(star_schema["fact_sales"]), None)
        records = fact_df.to_dict(orient="records")
        if records:
            cols = list(fact_df.columns)
            col_sql = ", ".join(cols)
            val_sql = ", ".join(f":{c}" for c in cols)
            conn.execute(
                text(f"INSERT INTO fact_sales ({col_sql}) VALUES ({val_sql}) ON CONFLICT (transaction_id) DO NOTHING"),
                records,
            )


def transform_and_load(frame, ingestion_date, *, database_url=None):
    """Main entry point: clean → build star schema → load into Postgres."""
    star = build_star_schema(clean_transactions(frame, ingestion_date))
    engine = create_engine(database_url or os.environ["DATABASE_URL"])
    load_star_schema(engine, star, ingestion_date)
    LOGGER.info("Loaded %d fact rows for %s", len(star["fact_sales"]), ingestion_date)
    return star


def parse_args():
    parser = argparse.ArgumentParser(description="Transform and load retail data into warehouse")
    parser.add_argument("--ingestion-date", required=True, type=date.fromisoformat)
    parser.add_argument("--input", type=Path, help="Local CSV instead of reading from S3")
    parser.add_argument("--bucket", default=os.getenv("LANDING_BUCKET", "insightguard-landing"))
    parser.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    if args.input:
        source = pd.read_csv(args.input)
    else:
        source = read_landing_transactions(args.bucket, args.ingestion_date, endpoint_url=args.endpoint_url)
    transform_and_load(source, args.ingestion_date, database_url=args.database_url)
