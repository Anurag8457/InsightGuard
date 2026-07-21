"""Clean landed transactions and load an idempotent PostgreSQL star schema."""

from __future__ import annotations

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


def _stable_key(prefix: str, value: object) -> str:
    digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def country_to_region(country: str) -> str:
    """Map source countries to a small, explainable reporting geography."""
    normalized = country.strip().lower()
    if normalized in {"united kingdom", "uk", "great britain"}:
        return "United Kingdom"
    if normalized in {
        "austria", "belgium", "cyprus", "denmark", "eire", "finland", "france",
        "germany", "greece", "iceland", "italy", "malta", "netherlands", "norway",
        "poland", "portugal", "spain", "sweden", "switzerland",
    }:
        return "Europe"
    return "Other"


def clean_transactions(frame: pd.DataFrame, ingestion_date: date) -> pd.DataFrame:
    """Normalize source types and remove unusable or exact duplicate rows."""
    required = {
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
        "UnitPrice", "CustomerID", "Country",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    cleaned = frame.copy()
    cleaned = cleaned.drop_duplicates().reset_index(drop=True)
    cleaned["InvoiceNo"] = cleaned["InvoiceNo"].astype("string").str.strip()
    cleaned["StockCode"] = cleaned["StockCode"].astype("string").str.strip()
    cleaned["Description"] = cleaned["Description"].fillna("Unknown product").astype("string").str.strip()
    cleaned["Country"] = cleaned["Country"].fillna("Unknown").astype("string").str.strip()
    cleaned["Quantity"] = pd.to_numeric(cleaned["Quantity"], errors="coerce")
    cleaned["UnitPrice"] = pd.to_numeric(cleaned["UnitPrice"], errors="coerce")
    cleaned["InvoiceDate"] = pd.to_datetime(cleaned["InvoiceDate"], errors="coerce", dayfirst=True)
    cleaned["CustomerID"] = pd.to_numeric(cleaned["CustomerID"], errors="coerce").astype("Int64")

    valid = (
        cleaned["InvoiceNo"].notna()
        & cleaned["StockCode"].notna()
        & cleaned["InvoiceDate"].notna()
        & cleaned["Quantity"].notna()
        & cleaned["UnitPrice"].notna()
        & cleaned["UnitPrice"].ge(0)
        & cleaned["Quantity"].ne(0)
    )
    cleaned = cleaned.loc[valid].copy()
    cleaned["InvoiceDate"] = cleaned["InvoiceDate"].dt.tz_localize(None)
    cleaned["ingestion_date"] = pd.Timestamp(ingestion_date)
    cleaned["currency_code"] = "GBP"
    cleaned["region_name"] = cleaned["Country"].map(country_to_region)
    cleaned["gross_sales"] = (cleaned["Quantity"] * cleaned["UnitPrice"]).round(2)
    cleaned["return_amount"] = cleaned["gross_sales"].where(cleaned["Quantity"].lt(0), 0).abs()
    cleaned["net_sales"] = cleaned["gross_sales"].where(cleaned["Quantity"].ge(0), 0)
    cleaned["is_return"] = cleaned["Quantity"].lt(0) | cleaned["InvoiceNo"].str.upper().str.startswith("C")
    return cleaned.reset_index(drop=True)


def build_star_schema(cleaned: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build dimension and fact frames with deterministic keys for safe reruns."""
    if cleaned.empty:
        raise ValueError("No valid transactions remain after cleaning")

    products = (
        cleaned[["StockCode", "Description"]]
        .drop_duplicates("StockCode")
        .sort_values("StockCode")
        .rename(columns={"StockCode": "stock_code", "Description": "description"})
        .reset_index(drop=True)
    )
    products.insert(0, "product_key", products["stock_code"].map(lambda x: _stable_key("prod", x)))

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

    regions = (
        cleaned[["region_name"]]
        .drop_duplicates()
        .sort_values("region_name")
        .reset_index(drop=True)
    )
    regions.insert(0, "region_key", regions["region_name"].map(lambda x: _stable_key("region", x)))

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

    facts = cleaned.rename(
        columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "CustomerID": "customer_id_raw",
            "InvoiceDate": "invoice_datetime",
            "UnitPrice": "unit_price",
            "Quantity": "quantity",
        }
    ).copy()
    facts["product_key"] = facts["stock_code"].map(lambda x: _stable_key("prod", x))
    facts["customer_key"] = facts["customer_id_raw"].astype("string").fillna("UNKNOWN").map(lambda x: _stable_key("cust", x))
    facts["region_key"] = facts["region_name"].map(lambda x: _stable_key("region", x))
    facts["date_key"] = facts["invoice_datetime"].dt.strftime("%Y%m%d").astype(int)
    facts["transaction_id"] = facts.apply(
        lambda row: _stable_key(
            "txn", f"{row['invoice_no']}|{row['stock_code']}|{row['invoice_datetime']}|{row['quantity']}|{row['unit_price']}"
        ),
        axis=1,
    )
    facts = facts[
        [
            "transaction_id", "invoice_no", "product_key", "customer_key", "date_key", "region_key",
            "quantity", "unit_price", "currency_code", "gross_sales", "return_amount", "net_sales",
            "is_return", "ingestion_date",
        ]
    ]
    facts["ingestion_date"] = pd.to_datetime(facts["ingestion_date"]).dt.date

    return {
        "dim_product": products,
        "dim_customer": customers,
        "dim_date": dates,
        "dim_region": regions,
        "fact_sales": facts,
    }


def read_landing_transactions(
    bucket: str,
    ingestion_date: date,
    *,
    endpoint_url: str | None = None,
) -> pd.DataFrame:
    """Read the validated transaction object for one landing partition."""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
    key = f"landing/ingestion_date={ingestion_date.isoformat()}/validated/transactions.csv"
    response = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(response["Body"])


def write_star_schema_to_s3(
    star_schema: dict[str, pd.DataFrame],
    bucket: str,
    ingestion_date: date,
    *,
    endpoint_url: str | None = None,
) -> list[str]:
    """Persist transformed tables as CSV artifacts for a later load task."""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)

    keys = []
    partition = f"curated/ingestion_date={ingestion_date.isoformat()}"
    for table_name, frame in star_schema.items():
        key = f"{partition}/{table_name}.csv"
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=frame.to_csv(index=False).encode("utf-8"),
            ContentType="text/csv",
        )
        keys.append(key)
    return keys


def read_star_schema_from_s3(
    bucket: str,
    ingestion_date: date,
    *,
    endpoint_url: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Read transformed table artifacts written by the transform task."""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
    partition = f"curated/ingestion_date={ingestion_date.isoformat()}"
    star_schema = {}
    for table_name in ("dim_product", "dim_customer", "dim_date", "dim_region", "fact_sales"):
        response = client.get_object(Bucket=bucket, Key=f"{partition}/{table_name}.csv")
        frame = pd.read_csv(response["Body"])
        if table_name == "dim_date":
            frame["full_date"] = pd.to_datetime(frame["full_date"]).dt.date
        if table_name == "fact_sales":
            frame["ingestion_date"] = pd.to_datetime(frame["ingestion_date"]).dt.date
        star_schema[table_name] = frame
    return star_schema


def create_star_schema(engine) -> None:
    """Create PostgreSQL tables and constraints if they do not exist."""
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
    with engine.begin() as connection:
        for statement in ddl.split(";"):
            if statement.strip():
                connection.execute(text(statement))


def load_star_schema(engine, star_schema: dict[str, pd.DataFrame], ingestion_date: date) -> None:
    """Load dimensions and replace only one ingestion-date fact partition."""
    create_star_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM fact_sales WHERE ingestion_date = :ingestion_date"), {"ingestion_date": ingestion_date})
        for table_name in ("dim_product", "dim_customer", "dim_date", "dim_region"):
            frame = star_schema[table_name]
            records = frame.where(pd.notna(frame), None).to_dict(orient="records")
            if not records:
                continue
            columns = list(frame.columns)
            column_sql = ", ".join(columns)
            value_sql = ", ".join(f":{column}" for column in columns)
            conflict_column = {
                "dim_product": "product_key",
                "dim_customer": "customer_key",
                "dim_date": "date_key",
                "dim_region": "region_key",
            }[table_name]
            connection.execute(
                text(f"INSERT INTO {table_name} ({column_sql}) VALUES ({value_sql}) ON CONFLICT ({conflict_column}) DO NOTHING"),
                records,
            )

        facts = star_schema["fact_sales"].where(pd.notna(star_schema["fact_sales"]), None)
        records = facts.to_dict(orient="records")
        if records:
            columns = list(facts.columns)
            column_sql = ", ".join(columns)
            value_sql = ", ".join(f":{column}" for column in columns)
            connection.execute(
                text(f"INSERT INTO fact_sales ({column_sql}) VALUES ({value_sql}) ON CONFLICT (transaction_id) DO NOTHING"),
                records,
            )


def transform_and_load(
    frame: pd.DataFrame,
    ingestion_date: date,
    *,
    database_url: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Clean, model, and load one landed partition."""
    star_schema = build_star_schema(clean_transactions(frame, ingestion_date))
    engine = create_engine(database_url or os.environ["DATABASE_URL"])
    load_star_schema(engine, star_schema, ingestion_date)
    LOGGER.info("Loaded %d fact rows for ingestion_date=%s", len(star_schema["fact_sales"]), ingestion_date)
    return star_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingestion-date", required=True, type=date.fromisoformat)
    parser.add_argument("--input", type=Path, help="Local validated CSV; otherwise read S3 landing storage")
    parser.add_argument("--bucket", default=os.getenv("LANDING_BUCKET", "insightguard-landing"))
    parser.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    source = pd.read_csv(args.input) if args.input else read_landing_transactions(args.bucket, args.ingestion_date, endpoint_url=args.endpoint_url)
    transform_and_load(source, args.ingestion_date, database_url=args.database_url)
