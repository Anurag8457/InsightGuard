"""Ingestion module — downloads, validates, and lands retail transaction data in S3."""

import argparse
import io
import json
import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import requests

LOGGER = logging.getLogger("insightguard.ingestion")

DEFAULT_SOURCE_URL = (
    "https://github.com/dbdmg/data-science-lab/raw/master/datasets/online_retail.csv"
)
REQUIRED_COLUMNS = {
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
}
OPTIONAL_NULLABLE_COLUMNS = {"CustomerID", "Description"}


@dataclass(frozen=True)
class ValidationReport:
    source_rows: int
    accepted_rows: int
    rejected_rows: int
    rejection_reasons: dict[str, int]


def create_s3_client(endpoint_url=None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


@contextmanager
def s3_backend(endpoint_url=None):
    """Either use in-process moto mock or hit the real/persistent endpoint."""
    if os.getenv("USE_MOTO_S3", "false").lower() == "true":
        from moto import mock_aws

        with mock_aws():
            yield create_s3_client(endpoint_url)
    else:
        yield create_s3_client(endpoint_url)


def read_source(source: str):
    """Read CSV or XLSX from a local path or URL. Returns (dataframe, raw_bytes, filename)."""
    source_path = Path(source)
    if source_path.exists():
        raw_bytes = source_path.read_bytes()
        name = source_path.name
    else:
        LOGGER.info("Downloading source: %s", source)
        resp = requests.get(source, timeout=120)
        resp.raise_for_status()
        raw_bytes = resp.content
        # grab just the filename from the URL
        name = source.split("?")[0].rsplit("/", 1)[-1] or "source.csv"

    ext = Path(name).suffix.lower()
    if ext in {".xlsx", ".xls"}:
        frame = pd.read_excel(io.BytesIO(raw_bytes))
    elif ext == ".csv" or not ext:
        frame = pd.read_csv(io.BytesIO(raw_bytes))
    else:
        raise ValueError(f"Unsupported format '{ext}' — use CSV or XLSX")
    return frame, raw_bytes, name


def _count_reasons(reasons):
    out = {}
    for r in reasons:
        out[r] = out.get(r, 0) + 1
    return out


def validate_schema(frame: pd.DataFrame):
    """Check that the dataframe matches the UCI Online Retail column contract.
    Returns (accepted_df, rejected_df, ValidationReport).
    """
    missing_cols = REQUIRED_COLUMNS.difference(frame.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing_cols))}")

    df = frame.copy()
    reasons = pd.Series("", index=df.index, dtype="string")

    def flag(mask, reason):
        reasons.loc[mask & reasons.eq("")] = reason

    # check for nulls in non-nullable columns
    for col in REQUIRED_COLUMNS - OPTIONAL_NULLABLE_COLUMNS:
        flag(df[col].isna(), f"missing_{col.lower()}")
    flag(df["Description"].isna(), "missing_description")

    # try coercing numeric/date columns
    qty = pd.to_numeric(df["Quantity"], errors="coerce")
    price = pd.to_numeric(df["UnitPrice"], errors="coerce")
    inv_date = pd.to_datetime(df["InvoiceDate"], errors="coerce", dayfirst=True)
    cust_id = pd.to_numeric(df["CustomerID"], errors="coerce")

    flag(qty.isna(), "invalid_quantity")
    flag(price.isna(), "invalid_unit_price")
    flag(inv_date.isna(), "invalid_invoice_date")
    flag(df["CustomerID"].notna() & cust_id.isna(), "invalid_customer_id")

    df["Quantity"] = qty.astype("Int64")
    df["UnitPrice"] = price.astype("Float64")
    df["InvoiceDate"] = inv_date
    df["CustomerID"] = cust_id.astype("Int64")

    rejected = df.loc[reasons.ne("")].copy()
    rejected["rejection_reason"] = reasons.loc[reasons.ne("")]
    accepted = df.loc[reasons.eq("")].copy()

    report = ValidationReport(
        source_rows=len(frame),
        accepted_rows=len(accepted),
        rejected_rows=len(rejected),
        rejection_reasons=_count_reasons(rejected["rejection_reason"].tolist()),
    )
    return accepted, rejected, report


def upload_to_s3(
    raw_bytes,
    accepted,
    rejected,
    report,
    *,
    bucket,
    ingestion_date,
    source_name,
    endpoint_url=None,
    client=None,
):
    """Write raw, accepted, rejected CSVs and a manifest JSON into the landing bucket."""
    client = client or create_s3_client(endpoint_url)

    # make sure bucket exists
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)

    prefix = f"landing/ingestion_date={ingestion_date.isoformat()}"
    raw_key = f"{prefix}/raw/{source_name}"
    accepted_key = f"{prefix}/validated/transactions.csv"
    rejected_key = f"{prefix}/rejected/rejected_rows.csv"
    manifest_key = f"{prefix}/manifest.json"

    client.put_object(Bucket=bucket, Key=raw_key, Body=raw_bytes)
    client.put_object(
        Bucket=bucket, Key=accepted_key,
        Body=accepted.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )
    client.put_object(
        Bucket=bucket, Key=rejected_key,
        Body=rejected.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )

    manifest = {
        "source_name": source_name,
        "ingestion_date": ingestion_date.isoformat(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": [raw_key, accepted_key, rejected_key, manifest_key],
        "validation": asdict(report),
    }
    client.put_object(
        Bucket=bucket, Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return [raw_key, accepted_key, rejected_key, manifest_key]


def ingest(
    source,
    *,
    bucket="insightguard-landing",
    ingestion_date=None,
    endpoint_url=None,
):
    frame, raw_bytes, source_name = read_source(source)
    accepted, rejected, report = validate_schema(frame)
    partition_date = ingestion_date or datetime.now(timezone.utc).date()

    with s3_backend(endpoint_url) as client:
        keys = upload_to_s3(
            raw_bytes, accepted, rejected, report,
            bucket=bucket,
            ingestion_date=partition_date,
            source_name=source_name,
            endpoint_url=endpoint_url,
            client=client,
        )

    LOGGER.info(
        "Ingestion done — %d source rows, %d accepted, %d rejected. Keys: %s",
        report.source_rows, report.accepted_rows, report.rejected_rows, keys,
    )
    return report


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest retail transaction data into S3 landing zone")
    parser.add_argument("--source", default=os.getenv("RAW_SOURCE", DEFAULT_SOURCE_URL))
    parser.add_argument("--bucket", default=os.getenv("LANDING_BUCKET", "insightguard-landing"))
    parser.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--ingestion-date", type=date.fromisoformat)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    ingest(
        args.source,
        bucket=args.bucket,
        ingestion_date=args.ingestion_date,
        endpoint_url=args.endpoint_url,
    )
