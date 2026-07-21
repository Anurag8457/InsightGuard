"""Download, validate, and land retail transactions in S3-compatible storage."""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

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


def create_s3_client(endpoint_url: str | None = None):
    """Create an S3 client for AWS or a locally running Moto server."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


@contextmanager
def s3_backend(endpoint_url: str | None = None):
    """Provide an in-process Moto mock when requested, otherwise use the endpoint."""
    if os.getenv("USE_MOTO_S3", "false").lower() == "true":
        from moto import mock_aws

        with mock_aws():
            yield create_s3_client(endpoint_url)
    else:
        yield create_s3_client(endpoint_url)


def read_source(source: str) -> tuple[pd.DataFrame, bytes, str]:
    """Read a local or HTTP CSV/XLSX source and return its raw bytes as well."""
    source_path = Path(source)
    if source_path.exists():
        raw_bytes = source_path.read_bytes()
        name = source_path.name
    else:
        LOGGER.info("Downloading source: %s", source)
        response = requests.get(source, timeout=120)
        response.raise_for_status()
        raw_bytes = response.content
        name = source.split("?")[0].rsplit("/", 1)[-1] or "source.csv"

    suffix = Path(name).suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        frame = pd.read_excel(io.BytesIO(raw_bytes))
    elif suffix == ".csv" or not suffix:
        frame = pd.read_csv(io.BytesIO(raw_bytes))
    else:
        raise ValueError(f"Unsupported source format '{suffix}'. Use CSV or XLSX.")
    return frame, raw_bytes, name


def _reason_counts(reasons: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reason in reasons:
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def validate_schema(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
    """Validate the UCI Online Retail column contract and separate bad rows."""
    missing_columns = REQUIRED_COLUMNS.difference(frame.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing}")

    working = frame.copy()
    reasons = pd.Series("", index=working.index, dtype="string")

    def mark(mask: pd.Series, reason: str) -> None:
        reasons.loc[mask & reasons.eq("")] = reason

    for column in REQUIRED_COLUMNS - OPTIONAL_NULLABLE_COLUMNS:
        mark(working[column].isna(), f"missing_{column.lower()}")
    mark(working["Description"].isna(), "missing_description")

    quantity = pd.to_numeric(working["Quantity"], errors="coerce")
    unit_price = pd.to_numeric(working["UnitPrice"], errors="coerce")
    invoice_date = pd.to_datetime(working["InvoiceDate"], errors="coerce", dayfirst=True)
    customer_id = pd.to_numeric(working["CustomerID"], errors="coerce")

    mark(quantity.isna(), "invalid_quantity")
    mark(unit_price.isna(), "invalid_unit_price")
    mark(invoice_date.isna(), "invalid_invoice_date")
    mark(working["CustomerID"].notna() & customer_id.isna(), "invalid_customer_id")

    working["Quantity"] = quantity.astype("Int64")
    working["UnitPrice"] = unit_price.astype("Float64")
    working["InvoiceDate"] = invoice_date
    working["CustomerID"] = customer_id.astype("Int64")

    rejected = working.loc[reasons.ne("")].copy()
    rejected["rejection_reason"] = reasons.loc[reasons.ne("")]
    accepted = working.loc[reasons.eq("")].copy()

    report = ValidationReport(
        source_rows=len(frame),
        accepted_rows=len(accepted),
        rejected_rows=len(rejected),
        rejection_reasons=_reason_counts(rejected["rejection_reason"].tolist()),
    )
    return accepted, rejected, report


def upload_to_s3(
    raw_bytes: bytes,
    accepted: pd.DataFrame,
    rejected: pd.DataFrame,
    report: ValidationReport,
    *,
    bucket: str,
    ingestion_date: date,
    source_name: str,
    endpoint_url: str | None = None,
    client=None,
) -> list[str]:
    """Upload raw, accepted, rejected, and manifest objects to the landing bucket."""
    client = client or create_s3_client(endpoint_url)
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)

    partition = f"landing/ingestion_date={ingestion_date.isoformat()}"
    raw_key = f"{partition}/raw/{source_name}"
    accepted_key = f"{partition}/validated/transactions.csv"
    rejected_key = f"{partition}/rejected/rejected_rows.csv"
    manifest_key = f"{partition}/manifest.json"

    client.put_object(Bucket=bucket, Key=raw_key, Body=raw_bytes)
    client.put_object(
        Bucket=bucket,
        Key=accepted_key,
        Body=accepted.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )
    client.put_object(
        Bucket=bucket,
        Key=rejected_key,
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
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return [raw_key, accepted_key, rejected_key, manifest_key]


def ingest(
    source: str,
    *,
    bucket: str = "insightguard-landing",
    ingestion_date: date | None = None,
    endpoint_url: str | None = None,
) -> ValidationReport:
    frame, raw_bytes, source_name = read_source(source)
    accepted, rejected, report = validate_schema(frame)
    partition_date = ingestion_date or datetime.now(timezone.utc).date()
    with s3_backend(endpoint_url) as client:
        keys = upload_to_s3(
            raw_bytes,
            accepted,
            rejected,
            report,
            bucket=bucket,
            ingestion_date=partition_date,
            source_name=source_name,
            endpoint_url=endpoint_url,
            client=client,
        )
    LOGGER.info(
        "Ingestion complete: source_rows=%d accepted_rows=%d rejected_rows=%d keys=%s reasons=%s",
        report.source_rows,
        report.accepted_rows,
        report.rejected_rows,
        keys,
        report.rejection_reasons,
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
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
