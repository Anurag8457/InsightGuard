import os

import boto3
import pandas as pd
from moto import mock_aws

from ingestion.ingest import ingest, upload_to_s3, validate_schema


def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "InvoiceNo": ["10001", "10002"],
            "StockCode": ["A", "B"],
            "Description": ["ITEM A", "ITEM B"],
            "Quantity": [2, -1],
            "InvoiceDate": ["01/12/2010 08:26", "01/12/2010 08:30"],
            "UnitPrice": [3.5, 2.0],
            "CustomerID": [12345, None],
            "Country": ["United Kingdom", "United Kingdom"],
        }
    )


def test_validate_schema_keeps_nullable_customer_id():
    accepted, rejected, report = validate_schema(sample_frame())

    assert report.source_rows == 2
    assert report.accepted_rows == 2
    assert rejected.empty
    assert accepted["Quantity"].dtype.name == "Int64"
    assert accepted["CustomerID"].isna().sum() == 1


@mock_aws
def test_upload_to_s3_writes_partitioned_objects():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    accepted, rejected, report = validate_schema(sample_frame())

    keys = upload_to_s3(
        b"raw,csv\n",
        accepted,
        rejected,
        report,
        bucket="insightguard-landing",
        ingestion_date=pd.Timestamp("2026-07-21").date(),
        source_name="online_retail.csv",
    )

    assert all(key.startswith("landing/ingestion_date=2026-07-21/") for key in keys)
    client = boto3.client("s3", region_name="us-east-1")
    objects = client.list_objects_v2(Bucket="insightguard-landing")["Contents"]
    assert {obj["Key"] for obj in objects} == set(keys)


@mock_aws
def test_ingest_runs_end_to_end_from_local_csv(tmp_path):
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["USE_MOTO_S3"] = "false"
    source = tmp_path / "transactions.csv"
    sample_frame().to_csv(source, index=False)

    report = ingest(
        str(source),
        bucket="insightguard-landing",
        ingestion_date=pd.Timestamp("2026-07-21").date(),
    )

    assert report.source_rows == 2
    assert report.accepted_rows == 2
    client = boto3.client("s3", region_name="us-east-1")
    objects = client.list_objects_v2(Bucket="insightguard-landing")["Contents"]
    keys = {obj["Key"] for obj in objects}
    assert "landing/ingestion_date=2026-07-21/raw/transactions.csv" in keys
    assert "landing/ingestion_date=2026-07-21/validated/transactions.csv" in keys
    assert "landing/ingestion_date=2026-07-21/rejected/rejected_rows.csv" in keys
    assert "landing/ingestion_date=2026-07-21/manifest.json" in keys
