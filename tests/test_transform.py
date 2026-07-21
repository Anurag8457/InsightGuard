import pandas as pd
from moto import mock_aws

from transform.transform import (
    build_star_schema,
    clean_transactions,
    country_to_region,
    read_star_schema_from_s3,
    write_star_schema_to_s3,
)


def raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "InvoiceNo": ["10001", "10001", "10002", "10003"],
            "StockCode": ["A", "A", "B", "C"],
            "Description": ["ITEM A", "ITEM A", None, "ITEM C"],
            "Quantity": [2, 2, -1, 0],
            "InvoiceDate": ["01/12/2010 08:26"] * 4,
            "UnitPrice": [3.5, 3.5, 2.0, 1.0],
            "CustomerID": [12345, 12345, None, 12346],
            "Country": ["United Kingdom", "United Kingdom", "France", "Canada"],
        }
    )


def test_clean_transactions_removes_duplicates_and_zero_quantity():
    cleaned = clean_transactions(raw_frame(), pd.Timestamp("2026-07-21").date())

    assert len(cleaned) == 2
    assert cleaned["gross_sales"].tolist() == [7.0, -2.0]
    assert cleaned["return_amount"].tolist() == [0.0, 2.0]
    assert cleaned["currency_code"].eq("GBP").all()


def test_country_mapping_is_explainable():
    assert country_to_region("United Kingdom") == "United Kingdom"
    assert country_to_region("France") == "Europe"
    assert country_to_region("Canada") == "Other"


def test_star_schema_uses_stable_keys_and_expected_tables():
    cleaned = clean_transactions(raw_frame(), pd.Timestamp("2026-07-21").date())
    star = build_star_schema(cleaned)

    assert set(star) == {"fact_sales", "dim_product", "dim_customer", "dim_date", "dim_region"}
    assert star["fact_sales"]["transaction_id"].is_unique
    assert star["dim_product"]["product_key"].str.startswith("prod_").all()
    assert star["fact_sales"]["product_key"].isin(star["dim_product"]["product_key"]).all()


@mock_aws
def test_curated_artifacts_round_trip_through_moto():
    import boto3

    boto3.setup_default_session(region_name="us-east-1")
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="insightguard-landing")
    ingestion_date = pd.Timestamp("2026-07-21").date()
    star = build_star_schema(clean_transactions(raw_frame(), ingestion_date))

    keys = write_star_schema_to_s3(star, "insightguard-landing", ingestion_date)
    restored = read_star_schema_from_s3("insightguard-landing", ingestion_date)

    assert len(keys) == 5
    assert set(restored) == set(star)
    assert len(restored["fact_sales"]) == len(star["fact_sales"])
