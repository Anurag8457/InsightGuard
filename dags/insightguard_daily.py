"""Daily InsightGuard pipeline: ingest, transform, load, detect, notify."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine

from ingestion.ingest import DEFAULT_SOURCE_URL, ingest
from models.anomaly.detect import run_anomaly_detection
from models.forecast.forecast import run_forecast
from transform.transform import (
    build_star_schema,
    clean_transactions,
    load_star_schema,
    read_landing_transactions,
    read_star_schema_from_s3,
    write_star_schema_to_s3,
)

LOGGER = logging.getLogger("insightguard.dag")


def _run_ingestion(**context) -> None:
    ingestion_date = context["logical_date"].date()
    ingest(
        os.getenv("RAW_SOURCE", DEFAULT_SOURCE_URL),
        bucket=os.getenv("LANDING_BUCKET", "insightguard-landing"),
        ingestion_date=ingestion_date,
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )


def _run_transform(**context) -> None:
    ingestion_date = context["logical_date"].date()
    bucket = os.getenv("LANDING_BUCKET", "insightguard-landing")
    source = read_landing_transactions(bucket, ingestion_date, endpoint_url=os.getenv("S3_ENDPOINT_URL"))
    star_schema = build_star_schema(clean_transactions(source, ingestion_date))
    keys = write_star_schema_to_s3(
        star_schema,
        bucket,
        ingestion_date,
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    LOGGER.info("Wrote curated artifacts: %s", keys)


def _run_load(**context) -> None:
    ingestion_date = context["logical_date"].date()
    bucket = os.getenv("LANDING_BUCKET", "insightguard-landing")
    star_schema = read_star_schema_from_s3(bucket, ingestion_date, endpoint_url=os.getenv("S3_ENDPOINT_URL"))
    engine = create_engine(os.environ["DATABASE_URL"])
    load_star_schema(engine, star_schema, ingestion_date)


def _run_anomaly_detection(**context) -> None:
    engine = create_engine(os.environ["DATABASE_URL"])
    flags = run_anomaly_detection(engine, run_date=context["logical_date"].date())
    LOGGER.info("Anomaly detection completed with %d rows", len(flags))


def _run_forecast(**context) -> None:
    engine = create_engine(os.environ["DATABASE_URL"])
    forecasts = run_forecast(engine, run_date=context["logical_date"].date())
    LOGGER.info("Forecasting completed with %d rows", len(forecasts))


def _notify_failure(context) -> None:
    """Stub Slack/email webhook callback; logs when no webhook is configured."""
    task = context.get("task_instance")
    message = {
        "text": f"InsightGuard task failed: {task.dag_id}.{task.task_id}" if task else "InsightGuard task failed"
    }
    webhook_url = os.getenv("FAILURE_WEBHOOK_URL")
    if webhook_url:
        requests.post(webhook_url, json=message, timeout=10).raise_for_status()
    else:
        LOGGER.error("Failure notification stub: %s", message["text"])


default_args = {
    "owner": "insightguard",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _notify_failure,
}

with DAG(
    dag_id="insightguard_daily",
    description="Daily retail ingestion, transformation, warehouse load, anomaly detection, and forecasting",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["insightguard", "retail", "analytics"],
) as dag:
    ingestion = PythonOperator(task_id="ingestion", python_callable=_run_ingestion)
    transform = PythonOperator(task_id="transform", python_callable=_run_transform)
    load = PythonOperator(task_id="load", python_callable=_run_load)
    anomaly_detection = PythonOperator(task_id="anomaly_detection", python_callable=_run_anomaly_detection)
    forecast = PythonOperator(task_id="forecast", python_callable=_run_forecast)
    failure_notification = PythonOperator(
        task_id="failure_notification",
        python_callable=_notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    ingestion >> transform >> load >> anomaly_detection >> forecast
    [ingestion, transform, load, anomaly_detection, forecast] >> failure_notification
