"""Airflow DAG — runs the full InsightGuard pipeline daily at 2am UTC."""

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


def _run_ingestion(**ctx):
    dt = ctx["logical_date"].date()
    ingest(
        os.getenv("RAW_SOURCE", DEFAULT_SOURCE_URL),
        bucket=os.getenv("LANDING_BUCKET", "insightguard-landing"),
        ingestion_date=dt,
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )


def _run_transform(**ctx):
    dt = ctx["logical_date"].date()
    bucket = os.getenv("LANDING_BUCKET", "insightguard-landing")
    raw = read_landing_transactions(bucket, dt, endpoint_url=os.getenv("S3_ENDPOINT_URL"))
    star = build_star_schema(clean_transactions(raw, dt))
    keys = write_star_schema_to_s3(
        star, bucket, dt,
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    LOGGER.info("Curated artifacts written: %s", keys)


def _run_load(**ctx):
    dt = ctx["logical_date"].date()
    bucket = os.getenv("LANDING_BUCKET", "insightguard-landing")
    star = read_star_schema_from_s3(bucket, dt, endpoint_url=os.getenv("S3_ENDPOINT_URL"))
    engine = create_engine(os.environ["DATABASE_URL"])
    load_star_schema(engine, star, dt)


def _run_anomaly(**ctx):
    engine = create_engine(os.environ["DATABASE_URL"])
    flags = run_anomaly_detection(engine, run_date=ctx["logical_date"].date())
    LOGGER.info("Anomaly detection done — %d rows", len(flags))


def _run_forecast_task(**ctx):
    engine = create_engine(os.environ["DATABASE_URL"])
    fc = run_forecast(engine, run_date=ctx["logical_date"].date())
    LOGGER.info("Forecast done — %d rows", len(fc))


def _on_failure(ctx):
    """Post to a webhook on failure, or just log it if no URL is configured."""
    ti = ctx.get("task_instance")
    msg = {"text": f"InsightGuard task failed: {ti.dag_id}.{ti.task_id}" if ti else "InsightGuard task failed"}
    webhook = os.getenv("FAILURE_WEBHOOK_URL")
    if webhook:
        requests.post(webhook, json=msg, timeout=10).raise_for_status()
    else:
        LOGGER.error("FAILURE (no webhook configured): %s", msg["text"])


default_args = {
    "owner": "insightguard",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="insightguard_daily",
    description="Daily retail pipeline: ingest → transform → load → anomaly detection → forecast",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["insightguard", "retail", "analytics"],
) as dag:
    t_ingest = PythonOperator(task_id="ingestion", python_callable=_run_ingestion)
    t_transform = PythonOperator(task_id="transform", python_callable=_run_transform)
    t_load = PythonOperator(task_id="load", python_callable=_run_load)
    t_anomaly = PythonOperator(task_id="anomaly_detection", python_callable=_run_anomaly)
    t_forecast = PythonOperator(task_id="forecast", python_callable=_run_forecast_task)
    t_fail = PythonOperator(
        task_id="failure_notification",
        python_callable=_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    t_ingest >> t_transform >> t_load >> t_anomaly >> t_forecast
    [t_ingest, t_transform, t_load, t_anomaly, t_forecast] >> t_fail
