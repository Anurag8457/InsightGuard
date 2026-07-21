"""Rolling z-score anomaly detection for daily regional revenue and returns."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import text

LOGGER = logging.getLogger("insightguard.models.anomaly")


def detect_anomalies(
    daily_metrics: pd.DataFrame,
    *,
    window: int = 7,
    min_periods: int = 3,
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Flag regional revenue/return values against a prior rolling baseline."""
    required = {"metric_date", "region_key", "region_name", "revenue", "returns"}
    missing = required.difference(daily_metrics.columns)
    if missing:
        raise ValueError(f"Missing daily metric columns: {', '.join(sorted(missing))}")
    if window < 2 or min_periods < 2:
        raise ValueError("window and min_periods must both be at least 2")

    flags = daily_metrics.copy()
    flags["metric_date"] = pd.to_datetime(flags["metric_date"]).dt.date
    flags = flags.sort_values(["region_key", "metric_date"]).reset_index(drop=True)
    grouped = flags.groupby("region_key", sort=False)

    for metric in ("revenue", "returns"):
        flags[metric] = pd.to_numeric(flags[metric], errors="coerce").fillna(0.0)
        flags[f"{metric}_baseline"] = grouped[metric].transform(
            lambda series: series.shift(1).rolling(window=window, min_periods=min_periods).mean()
        )
        flags[f"{metric}_baseline_std"] = grouped[metric].transform(
            lambda series: series.shift(1).rolling(window=window, min_periods=min_periods).std(ddof=0)
        )
        flags[f"{metric}_zscore"] = (
            (flags[metric] - flags[f"{metric}_baseline"])
            .div(flags[f"{metric}_baseline_std"].replace(0, np.nan))
        )

    flags["revenue_zscore"] = flags["revenue_zscore"].replace([np.inf, -np.inf], np.nan)
    flags["returns_zscore"] = flags["returns_zscore"].replace([np.inf, -np.inf], np.nan)
    revenue_alert = flags["revenue_zscore"].abs().ge(z_threshold)
    returns_alert = flags["returns_zscore"].abs().ge(z_threshold)
    flags["is_anomaly"] = revenue_alert | returns_alert
    flags["anomaly_type"] = np.select(
        [revenue_alert & returns_alert, revenue_alert, returns_alert],
        ["revenue_and_returns", "revenue", "returns"],
        default="none",
    )
    flags["severity_score"] = flags[["revenue_zscore", "returns_zscore"]].abs().max(axis=1).fillna(0.0)
    flags["severity"] = pd.cut(
        flags["severity_score"],
        bins=[-np.inf, z_threshold, 4.0, np.inf],
        labels=["normal", "medium", "high"],
        right=False,
    ).astype("string")
    return flags[
        [
            "metric_date", "region_key", "region_name", "revenue", "returns", "revenue_baseline",
            "returns_baseline", "revenue_zscore", "returns_zscore", "is_anomaly", "anomaly_type",
            "severity_score", "severity",
        ]
    ]


def fetch_daily_metrics(engine, run_date: date | None = None) -> pd.DataFrame:
    query = """
        SELECT d.full_date AS metric_date,
               f.region_key,
               r.region_name,
               SUM(f.net_sales) AS revenue,
               SUM(f.return_amount) AS returns
        FROM fact_sales f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_region r ON r.region_key = f.region_key
        WHERE (:run_date IS NULL OR d.full_date <= :run_date)
        GROUP BY d.full_date, f.region_key, r.region_name
        ORDER BY d.full_date, f.region_key
    """
    return pd.read_sql(text(query), engine, params={"run_date": run_date})


def create_anomaly_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS anomaly_flags (
        metric_date DATE NOT NULL,
        region_key VARCHAR(64) NOT NULL REFERENCES dim_region(region_key),
        region_name VARCHAR(64) NOT NULL,
        revenue NUMERIC(14, 2) NOT NULL,
        returns NUMERIC(14, 2) NOT NULL,
        revenue_baseline NUMERIC(14, 2),
        returns_baseline NUMERIC(14, 2),
        revenue_zscore NUMERIC(12, 4),
        returns_zscore NUMERIC(12, 4),
        is_anomaly BOOLEAN NOT NULL,
        anomaly_type VARCHAR(32) NOT NULL,
        severity_score NUMERIC(12, 4) NOT NULL,
        severity VARCHAR(16) NOT NULL,
        detected_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (metric_date, region_key)
    );
    """
    with engine.begin() as connection:
        connection.execute(text(ddl))


def persist_anomalies(engine, flags: pd.DataFrame) -> None:
    """Replace the supplied metric range, making reruns deterministic."""
    if flags.empty:
        return
    create_anomaly_table(engine)
    rows = flags.copy()
    rows["detected_at"] = datetime.now(timezone.utc)
    records = rows.astype(object).where(pd.notna(rows), None).to_dict(orient="records")
    for record in records:
        if isinstance(record["detected_at"], pd.Timestamp):
            record["detected_at"] = record["detected_at"].to_pydatetime()
    columns = [
        "metric_date", "region_key", "region_name", "revenue", "returns", "revenue_baseline",
        "returns_baseline", "revenue_zscore", "returns_zscore", "is_anomaly", "anomaly_type",
        "severity_score", "severity", "detected_at",
    ]
    with engine.begin() as connection:
        connection.execute(
            text("DELETE FROM anomaly_flags WHERE metric_date BETWEEN :start_date AND :end_date"),
            {"start_date": rows["metric_date"].min(), "end_date": rows["metric_date"].max()},
        )
        connection.execute(
            text(
                f"INSERT INTO anomaly_flags ({', '.join(columns)}) "
                f"VALUES ({', '.join(':' + c for c in columns)}) "
                "ON CONFLICT (metric_date, region_key) DO UPDATE SET "
                + ", ".join(f"{c}=EXCLUDED.{c}" for c in columns[2:])
            ),
            records,
        )


def run_anomaly_detection(engine, *, run_date: date | None = None) -> pd.DataFrame:
    metrics = fetch_daily_metrics(engine, run_date)
    flags = detect_anomalies(metrics)
    persist_anomalies(engine, flags)
    LOGGER.info("Persisted %d regional daily anomaly flags", len(flags))
    return flags
