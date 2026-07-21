"""Regional revenue forecasts using simple exponential smoothing."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text
from statsmodels.tsa.holtwinters import SimpleExpSmoothing

LOGGER = logging.getLogger("insightguard.models.forecast")


def forecast_revenue(
    daily_metrics: pd.DataFrame,
    *,
    horizons: tuple[int, ...] = (7, 30),
    run_date: date | None = None,
) -> pd.DataFrame:
    """Forecast daily regional revenue for the requested horizons."""
    required = {"metric_date", "region_key", "region_name", "revenue"}
    missing = required.difference(daily_metrics.columns)
    if missing:
        raise ValueError(f"Missing forecast columns: {', '.join(sorted(missing))}")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive day counts")

    source = daily_metrics.copy()
    source["metric_date"] = pd.to_datetime(source["metric_date"])
    source["revenue"] = pd.to_numeric(source["revenue"], errors="coerce").fillna(0.0)
    rows = []
    max_horizon = max(horizons)
    for (region_key, region_name), group in source.groupby(["region_key", "region_name"], sort=True):
        series = (
            group.set_index("metric_date")["revenue"]
            .resample("D")
            .sum()
            .asfreq("D", fill_value=0.0)
        )
        if series.empty:
            continue
        if len(series) >= 2 and series.nunique() > 1:
            fitted = SimpleExpSmoothing(series, initialization_method="estimated").fit(optimized=True)
            predictions = fitted.forecast(max_horizon)
        else:
            predictions = pd.Series(float(series.iloc[-1]), index=pd.date_range(series.index[-1] + pd.Timedelta(days=1), periods=max_horizon, freq="D"))
        predictions = predictions.clip(lower=0.0)
        for horizon in horizons:
            for forecast_date, value in predictions.iloc[:horizon].items():
                rows.append(
                    {
                        "forecast_run_date": run_date or series.index.max().date(),
                        "forecast_date": forecast_date.date(),
                        "region_key": region_key,
                        "region_name": region_name,
                        "horizon_days": horizon,
                        "forecast_revenue": round(float(value), 2),
                        "model_name": "simple_exponential_smoothing",
                    }
                )
    return pd.DataFrame(rows)


def create_forecast_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS revenue_forecasts (
        forecast_run_date DATE NOT NULL,
        forecast_date DATE NOT NULL,
        region_key VARCHAR(64) NOT NULL REFERENCES dim_region(region_key),
        region_name VARCHAR(64) NOT NULL,
        horizon_days INTEGER NOT NULL,
        forecast_revenue NUMERIC(14, 2) NOT NULL,
        model_name VARCHAR(64) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (forecast_run_date, forecast_date, region_key, horizon_days)
    );
    """
    with engine.begin() as connection:
        connection.execute(text(ddl))


def persist_forecasts(engine, forecasts: pd.DataFrame, run_date: date) -> None:
    if forecasts.empty:
        return
    create_forecast_table(engine)
    rows = forecasts.copy()
    rows["created_at"] = datetime.now(timezone.utc)
    records = rows.astype(object).where(pd.notna(rows), None).to_dict(orient="records")
    for record in records:
        if isinstance(record["created_at"], pd.Timestamp):
            record["created_at"] = record["created_at"].to_pydatetime()
    columns = [
        "forecast_run_date", "forecast_date", "region_key", "region_name", "horizon_days",
        "forecast_revenue", "model_name", "created_at",
    ]
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM revenue_forecasts WHERE forecast_run_date = :run_date"), {"run_date": run_date})
        connection.execute(
            text(
                f"INSERT INTO revenue_forecasts ({', '.join(columns)}) "
                f"VALUES ({', '.join(':' + c for c in columns)}) "
                "ON CONFLICT (forecast_run_date, forecast_date, region_key, horizon_days) DO UPDATE SET "
                + ", ".join(f"{c}=EXCLUDED.{c}" for c in columns[3:])
            ),
            records,
        )


def run_forecast(engine, *, run_date: date | None = None) -> pd.DataFrame:
    from models.anomaly.detect import fetch_daily_metrics

    metrics = fetch_daily_metrics(engine, run_date)
    forecasts = forecast_revenue(metrics, run_date=run_date)
    effective_run_date = run_date or pd.to_datetime(metrics["metric_date"]).max().date()
    persist_forecasts(engine, forecasts, effective_run_date)
    LOGGER.info("Persisted %d revenue forecast rows", len(forecasts))
    return forecasts
