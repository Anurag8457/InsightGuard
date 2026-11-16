"""Forecast regional revenue using simple exponential smoothing."""

import logging
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text

LOGGER = logging.getLogger("insightguard.models.forecast")


def _ses_forecast(series, horizon):
    """Basic SES — compute alpha from series length, then project a flat forecast."""
    alpha = min(1.0, max(0.01, 2.0 / (len(series) + 1)))
    level = float(series.iloc[0])
    for val in series.iloc[1:]:
        level = alpha * float(val) + (1.0 - alpha) * level
    idx = pd.date_range(series.index[-1] + pd.Timedelta(days=1), periods=horizon, freq="D")
    return pd.Series(level, index=idx)


def forecast_revenue(daily_metrics, *, horizons=(7, 30), run_date=None):
    """Generate forecasts for each region at each horizon."""
    required = {"metric_date", "region_key", "region_name", "revenue"}
    missing = required.difference(daily_metrics.columns)
    if missing:
        raise ValueError(f"Missing columns for forecast: {', '.join(sorted(missing))}")
    if not horizons or any(h <= 0 for h in horizons):
        raise ValueError("horizons must be positive integers")

    src = daily_metrics.copy()
    src["metric_date"] = pd.to_datetime(src["metric_date"])
    src["revenue"] = pd.to_numeric(src["revenue"], errors="coerce").fillna(0.0)

    rows = []
    max_h = max(horizons)

    for (rk, rn), grp in src.groupby(["region_key", "region_name"], sort=True):
        ts = (
            grp.set_index("metric_date")["revenue"]
            .resample("D").sum()
            .asfreq("D", fill_value=0.0)
        )
        if ts.empty:
            continue

        preds = _ses_forecast(ts, max_h)
        preds = preds.clip(lower=0.0)

        for h in horizons:
            for fc_date, val in preds.iloc[:h].items():
                rows.append({
                    "forecast_run_date": run_date or ts.index.max().date(),
                    "forecast_date": fc_date.date(),
                    "region_key": rk,
                    "region_name": rn,
                    "horizon_days": h,
                    "forecast_revenue": round(float(val), 2),
                    "model_name": "simple_exponential_smoothing",
                })

    return pd.DataFrame(rows)


def create_forecast_table(engine):
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
    with engine.begin() as conn:
        conn.execute(text(ddl))


def persist_forecasts(engine, forecasts, run_date):
    if forecasts.empty:
        return
    create_forecast_table(engine)

    rows = forecasts.copy()
    rows["created_at"] = datetime.now(timezone.utc)
    records = rows.astype(object).where(pd.notna(rows), None).to_dict(orient="records")
    for rec in records:
        if isinstance(rec["created_at"], pd.Timestamp):
            rec["created_at"] = rec["created_at"].to_pydatetime()

    cols = [
        "forecast_run_date", "forecast_date", "region_key", "region_name", "horizon_days",
        "forecast_revenue", "model_name", "created_at",
    ]
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM revenue_forecasts WHERE forecast_run_date = :rd"),
            {"rd": run_date},
        )
        conn.execute(
            text(
                f"INSERT INTO revenue_forecasts ({', '.join(cols)}) "
                f"VALUES ({', '.join(':' + c for c in cols)}) "
                "ON CONFLICT (forecast_run_date, forecast_date, region_key, horizon_days) DO UPDATE SET "
                + ", ".join(f"{c}=EXCLUDED.{c}" for c in cols[3:])
            ),
            records,
        )


def run_forecast(engine, *, run_date=None):
    from models.anomaly.detect import fetch_daily_metrics

    metrics = fetch_daily_metrics(engine, run_date)
    forecasts = forecast_revenue(metrics, run_date=run_date)
    effective_date = run_date or pd.to_datetime(metrics["metric_date"]).max().date()
    persist_forecasts(engine, forecasts, effective_date)
    LOGGER.info("Wrote %d forecast rows", len(forecasts))
    return forecasts
