import pandas as pd

from models.anomaly.detect import detect_anomalies
from models.forecast.forecast import forecast_revenue


def daily_metrics() -> pd.DataFrame:
    dates = pd.date_range("2026-07-01", periods=8, freq="D")
    return pd.DataFrame(
        {
            "metric_date": dates,
            "region_key": ["region_uk"] * 8,
            "region_name": ["United Kingdom"] * 8,
            "revenue": [100, 110, 90, 105, 95, 100, 102, 1000],
            "returns": [2, 2, 3, 2, 2, 3, 2, 50],
        }
    )


def test_rolling_z_score_flags_revenue_and_returns_spike():
    flags = detect_anomalies(daily_metrics(), window=5, min_periods=3, z_threshold=3.0)
    last = flags.iloc[-1]

    assert bool(last["is_anomaly"])
    assert last["anomaly_type"] == "revenue_and_returns"
    assert float(last["severity_score"]) >= 3.0


def test_forecast_emits_7_and_30_day_horizons():
    forecasts = forecast_revenue(daily_metrics(), run_date=pd.Timestamp("2026-07-08").date())

    assert set(forecasts["horizon_days"]) == {7, 30}
    assert len(forecasts[forecasts["horizon_days"] == 7]) == 7
    assert len(forecasts[forecasts["horizon_days"] == 30]) == 30
    assert forecasts["forecast_revenue"].ge(0).all()
    assert forecasts["model_name"].eq("simple_exponential_smoothing").all()

