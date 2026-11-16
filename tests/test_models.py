import pandas as pd

from models.anomaly.detect import detect_anomalies
from models.forecast.forecast import forecast_revenue


def _make_metrics():
    dates = pd.date_range("2026-07-01", periods=8, freq="D")
    return pd.DataFrame({
        "metric_date": dates,
        "region_key": ["region_uk"] * 8,
        "region_name": ["United Kingdom"] * 8,
        "revenue": [100, 110, 90, 105, 95, 100, 102, 1000],  # last one is a spike
        "returns": [2, 2, 3, 2, 2, 3, 2, 50],
    })


def test_zscore_flags_spike():
    flags = detect_anomalies(_make_metrics(), window=5, min_periods=3, z_threshold=3.0)
    last_row = flags.iloc[-1]

    assert bool(last_row["is_anomaly"])
    assert last_row["anomaly_type"] == "revenue_and_returns"
    assert float(last_row["severity_score"]) >= 3.0


def test_forecast_horizons():
    fc = forecast_revenue(_make_metrics(), run_date=pd.Timestamp("2026-07-08").date())

    assert set(fc["horizon_days"]) == {7, 30}
    assert len(fc[fc["horizon_days"] == 7]) == 7
    assert len(fc[fc["horizon_days"] == 30]) == 30
    assert fc["forecast_revenue"].ge(0).all()
    assert fc["model_name"].eq("simple_exponential_smoothing").all()
