-- InsightGuard BI layer.
-- Run this script against the PostgreSQL warehouse after the model tables exist.

CREATE OR REPLACE VIEW vw_kpi_summary AS
SELECT
    MAX(d.full_date) AS latest_sales_date,
    MIN(d.full_date) AS earliest_sales_date,
    COUNT(DISTINCT f.invoice_no) AS order_count,
    COUNT(DISTINCT NULLIF(c.customer_id, 'UNKNOWN')) AS customer_count,
    COUNT(DISTINCT f.product_key) AS product_count,
    SUM(f.quantity) FILTER (WHERE f.quantity > 0) AS units_sold,
    ROUND(SUM(f.net_sales), 2) AS total_revenue,
    ROUND(SUM(f.return_amount), 2) AS total_returns,
    COUNT(*) FILTER (WHERE f.is_return) AS return_line_count,
    (
        SELECT COUNT(*)
        FROM anomaly_flags anomaly_summary
        WHERE anomaly_summary.is_anomaly
    ) AS anomaly_count
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN anomaly_flags a
    ON a.metric_date = d.full_date
   AND a.region_key = f.region_key
;

CREATE OR REPLACE VIEW vw_sales_trend AS
SELECT
    d.full_date AS sales_date,
    r.region_name,
    COUNT(DISTINCT f.invoice_no) AS order_count,
    SUM(f.quantity) FILTER (WHERE f.quantity > 0) AS units_sold,
    ROUND(SUM(f.net_sales), 2) AS revenue,
    ROUND(SUM(f.return_amount), 2) AS returns,
    ROUND(AVG(f.net_sales), 2) AS average_line_revenue
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_region r ON r.region_key = f.region_key
GROUP BY d.full_date, r.region_name
;

-- Online Retail does not include a product category column. This view exposes
-- a replaceable, description-keyword heuristic for portfolio dashboarding.
CREATE OR REPLACE VIEW vw_region_category_breakdown AS
WITH categorized_products AS (
    SELECT
        product_key,
        CASE
            WHEN LOWER(description) LIKE ANY (ARRAY['%light%', '%lamp%', '%candle%']) THEN 'Lighting & Decor'
            WHEN LOWER(description) LIKE ANY (ARRAY['%bag%', '%box%', '%basket%', '%storage%']) THEN 'Storage & Bags'
            WHEN LOWER(description) LIKE ANY (ARRAY['%kitchen%', '%cup%', '%plate%', '%mug%', '%cake%']) THEN 'Kitchen & Dining'
            WHEN LOWER(description) LIKE ANY (ARRAY['%card%', '%paper%', '%notebook%', '%pen%', '%stationery%']) THEN 'Stationery & Gifts'
            WHEN LOWER(description) LIKE ANY (ARRAY['%heart%', '%flower%', '%bird%', '%christmas%', '%easter%']) THEN 'Seasonal & Giftware'
            ELSE 'Other'
        END AS category
    FROM dim_product
)
SELECT
    r.region_name,
    cp.category,
    COUNT(DISTINCT f.invoice_no) AS order_count,
    SUM(f.quantity) FILTER (WHERE f.quantity > 0) AS units_sold,
    ROUND(SUM(f.net_sales), 2) AS revenue,
    ROUND(SUM(f.return_amount), 2) AS returns
FROM fact_sales f
JOIN dim_region r ON r.region_key = f.region_key
JOIN categorized_products cp ON cp.product_key = f.product_key
GROUP BY r.region_name, cp.category
;

CREATE OR REPLACE VIEW vw_anomalies AS
SELECT
    a.metric_date,
    a.region_name,
    ROUND(a.revenue, 2) AS revenue,
    ROUND(a.returns, 2) AS returns,
    ROUND(a.revenue_baseline, 2) AS revenue_baseline,
    ROUND(a.returns_baseline, 2) AS returns_baseline,
    ROUND(a.revenue_zscore, 4) AS revenue_zscore,
    ROUND(a.returns_zscore, 4) AS returns_zscore,
    a.anomaly_type,
    a.severity,
    ROUND(a.severity_score, 4) AS severity_score,
    a.detected_at
FROM anomaly_flags a
WHERE a.is_anomaly
;

CREATE OR REPLACE VIEW vw_revenue_forecast AS
SELECT
    forecast_run_date,
    forecast_date,
    region_name,
    horizon_days,
    ROUND(forecast_revenue, 2) AS forecast_revenue,
    model_name,
    created_at
FROM revenue_forecasts
;
