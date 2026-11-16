# BI connection guide

InsightGuard exposes a set of Postgres views for Power BI or Tableau — no Python dashboard included.

## Connecting

Make sure Postgres is running and you've loaded the warehouse tables + run `dashboard/views.sql`.

Default local connection:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `insightguard` |
| User | `insightguard` |
| Password | `insightguard` |

**Power BI:** Get Data → PostgreSQL database → enter the above → select the views listed below.

**Tableau:** Connect → To a Server → PostgreSQL → same connection info → add views as data sources.

## Available views

- `vw_kpi_summary` — headline numbers (revenue, returns, order count, etc.)
- `vw_sales_trend` — daily revenue/returns/orders broken down by region
- `vw_region_category_breakdown` — region × product category comparison
- `vw_anomalies` — flagged anomalies with z-scores and severity
- `vw_revenue_forecast` — 7-day and 30-day forecast rows

## Notes

The category field is a keyword heuristic on product descriptions since the source data doesn't have a real category column. Replace the CASE expression in `vw_region_category_breakdown` with a proper product taxonomy when you have one.

For production, swap `localhost` with your Redshift/warehouse endpoint and keep the view names the same.
