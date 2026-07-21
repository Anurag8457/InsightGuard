# BI connection guide

InsightGuard exposes PostgreSQL views for Power BI or Tableau. No Python dashboard is included.

## Connection

Start a local PostgreSQL instance and apply the warehouse/model scripts first. Then run `dashboard/views.sql` as the warehouse user.

Default local connection values from the project configuration are:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `insightguard` |
| User | `insightguard` |
| Password | `insightguard` |

In Power BI, choose **Get Data → PostgreSQL database**, enter the connection values, and select these views:

- `vw_kpi_summary` — headline KPIs and date range
- `vw_sales_trend` — daily revenue, returns, orders, and units by region
- `vw_region_category_breakdown` — regional/category comparison
- `vw_anomalies` — anomaly list with z-scores and severity
- `vw_revenue_forecast` — 7-day and 30-day forecast rows

In Tableau, choose **Connect → To a Server → PostgreSQL**, enter the same values, and add the views as logical tables or custom SQL sources.

For production, replace the local host/database with the Redshift-compatible warehouse endpoint and keep the view names stable. The category field is a documented keyword heuristic because the source dataset contains product descriptions but no native category column; replace that CASE expression with a maintained product taxonomy when one becomes available.

