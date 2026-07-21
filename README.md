# InsightGuard

InsightGuard is a cloud-native sales analytics and anomaly-detection pipeline for retail transactions. It turns raw e-commerce line items into an auditable landing zone, a PostgreSQL star schema, regional anomaly flags, revenue forecasts, and BI-ready views for Power BI or Tableau.

## Problem statement

Retail teams need to answer two questions reliably:

1. What happened to sales, returns, customers, products, and regions over time?
2. Which daily revenue or return patterns deserve investigation before they become business problems?

InsightGuard addresses both with a reproducible batch pipeline. It uses the UCI Online Retail data shape: invoice number, product code, description, quantity, invoice date, unit price, customer, and country. The source dataset has no native product category, so the BI layer exposes a documented description-keyword heuristic that can later be replaced with a maintained taxonomy.

## Architecture

```mermaid
flowchart LR
    A[Online Retail CSV] --> B[Python ingestion]
    B --> C[Raw validated rejected objects]
    C --> D[Persistent Moto S3 endpoint]
    D --> E[Python transformation]
    E --> F[Curated S3 table artifacts]
    F --> G[PostgreSQL star schema]
    G --> H[Rolling z-score detector]
    G --> I[Exponential smoothing forecast]
    H --> J[anomaly_flags]
    I --> K[revenue_forecasts]
    G --> L[BI SQL views]
    J --> L
    K --> L
    L --> M[Power BI / Tableau]
    N[Airflow daily DAG] -. orchestrates .-> B
    N -. orchestrates .-> E
    N -. orchestrates .-> G
    N -. orchestrates .-> H
    N -. orchestrates .-> I
```

### Local-to-cloud mapping

| Local component | AWS-pattern equivalent | Purpose |
|---|---|---|
| Persistent Moto server | Amazon S3 | Object landing and curated artifacts |
| Local PostgreSQL | Amazon Redshift or Athena-facing warehouse layer | Dimensional analytics tables |
| Airflow | Amazon MWAA | Scheduling, retries, and dependencies |
| Python/pandas | AWS Glue or containerized jobs | Validation and transformation |
| Power BI/Tableau | BI service connected to warehouse | Reporting and exploration |

## Repository layout

```text
ingestion/                 Source acquisition, validation, and landing
transform/                 Cleaning, star schema, and PostgreSQL loading
models/anomaly/            Rolling z-score anomaly detection
models/forecast/           7/30-day exponential-smoothing forecasts
dags/                      Airflow orchestration
dashboard/views.sql        BI-facing PostgreSQL views
dashboard/README.md        Power BI/Tableau connection guide
tests/                     Ingestion, transformation, and model tests
docs/                      Project and insights documentation
```

## Dataset

The default source is a public CSV conversion of the UCI Online Retail dataset. The ingestion layer also accepts a local CSV or XLSX file through `--source`. Expected columns are:

`InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, and `Country`.

Returns are retained as negative-quantity transactions. Missing `CustomerID` is allowed because it occurs in the source data; unknown customers receive a stable `UNKNOWN` dimension member during transformation.

## Local setup

The core pipeline runs with a local PostgreSQL server and a persistent Moto S3-compatible server.

### 1. Install Python dependencies

Use Python 3.11:

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env           # Windows: copy .env.example .env
```

### 2. Start local services

Create a PostgreSQL database named `insightguard`, then start a persistent Moto endpoint in a separate terminal:

```bash
moto_server -H 0.0.0.0 -p 5000
```

Set the values from `.env`, especially:

```text
S3_ENDPOINT_URL=http://localhost:5000
USE_MOTO_S3=false
DATABASE_URL=postgresql+psycopg2://insightguard:insightguard@localhost:5432/insightguard
```

`USE_MOTO_S3=true` is intended for isolated tests or a single-process smoke run. Airflow task boundaries require the persistent Moto server so objects survive between tasks.

### 3. Run the pipeline stages independently

Choose an ingestion partition date, for example `2026-07-21`:

```bash
python -m ingestion.ingest --ingestion-date 2026-07-21
python -m transform.transform --ingestion-date 2026-07-21
```

The transform stage reads the validated landing object, writes curated table artifacts, and loads PostgreSQL. To run it from a local validated CSV instead:

```bash
python -m transform.transform \
  --input ./data/validated_transactions.csv \
  --ingestion-date 2026-07-21 \
  --database-url "$DATABASE_URL"
```

Apply the BI views with:

```bash
psql "$DATABASE_URL" -f dashboard/views.sql
```

## Data flow and idempotency

Each ingestion run writes:

```text
landing/ingestion_date=YYYY-MM-DD/raw/<source>
landing/ingestion_date=YYYY-MM-DD/validated/transactions.csv
landing/ingestion_date=YYYY-MM-DD/rejected/rejected_rows.csv
landing/ingestion_date=YYYY-MM-DD/manifest.json
```

The raw file is preserved unchanged. Validation produces accepted and rejected outputs with rejection reasons and row counts in the manifest. Transformation writes curated star-schema CSV artifacts under `curated/ingestion_date=YYYY-MM-DD/`.

Warehouse dimension keys are deterministic hashes of stable business identifiers. Re-running a partition deletes and reloads only that partition’s fact rows, while dimension inserts use conflict protection. Anomaly flags are replaced for their metric date range, and forecasts are replaced for the same forecast run date.

## Warehouse and model outputs

- `fact_sales` — transaction grain with revenue, returns, quantities, dates, regions, and stable dimension keys.
- `dim_product`, `dim_customer`, `dim_date`, `dim_region` — reporting dimensions.
- `anomaly_flags` — regional daily metrics, rolling baselines, z-scores, anomaly type, and severity.
- `revenue_forecasts` — regional daily forecasts for 7-day and 30-day horizons.

## Orchestration

`dags/insightguard_daily.py` runs daily at 02:00 UTC:

```text
ingestion → transform → load → anomaly_detection → forecast
```

Tasks retry twice with five-minute delays. The failure callback logs a notification stub and optionally posts to `FAILURE_WEBHOOK_URL`, which can later be replaced with a Slack or email integration.

## Testing and CI

Run locally:

```bash
ruff check .
pytest -q
```

GitHub Actions runs the same Ruff and pytest gates on every push and pull request. Tests cover Moto S3 round trips, schema validation, transformation cleaning, stable star-schema keys, anomaly detection, forecasting horizons, and model artifact idempotency smoke paths.

## BI dashboard preparation

Run `dashboard/views.sql` and connect Power BI or Tableau to:

- `vw_kpi_summary`
- `vw_sales_trend`
- `vw_region_category_breakdown`
- `vw_anomalies`
- `vw_revenue_forecast`

Connection details and view descriptions are in [dashboard/README.md](dashboard/README.md). The actual dashboard is intentionally left to the portfolio author.

## Results

Add dashboard screenshots and findings here after exploration:

1. **Finding 1:** `[Insert the most important revenue or regional insight.]`
2. **Finding 2:** `[Insert the most important return, customer, or category insight.]`
3. **Finding 3:** `[Insert the most important anomaly or forecast insight.]`

Suggested evidence includes total revenue, returns, orders, anomaly counts by severity and region, forecast horizon, and screenshots from the Power BI/Tableau report.

## Interview-ready design decisions

- Preserve raw data before validation to support auditability and replay.
- Use deterministic dimension keys and partition-level replacement for safe reruns.
- Use shifted rolling z-scores for explainable anomaly detection without leakage.
- Use exponential smoothing for a transparent baseline forecast.
- Keep the dashboard in Power BI/Tableau and expose stable SQL views rather than coupling reporting to Python.

## Limitations and next steps

- The source dataset is historical and not a live retail feed.
- Currency is standardized to GBP because the source provides a single sterling price field.
- Product categories are heuristic until a maintained taxonomy is supplied.
- A production deployment would add secrets management, data-quality alerts, partition retention, warehouse roles, and managed AWS services.
