# InsightGuard

Sales analytics + anomaly detection pipeline built around the UCI Online Retail dataset. Takes raw CSV transaction data, validates and lands it in S3, transforms it into a star schema in PostgreSQL, runs anomaly detection (rolling z-scores) and revenue forecasting (exponential smoothing), then exposes everything through SQL views that Power BI / Tableau can connect to.

## Why this exists

I wanted a proper end-to-end data pipeline project — not just a notebook that reads a CSV and makes some charts. The idea is two questions every retail team cares about:

1. What's actually happening with sales, returns, products, and regions over time?
2. Are there any weird revenue/return spikes worth looking into before they become real problems?

The UCI dataset doesn't have product categories, so the BI layer uses a keyword-based heuristic on descriptions. It's hacky but it works — you'd swap it out for a real taxonomy in production.

## How it works

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

Locally everything runs against Moto (mock S3) and a real Postgres instance. In production you'd swap Moto for actual S3, Postgres for Redshift, etc.

| Local | Production equivalent | What it does |
|---|---|---|
| Moto server | S3 | Object storage for raw + curated data |
| PostgreSQL | Redshift / Athena | Warehouse layer |
| Airflow (docker-compose) | MWAA | Orchestration |
| pandas | Glue / containerized jobs | Transform logic |

## Repo structure

```text
ingestion/          Download, validate, land raw data in S3
transform/          Cleaning + star schema build + Postgres load
models/anomaly/     Rolling z-score anomaly flagging
models/forecast/    Exponential smoothing forecasts (7d + 30d)
dags/               Airflow DAG
dashboard/          SQL views + BI connection docs
tests/              pytest suite
docs/               Write-up template
```

## Dataset

Uses the [UCI Online Retail](https://archive.ics.uci.edu/ml/datasets/Online+Retail) dataset (or any CSV/XLSX with the same columns). You can also point `--source` at a local file.

Expected columns: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.

Negative quantities = returns. Missing `CustomerID` is fine (it's common in the source data) — those get mapped to an `UNKNOWN` dimension key during transform.

## Getting started

Need Python 3.11 and a running PostgreSQL instance.

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env
```

Fire up a Moto server in another terminal (or use `USE_MOTO_S3=true` for quick tests):

```bash
moto_server -H 0.0.0.0 -p 5000
```

Make sure `.env` has the right values:
```
S3_ENDPOINT_URL=http://localhost:5000
USE_MOTO_S3=false
DATABASE_URL=postgresql+psycopg2://insightguard:insightguard@localhost:5432/insightguard
```

> **Note:** `USE_MOTO_S3=true` spins up an in-process mock — fine for tests but won't work with Airflow since objects don't persist across task boundaries.

### Running the pipeline

Pick a partition date and run:

```bash
python -m ingestion.ingest --ingestion-date 2026-07-21
python -m transform.transform --ingestion-date 2026-07-21
```

Or if you already have a validated CSV locally:

```bash
python -m transform.transform \
  --input ./data/validated_transactions.csv \
  --ingestion-date 2026-07-21 \
  --database-url "$DATABASE_URL"
```

Then apply the BI views:

```bash
psql "$DATABASE_URL" -f dashboard/views.sql
```

## How data flows

Each run creates a partitioned structure in S3:

```
landing/ingestion_date=YYYY-MM-DD/raw/<source file>
landing/ingestion_date=YYYY-MM-DD/validated/transactions.csv
landing/ingestion_date=YYYY-MM-DD/rejected/rejected_rows.csv
landing/ingestion_date=YYYY-MM-DD/manifest.json
```

Raw file is kept as-is for auditability. Rejected rows include a reason column so you can see what got filtered out.

Dimension keys are SHA1-based so they're deterministic — re-running the same partition is safe (it deletes + reloads facts, and dimensions use `ON CONFLICT DO NOTHING`).

## What's in the warehouse

- **fact_sales** — one row per line item. Has revenue, return amounts, quantities, and FK references to all dims.
- **dim_product**, **dim_customer**, **dim_date**, **dim_region** — standard SCD Type 1 dimensions.
- **anomaly_flags** — daily metrics per region with rolling baselines, z-scores, severity labels.
- **revenue_forecasts** — 7-day and 30-day horizon predictions per region.

## Airflow DAG

Runs daily at 02:00 UTC: `ingestion → transform → load → anomaly_detection → forecast`

2 retries, 5 min between. There's a failure callback that hits a webhook URL if you set `FAILURE_WEBHOOK_URL` (otherwise just logs it).

## Tests

```bash
ruff check .
pytest -q
```

CI runs both on every push/PR. Tests cover S3 round-trips, validation, transform logic, stable key generation, anomaly detection, and forecast outputs.

## Dashboard

After running `dashboard/views.sql`, connect Power BI or Tableau to these views:

- `vw_kpi_summary` — top-level numbers
- `vw_sales_trend` — daily trends by region
- `vw_region_category_breakdown` — revenue by region × product category
- `vw_anomalies` — flagged anomalies with z-scores
- `vw_revenue_forecast` — forecast output

See [dashboard/README.md](dashboard/README.md) for connection details.

## Key findings from the UCI dataset

Running the full dataset (~541k rows, 38 countries) through the pipeline:

- **UK dominates** — roughly 80%+ of revenue and most of the transaction volume. But countries like Netherlands, Ireland, Germany have noticeably higher average order values (wholesale/bulk buyers).
- **Returns are small but concentrated** — cancellations (C-prefixed invoices) are ~1.7% of invoices but they cluster around seasonal giftware and fragile items. About 25% of rows have no CustomerID (guest checkouts) — keeping those as `UNKNOWN` preserves ~£1.4M in revenue that'd be lost with a naive inner join.
- **Anomaly detector picks up real things** — the z-score flagging catches Q4 holiday spikes and those big wholesale cancellation reversals (some single invoices worth £70k-170k). The 7-day smoothing tracks weekly cycles well; the 30-day gives a steadier baseline.

## Design choices

- Raw data preserved before any validation (replay + audit)
- Deterministic dimension keys → safe re-runs
- Shifted rolling z-score so today's value doesn't leak into its own baseline
- Exponential smoothing over ARIMA because it's simpler to explain and the data doesn't really need seasonality decomposition
- Dashboard lives in Power BI/Tableau, not Python — the pipeline just exposes views

## Known limitations

- Historical dataset, not a live feed
- Everything is in GBP (single currency in source)
- Product categories are just keyword matching on descriptions
- No secrets management, data quality alerts, or partition retention — would need those for a real deployment
