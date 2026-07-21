# InsightGuard

InsightGuard is a cloud-native sales analytics and anomaly-detection pipeline for a retail transactions dataset. It is designed to be reproducible locally with Docker Compose and Moto-backed S3 mocking, while remaining portable to AWS services such as S3, Redshift, and Athena.

## Architecture

```text
[Online Retail II CSV]
          |
          v
 [Python ingestion] ---> [Moto S3 landing / AWS S3]
          |
          v
 [Python transforms] ---> [PostgreSQL warehouse]
          |
          v
 [Airflow orchestration] ---> [Anomaly + forecast models]
                                  |
                                  v
                         [Power BI / Tableau views]
```

## Project layout

- `ingestion/` — raw data acquisition and landing-zone writes
- `transform/` — cleaning, dimensional modeling, and warehouse loading
- `models/` — anomaly detection and revenue forecasting
- `dags/` — Airflow workflows
- `dashboard/` — BI-facing SQL views and connection guidance
- `tests/` — automated tests
- `docs/` — project documentation and insight templates

## Local setup

Phase 1 and Phase 2 establish the scaffold, service configuration, and ingestion layer. Detailed pipeline instructions will be added as each phase is implemented.

```bash
cp .env.example .env
docker compose up -d
```

For a persistent local S3-compatible endpoint, install the requirements and run Moto separately. Use the persistent server for Airflow because separate tasks need to share landed objects:

```bash
moto_server -H 0.0.0.0 -p 5000
python -m ingestion.ingest --source ./data/online_retail.csv
```

For isolated tests or a single-process smoke run, set `USE_MOTO_S3=true`. Moto then mocks S3 in-process and no object-store container is required.

The daily Airflow DAG is in `dags/insightguard_daily.py`. It runs ingestion → transformation → warehouse load → anomaly detection → forecasting, retries failed tasks twice, and calls a configurable `FAILURE_WEBHOOK_URL` when a task fails.

## Results

Dashboard screenshots and project findings will be added after the pipeline and BI views are complete.
