# Automated Medallion Lakehouse ETL/ELT Pipeline

[![ci](https://github.com/<your-user>/lakehouse-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-user>/lakehouse-pipeline/actions)

A production-style, **100% free and local-first** data platform: raw API data lands in an
S3-compatible lake (LocalStack), is cleaned by **PySpark**, validated by **Soda Core data
contracts**, modelled into a **star schema with dbt-duckdb**, and orchestrated by **Apache Airflow**.
One command starts everything.

## Architecture

```
 [DummyJSON API]   [Seeded transaction generator]
        \               /
         v             v
   +-----------------------------+
   | 1. INGEST (Python/boto3)    |  retries, idempotent keys
   +--------------+--------------+
                  | JSON Lines
                  v
   +==============================================+
   |  LocalStack S3  s3://lakehouse               |
   |  bronze/<entity>/ingestion_date=YYYY-MM-DD/  |
   +==============================================+
                  |
                  v
   +-----------------------------+      bad rows
   | 2. PYSPARK (bronze->silver) |-----> quarantine/transactions/
   |  cast, clean, dedupe,       |
   |  partition                  |
   +--------------+--------------+
                  | Parquet
                  v
   +==============================================+
   |  silver/transactions/transaction_date=.../   |
   |  silver/customers/  silver/products/         |
   +==============================================+
                  |
                  v
   +-----------------------------+
   | 3. SODA CORE contract gate  |-- FAIL --> pipeline stops, alert
   +--------------+--------------+
                  | PASS
                  v
   +-----------------------------+
   | 4. dbt-duckdb               |
   |  silver models -> GOLD      |
   +--------------+--------------+
                  v
   +==============================================+
   | warehouse/lakehouse.duckdb                   |
   |  gold.dim_date dim_customer dim_product      |
   |  gold.fact_sales  (star schema)              |
   +==============================================+
                  |
                  v
   5. dbt tests -> BI / SQL / notebooks

 Orchestrated by Apache Airflow (retries, SLAs, alerts); metadata in Postgres.
```

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 (LocalExecutor, TaskFlow API) + Postgres |
| Lake storage | LocalStack S3 (bronze / silver / quarantine) |
| Processing | PySpark 3.5 (local mode, S3A connector) |
| Data quality | Soda Core (SodaCL contract) + dbt tests |
| Transformation | dbt-duckdb (Silver models, Gold star schema) |
| Dev tooling | Docker Compose, ruff, pytest, GitHub Actions |

## Quick start

Prerequisites: Docker Desktop / Engine (>= 6 GB RAM allocated), Docker Compose v2, Git.

```bash
git clone https://github.com/<your-user>/lakehouse-pipeline.git && cd lakehouse-pipeline
cp .env.example .env            # Linux: set AIRFLOW_UID=$(id -u)
docker compose up --build -d    # first build takes ~5-10 min
```

Open http://localhost:8080 (login `admin` / `admin`). The DAG `dbt_lakehouse_dag` is unpaused
and runs automatically. Re-trigger: `make trigger`. Inspect Gold: `make query`.

## Project layout

```
dags/dbt_lakehouse_dag.py      Airflow DAG (TaskFlow + BashOperator)
src/lakehouse/                 ingestion.py, bronze_to_silver.py (PySpark), config.py
quality/                       checks.yml (Soda contract) + run_soda.py
dbt_project/models/silver|gold dbt models + schema tests
tests/                         pytest unit tests
docker-compose.yml, Dockerfile Infra (Airflow, Postgres, LocalStack)
```

## Design decisions (interview talking points)

- **Idempotency:** Bronze keys are deterministic per `ds`; Silver is rebuilt from Bronze with dynamic partition overwrite.
- **Deduplication:** window function on `transaction_id`, latest `ingested_at` wins.
- **Quarantine, don't drop:** invalid rows go to `quarantine/` with a `reject_reason`.
- **Data contract as a gate:** Soda runs on Silver *before* Gold; failure stops the DAG.
- **Explicit schemas** for JSON reads; no inference drift.
- **Isolated virtualenvs** for dbt and Soda to avoid dependency conflicts with Airflow.
- **Star schema:** `fact_sales` (grain = transaction) + `dim_date`, `dim_customer`, `dim_product`.
- **Reliability:** retries with exponential backoff, timeouts, SLAs, failure/SLA webhook alerts.

## Sample analytics query

```sql
select p.category, round(sum(f.recognized_revenue), 2) as revenue
from gold.fact_sales f join gold.dim_product p using (product_key)
group by 1 order by 2 desc;
```

## Troubleshooting

- **Permission errors on Linux:** set `AIRFLOW_UID=$(id -u)` in `.env`, then `docker compose down && up`.
- **Spark OOM:** raise Docker memory or lower `SYNTHETIC_ROWS` in `docker-compose.yml`.
- **LocalStack image:** pinned to 3.8.1 (newer tags need an auth token). MinIO is a drop-in alternative.
- **Great Expectations instead of Soda:** swap `quality/run_soda.py`; the DAG task contract (non-zero exit = fail) is unchanged.

## Roadmap ideas

Incremental dbt models, Iceberg/Delta tables, OpenLineage + Marquez, Metabase/Superset dashboard, Terraform to real AWS.

---

## How to showcase this project

**GitHub**
1. Repo name: `medallion-lakehouse-pipeline`; add topics: `data-engineering airflow pyspark dbt duckdb soda localstack`.
2. Put the architecture diagram at the top of the README and add 3 screenshots: Airflow DAG graph (green), Soda output, a Gold query result.
3. Keep CI green, commit in small meaningful steps (conventional commits), pin the repo on your profile.
4. Add a 2-minute Loom/GIF of `docker compose up` through a successful run.

**LinkedIn post template**
> I built an end-to-end Medallion Lakehouse pipeline that runs entirely free on my laptop.
> API -> S3 (LocalStack) -> PySpark -> Soda data contracts -> dbt star schema in DuckDB, orchestrated by Airflow with retries and SLA alerts.
> Highlights: quarantine for bad records, idempotent reruns, contract gate before Gold, CI with lint + tests.
> What I learned: <1-2 real lessons>. Repo + diagram: <link>. #dataengineering #airflow #dbt #pyspark

**Resume bullets**
- Built a containerised Medallion lakehouse (Airflow, PySpark, dbt-duckdb, LocalStack) processing 50K+ daily transactions with idempotent reruns.
- Implemented Soda Core data contracts gating Silver->Gold promotion, plus quarantine of invalid records.
- Modelled a star schema (1 fact, 3 dimensions) with dbt tests (uniqueness, referential integrity); CI via GitHub Actions.
