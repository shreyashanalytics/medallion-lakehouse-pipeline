"""Airflow DAG: Medallion lakehouse (Bronze -> Silver -> quality gate -> Gold).

ingest_raw -> bronze_to_silver_spark -> data_quality_gate
           -> dbt_silver -> dbt_gold -> dbt_test -> pipeline_success

Reliability features: retries with exponential backoff, execution timeouts,
per-task SLAs, failure + SLA-miss alert callbacks, single active run.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

VENV_ROOT = "/home/airflow/venvs"
DBT = f"{VENV_ROOT}/dbt/bin/dbt"
SODA_PYTHON = f"{VENV_ROOT}/soda/bin/python"
DBT_DIR = "/opt/airflow/dbt_project"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"


# --------------------------------------------------------------------------- #
# Alerting
# --------------------------------------------------------------------------- #
def _alert(message: str) -> None:
    """Log the alert and, if configured, post it to a Slack-compatible webhook."""
    log.error(message)
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    try:
        requests.post(webhook, json={"text": message}, timeout=10)
    except requests.RequestException:
        log.exception("Could not deliver alert to webhook")


def notify_failure(context: dict) -> None:
    """``on_failure_callback`` fired once a task has exhausted its retries."""
    ti = context["task_instance"]
    _alert(f"[FAILED] {ti.dag_id}.{ti.task_id} (run {context['run_id']}) - {ti.log_url}")


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # noqa: A002
    """``sla_miss_callback``: tasks that finished (or are still running) too late."""
    _alert(f"[SLA MISS] DAG {dag.dag_id}: {task_list}")


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="dbt_lakehouse_dag",
    description="Medallion lakehouse: API -> S3 -> PySpark -> Soda -> dbt-duckdb",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=notify_sla_miss,
    tags=["lakehouse", "medallion", "dbt", "spark"],
)
def dbt_lakehouse_dag():
    @task(sla=timedelta(minutes=10))
    def ingest_raw(ds: str | None = None) -> dict:
        """Bronze: pull API data + synthetic transactions into LocalStack S3."""
        from lakehouse.ingestion import run_ingestion  # lazy: keeps DAG parsing fast

        counts = run_ingestion(ds)
        log.info("Bronze ingestion counts: %s", counts)
        return counts

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_spark",
        bash_command="python -m lakehouse.bronze_to_silver",
        sla=timedelta(minutes=20),
    )

    data_quality_gate = BashOperator(
        task_id="data_quality_gate",
        bash_command=f"{SODA_PYTHON} /opt/airflow/quality/run_soda.py",
        sla=timedelta(minutes=25),
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=f"{DBT} run --select path:models/silver {DBT_FLAGS}",
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f"{DBT} run --select path:models/gold {DBT_FLAGS}",
        sla=timedelta(minutes=40),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS}",
    )

    @task
    def pipeline_success() -> None:
        log.info("Lakehouse refresh finished: Gold star schema is up to date.")

    (
        ingest_raw()
        >> bronze_to_silver
        >> data_quality_gate
        >> dbt_silver
        >> dbt_gold
        >> dbt_test
        >> pipeline_success()
    )


dbt_lakehouse_dag()
