.PHONY: up down reset logs trigger test lint query

up:
	docker compose up --build -d

down:
	docker compose down

reset:  ## wipe containers, volumes and the local DuckDB file
	docker compose down -v
	rm -f warehouse/*.duckdb warehouse/*.wal

logs:
	docker compose logs -f airflow-scheduler

trigger:
	docker compose exec airflow-scheduler airflow dags trigger dbt_lakehouse_dag

test:
	pytest -q

lint:
	ruff check . && ruff format --check .

query:
	docker compose exec airflow-scheduler /home/airflow/venvs/dbt/bin/python /opt/airflow/scripts/show_results.py
