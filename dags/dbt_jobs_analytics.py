from __future__ import annotations

import os

import pendulum
import requests
from airflow.decorators import dag, task


@dag(
    dag_id="dbt_jobs_analytics",
    # An hour after the 07:00/19:00 ingestion DAGs — a time-based buffer, not a real
    # dependency. If an ingestion DAG runs long, dbt can still pick up stale data.
    schedule="0 8,20 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["dbt", "jobs_analytics"],
)
def dbt_jobs_analytics():
    @task
    def trigger_dbt_run():
        url = os.environ["DBT_API_URL"].rstrip("/") + "/run"
        headers = {"X-API-Key": os.environ.get("DBT_API_KEY", "")}
        resp = requests.post(url, headers=headers, timeout=1800)
        print(resp.json().get("stdout") or resp.text)
        resp.raise_for_status()

    trigger_dbt_run()


dbt_jobs_analytics()
