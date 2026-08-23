from __future__ import annotations

import os

import pendulum
import requests
from airflow.decorators import dag, task


@dag(
    dag_id="dbt_jobs_analytics",
    # A few hours after the @monthly ingestion DAGs (midnight on the 1st) — a time-based
    # buffer, not a real dependency. If an ingestion DAG runs long, this can still race it;
    # wire up ExternalTaskSensor/TriggerDagRunOperator if that becomes a problem.
    schedule="0 6 1 * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="UTC"),
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
