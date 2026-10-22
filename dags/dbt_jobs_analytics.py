from __future__ import annotations

import os

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

# Ingestion DAGs dbt reads from. They must share this DAG's schedule so the sensors
# can match runs by logical date.
UPSTREAM_DAG_IDS = [
    "cb_policy_rate_data",
    "country_mapping",
    "cpi_data",
    "fetch_exchange_rate",
    "gdp_data",
    "global_manufacturing_pmi_data",
    "ipi_data",
    "merchandise_exports_data",
    "merchandise_imports_data",
    "population_data",
    "retail_sales_data",
    "trade_in_services_data",
    "unemployment_data",
]


@dag(
    dag_id="dbt_jobs_analytics",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["dbt", "jobs_analytics"],
)
def dbt_jobs_analytics():
    wait_for_upstream = [
        ExternalTaskSensor(
            task_id=f"wait_for_{dag_id}",
            external_dag_id=dag_id,
            external_task_id=None,  # wait for the whole DAG run
            allowed_states=["success"],
            failed_states=["failed"],
            mode="reschedule",  # free the worker slot between pokes
            poke_interval=60,
            timeout=2 * 60 * 60,
        )
        for dag_id in UPSTREAM_DAG_IDS
    ]

    # all_done: a single broken source shouldn't block analytics — dbt still runs on
    # the tables that refreshed (the failed one keeps its previous data).
    @task(trigger_rule="all_done")
    def trigger_dbt_run():
        url = os.environ["DBT_API_URL"].rstrip("/") + "/run"
        headers = {"X-API-Key": os.environ.get("DBT_API_KEY", "")}
        resp = requests.post(url, headers=headers, timeout=1800)
        print(resp.json().get("stdout") or resp.text)
        resp.raise_for_status()

    wait_for_upstream >> trigger_dbt_run()


dbt_jobs_analytics()
