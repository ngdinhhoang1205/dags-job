from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="population_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["population_data"],
)
def population_data():
    @task
    def fetch_population_data():
        import pandas as pd
        from utils import get_business_db_engine, log_job_run, replace_table, fetch_weo
        from config import dict_3_char

        table_name = "population_data"
        engine = get_business_db_engine()
        try:
            population = fetch_weo("LP", list(dict_3_char.keys()))
            population['period'] = pd.to_datetime(population['period'], format="%Y")

            replace_table(population, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(population), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_population_data()

population_data()
