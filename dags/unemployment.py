from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="unemployment_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["unemployment_data"],
)
def unemployment_data():
    @task
    def fetch_unemployment_data():
        import pandas as pd
        from dbnomics import fetch_series
        from utils import get_business_db_engine, log_job_run, replace_table
        from config import dict_3_char

        table_name = "unemployment_data"
        engine = get_business_db_engine()
        try:
            unemployment_raw = fetch_series(
                provider_code="IMF",
                dataset_code="WEO:latest",
                max_nb_series=200,
                dimensions={
                    "weo-country": list(dict_3_char.keys()),
                    "weo-subject": ["LUR"],
                    "unit": ["pcent_total_labor_force"]
                }
            )
            unemployment_raw = unemployment_raw[unemployment_raw['period'] <= pd.Timestamp.today()]

            replace_table(unemployment_raw, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(unemployment_raw), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_unemployment_data()

unemployment_data()
