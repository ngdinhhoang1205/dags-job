from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="cpi_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["cpi_data"],
)
def cpi_data():
    @task
    def fetch_cpi_data():

        from utils import get_business_db_engine, log_job_run, fetch_imf_data, replace_table
        from config import dict_2_char

        table_name = "cpi_data"
        engine = get_business_db_engine()
        try:
            imf_cpi = fetch_imf_data(country_dict=dict_2_char, dataset_code = "CPI", metric_suffix="PCPI_IX", frequency="M").dropna(subset=['value'])

            # Export data to sql
            replace_table(imf_cpi, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(imf_cpi), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_cpi_data()

cpi_data()
