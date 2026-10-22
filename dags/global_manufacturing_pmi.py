from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="global_manufacturing_pmi_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["global_manufacturing_pmi_data"],
)
def global_manufacturing_pmi_data():
    @task
    def fetch_global_manufacturing_pmi_data():
        from dbnomics import fetch_series
        from utils import get_business_db_engine, log_job_run, replace_table

        table_name = "global_manufacturing_pmi_data"
        engine = get_business_db_engine()
        try:
            global_manufacturing_pmi = fetch_series(
                provider_code="OECD",
                dataset_code="MEI",
                dimensions={
                    "SUBJECT": ["BSCICP02"],
                    "FREQUENCY": ["M"],
                }
            )

            replace_table(global_manufacturing_pmi, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(global_manufacturing_pmi), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_global_manufacturing_pmi_data()

global_manufacturing_pmi_data()
