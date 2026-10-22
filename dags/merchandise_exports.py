from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="merchandise_exports_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["merchandise_exports_data"],
)
def merchandise_exports_data():
    @task
    def fetch_merchandise_exports_data():
        import pandas as pd
        from utils import get_business_db_engine, log_job_run, replace_table, fetch_imts

        table_name = "merchandise_exports_data"
        engine = get_business_db_engine()
        try:
            merchandise_exports = fetch_imts("XG_FOB_USD", start_period="2010-01")
            merchandise_exports['period'] = pd.to_datetime(merchandise_exports['period'], format="%Y-M%m").dt.strftime('%Y-%m')

            replace_table(merchandise_exports, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(merchandise_exports), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_merchandise_exports_data()

merchandise_exports_data()
