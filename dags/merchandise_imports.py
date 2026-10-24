from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="merchandise_imports_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["merchandise_imports_data"],
)
def merchandise_imports_data():
    @task
    def fetch_merchandise_imports_data():
        import pandas as pd
        from utils import get_business_db_engine, log_job_run, replace_table, fetch_imts

        table_name = "merchandise_imports_data"
        engine = get_business_db_engine()
        try:
            imports_fob = fetch_imts("MG_FOB_USD", start_period="2010-01")
            imports_fob['period'] = pd.to_datetime(imports_fob['period'], format="%Y-M%m").dt.strftime('%Y-%m')
            fob_countries = list(imports_fob['COUNTRY'].unique())

            # Fall back to CIF valuation for countries that don't report FOB
            imports_cif = fetch_imts("MG_CIF_USD", start_period="2010-01")
            imports_cif = imports_cif[~imports_cif['COUNTRY'].isin(fob_countries)]
            imports_cif['period'] = pd.to_datetime(imports_cif['period'], format="%Y-M%m").dt.strftime('%Y-%m')

            merchandise_imports = pd.concat([imports_fob, imports_cif], ignore_index=True)

            replace_table(merchandise_imports, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(merchandise_imports), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_merchandise_imports_data()

merchandise_imports_data()
