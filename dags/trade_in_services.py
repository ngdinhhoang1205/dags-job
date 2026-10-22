from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="trade_in_services_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["trade_in_services_data"],
)
def trade_in_services_data():
    @task
    def fetch_trade_in_services_data():
        import pandas as pd
        from utils import get_business_db_engine, log_job_run, replace_table, fetch_bop_services
        from config import dict_3_char

        engine = get_business_db_engine()
        services_target = list(dict_3_char.keys())

        def quarterly_to_monthly(df):
            """BOP services data is quarterly — split each value evenly across its 3 months."""
            df = df.copy()
            df['period'] = pd.to_datetime(df['period'])
            df['value'] = df['value'] / 3
            df = df.loc[df.index.repeat(3)].reset_index(drop=True)
            df['period'] = (pd.PeriodIndex(df['period'], freq='M') + df.index % 3).to_timestamp()
            return df

        exports_table = "trade_in_services_exports"
        try:
            services_exports = fetch_bop_services("CD_T", services_target, start_period="2010-01")
            services_exports = quarterly_to_monthly(services_exports)
            replace_table(services_exports, exports_table, engine)
            log_job_run(engine, table_written=exports_table, rows_written=len(services_exports), status="success")
        except Exception as e:
            log_job_run(engine, table_written=exports_table, rows_written=None, status="failed", extra={"error": str(e)})
            raise

        imports_table = "trade_in_services_imports"
        try:
            services_imports = fetch_bop_services("DB_T", services_target, start_period="2010-01")
            services_imports = quarterly_to_monthly(services_imports)
            replace_table(services_imports, imports_table, engine)
            log_job_run(engine, table_written=imports_table, rows_written=len(services_imports), status="success")
        except Exception as e:
            log_job_run(engine, table_written=imports_table, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_trade_in_services_data()

trade_in_services_data()
