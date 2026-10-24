from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="retail_sales_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["retail_sales_data"],
)
def retail_sales_data():
    @task
    def fetch_retail_sales_data():
        import pandas as pd
        from dbnomics import fetch_series
        from utils import get_business_db_engine, log_job_run, replace_table

        table_name = "retail_sales_data"
        engine = get_business_db_engine()
        try:
            # OECD (MEI) doesn't report retail sales in one consistent unit across countries —
            # index-based (IXEB) for most, monthly-level (ML) for a few others.
            retail_sales_set_1 = fetch_series(
                provider_code="OECD",
                dataset_code="MEI",
                dimensions={
                    "SUBJECT": ["SLRTTO02"],  # Sales > Retail trade > Total retail trade > Value
                    "MEASURE": ["IXEB"],      # Index, US Dollars, monthly level
                    "FREQUENCY": ["M"],
                    "LOCATION": ['AUT', 'BEL', 'BGR', 'CHE', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'NLD', 'NOR', 'PRT', 'SVK', 'TUR']
                }
            )
            retail_sales_set_2 = fetch_series(
                provider_code="OECD",
                dataset_code="MEI",
                dimensions={
                    "SUBJECT": ["SLRTTO02"],
                    "MEASURE": ["ML"],
                    "FREQUENCY": ["M"],
                    "LOCATION": ['CHN', 'RUS', 'USA', 'ZAF']
                }
            )
            retail_sales = pd.concat([retail_sales_set_1, retail_sales_set_2], ignore_index=True)

            replace_table(retail_sales, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(retail_sales), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_retail_sales_data()

retail_sales_data()
