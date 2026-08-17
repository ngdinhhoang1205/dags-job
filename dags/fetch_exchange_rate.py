from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="fetch_exchange_rate",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 8, 13, tz="UTC"),
    catchup=True,
    tags=["fetch_exchange_rate"],
)
def exchange_rate():
    @task
    def fetch_exchange_rate():
        from dbnomics import fetch_series
        from utils import get_business_db_engine, replace_table, log_job_run
        table_name = "exchange_rate"
        engine = get_business_db_engine()
        try:
            exchange_rate = fetch_series(
                provider_code="IMF",
                dataset_code="IFS", # Exchange Rates, US Dollar per Domestic Currency, Period Average, Rate
                max_nb_series = 5000,
                dimensions={
                    "FREQ": ["M"],
                    "INDICATOR": ['EDNA_USD_XDC_RATE']
                }
            )
            replace_table(exchange_rate, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(exchange_rate), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_exchange_rate()

exchange_rate()
