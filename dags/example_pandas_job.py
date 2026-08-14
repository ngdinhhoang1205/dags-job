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
        from utils import fetch_series, get_business_db_engine, replace_table
        exchange_rate = fetch_series(
            provider_code="IMF",
            dataset_code="IFS", # Exchange Rates, US Dollar per Domestic Currency, Period Average, Rate
            max_nb_series = 5000,
            dimensions={
                "FREQ": ["M"],
                "INDICATOR": ['EDNA_USD_XDC_RATE']
            }
        )
        engine = get_business_db_engine()
        replace_table(exchange_rate, "exchange_rate", engine)
    fetch_exchange_rate()




exchange_rate()
