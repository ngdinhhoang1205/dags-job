from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="gdp_data",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 8, 13, tz="UTC"),
    catchup=True,
    tags=["gdp_data"],
)
def gdp_data():
    @task
    def fetch_gdp_data():
        import pandas as pd
        from dbnomics import fetch_series
        from utils import get_business_db_engine, replace_table, log_job_run
        table_name = "gdp_data"
        engine = get_business_db_engine()
        try:
            imf_gdp = fetch_series(
            provider_code="IMF", 
            dataset_code="IFS", 
            max_nb_series=5000,
            dimensions={
                "FREQ": ["A"],
                "INDICATOR": ['NGDP_XDC']
                    }
                ).dropna(subset=['value'])

            # Add exchange rate — annual average per country, aggregated from the monthly
            # exchange_rate table (see fetch_exchange_rate.py) already loaded in the business DB.
            exchange_rate_summary = pd.read_sql(
                """
                SELECT "REF_AREA", CAST(EXTRACT(YEAR FROM period) AS TEXT) AS year, AVG(value) AS value
                FROM exchange_rate
                GROUP BY "REF_AREA", EXTRACT(YEAR FROM period)
                """,
                engine,
            )
            imf_gdp = imf_gdp.merge(exchange_rate_summary, left_on=['REF_AREA', 'original_period'], right_on=['REF_AREA', 'year'], how='left')
            imf_gdp = imf_gdp.rename(columns={
                    "value_x": "value",
                    'value_y': 'usd/local'
            })
            imf_gdp = imf_gdp[['@frequency', 'provider_code', 'dataset_code', 'dataset_name',
                'series_code', 'series_name', 'original_period', 'period',
                'original_value', 'value', 'FREQ', 'REF_AREA', 'INDICATOR', 'Frequency',
                'Reference Area', 'Indicator', "usd/local"]]

            imf_gdp['local/usd'] = 1/imf_gdp['usd/local']
            imf_gdp['value_usd'] = imf_gdp['value']*imf_gdp['usd/local']

            # Remove Macao since it's not a country
            imf_gdp = imf_gdp[imf_gdp['REF_AREA']!='MO']

            replace_table(imf_gdp, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(imf_gdp), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_gdp_data()

gdp_data()
