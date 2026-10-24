from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="ipi_data",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["ipi_data"],
)
def ipi_data():
    @task
    def fetch_ipi_data():
        import pandas as pd
        from utils import get_business_db_engine, log_job_run, fetch_imf_bulk, fetch_ipi, replace_table
        from config import dict_2_char, dict_3_char
        table_name = "ipi_data"
        engine = get_business_db_engine()
        try:
            ipi_db_nomics = fetch_imf_bulk(
                dataset_code="IFS",
                metric_code="AIP_IX", 
                frequency="M",
                country_list=list(dict_2_char.keys())
            )

            # 2. INDUSTRIAL PRODUCTION INDEX - IPI (Source: https://data.imf.org/en/Data-Explorer?datasetUrn=IMF.STA:PI(2.0.0))
            ipi_raw_imf = fetch_ipi(list(dict_3_char.keys()))
            ipi_raw_imf['period'] = pd.to_datetime(ipi_raw_imf['period'], format="%Y-M%m")
            # Get df_country_mapping
            df_country_mapping = pd.read_sql(
                            """
                            SELECT iso3_code, iso2_code, country_name
                            FROM country_mapping
                            """,
                            engine,
                        )
            # Get iso2_code and country_name
            ipi_raw_imf = ipi_raw_imf.merge(df_country_mapping, left_on='COUNTRY', right_on='iso3_code', how='left')
            ipi_raw_imf = ipi_raw_imf[['period', 'value', 'iso2_code', 'country_name']]
            # rename columns
            ipi_raw_imf = ipi_raw_imf.rename(columns={
                    'period': 'Date',
                    'iso2_code': 'country_code'
            })

            # Prefer data.imf.org for any country it covers; fall back to dbnomics only where imf.org has no data
            ipi_raw_imf['Source'] = 'data.imf.org'
            ipi_db_nomics['Source'] = 'dbnomics'

            imf_covered_countries = set(ipi_raw_imf['country_code'].unique())
            ipi_db_nomics_fallback = ipi_db_nomics[~ipi_db_nomics['country_code'].isin(imf_covered_countries)]

            ipi_combined = pd.concat([ipi_raw_imf, ipi_db_nomics_fallback], ignore_index=True)

            # Export data to sql
            replace_table(ipi_combined, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(ipi_combined), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_ipi_data()

ipi_data()
