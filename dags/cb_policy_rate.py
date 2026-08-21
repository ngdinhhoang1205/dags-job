from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="cb_policy_rate_data",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 8, 13, tz="UTC"),
    catchup=True,
    tags=["cb_policy_rate_data"],
)
def cb_policy_rate_data():
    @task
    def fetch_cb_policy_rate_data():
        import pandas as pd
        from dbnomics import fetch_series
        from utils import get_business_db_engine, log_job_run, replace_table, fetch_policy_rate
        from config import dict_2_char, dict_3_char

        table_name = "cb_policy_rate_data"
        engine = get_business_db_engine()
        try:
            cb_policy_rates_bis = fetch_series(
                provider_code="BIS",
                dataset_code="WS_CBPOL",
                dimensions={
                    "FREQ": ["M"],
                    "REF_AREA": list(dict_2_char.keys())
                }
            )

            # Prefer data.imf.org (broader coverage than BIS); reshaped into cb_policy_rates_bis's
            # raw dbnomics column format so both sources can be concatenated as-is. Columns
            # dbnomics provides that api.imf.org has no equivalent for (dataset_name, series_code,
            # series_name) are left blank for IMF-sourced rows.
            df_country_mapping = pd.read_sql(
                """
                SELECT iso3_code, iso2_code, country_name
                FROM country_mapping
                """,
                engine,
            )
            cb_policy_rates_imf_raw = fetch_policy_rate(list(dict_3_char.keys()))
            cb_policy_rates_imf_raw = cb_policy_rates_imf_raw.merge(df_country_mapping, left_on='COUNTRY', right_on='iso3_code', how='left')
            cb_policy_rates_imf_raw['period'] = pd.to_datetime(cb_policy_rates_imf_raw['period'], format="%Y-M%m")

            cb_policy_rates_imf = pd.DataFrame({
                '@frequency': 'monthly',
                'provider_code': 'IMF',
                'dataset_code': 'MFS_IR',
                'dataset_name': pd.NA,
                'series_code': pd.NA,
                'series_name': pd.NA,
                'original_period': cb_policy_rates_imf_raw['period'].dt.strftime('%Y-%m'),
                'period': cb_policy_rates_imf_raw['period'],
                'original_value': cb_policy_rates_imf_raw['value'],
                'value': cb_policy_rates_imf_raw['value'],
                'FREQ': 'M',
                'REF_AREA': cb_policy_rates_imf_raw['iso2_code'],
                'Frequency': 'Monthly',
                'Reference area': cb_policy_rates_imf_raw['country_name'],
            })

            # Fall back to BIS/dbnomics for Eurozone members (ECB sets one shared rate) and
            # others MFS_IR omits (e.g. GB, SA, KW)
            imf_covered_countries = set(cb_policy_rates_imf['REF_AREA'].dropna().unique())
            cb_policy_rates_bis_fallback = cb_policy_rates_bis[~cb_policy_rates_bis['REF_AREA'].isin(imf_covered_countries)]

            cb_policy_rate_combined = pd.concat([cb_policy_rates_imf, cb_policy_rates_bis_fallback], ignore_index=True)
            cb_policy_rate_combined = cb_policy_rate_combined.dropna(subset=['value'])

            replace_table(cb_policy_rate_combined, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(cb_policy_rate_combined), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_cb_policy_rate_data()

cb_policy_rate_data()
