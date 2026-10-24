from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="country_mapping",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2026, 8, 13, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=15)},
    tags=["country_mapping"],
)
def country_mapping():
    @task
    def fetch_country_mapping():
        import pandas as pd
        from utils import get_business_db_engine, replace_table, log_job_run, get_continent
        from config import dict_3_char, dict_2_char
        table_name = "country_mapping"
        engine = get_business_db_engine()
        try:
            # Tạo DataFrame Mapping
            df_map_2_char = pd.DataFrame(list(dict_2_char.items()), columns=['iso2_code', 'country_name_2c'])
            df_map_3_char = pd.DataFrame(list(dict_3_char.items()), columns=['iso3_code', 'country_name'])

            # Ghép theo index để tạo bảng mapping 3 cột chuẩn
            df_country_mapping = pd.concat([df_map_2_char[['iso2_code']], df_map_3_char], axis=1)

            # Reorder cột cho đẹp: iso2_code | iso3_code | country_name
            df_country_mapping = df_country_mapping[['iso2_code', 'iso3_code', 'country_name']]

            df_country_mapping['continent'] = df_country_mapping['iso2_code'].apply(get_continent)

            replace_table(df_country_mapping, table_name, engine)
            log_job_run(engine, table_written=table_name, rows_written=len(df_country_mapping), status="success")
        except Exception as e:
            log_job_run(engine, table_written=table_name, rows_written=None, status="failed", extra={"error": str(e)})
            raise
    fetch_country_mapping()

country_mapping()
