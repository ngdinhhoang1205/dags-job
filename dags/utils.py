import json
import os
import re
import pandas as pd
import requests
from dbnomics import fetch_series
from dotenv import load_dotenv
from pycountry_convert import country_alpha2_to_continent_code, convert_continent_code_to_continent_name
from sqlalchemy import create_engine, inspect, text

load_dotenv()


def get_business_db_engine():
    """SQLAlchemy engine for the business Postgres database (see docker-compose.yaml's postgres-business service)."""
    user = os.environ.get("BUSINESS_POSTGRES_USER", "business")
    password = os.environ.get("BUSINESS_POSTGRES_PASSWORD", "business")
    db = os.environ.get("BUSINESS_POSTGRES_DB", "business")
    host = os.environ.get("BUSINESS_POSTGRES_HOST", "postgres-business")
    port = os.environ.get("BUSINESS_POSTGRES_PORT", "5432")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def replace_table(df, table_name, engine):
    """Atomically replace the full contents of `table_name` with `df` (truncate + insert in one
    transaction). Use this for jobs that fetch a full snapshot each run rather than only new rows —
    truncating keeps the table's indexes/constraints intact and never leaves it empty or half-written
    if the job fails partway through. Creates the table on first run if it doesn't exist yet."""
    with engine.begin() as conn:
        if inspect(conn).has_table(table_name):
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
        df.to_sql(table_name, conn, if_exists="append", index=False)


def log_job_run(engine, table_written, rows_written, status, extra=None):
    """Record one job run in the `job_runs` audit table (business DB) — separate from Airflow's own
    metadata DB, which only tracks task state/timing, not what a task actually did. `extra` is
    optional free-form data (dict), stored as JSONB. Creates the table on first run if needed."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_runs (
                id SERIAL PRIMARY KEY,
                run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                table_written TEXT NOT NULL,
                rows_written INTEGER,
                status TEXT NOT NULL,
                extra JSONB
            )
        """))
        conn.execute(
            text("""
                INSERT INTO job_runs (table_written, rows_written, status, extra)
                VALUES (:table_written, :rows_written, :status, CAST(:extra AS JSONB))
            """),
            {
                "table_written": table_written,
                "rows_written": rows_written,
                "status": status,
                "extra": json.dumps(extra) if extra is not None else None,
            },
        )

IMF_API_BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow"

# pycountry_convert doesn't recognize these — historical entities and aggregates, not real ISO countries.
_CONTINENT_OVERRIDES = {
    "TL": "Asia",             # Timor-Leste
    "VA": "Europe",           # Holy See / Vatican City
    "SX": "North America",    # Sint Maarten (Dutch part)
    "SUH": "Europe",          # Former U.S.S.R. (historical, spanned Europe/Asia)
    "U2": "Europe",           # Euro Area (aggregate, not a real country)
}


def get_continent(iso2_code):
    """Map an ISO2 code to a continent name, with manual overrides for codes
    pycountry_convert doesn't recognize (historical entities, aggregates)."""
    if iso2_code in _CONTINENT_OVERRIDES:
        return _CONTINENT_OVERRIDES[iso2_code]
    try:
        return convert_continent_code_to_continent_name(country_alpha2_to_continent_code(iso2_code))
    except KeyError:
        return None
IMF_STRUCTURE_BASE = "https://api.imf.org/external/sdmx/3.0/structure/codelist"


# IMF regional-department groupings that are (unhelpfully) coded as plain 3-letter
# IDs, so they pass a naive [A-Z]{3} filter alongside real ISO3 country codes.
_IMTS_REGIONAL_CODES = {"AFR", "EUR", "APD", "MCD", "WHD"}


def get_imts_countries(agency="IMF.STA", codelist_id="CL_IMTS_COUNTRY", version="1.0.0"):
    """
    Real ISO3 reporting countries for the IMTS dataflow, straight from IMF's public
    codelist (no API key needed) — excludes regional/grouping aggregates like G001,
    GX170, TX799, U019 (which all contain digits) and the handful of IMF regional
    departments (AFR, EUR, APD, MCD, WHD) that are coded as plain 3-letter IDs.
    """
    resp = requests.get(
        f"{IMF_STRUCTURE_BASE}/{agency}/{codelist_id}/{version}",
        params={"detail": "full"},
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    codes = resp.json()["data"]["codelists"][0]["codes"]
    return sorted(
        c["id"] for c in codes
        if re.fullmatch(r"[A-Z]{3}", c["id"]) and c["id"] not in _IMTS_REGIONAL_CODES
    )


def _parse_imf_sdmx_json(payload):
    """Flatten an SDMX-JSON 3.0 data message (api.imf.org) into a DataFrame."""
    structure = payload["data"]["structures"][0]
    series_dims = structure["dimensions"]["series"]
    time_values = [v["value"] for v in structure["dimensions"]["observation"][0]["values"]]

    rows = []
    for series_key, series in payload["data"]["dataSets"][0].get("series", {}).items():
        dim_indexes = [int(i) for i in series_key.split(":")]
        dim_values = {
            dim["id"]: dim["values"][idx]["id"]
            for dim, idx in zip(series_dims, dim_indexes)
        }
        for obs_index, obs in series.get("observations", {}).items():
            rows.append({**dim_values, "period": time_values[int(obs_index)], "value": obs[0]})

    df = pd.DataFrame(rows)
    if not df.empty:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def chunked(items, size=150):
    """Split a list into chunks — IMF's API rejects a key with all ~260 countries joined by '+' (400 Bad Request)."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_imf_api(dataflow_id, key, agency="IMF.STA", version="+", start_period=None, end_period=None, api_key=None):
    """
    Fetch data directly from IMF's new SDMX 3.0 API (api.imf.org), bypassing DBnomics.
    Requires a subscription key from portal.api.imf.org (pass explicitly or set IMF_API_KEY env var).

    key : SDMX dot-separated dimension key, e.g. "VNM.MG_FOB_USD.G001.M" for IMTS.
    """
    api_key = api_key or os.environ["IMF_API_KEY"]
    url = f"{IMF_API_BASE}/{agency}/{dataflow_id}/{version}/{key}"
    params = {
        "dimensionAtObservation": "TIME_PERIOD",
        "attributes": "all",
        "measures": "all",
        "includeHistory": "true",
    }
    if start_period or end_period:
        bound = f"ge:{start_period}" if start_period else ""
        bound += f"+le:{end_period}" if end_period else ""
        params["c[TIME_PERIOD]"] = bound

    resp = requests.get(
        url,
        params=params,
        headers={"Accept": "application/json", "Ocp-Apim-Subscription-Key": api_key},
    )
    resp.raise_for_status()
    return _parse_imf_sdmx_json(resp.json())


def fetch_ipi(countries, production_index="IND", transformation="IX", frequency="M", start_period=None, end_period=None, api_key=None):
    """
    Fetch an IMF Production Index (PI dataflow) series (default: overall Industrial
    Production, raw index level) for a list of countries. No counterpart-country
    dimension, key is COUNTRY.PRODUCTION_INDEX.TYPE_OF_TRANSFORMATION.FREQUENCY.
    Coverage is narrower than the DBnomics IFS/AIP_IX series for some countries
    (e.g. AUT, BEL, FIN, GBR, SWE, MYS, PHL) — check overlap before replacing it outright.
    """
    dfs = []
    for chunk in chunked(countries):
        key = f"{'+'.join(chunk)}.{production_index}.{transformation}.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="PI", key=key,
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_policy_rate(countries, indicator="MFS166_RT_PT_A_PT", frequency="M", start_period=None, end_period=None, api_key=None):
    """
    Fetch the IMF's Monetary Policy-Related Interest Rate (MFS_IR dataflow) for a list of
    countries. No counterpart-country dimension, key is COUNTRY.INDICATOR.FREQUENCY.
    No monthly/quarterly/annual coverage for Eurozone members (ECB sets one shared rate,
    not reported per-country here), the UK, Saudi Arabia, or Kuwait — fall back to BIS/
    dbnomics (WS_CBPOL) for those. Also note this dataflow tops out at monthly frequency,
    coarser than BIS's daily series.
    """
    dfs = []
    for chunk in chunked(countries):
        key = f"{'+'.join(chunk)}.{indicator}.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="MFS_IR", key=key,
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_policy_rate(countries, frequency="M", start_period=None, end_period=None, api_key=None):
    """
    Fetch IMF's central bank policy-related interest rate (MFS_IR dataflow, indicator
    MFS166_RT_PT_A_PT — "Monetary policy-related, Rate, Percent per annum") for a list
    of countries. Key is COUNTRY.INDICATOR.FREQUENCY.

    Coverage gap: Eurozone members aren't reported individually (ECB sets one shared
    rate), and a handful of others (e.g. GBR, SAU, KWT) aren't covered by this indicator
    at all — fall back to BIS/WS_CBPOL (dbnomics) for those.
    """
    dfs = []
    for chunk in chunked(countries):
        key = f"{'+'.join(chunk)}.MFS166_RT_PT_A_PT.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="MFS_IR", key=key,
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_weo(indicator, countries, frequency="A", start_period=None, end_period=None, api_key=None):
    """
    Fetch a WEO indicator (e.g. LP = population) for a list of countries. Same shape as
    fetch_bop_services — WEO has no counterpart-country dimension, just COUNTRY.INDICATOR.FREQUENCY.
    Note: WEO includes forecast years alongside actuals (it's a projections dataset).
    """
    dfs = []
    for chunk in chunked(countries):
        key = f"{'+'.join(chunk)}.{indicator}.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="WEO", key=key, agency="IMF.RES",
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_bop_services(entry, countries, unit="USD", frequency="Q", start_period=None, end_period=None, api_key=None):
    """
    Fetch BOP trade-in-services value ('CD_T' credits = exports, 'DB_T' debits = imports)
    for a list of countries. Unlike IMTS, BOP has no counterpart-country dimension — each
    country reports one total against the rest of world — so no bilateral chunking or
    aggregate-code filtering is needed, only chunking the reporting-country list itself.
    """
    dfs = []
    for chunk in chunked(countries):
        key = f"{'+'.join(chunk)}.{entry}.S.{unit}.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="BOP", key=key,
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def fetch_imts(indicator, country_key="*", frequency="M", start_period=None, end_period=None, api_key=None):
    """
    Fetch an IMTS indicator (e.g. MG_FOB_USD, MG_CIF_USD, XG_FOB_USD) across every real
    bilateral counterpart country, chunked to stay under the API's URL-length limit.
    Uses get_imts_countries() instead of a '*' wildcard on COUNTERPART_COUNTRY so
    regional/grouping aggregates (G001, GX170, ...) never enter the result — summing
    this DataFrame's 'value' per COUNTRY/period gives the correct total, no double-counting.
    """
    dfs = []
    for chunk in chunked(get_imts_countries()):
        key = f"{country_key}.{indicator}.{'+'.join(chunk)}.{frequency}"
        dfs.append(fetch_imf_api(
            dataflow_id="IMTS", key=key,
            start_period=start_period, end_period=end_period, api_key=api_key,
        ))
    dfs = [df for df in dfs if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def fetch_imf_data(country_dict, dataset_code="IFS", metric_suffix="PCPI_IX", frequency="M"):
    """Fetch macroeconomic data from the IMF via DBnomics with dynamic dataset mapping.

    Parameters:
    -----------
    country_dict : dict
        A dictionary containing country codes (keys) and display names (values).
    dataset_code : str
        The IMF dataset code on DBnomics. 'CPI' for inflation only, 'IFS' for general macro data.
    metric_suffix : str
        The specific IMF metric code (e.g., 'NGDP_R_XDC', 'LUR_P_PE_NUM').
    frequency : str
        Data frequency: 'M' (Monthly), 'Q' (Quarterly), 'A' (Annual).
    """
    all_dfs = []

    for code, country_name in country_dict.items():
        # Tạo endpoint chính xác dựa trên dataset_code
        series_id = f"IMF/{dataset_code}/{frequency}.{code}.{metric_suffix}"

        try:
            df = fetch_series(series_id)
            df["country_name"] = country_name
            df["country_code"] = code
            all_dfs.append(df)
        except Exception as e:
            # Nếu chạy IFS bị lỗi (một số nước nộp dữ liệu trễ), hệ thống sẽ log ra để kiểm tra
            print(f"Error {country_name} ({series_id}): {e}")

    if all_dfs:
        
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("No data was successfully loaded.")
        return pd.DataFrame()
    

def fetch_imf_bulk(dataset_code=None, metric_code=None, frequency=None, country_list=None, max_nb_series=5000):
    """
    Fetch macro data from the IMF using DBnomics' dimension-filtering method.
    Avoids errors from incorrectly concatenated series ID strings.
    """
    # Set up the dimension filters per the DBnomics API convention.
    # FREQ: frequency (M, Q, A)
    # INDICATOR: IMF macro indicator code
    if dataset_code=="WEO:2025-04" and metric_code=="LUR":
        dimensions = {
            "weo-subject": [metric_code],
            "unit": 'pcent'
        }
        # If the caller passed a specific list of country codes (e.g. ['VN', 'US', 'DE'])
        if country_list:
            dimensions["weo-country"] = country_list
    else:
        dimensions = {
            "FREQ": [frequency],
            "INDICATOR": [metric_code]
        }
        # If the caller passed a specific list of country codes (e.g. ['VN', 'US', 'DE'])
        if country_list:
            dimensions["REF_AREA"] = country_list

    try:
        # Call the bulk-download API with the filters above.
        df = fetch_series(provider_code="IMF", dataset_code=dataset_code, dimensions=dimensions, max_nb_series=max_nb_series)
        if df.empty:
            return pd.DataFrame()
        # Rename columns to match the dashboard's expected structure.
        # DBnomics returns 'REF_AREA' as the country code and 'Reference Area' as the country name.
        df = df.rename(columns={
            "REF_AREA": "country_code",
            "Reference Area": "country_name",
            "period": "Date"
        })
        # Keep only the core columns needed for the dashboard.
        core_columns = ["Date", "value", "country_code", "country_name"]
        df = df[[col for col in core_columns if col in df.columns]]
        return df
    except Exception as e:
        return pd.DataFrame()