# jobs-run

Airflow (Python 3.11, LocalExecutor) running in Docker, with a separate Postgres database for
business/application data and pgAdmin for browsing both databases.

## Prerequisites

- Docker Desktop

## Setup

1. Copy the environment template and fill in real values:

   ```bash
   cp .env.example .env        # bash / Git Bash
   Copy-Item .env.example .env # PowerShell
   ```

   `.env` is gitignored — it holds local secrets and is never committed. See
   [Environment variables](#environment-variables) below for what each value means and where to
   get it.

2. Build and start everything:

   ```bash
   docker compose up -d --build
   ```

3. Access points:

   | Service | URL | Login |
   |---|---|---|
   | Airflow UI | http://localhost:8080 | `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `.env` (default `airflow` / `airflow`) |
   | pgAdmin | http://localhost:5050 | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` from `.env` (default `admin@example.com` / `admin`). Both Postgres servers are pre-registered — DB password on first connect is `airflow`/`airflow` or `business`/`business` by default. |
   | Business Postgres | `localhost:5433` (host) / `postgres-business:5432` (from other containers) | `BUSINESS_POSTGRES_*` from `.env` |

## Environment variables

| Variable | Purpose | Where to get it |
|---|---|---|
| `AIRFLOW_UID` | Host UID Airflow's containers run as. Leave at `50000` unless you know you need otherwise. | — |
| `AIRFLOW_FERNET_KEY` | Encrypts Connection passwords in Airflow's metadata DB. Blank is fine for local dev. | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Signs Airflow's session cookies. | Any random string; change before non-local use. |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Airflow UI admin login, created on first `docker compose up`. | Pick your own. |
| `BUSINESS_POSTGRES_USER` / `_PASSWORD` / `_DB` / `_PORT` | Credentials and host port for the business Postgres instance. | Pick your own. |
| `PGADMIN_DEFAULT_EMAIL` / `_PASSWORD` / `_PORT` | pgAdmin's own login and host port. | Pick your own. |
| `IMF_API_KEY` | Required by `dags/utils.py`'s `fetch_imf_api()` / `fetch_ipi()` (IMF's SDMX 3.0 API at `api.imf.org`) — DAGs that call those (e.g. `ipi_data`) fail without it. Not needed for DAGs that only use DBnomics (`fetch_series`), e.g. `fetch_exchange_rate`, `gdp_data`. | Register for a free key at https://portal.api.imf.org/ |

## DAGs

| DAG id | File | What it does |
|---|---|---|
| `country_mapping` | `dags/country_mapping.py` | Builds ISO2/ISO3/continent reference data, writes `country_mapping`. Run this before `ipi_data`. |
| `fetch_exchange_rate` | `dags/fetch_exchange_rate.py` | Pulls monthly USD exchange rates (IMF/IFS via DBnomics) into `exchange_rate` in the business DB. |
| `gdp_data` | `dags/gdp.py` | Pulls annual GDP (IMF/IFS via DBnomics), converts to USD using `exchange_rate`, writes `gdp_data`. |
| `ipi_data` | `dags/ipi.py` | Industrial Production Index: prefers IMF's direct API (needs `IMF_API_KEY`) per country, falls back to DBnomics for countries it doesn't cover. Reads `country_mapping` to attach ISO2 codes/names, so `country_mapping` must have run first. Writes `ipi_data`. |

Shared fetch/DB helpers live in `dags/utils.py` (`get_business_db_engine`, `replace_table`,
`log_job_run`, plus the IMF/DBnomics fetch functions). Country reference lists live in
`dags/config.py`.

## Useful commands

Test a single task without going through the scheduler (fast feedback loop, prints output to
your terminal):

```bash
docker compose exec -T airflow-scheduler airflow tasks test <dag_id> <task_id> <YYYY-MM-DD>
```

Check for DAG parse errors:

```bash
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Query the business database directly:

```bash
docker compose exec -T postgres-business psql -U business -d business
```

Stop everything:

```bash
docker compose down
```

## Dependencies

- `requirements.txt` — installed into the Airflow image, used by DAG code at runtime. Kept
  unpinned for direct dependencies only; installed against Airflow's own constraints file so
  transitive versions stay compatible with Airflow itself (see `Dockerfile`).
- `requirements-dev.txt` — local-only tooling (Jupyter, pytest, etc.), not installed in the image.
