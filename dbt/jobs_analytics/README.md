Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Running via Docker

This project is packaged as a FastAPI service (`../server.py`) that runs `dbt run` on demand. It runs as the `dbt-api` service in the jobs-run `docker-compose.yaml` and reads `DBT_API_KEY` / `BUSINESS_POSTGRES_*` from the jobs-run `.env` — see "dbt Transformations" in the top-level README.

1. Build and start it (from the jobs-run root):
   ```
   docker compose up -d --build dbt-api
   ```

2. Check it's healthy:
   ```
   curl http://localhost:8001/health
   ```

3. Trigger a `dbt run`:
   ```
   curl -X POST http://localhost:8001/run -H "X-API-Key: <your DBT_API_KEY>"
   ```

The API port is `DBT_API_PORT` from the jobs-run `.env` (default `8001`, published on 127.0.0.1 only), mapped to container port `8000`.
