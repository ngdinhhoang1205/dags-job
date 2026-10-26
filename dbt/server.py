import os
import subprocess

from fastapi import FastAPI, Header, HTTPException

app = FastAPI()

DBT_PROJECT_DIR = "/app/jobs_analytics"
DBT_PROFILES_DIR = "/root/.dbt"
API_KEY = os.environ.get("DBT_API_KEY", "")


def _check_key(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="missing or invalid X-API-Key")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run(x_api_key: str | None = Header(default=None)):
    _check_key(x_api_key)
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail={"stdout": result.stdout, "stderr": result.stderr})
    return {"stdout": result.stdout, "stderr": result.stderr}
