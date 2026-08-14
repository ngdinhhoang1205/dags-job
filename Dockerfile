ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Add OS-level packages here if a DAG needs them, e.g.:
# USER root
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*
# USER airflow

COPY requirements.txt /requirements.txt

# Install against Airflow's own constraints file so pip won't touch a package
# (numpy, requests, etc.) that Airflow itself depends on.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /requirements.txt
