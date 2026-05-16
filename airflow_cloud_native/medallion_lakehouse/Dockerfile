ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2-python3.12
FROM ${AIRFLOW_IMAGE_NAME}

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Bring in `uv` as a single static binary from the official image.
COPY --from=ghcr.io/astral-sh/uv:0.5.10 /uv /usr/local/bin/uv

# Install runtime deps (the `image` optional-extra adds Airflow providers
# plus dbt-duckdb / dbt-core / duckdb — the dbt CLI lives in this image).
# Target the airflow user's Python explicitly — /home/airflow/.local is a
# venv-style isolated env, so `uv pip install --system` would write to a
# different site-packages that the runtime never imports from.
COPY pyproject.toml /tmp/build/pyproject.toml
RUN uv pip install --python /home/airflow/.local/bin/python --no-cache --extra image -r /tmp/build/pyproject.toml

USER airflow
