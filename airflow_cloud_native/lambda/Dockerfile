ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2-python3.12
FROM ${AIRFLOW_IMAGE_NAME}

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
