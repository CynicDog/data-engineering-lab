FROM apache/airflow:3.0.2

USER root

# Install OpenJDK 17 — required for PySpark.
# On ARM64 (Apple Silicon), the JVM path ends in -arm64 instead of -amd64;
# we create a stable symlink so JAVA_HOME is architecture-independent.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s "$(readlink -f "$(which java)" | sed 's|/bin/java||')" /opt/java-home

ENV JAVA_HOME=/opt/java-home
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy the lakehouse package and config so they're available inside containers.
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root config/ /opt/airflow/config/

RUN pip install --no-cache-dir \
    "pyspark>=3.5.3,<4.0" \
    "delta-spark==3.3.0" \
    "boto3>=1.35" \
    "s3fs>=2024.12.0" \
    "pyarrow>=18.1.0" \
    "pyyaml>=6.0" \
    "psycopg2-binary>=2.9" \
    "faker>=30.0" \
    "pandas>=2.2" \
    "pytest>=8" \
    "apache-airflow-providers-amazon>=9.0"

# Pre-warm the Delta JAR cache so the first task doesn't download from Maven.
# In airgapped environments (망분리), replace this with a local Nexus/Artifactory mirror.
RUN python3 -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
spark = configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate(); \
spark.stop()" 2>&1 | tail -3 || true
