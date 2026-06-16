# Spark image — runs the medallion jobs as SparkApplication driver/executor pods.
# Also reused by the seed Job (it bundles generate_source_data.py + boto3/faker).
#
# Build context is the PROJECT ROOT (see scripts/build-images.sh):
#   docker build -f images/spark/Dockerfile -t lakehouse/spark:dev .
#
# Version alignment (see docs/03_version_alignment.md):
#   Spark 3.5.3  <->  pyspark 3.5.3 (source project)
#   delta-spark 3.3.0 + delta-storage 3.3.0   (Scala 2.12 build)
#   hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262  (= Spark 3.5's bundled Hadoop)
ARG SPARK_BASE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_BASE}

USER root

# curl for jar download; pip to add the python deps.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Python deps for the jobs (delta API) + the seed generator (boto3/faker/pandas).
# Versions mirror databricks_lakehouse exactly.
RUN python3 -m pip install --no-cache-dir \
        "delta-spark==3.3.0" \
        "boto3>=1.35" \
        "pyarrow>=18.1.0" \
        "pyyaml>=6.0" \
        "pandas>=2.2" \
        "faker>=30.0"

# Bake the JVM jars into the Spark classpath so cluster jobs never reach Maven
# at runtime (no configure_spark_with_delta_pip / Ivy resolve in the pod).
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK=1.12.262
ARG DELTA=3.3.0
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd "${SPARK_HOME}/jars" && \
    curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar && \
    curl -fsSLO ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar && \
    curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.12/${DELTA}/delta-spark_2.12-${DELTA}.jar && \
    curl -fsSLO ${MAVEN}/io/delta/delta-storage/${DELTA}/delta-storage-${DELTA}.jar

# App code. PYTHONPATH covers the package (/opt/lakehouse/src) and the job
# entrypoints (/opt/lakehouse/jobs, so `import _bootstrap` works). The jobs also
# insert these paths defensively in case Spark rewrites PYTHONPATH.
ENV PYTHONPATH=/opt/lakehouse/src:/opt/lakehouse/jobs
COPY lakehouse/src /opt/lakehouse/src
COPY lakehouse/config /opt/lakehouse/config
COPY jobs /opt/lakehouse/jobs
COPY lakehouse/generate_source_data.py /opt/lakehouse/generate_source_data.py

# Back to the unprivileged user the Spark Operator expects (uid 185).
USER 185
