FROM apache/spark:3.5.0-python3

USER root

# Install essential tools
RUN apt-get update && \
    apt-get install -y tree curl && \
    pip install --no-cache-dir pyspark==3.5.0

# Setup workspace
WORKDIR /app
RUN mkdir -p /app/data/raw /app/data/checkpoints /app/data/warehouse

# Ensure permissions
RUN chmod -R 777 /app/data

USER spark