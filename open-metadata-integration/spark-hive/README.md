# MTA New York City Subway Data Processed in Spark

### 1. Start Environment

Spin up the container using Docker Compose:

```bash
docker-compose up -d
```

### 2. Run Pipeline

trigger the execution:

```bash
docker exec -it mta_spark bash 
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.openmetadata.transport.jwtToken=(YOUR_TOKEN)"
/opt/spark/bin/spark-submit /app/mta_pipeline.py
```