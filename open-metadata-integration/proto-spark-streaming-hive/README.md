# Proto → Spark-to-Hive → OpenMetadata (Quick Setup)

## Services

| Component                           | Description                                                                                                                                                              |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Spark Worker** (`4040`)           | The execution engine. It hosts the ingestion script, fetches Protobufs from the MTA API, parses them into DataFrames, and writes the actual ORC files to the warehouse.  |
| **Metastore DB** (`5432`)           | Stores table metadata (schemas, partitions, locations). No actual data files.                                                                                            |
| **Hive Metastore Service** (`9083`) | Metadata API layer used by Spark, HiveServer2, and OpenMetadata.                                                                                                         |
| **HiveServer2** (`10000`, `10002`)  | SQL endpoint (JDBC/ODBC) for running Hive queries via Beeline or BI tools.                                                                                               |

## Start Containers 
```bash
docker compose up 
```

## Launch Infrastructure  

### 1. Enter the worker
```bash
docker exec -it spark_worker bash
```

### 2. Install requirements
```bash
pip install --upgrade pip
pip install --upgrade setuptools wheel
pip install protobuf grpcio-tools
```

### 3. Navigate to ingestion folder
```bash
cd /opt/spark/work-dir/ingestion
```

### 4. Compile Protos (Optional)
> Note: Skip this step since `gtfs_realtime_pb2.py` and `gtfs_realtime_NYCT_pb2.py` already exist in the directory 

```bash
python3 -m grpc_tools.protoc \
    --proto_path=protos \
    --python_out=src/proto \
    protos/gtfs-realtime-NYCT.proto \
    protos/com/google/transit/realtime/gtfs-realtime.proto
``` 

## Run the Job

### 1. Add both 'src' AND 'src/proto' to the path
```
export PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir/ingestion/src:/opt/spark/work-dir/ingestion/src/proto
```

### 2. Run the submit again
```bash
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.openmetadata.transport.jwtToken=(YOUR_JWT_TOKEN)"
/opt/spark/bin/spark-submit \
  --master "local[*]" \
  --jars /opt/spark/conf/extra-jars/openmetadata-spark-agent-1.1.jar \
  /opt/spark/work-dir/ingestion/src/main.py
```

## Register Hive database service 
<details><summary>Add Hive service</summary>

  <img width="740" height="1829" alt="Screenshot 2026-03-02 at 7 53 01 PM" src="https://github.com/user-attachments/assets/7f467499-95ad-4012-b8be-4e125e874666" />
  
</details>

## Add Lineage Agent and Trigger the Run 

<details><summary>Add Lineage Agent</summary>

  <img width="1660" height="1265" alt="image" src="https://github.com/user-attachments/assets/12086cd9-88b7-4dbc-b3b3-bad18acd7394" />
  
</details>

## Verify Hive service and fed data 
```bash
docker exec -it hive_server beeline -u jdbc:hive2://localhost:10000 -n hive -e "SHOW SCHEMAS;"
docker exec -it hive_server beeline -u jdbc:hive2://localhost:10000 -n hive -p hive_password -e "SELECT * FROM default.mta_subway_status LIMIT 10;"
```

<img width="1500" height="1666" alt="Screenshot 2026-03-03 at 10 44 14 PM" src="https://github.com/user-attachments/assets/6d97ae2f-84a0-456f-bc63-6f22cb2a8bb4" />


