# Proto → Spark-to-Hive → Trino → OpenMetadata (Quick Setup)

## Services

| Component                             | Description                                                                                                                                                                                             |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Spark Worker** (`4040`)             | The execution engine. It hosts the ingestion script, fetches Protobufs from the MTA API, parses them into DataFrames, and writes ORC files to the warehouse.                                            |
| **Metastore DB** (`5432`)             | Stores table metadata (schemas, partitions, locations). No actual data files are stored here.                                                                                                           |
| **Hive Metastore Service** (`9083`)   | Thrift metadata service used by Spark and Trino to read table definitions and storage locations.                                                                                                        |
| **Trino** (`8081` → container `8080`) | Distributed SQL query engine. It connects to the Hive Metastore, reads ORC files directly from the warehouse, and serves ANSI SQL over HTTP. Used by OpenMetadata for metadata ingestion and profiling. |


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

### 2. Submit the Spark Job 
```bash
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.openmetadata.transport.jwtToken=(YOUR_JWT_TOKEN)"
/opt/spark/bin/spark-submit \
  --master "local[*]" \
  --jars /opt/spark/conf/extra-jars/openmetadata-spark-agent-1.1.jar \
  /opt/spark/work-dir/ingestion/src/main.py
```

## Register Trino database service 
<details><summary>Add Trino service</summary>

<img width="1391" height="494" alt="Screenshot 2026-03-04 at 10 58 18 PM" src="https://github.com/user-attachments/assets/31ed1b8a-aa3f-440a-82c4-910d1828d91f" />
  
</details>

## Add a Profiler Agent 

<details><summary>Add Profiler Agent</summary>

<img width="692" height="1968" alt="Screenshot 2026-03-04 at 10 58 41 PM" src="https://github.com/user-attachments/assets/bdd0cc7e-ca0a-431a-805f-d1a64b6bb089" />

</details>


<img width="2151" height="1464" alt="image" src="https://github.com/user-attachments/assets/dd8dc630-e170-4d7b-a229-4128fc9a40b0" />
