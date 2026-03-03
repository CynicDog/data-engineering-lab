# Spark–Hive → OpenMetadata (Quick Setup)

## Services

| Component                         | Description |
|-----------------------------------|-------------|
| **Metastore DB** (`5432`)     | Stores table metadata (schemas, partitions, locations). No actual data files. |
| **Hive Metastore Service** (`9083`) | Metadata API layer used by Spark, HiveServer2, and OpenMetadata. |
| **HiveServer2** (`10000`, `10002`)   | SQL endpoint (JDBC/ODBC) for running Hive queries via Beeline or BI tools. |

## Start Containers 
```bash
docker compose up 
```

## Verify metastore database
```bash
docker exec -it metastore_db psql -U hive -d metastore_db -c "\dt"
```

## Verify Hive service 
```bash
docker exec -it hive_server beeline -u jdbc:hive2://localhost:10000 -n hive -e "SHOW SCHEMAS;"
```


## Register Hive database service 
<details><summary>Add Hive service</summary>

  <img width="740" height="1829" alt="Screenshot 2026-03-02 at 7 53 01 PM" src="https://github.com/user-attachments/assets/7f467499-95ad-4012-b8be-4e125e874666" />
  
</details>


## Create a seed table 
```bash 
docker exec -it hive_server beeline -u jdbc:hive2://localhost:10000/ -n hive -e "
CREATE TABLE IF NOT EXISTS employee (
    id INT,
    name STRING,
    role STRING,
    updated_at TIMESTAMP
)
STORED AS ORC;

INSERT INTO TABLE employee VALUES 
(1, 'Eunsang', 'Data Engineer', CURRENT_TIMESTAMP()),
(2, 'Alice', 'Data Scientist', CURRENT_TIMESTAMP()),
(3, 'Bob', 'Platform Engineer', CURRENT_TIMESTAMP());
"
```

## Audit the seed table's metadata 
```bash 
docker exec -it metastore_db psql -U hive -d metastore_db -c "
SELECT \"TBL_ID\", \"TBL_NAME\", \"OWNER\", \"CREATE_TIME\"
FROM \"TBLS\"
WHERE \"TBL_NAME\" = 'employee';
"
```


