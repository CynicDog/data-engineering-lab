# Spark–MySQL → OpenMetadata (Quick Setup)

## Start Containers 
```bash
docker compose up 
```

## Grant MySQL Log Access

```bash
docker exec -it openmetadata_mysql mysql -uroot -ppassword -e "
GRANT SELECT ON mysql.general_log TO 'openmetadata_user'@'%';
GRANT SELECT ON mysql.slow_log TO 'openmetadata_user'@'%';
FLUSH PRIVILEGES;
"
```

## Create Sample Table

```bash
docker exec -it openmetadata_mysql mysql -uopenmetadata_user -popenmetadata_password -D openmetadata_db -e "
CREATE TABLE IF NOT EXISTS employee (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    role VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO employee (id, name, role) VALUES 
(1, 'Eunsang', 'Data Engineer'),
(2, 'Alice', 'Data Scientist'),
(3, 'Bob', 'Platform Engineer')
ON DUPLICATE KEY UPDATE name=name;
"
```

## Run Spark Job

Enter container:

```bash
docker exec -it spark_worker bash
```

Set JWT (Settings → Bots → Ingestion Bot) in the `spark_worker` container:

```bash
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.openmetadata.transport.jwtToken=(YOUR_JWT_TOKEN)"
```

Submit job:

```bash
/opt/spark/bin/spark-submit /app/employee.py
```
