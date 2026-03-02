# Spark–Hive → OpenMetadata (Quick Setup)

## Start Containers 
```bash
docker compose up 
```

## Register Hive database service 
<details><summary>Add Hive service</summary>

  <img width="740" height="1829" alt="Screenshot 2026-03-02 at 7 53 01 PM" src="https://github.com/user-attachments/assets/7f467499-95ad-4012-b8be-4e125e874666" />
  
</details>


```bash 
docker exec -it hive_server beeline -u jdbc:hive2://localhost:10000 -n hive -e "
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

