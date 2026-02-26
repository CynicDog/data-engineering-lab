```bash
docker exec -it openmetadata_mysql mysql -uroot -ppassword -e "
GRANT SELECT ON mysql.general_log TO 'openmetadata_user'@'%';
GRANT SELECT ON mysql.slow_log TO 'openmetadata_user'@'%';
FLUSH PRIVILEGES;
"
```

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

```bash
docker exec -it spark_worker bash 
export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dspark.openmetadata.transport.jwtToken=eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJ1c2VybmFtZSI6ImluZ2VzdGlvbi1ib3QiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJpbmdlc3Rpb24tYm90IiwiaWF0IjoxNzcyMDc2NDMyLCJleHAiOm51bGx9.Des3yG9pNkb8zkUtcJWeg96LMV_clk6ESmLLjcQ-uUJishPmlrEu5_dE7SxJVzs6rgQPqyNS5qcuzq9eGxNJkUfPRwmwFtO_y4sO1Y0a7mFYddTUn5Z4eCEiJdMm-kqjdFHXqfr0HGpxpYb1pu5HsZUVxUCRFY1GOZu63XHBTgvfp2xGaONGZ__rKSC870zGZ05VUYcm892STPMgZuPL_SgYkCuHwsZFaXCavaJty--Ut3vqE92LhAL4yrOlMzBletAm-TraDXGyVpvzHJaXaCGVboJF76FrZIsaqBGCrYYkXuibnisiY0HWPvosm-D5xLKtKT_48CuOa1joYacLzQ"
/opt/spark/bin/spark-submit /app/employee.py
```

