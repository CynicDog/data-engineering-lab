# Ingestion Pipeline — ADF → Bronze

## Current Architecture (Anti-Pattern)

```
On-premise ODS
    │
    ▼ (SHIR / ADF)
ADLS landing zone
    ├── landing/customer/dt=2024-03-15/part.parquet
    ├── landing/customer/dt=2024-03-15_h10/part.parquet  ← hourly conflict!
    └── control/status.txt   ← single file, overwritten by each schedule
                              "customer|2024-03-15_h10|landing/customer/dt=2024-03-15_h10/part.parquet"
    │
    ▼ (Databricks Job)
Notebook reads status.txt
    ├── Parse each line
    ├── Read parquet from path in line
    ├── Write to Delta bronze table
    └── DELETE status.txt   ← all history gone
```

### Why this breaks

**The overlap problem:**
ADF has two schedules — daily (full extract) and hourly (incremental). Both
write to the same `control/status.txt` key. ADLS blob storage has no atomic
compare-and-swap — the last writer wins. The hourly job runs at 10:00,
overwrites the daily status file, and the Databricks job reads only the
hourly parquet. The daily data (potentially millions of rows) sits in ADLS
unprocessed, with no record it was ever there.

**The file-as-IPC problem:**
Files on blob storage are a terrible IPC mechanism:
- No message queue semantics (no "at least once" delivery guarantee)
- No history (deletion destroys the record)
- No schema (any format, any encoding)
- No query interface (you must download and parse to inspect)
- No concurrency control (race conditions between daily and hourly)

**The debugging problem:**
When the pipeline fails at 03:00 and you arrive at 09:00:
1. Start a Job Cluster → wait 7 minutes
2. Navigate to the status file path → file doesn't exist (was deleted or never created)
3. Open storage explorer → manually browse to find which parquets exist
4. Re-run manually → hope the status file gets created this time
5. Repeat


## Better Architecture (Delta Audit Table)

```
On-premise ODS
    │
    ▼ (SHIR / ADF)
ADLS landing zone
    ├── landing/customer/dt=2024-03-15/part.parquet     (daily)
    └── landing/customer/dt=2024-03-15_h10/part.parquet (hourly)
    │
    ▼ (Airflow bronze_dag.py / Databricks Job)
For each table:
    ├── Read parquet
    ├── Write to Delta bronze table (replaceWhere partition)
    └── APPEND to control/ingestion_log Delta table:
            {run_id, source_table, landing_path, schedule_type,
             dt, ingested_at, row_count, status, error_msg}
```

### Why this works

**Separate schedule types** are tracked as separate rows with `schedule_type='daily'`
and `schedule_type='hourly'`. They can never overwrite each other.

**Full history** is preserved. Every ingestion event — success or failure — has
a permanent record. Querying "what ran last Tuesday?" is a SQL filter, not a
storage crawl.

**Recovery is a query:**
```sql
-- Find all failed ingestions in the last 7 days
SELECT source_table, dt, error_msg, ingested_at
FROM delta.`s3://lakehouse/control/ingestion_log`
WHERE status = 'error'
  AND ingested_at > current_timestamp - interval 7 days
ORDER BY ingested_at DESC;
```

**Silver trigger** is based on the audit log, not a file:
```python
# Old: does this file exist?
check_file = "s3://lakehouse/control/bronze_done_customer.flag"

# New: query the audit log
completed = get_successful_tables(spark, audit_path, schedule_type="daily", dt="2024-03-15")
if "customer" in completed:
    trigger_silver()
```


## Implementing the Fix in Databricks

The `bronze/audit.py` module in this lab is the reference implementation.
To use it in Databricks, copy `src/lakehouse/bronze/audit.py` into a
Databricks Repo or Python wheel and import it in your notebooks/jobs.

```python
# In your existing Bronze Databricks notebook:
from lakehouse.bronze.audit import write_audit, get_successful_tables

# Replace the status.txt delete at the end of each table processing:
write_audit(spark, audit_path, {
    "run_id": dbutils.widgets.get("run_id"),
    "source_table": table_name,
    "landing_path": parquet_path,
    "schedule_type": schedule_type,
    "dt": processing_date,
    "ingested_at": datetime.now(timezone.utc),
    "row_count": df.count(),
    "status": "success",
    "error_msg": None,
})
```


## What to Do About the Existing Status.txt Files

1. Keep the old status.txt flow running in parallel for one sprint
2. Add `write_audit()` calls alongside it (dual-write)
3. Verify audit log completeness matches expected ingestion volume
4. Cut over Silver to read from audit log instead of checking files
5. Remove status.txt writes
6. Document the change in the runbook

Do not delete the existing status files until Silver has been verified against
the audit log for at least one full week of production data.


## Production Proposal

### The Real Problem at Your Company

Your ADF pipeline uses SHIR to pull from on-premise ODS channel databases
(MLCRP, MLVOC, MLIWT, MLSQP) and drops parquet files into ADLS. After each
successful copy activity, ADF writes a line to a status.txt file in a control
folder. Databricks reads this file, parses it, processes the referenced parquets,
and deletes the file.

You run daily (full extract) and hourly (incremental) schedules. Both write to
the same control folder with the same file extension. Last writer wins. You lose
track of ingestions. You don't know what ran, what failed, or what was skipped.
Debugging requires starting a compute cluster — 6-7 minutes — just to `ls` a path.

### Solution: Replace status.txt with a Unity Catalog Delta Table

Create a single `ingestion_log` Delta table in Unity Catalog under a shared
`control` schema. ADF writes one row per successful parquet drop. Databricks
reads with SQL — no glob, no file collisions, no deletion.

**Step 1 — Create the table (deploy once via ESM):**
```sql
CREATE TABLE IF NOT EXISTS VOCD.control.ingestion_log (
    run_id        STRING,
    source_table  STRING,
    catalog_name  STRING,
    schedule_type STRING,  -- 'DAILY' or 'HOURLY'
    adls_path     STRING,
    row_count     BIGINT,
    status        STRING,  -- 'PENDING', 'IN_PROGRESS', 'DONE', 'FAILED'
    error_msg     STRING,
    ingested_at   TIMESTAMP,
    processed_at  TIMESTAMP
) USING DELTA
PARTITIONED BY (DATE(ingested_at));
```

**Step 2 — ADF writes the log row:**
In the ADF pipeline, after each successful Copy Activity, add a
**Databricks Notebook Activity** (or Script Activity via SQL connector) that runs:
```sql
INSERT INTO VOCD.control.ingestion_log
VALUES (
    '${pipeline().RunId}',
    '${pipeline().parameters.source_table}',
    '${pipeline().parameters.catalog_name}',
    '${pipeline().parameters.schedule_type}',
    '${pipeline().parameters.adls_path}',
    ${pipeline().parameters.row_count},
    'PENDING',
    NULL,
    current_timestamp(),
    NULL
);
```

ADF already has a Databricks linked service for compute — reuse that connection.
No new infrastructure.

**Step 3 — Databricks Bronze reads from the log table:**
```python
pending = (spark.read.table("VOCD.control.ingestion_log")
           .filter("status = 'PENDING'")
           .filter(f"schedule_type = '{schedule_type}'")
           .filter(f"catalog_name = '{catalog_name}'"))

for row in pending.collect():
    spark.sql(f"""
        UPDATE VOCD.control.ingestion_log
        SET status = 'IN_PROGRESS'
        WHERE run_id = '{row.run_id}'
    """)
    try:
        df = spark.read.parquet(row.adls_path)
        # ... write to bronze delta table ...
        spark.sql(f"""
            UPDATE VOCD.control.ingestion_log
            SET status = 'DONE', processed_at = current_timestamp()
            WHERE run_id = '{row.run_id}'
        """)
    except Exception as e:
        spark.sql(f"""
            UPDATE VOCD.control.ingestion_log
            SET status = 'FAILED', error_msg = '{str(e)}'
            WHERE run_id = '{row.run_id}'
        """)
```

Daily and hourly schedules write separate rows with `schedule_type = 'DAILY'`
and `schedule_type = 'HOURLY'`. They are permanently distinct — no collision possible.

### Recovery Without Compute

When something breaks, recovery is a SQL UPDATE. No cluster needed to inspect state:
```sql
-- Check what failed today
SELECT source_table, schedule_type, error_msg, ingested_at
FROM VOCD.control.ingestion_log
WHERE status = 'FAILED'
  AND DATE(ingested_at) = current_date();

-- Re-queue a failed table for reprocessing
UPDATE VOCD.control.ingestion_log
SET status = 'PENDING', error_msg = NULL
WHERE run_id = 'some-adf-run-id';
```

You can run this query from Databricks SQL (no Job Cluster, no wait) or from
Azure Data Studio against the UC SQL endpoint.

### Migration Path from status.txt

1. Deploy the `ingestion_log` Delta table via ESM (one-time schema change)
2. Add the ADF Insert activity alongside the existing status.txt write (dual-write)
3. Modify Databricks Bronze to read from the log table instead of status.txt
   — keep the status.txt fallback for one sprint
4. Verify: compare log table row counts against expected daily volumes for 1 week
5. Remove ADF's status.txt write activity
6. Remove Databricks' status.txt parsing code

Each step is independently deployable through the normal ESM cycle. No big-bang migration.
