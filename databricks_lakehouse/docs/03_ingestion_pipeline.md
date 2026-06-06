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
