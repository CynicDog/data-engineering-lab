import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Bronze Ingestion Anti-Patterns

        This notebook live-demonstrates the status.txt problem you're experiencing,
        then shows what the Delta audit log replacement looks like side-by-side.

        **Pain point being addressed:**
        > ADF writes a status text file. Databricks parses it line by line,
        > finds parquet paths, processes them, deletes the file.
        > Daily schedule file overlaps with hourly schedule file → silent data loss.
        > If a file isn't created → no way to know what ran → grep all of storage.

        **Run locally:**
        ```bash
        uv run marimo edit notebooks/02_bronze_antipatterns.py
        ```
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import os
    import subprocess
    from pathlib import Path

    try:
        java_home = Path(
            subprocess.check_output(
                ["/usr/libexec/java_home", "-v", "17"],
                text=True, stderr=subprocess.DEVNULL
            ).strip()
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        java_bin = subprocess.check_output(["which", "java"], text=True).strip()
        java_home = Path(java_bin).resolve().parents[1]

    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    return (java_home,)


@app.cell
def _(java_home):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    spark = configure_spark_with_delta_pip(
        SparkSession.builder.appName("bronze-antipatterns")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return (spark,)


@app.cell
def _():
    import shutil
    from pathlib import Path

    BASE = Path("/tmp/lakehouse_lab/02")
    shutil.rmtree(BASE, ignore_errors=True)
    BASE.mkdir(parents=True)

    STATUS_DIR = BASE / "control"
    STATUS_DIR.mkdir()
    PARQUET_DIR = BASE / "landing"
    PARQUET_DIR.mkdir()
    AUDIT_PATH = str(BASE / "delta_audit")

    print(f"Lab dir: {BASE}")
    return BASE, AUDIT_PATH, PARQUET_DIR, STATUS_DIR


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Part 1 — The Anti-Pattern (status.txt)

        Simulate what ADF does: write a status file that contains metadata about
        what parquet files were just produced.

        This is `generate_source_data.py --demo-overlap` in miniature.
        """
    )
    return


@app.cell
def _(PARQUET_DIR, STATUS_DIR, spark):
    import pandas as pd
    from pathlib import Path

    def write_daily_landing(dt: str):
        """Simulate ADF writing daily parquets + a status file."""
        # Write parquet
        df = pd.DataFrame({"customer_id": [f"C{i}" for i in range(100)], "dt": dt})
        parquet_path = PARQUET_DIR / f"customer/dt={dt}/part.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_path, index=False)

        # Write status file (the anti-pattern)
        status_file = STATUS_DIR / "status.txt"
        status_file.write_text(f"customer|{dt}|landing/customer/dt={dt}/part.parquet\n")
        print(f"  [daily]  Wrote {len(df)} rows + status.txt for {dt}")
        return len(df)

    def write_hourly_landing(dt: str, hour: str):
        """Simulate ADF writing hourly parquets + OVERWRITING the same status file."""
        df = pd.DataFrame({"customer_id": [f"CH{i}" for i in range(5)], "dt": dt, "hour": hour})
        parquet_path = PARQUET_DIR / f"customer/dt={dt}_h{hour}/part.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_path, index=False)

        # THE BUG: writes to the same status.txt as the daily job
        status_file = STATUS_DIR / "status.txt"
        status_file.write_text(
            f"customer|{dt}|landing/customer/dt={dt}_h{hour}/part.parquet\n"
        )
        print(f"  [hourly] Wrote {len(df)} rows + OVERWROTE status.txt for {dt} hour {hour}")
        return len(df)

    return (write_daily_landing, write_hourly_landing)


@app.cell
def _(STATUS_DIR, write_daily_landing, write_hourly_landing):
    # Simulate a day in the life:
    print("=== 09:00 Daily ADF job runs ===")
    write_daily_landing("2024-03-15")

    print("\n=== 10:00 Hourly ADF job runs ===")
    write_hourly_landing("2024-03-15", "10")

    print(f"\n=== Now read status.txt — what does Databricks see? ===")
    status_file = STATUS_DIR / "status.txt"
    print(status_file.read_text())
    print("The daily data (100 customers) is GONE from tracking.")
    print("Databricks will process only 5 hourly rows for 2024-03-15.")
    print("The 100 daily rows sit in storage, unprocessed, with no record they existed.")
    return (status_file,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Part 2 — The Cascade of Problems

        Now let's simulate what happens in Databricks when it reads this broken status file.
        """
    )
    return


@app.cell
def _(PARQUET_DIR, STATUS_DIR, spark):
    from pathlib import Path

    def databricks_bronze_job_antipattern(status_file_path: Path):
        """This is what your current Databricks notebook does.
        It reads the status file, processes parquets, then deletes the file.
        """
        if not status_file_path.exists():
            print("ERROR: Status file not found! Was it created? Was it already deleted?")
            print("There's no way to know without grep-ing all of storage.")
            return 0

        content = status_file_path.read_text().strip()
        if not content:
            print("Status file is empty — probably a race condition with another job.")
            return 0

        total_rows = 0
        for line in content.splitlines():
            table, dt, path = line.split("|")
            full_path = str(PARQUET_DIR / path.replace("landing/", ""))
            try:
                df = spark.read.parquet(full_path)
                df.write.format("delta").mode("append").save(
                    str(PARQUET_DIR.parent / f"bronze/{table}")
                )
                total_rows += df.count()
                print(f"  Processed {table}/{dt}: {df.count()} rows from {path}")
            except Exception as e:
                print(f"  ERROR on {table}/{dt}: {e}")

        # Deletes the status file — no history kept
        status_file_path.unlink()
        print(f"\n  Status file deleted. Total: {total_rows} rows written.")
        return total_rows

    rows_processed = databricks_bronze_job_antipattern(STATUS_DIR / "status.txt")
    print(f"\nResult: {rows_processed} rows processed.")
    print("Missing: 100 daily customer rows — completely lost, no error, no trace.")
    return (databricks_bronze_job_antipattern, rows_processed)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Part 3 — The Fix: Delta Audit Table

        Replace the status.txt file with a Delta table at
        `control/ingestion_log`. Every ingestion event writes a row.
        The log is immutable — nothing is ever deleted.

        Questions that were impossible before are now a SQL query:
        - "Did the daily job run today?" → `WHERE schedule_type='daily' AND dt='2024-03-15'`
        - "What ran in the last 24 hours?" → `WHERE ingested_at > current_timestamp - interval 1 day`
        - "How many rows did we get from claims last Tuesday?" → trivial
        - "Did anything fail this week?" → `WHERE status = 'error'`
        """
    )
    return


@app.cell
def _(AUDIT_PATH, PARQUET_DIR, STATUS_DIR, spark, write_daily_landing, write_hourly_landing):
    import uuid
    from datetime import datetime, timezone
    from lakehouse.bronze.audit import write_audit, get_successful_tables

    def bronze_job_with_audit(table: str, dt: str, path: str, schedule_type: str):
        """This is what your bronze job should do."""
        run_id = str(uuid.uuid4())
        full_path = str(PARQUET_DIR / path.replace("landing/", ""))

        try:
            df = spark.read.parquet(full_path)
            # (In real pipeline: write to Delta bronze table here)
            row_count = df.count()

            write_audit(spark, AUDIT_PATH, {
                "run_id": run_id,
                "source_table": table,
                "landing_path": path,
                "schedule_type": schedule_type,
                "dt": dt,
                "ingested_at": datetime.now(timezone.utc),
                "row_count": row_count,
                "status": "success",
                "error_msg": None,
            })
            print(f"  [{schedule_type}] {table}/{dt}: {row_count} rows — logged.")

        except Exception as e:
            write_audit(spark, AUDIT_PATH, {
                "run_id": run_id,
                "source_table": table,
                "landing_path": path,
                "schedule_type": schedule_type,
                "dt": dt,
                "ingested_at": datetime.now(timezone.utc),
                "row_count": 0,
                "status": "error",
                "error_msg": str(e),
            })
            print(f"  [{schedule_type}] {table}/{dt}: ERROR — {e}")

    print("=== 09:00 Daily job writes data ===")
    write_daily_landing("2024-03-16")
    bronze_job_with_audit("customer", "2024-03-16",
                          "landing/customer/dt=2024-03-16/part.parquet", "daily")

    print("\n=== 10:00 Hourly job writes data ===")
    write_hourly_landing("2024-03-16", "10")
    bronze_job_with_audit("customer", "2024-03-16",
                          "landing/customer/dt=2024-03-16_h10/part.parquet", "hourly")

    print("\n=== Both runs recorded. Status.txt is gone. Audit log lives forever. ===")
    return (bronze_job_with_audit,)


@app.cell
def _(AUDIT_PATH, spark):
    from pyspark.sql import functions as F

    print("Full audit log:")
    spark.read.format("delta").load(AUDIT_PATH).orderBy(F.col("ingested_at")).show(
        truncate=False
    )
    return


@app.cell
def _(AUDIT_PATH, spark):
    from lakehouse.bronze.audit import get_successful_tables

    # "Did the daily job run on 2024-03-16?"
    daily_done = get_successful_tables(spark, AUDIT_PATH, "daily", "2024-03-16")
    hourly_done = get_successful_tables(spark, AUDIT_PATH, "hourly", "2024-03-16")

    print(f"Daily tables completed for 2024-03-16: {daily_done}")
    print(f"Hourly tables completed for 2024-03-16: {hourly_done}")
    print("\nNo file-system crawl. No deleted files. Both schedules tracked independently.")
    return (daily_done, hourly_done)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Summary

        | Dimension | status.txt | Delta audit log |
        |-----------|-----------|----------------|
        | Overlap handling | Silently overwrites | Both runs recorded independently |
        | Error visibility | Silent (file deleted) | `status='error'` row with message |
        | History | Deleted on processing | Immutable, queryable forever |
        | Recovery | Grep all of storage | `SELECT * WHERE status='error'` |
        | Debugging | "Was the file created?" | `SELECT * WHERE dt='...'` |
        | Time to diagnose | 30+ minutes + compute startup | Seconds via Delta query |

        **Next**: `03_silver_design.py` — what the Silver layer should actually do.
        """
    )
    return


if __name__ == "__main__":
    app.run()
