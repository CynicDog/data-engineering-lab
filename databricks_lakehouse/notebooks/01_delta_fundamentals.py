import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Delta Lake Fundamentals

        This notebook builds an intuition for what Delta Lake actually *is* —
        the thing underneath every Databricks table that makes it reliable.

        **Topics covered:**
        1. The `_delta_log/` transaction log — what it stores and why it matters
        2. ACID guarantees — how Delta prevents silent data corruption
        3. Time travel — querying historical versions
        4. Schema evolution — adding columns without breaking existing readers
        5. MERGE / upsert — the core primitive for CDC and incremental loads
        6. OPTIMIZE + Z-ORDER — compaction and co-location for query performance

        **Run locally:**
        ```bash
        uv run marimo edit notebooks/01_delta_fundamentals.py
        ```
        Delta tables are stored in `/tmp/lakehouse_lab/01/`.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"## 0. Environment Setup")
    return


@app.cell
def _():
    import os
    import subprocess
    from pathlib import Path

    # Detect JAVA_HOME across macOS and Linux
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
    os.environ["PATH"] = f"{java_home / 'bin'}:{os.environ['PATH']}"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    print(f"JAVA_HOME: {java_home}")
    return (java_home,)


@app.cell
def _(java_home):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _builder = (
        SparkSession.builder.appName("delta-fundamentals")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(_builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} ready.")
    return (spark,)


@app.cell
def _():
    import shutil
    from pathlib import Path

    BASE = Path("/tmp/lakehouse_lab/01")
    if BASE.exists():
        shutil.rmtree(BASE)
    BASE.mkdir(parents=True)

    POLICY_PATH = str(BASE / "policy")
    print(f"Lab directory: {BASE}")
    return BASE, POLICY_PATH


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 1. The Transaction Log (`_delta_log/`)

        Every Delta table is just Parquet files + a `_delta_log/` directory.
        The log is a sequence of JSON files (and Parquet checkpoints) that record
        every change ever made to the table.

        This is what gives Delta its ACID guarantees — not magic, just a log.
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from pyspark.sql import Row
    from pyspark.sql.functions import col, lit

    # Create a small Delta table
    policies = [
        Row(policy_id="P001", customer_id="C001", product="HEALTH", premium=120000.0, status="active"),
        Row(policy_id="P002", customer_id="C002", product="LIFE",   premium=350000.0, status="active"),
        Row(policy_id="P003", customer_id="C003", product="AUTO",   premium=80000.0,  status="active"),
    ]
    df = spark.createDataFrame(policies)
    df.write.format("delta").mode("overwrite").save(POLICY_PATH)
    print("Initial write done.")
    return (df, policies)


@app.cell
def _(POLICY_PATH):
    import json
    from pathlib import Path

    log_dir = Path(POLICY_PATH) / "_delta_log"
    log_files = sorted(log_dir.glob("*.json"))
    print(f"Transaction log files: {[f.name for f in log_files]}")

    print("\n--- Contents of 00000000000000000000.json ---")
    with open(log_files[0]) as f:
        for line in f:
            entry = json.loads(line)
            action = list(entry.keys())[0]
            print(f"  Action: {action}")
            if action == "add":
                print(f"    path: {entry['add']['path']}")
                print(f"    size: {entry['add']['size']} bytes")
    return (log_dir, log_files)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 2. ACID — Why Delta Doesn't Lose Your Data

        **Atomicity**: Each write is either fully committed or fully rolled back.
        The log file is written *after* all Parquet files are staged. A crash mid-write
        leaves staged files but no log entry — readers never see partial data.

        **Isolation**: Concurrent readers and writers use optimistic concurrency.
        Each transaction reads a consistent snapshot at a given log version.

        Try it: write twice simultaneously and see that both commits succeed
        (or one retries on conflict).
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from pyspark.sql import functions as F

    # Second write — appends more rows
    new_policies = spark.createDataFrame([
        ("P004", "C004", "FIRE", 55000.0, "active"),
        ("P005", "C005", "HEALTH", 200000.0, "lapsed"),
    ], ["policy_id", "customer_id", "product", "premium", "status"])

    new_policies.write.format("delta").mode("append").save(POLICY_PATH)

    # The table now has 5 rows, transaction log has 2 entries
    spark.read.format("delta").load(POLICY_PATH).show()
    return (new_policies,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 3. Time Travel — Querying Historical Versions

        Because the transaction log never deletes old entries (until VACUUM),
        you can query any previous version of the table.

        In Databricks this is invaluable for:
        - Debugging "what did the table look like yesterday?"
        - Recovering from bad writes without a full restore
        - Reproducible ML training on a point-in-time snapshot
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from delta.tables import DeltaTable

    # Show the full version history
    dt = DeltaTable.forPath(spark, POLICY_PATH)
    dt.history().select("version", "timestamp", "operation", "operationParameters").show(
        truncate=False
    )
    return (dt,)


@app.cell
def _(POLICY_PATH, spark):
    # Read version 0 (before the append)
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(POLICY_PATH)
    print("Version 0 (initial write):")
    v0.show()

    # Read current version
    v_now = spark.read.format("delta").load(POLICY_PATH)
    print(f"Current version: {v_now.count()} rows")
    return (v0, v_now)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 4. Schema Evolution

        Delta Lake enforces schema by default — you can't accidentally write
        a column with the wrong type or silently drop a column.

        To add new columns, use `mergeSchema`. This is how you handle ODS
        schema changes without breaking existing pipeline code.
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from pyspark.sql import functions as F

    # Try writing with a new column — this will FAIL without mergeSchema
    enriched = spark.createDataFrame([
        ("P006", "C006", "LIFE", 400000.0, "active", "설계사"),
    ], ["policy_id", "customer_id", "product", "premium", "status", "channel"])

    try:
        enriched.write.format("delta").mode("append").save(POLICY_PATH)
        print("Unexpected: write succeeded without mergeSchema")
    except Exception as e:
        print(f"Expected error: {type(e).__name__}")
        print("Schema mismatch blocked the write. Good.")
    return (enriched,)


@app.cell
def _(POLICY_PATH, enriched):
    # Now with mergeSchema=true — adds the new column, backfills NULL for old rows
    enriched.write.format("delta").mode("append").option("mergeSchema", "true").save(POLICY_PATH)
    print("Write with mergeSchema succeeded.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 5. MERGE / Upsert — The Core of Incremental Loads

        MERGE is how you implement CDC (Change Data Capture) from your ODS.
        Instead of dropping and re-creating the table every day,
        you apply only the changes: INSERTs for new rows, UPDATEs for modified rows.

        This is what the Silver layer should do for slowly changing data.
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    target = DeltaTable.forPath(spark, POLICY_PATH)

    # Simulate incoming ODS delta: P002 status changed, P007 is new
    updates = spark.createDataFrame([
        ("P002", "C002", "LIFE", 350000.0, "cancelled", None),  # status change
        ("P007", "C007", "AUTO",  90000.0, "active",    None),  # new record
    ], ["policy_id", "customer_id", "product", "premium", "status", "channel"])

    (
        target.alias("tgt")
        .merge(updates.alias("src"), "tgt.policy_id = src.policy_id")
        .whenMatchedUpdate(set={"status": F.col("src.status")})
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("After MERGE:")
    spark.read.format("delta").load(POLICY_PATH).orderBy("policy_id").show()
    return (target, updates)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 6. OPTIMIZE + Z-ORDER

        As you keep appending to a Delta table, you accumulate many small Parquet
        files ("the small files problem"). OPTIMIZE compacts them.

        Z-ORDER co-locates rows by a column's values, dramatically reducing
        the data scanned for filter queries (e.g., WHERE product = 'HEALTH').

        In Databricks this runs as a background job. In production, schedule
        OPTIMIZE + VACUUM weekly on large tables.
        """
    )
    return


@app.cell
def _(POLICY_PATH, spark):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    dt = DeltaTable.forPath(spark, POLICY_PATH)

    # Show current file count (many small files from repeated writes)
    files_before = (
        spark.read.format("delta").load(POLICY_PATH)
        ._jdf.queryExecution().analyzed()
        .stats().sizeInBytes()
    )

    # OPTIMIZE (compaction) + Z-ORDER by product (a common filter column)
    dt.optimize().executeZOrderBy("product")

    print("OPTIMIZE + Z-ORDER complete.")
    print("In Databricks: run this as a scheduled Workflow, not in a pipeline job.")
    print("Remove old file versions (keep last 7 days):")
    dt.vacuum(retentionHours=168)
    return (dt, files_before)


@app.cell
def _(POLICY_PATH, spark):
    # Z-ORDER effect: filter by product now reads far fewer Parquet row groups
    from pyspark.sql import functions as F

    result = (
        spark.read.format("delta")
        .load(POLICY_PATH)
        .filter(F.col("product") == "HEALTH")
    )
    result.explain("cost")
    result.show()
    return (result,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Summary

        | Feature | What it solves |
        |---------|---------------|
        | Transaction log | Atomicity: no partial writes ever reach readers |
        | Time travel | Audit, debugging, reproducible ML training |
        | Schema enforcement | Catch ODS changes before they corrupt downstream |
        | mergeSchema | Adopt ODS schema changes without pipeline rewrites |
        | MERGE | Efficient incremental loads — no daily full-table rewrites |
        | OPTIMIZE + VACUUM | Query performance + storage cost over time |

        **Next**: `02_bronze_antipatterns.py` — see how your status.txt pipeline
        breaks and what the Delta audit log replacement looks like.
        """
    )
    return


if __name__ == "__main__":
    app.run()
