import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Silver Layer Design

        The question you asked: *"We don't really know what to implement in Silver."*

        This notebook answers that question with runnable patterns.

        **Silver's contract** (one sentence):
        > Silver is the *clean data API* — deduplicated, correctly typed, PII-safe,
        > and structurally stable — that every downstream consumer builds on.

        **Topics:**
        1. The Bronze → Silver boundary: what Silver is NOT responsible for
        2. Deduplication (SCD Type 1 with window functions)
        3. Type casting and data validation
        4. PII masking (개인정보보호법 compliance)
        5. What constitutes a Silver-level "bug" vs a Gold-level concern

        **Run locally:**
        ```bash
        uv run marimo edit notebooks/03_silver_design.py
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
        SparkSession.builder.appName("silver-design")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 1. The Bronze → Silver Boundary

        **What Silver DOES:**
        - Deduplicate rows (keep the latest version of each entity)
        - Cast columns to correct types (string → date, string → double)
        - Mask PII (RRN, phone, email — required by 개인정보보호법)
        - Standardize code values (trim whitespace, normalize case)
        - Drop system/ingest metadata columns (`_ingest_ts`, `_dt`) when not needed downstream

        **What Silver does NOT do:**
        - Cross-table joins (e.g., joining policy to customer) — that's Gold
        - Business metric calculations (e.g., "30-day retention rate") — that's Gold
        - Aggregations or rollups — that's Gold
        - Any query that requires knowing business rules — that's Gold

        **Why this boundary matters:**
        When Gold produces a wrong number (and it will), you fix Gold.
        Silver is stable. You don't re-ingest Bronze to fix a Gold query.
        """
    )
    return


@app.cell
def _(spark):
    from datetime import datetime
    from pyspark.sql import Row

    # Simulate raw Bronze data: duplicates, messy types, unmasked PII
    bronze_customers = spark.createDataFrame([
        # C001 appears twice — second row is the latest ODS update
        Row(customer_id="C001", name="김철수", rrn_masked="900101-1234567",
            birth_date="1990-01-01", gender="M", channel=" 설계사 ",  # dirty whitespace
            phone_masked="010-1234-5678", email="kim@example.com",
            created_at="2024-01-01 09:00:00", updated_at="2024-01-10 14:30:00",
            _ingest_ts=datetime(2024, 1, 15, 9, 0), _dt="2024-01-15"),
        Row(customer_id="C001", name="김철수",  rrn_masked="900101-1234567",
            birth_date="1990-01-01", gender="M", channel="설계사",
            phone_masked="010-9999-8888",  # phone number changed
            email="kim@example.com",
            created_at="2024-01-01 09:00:00", updated_at="2024-01-15 10:00:00",
            _ingest_ts=datetime(2024, 1, 15, 10, 0), _dt="2024-01-15"),  # later ingest
        Row(customer_id="C002", name="이영희", rrn_masked="950605-2345678",
            birth_date="1995-06-05", gender="F", channel="온라인",
            phone_masked="010-5678-1234", email="lee@example.com",
            created_at="2024-01-02 11:00:00", updated_at="2024-01-02 11:00:00",
            _ingest_ts=datetime(2024, 1, 15, 9, 0), _dt="2024-01-15"),
    ])

    print(f"Bronze rows: {bronze_customers.count()}")
    bronze_customers.show(truncate=False)
    return (bronze_customers,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"## 2. Deduplication — SCD Type 1 (Keep Latest)")
    return


@app.cell
def _(bronze_customers, spark):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # Window ordered by ingest time, partitioned by primary key
    # row_number() = 1 means "most recently ingested"
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

    deduped = (
        bronze_customers
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    print(f"After dedup: {deduped.count()} rows (was {bronze_customers.count()})")
    print("C001 row that survived:")
    deduped.filter("customer_id = 'C001'").select("customer_id", "phone_masked", "_ingest_ts").show()
    return (deduped,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 3. PII Masking — 개인정보보호법 Compliance

        The Personal Information Protection Act (개인정보보호법) requires that
        Resident Registration Numbers (주민등록번호) cannot be stored or processed
        in full form beyond the minimum necessary scope.

        Silver is the enforcement point. If unmasked RRNs reach Silver, they will
        eventually reach Gold, BI tools, and data exports. Stop them here.

        Pattern: Silver validates AND re-masks, even if Bronze already masked.
        Defense in depth — don't trust upstream.
        """
    )
    return


@app.cell
def _(deduped, spark):
    from pyspark.sql import functions as F

    # Validate: any unmasked RRNs?
    unmasked = deduped.filter(F.col("rrn_masked").rlike(r"\d{6}-\d{7}"))
    print(f"Unmasked RRNs in Bronze: {unmasked.count()}")
    unmasked.select("customer_id", "rrn_masked").show()
    return (unmasked,)


@app.cell
def _(deduped, spark):
    from pyspark.sql import functions as F

    # Re-mask as a Silver-level guarantee (even if value is already masked)
    silver = (
        deduped
        .withColumn(
            "rrn_masked",
            F.regexp_replace(F.col("rrn_masked"), r"(\d{6})-\d{7}", r"$1-*******"),
        )
        .withColumn(
            "phone_masked",
            F.regexp_replace(F.col("phone_masked"), r"(\d{3})-\d{4}-(\d{4})", r"$1-****-$2"),
        )
        .withColumn("channel", F.trim(F.col("channel")))
        .withColumn("birth_date", F.col("birth_date").cast("date"))
        .withColumn("created_at", F.col("created_at").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )

    print("Silver customer after masking + type casting:")
    silver.select("customer_id", "rrn_masked", "phone_masked", "channel", "birth_date").show(
        truncate=False
    )
    return (silver,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 4. What Is and Isn't a Silver Bug

        | Scenario | Layer | Why |
        |----------|-------|-----|
        | Duplicate C001 rows in Silver | Silver bug | Dedup is Silver's job |
        | Unmasked RRN in Silver | Silver bug | PII masking is Silver's job |
        | Wrong VOC resolution rate in Gold | Gold bug | Business logic is Gold's job |
        | Premium sum is wrong | Gold bug | Aggregation is Gold's job |
        | Bronze → Silver data loss | Bronze bug | Ingest fidelity is Bronze's job |
        | Claim status 'unknown' in Silver | Bronze bug | Unknown source data |

        **The heuristic**: If the bug can be fixed without touching business rules,
        it's Bronze or Silver. If fixing it requires understanding KPI definitions
        or business logic, it's Gold.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## 5. The MERGE Pattern for Incremental Silver Updates

        When Bronze runs incrementally (not a full daily reload), Silver should
        also run incrementally. Use MERGE to apply Bronze deltas to Silver.
        """
    )
    return


@app.cell
def _(silver, spark):
    import shutil
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pathlib import Path

    silver_path = "/tmp/lakehouse_lab/03/silver/customer"
    shutil.rmtree(silver_path, ignore_errors=True)

    # Initial Silver write
    silver.write.format("delta").mode("overwrite").save(silver_path)

    # Simulate next day's Bronze delta: C001 email changed, C003 is new
    bronze_delta = spark.createDataFrame([
        ("C001", "kim_new@example.com"),
        ("C003", "park@example.com"),
    ], ["customer_id", "email"])

    target = DeltaTable.forPath(spark, silver_path)
    (
        target.alias("tgt")
        .merge(bronze_delta.alias("src"), "tgt.customer_id = src.customer_id")
        .whenMatchedUpdate(set={"email": F.col("src.email")})
        .whenNotMatchedInsert(values={
            "customer_id": F.col("src.customer_id"),
            "name": F.lit(None),
            "rrn_masked": F.lit(None),
            "email": F.col("src.email"),
            # other columns would be filled from the full delta in production
        })
        .execute()
    )

    print("Silver after incremental MERGE:")
    spark.read.format("delta").load(silver_path).select(
        "customer_id", "email"
    ).show()
    return (bronze_delta, silver_path, target)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Summary

        Silver answers the question "what is the current state of this entity?"
        — nothing more, nothing less.

        **Next**: `04_gold_marts.py` — how to build business-ready mart tables
        that BI tools can query directly without re-implementing rules.
        """
    )
    return


if __name__ == "__main__":
    app.run()
