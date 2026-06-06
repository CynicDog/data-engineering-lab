import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Gold Layer — Business-Ready Mart Design

        You said Gold is "fucking heavy with full of mart custom query."
        That's actually correct — Gold *should* be heavy. That's its job.
        The problem isn't that Gold is heavy, it's that the current Gold
        is also the only layer doing any work, which means Bronze + Silver
        gaps accumulate there too.

        This notebook shows:
        1. What a well-structured Gold mart looks like
        2. Why Gold cross-joins Silver tables (and that's fine)
        3. How to make Gold reusable and testable despite its complexity
        4. The mart patterns that map to your VOC / policy / claims use cases

        **Run locally:**
        ```bash
        uv run marimo edit notebooks/04_gold_marts.py
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
        SparkSession.builder.appName("gold-marts")
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
    from datetime import date, datetime, timedelta
    import random

    BASE = Path("/tmp/lakehouse_lab/04")
    shutil.rmtree(BASE, ignore_errors=True)
    BASE.mkdir(parents=True)
    random.seed(42)
    return BASE, date, datetime, random, timedelta


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"## Build Silver-like test data")
    return


@app.cell
def _(BASE, date, datetime, random, spark, timedelta):
    from pyspark.sql import Row
    from pyspark.sql import functions as F

    PRODUCTS = ["LIFE", "HEALTH", "AUTO", "FIRE"]
    CHANNELS = ["설계사", "온라인", "방카슈랑스", "다이렉트"]

    # Silver customers
    customers = spark.createDataFrame([
        Row(customer_id=f"C{i:04d}", name=f"Customer{i}",
            channel=random.choice(CHANNELS))
        for i in range(1, 51)
    ])
    customers.write.format("delta").mode("overwrite").save(str(BASE / "silver/customer"))

    # Silver policies
    policies = spark.createDataFrame([
        Row(
            policy_id=f"P{i:04d}",
            customer_id=f"C{(i % 50) + 1:04d}",
            product_code=random.choice(PRODUCTS),
            premium=float(random.randint(50000, 2000000)),
            status=random.choices(["active", "active", "active", "lapsed"], weights=[3,3,3,1])[0],
            policy_age_days=random.randint(30, 3000),
        )
        for i in range(1, 101)
    ])
    policies.write.format("delta").mode("overwrite").save(str(BASE / "silver/policy"))

    # Silver VOC
    today = date(2024, 3, 15)
    COMPLAINT_TYPES = ["보험료", "보험금", "상품", "서비스", "기타"]
    VOC_STATUSES = ["open", "in_progress", "resolved", "closed"]
    voc = spark.createDataFrame([
        Row(
            voc_id=f"V{i:04d}",
            customer_id=f"C{(i % 50) + 1:04d}",
            complaint_type=random.choice(COMPLAINT_TYPES),
            channel=random.choice(["전화", "온라인", "방문"]),
            priority=random.choices(["high", "normal", "low"], weights=[1, 7, 2])[0],
            status=random.choice(VOC_STATUSES),
            created_at=datetime.combine(
                today - timedelta(days=random.randint(0, 30)), datetime.min.time()
            ),
            resolved_at=datetime.combine(
                today - timedelta(days=random.randint(0, 5)), datetime.min.time()
            ) if random.random() > 0.4 else None,
            resolution_hours=float(random.randint(1, 72)) if random.random() > 0.4 else None,
        )
        for i in range(1, 201)
    ])
    voc.write.format("delta").mode("overwrite").save(str(BASE / "silver/voc"))

    # Silver claims
    CLAIM_STATUSES = ["접수", "심사중", "지급완료", "부지급"]
    claims = spark.createDataFrame([
        Row(
            claim_id=f"CL{i:04d}",
            policy_id=f"P{(i % 100) + 1:04d}",
            customer_id=f"C{(i % 50) + 1:04d}",
            claim_amount=float(random.randint(100000, 20000000)),
            settled_amount=float(random.randint(50000, 10000000)) if random.random() > 0.4 else None,
            status=random.choice(CLAIM_STATUSES),
            processing_days=random.randint(1, 30) if random.random() > 0.3 else None,
        )
        for i in range(1, 81)
    ])
    claims.write.format("delta").mode("overwrite").save(str(BASE / "silver/claims"))

    print(f"Silver tables created in {BASE / 'silver'}")
    return CHANNELS, COMPLAINT_TYPES, PRODUCTS, VOC_STATUSES, claims, customers, policies, today, voc


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Mart 1: `voc_daily`

        The VOC daily mart is the operational heartbeat of the contact center.
        It answers: how many complaints today, which types, are they being resolved?

        In your current setup this is probably a giant Gold notebook with
        hardcoded catalog names and no tests. Here it's a named function
        in `gold/mart.py` — testable, versionable, readable.
        """
    )
    return


@app.cell
def _(BASE, spark):
    from pyspark.sql import functions as F

    voc = spark.read.format("delta").load(str(BASE / "silver/voc"))
    customer = spark.read.format("delta").load(str(BASE / "silver/customer"))

    voc_daily = (
        voc.join(customer.select("customer_id", "channel"), on="customer_id", how="left")
        .groupBy(
            F.to_date(F.col("created_at")).alias("date"),
            F.col("complaint_type"),
            F.col("channel"),
        )
        .agg(
            F.count("voc_id").alias("total_voc"),
            F.sum(F.when(F.col("status") == "resolved", 1).otherwise(0)).alias("resolved_count"),
            F.avg(F.col("resolution_hours")).alias("avg_resolution_hours"),
            F.sum(F.when(F.col("priority") == "high", 1).otherwise(0)).alias("high_priority_count"),
        )
        .withColumn(
            "resolution_rate",
            F.round(F.col("resolved_count") / F.col("total_voc"), 4),
        )
        .orderBy("date", "complaint_type")
    )

    print(f"voc_daily mart: {voc_daily.count()} rows")
    voc_daily.show(10, truncate=False)
    return (customer, voc, voc_daily)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Mart 2: `policy_summary`

        Active portfolio snapshot: how many policies by product and channel,
        what's the total/average premium?

        This kind of mart is what the actuarial team queries daily.
        """
    )
    return


@app.cell
def _(BASE, customer, spark):
    from pyspark.sql import functions as F

    policy = spark.read.format("delta").load(str(BASE / "silver/policy"))

    policy_summary = (
        policy.join(customer.select("customer_id", "channel"), on="customer_id", how="left")
        .filter(F.col("status") == "active")
        .groupBy(F.col("product_code"), F.col("channel"))
        .agg(
            F.count("policy_id").alias("active_policies"),
            F.sum("premium").alias("total_premium"),
            F.avg("premium").alias("avg_premium"),
            F.avg("policy_age_days").alias("avg_policy_age_days"),
        )
        .orderBy("product_code", "channel")
    )

    print(f"policy_summary mart: {policy_summary.count()} rows")
    policy_summary.show(truncate=False)
    return (policy, policy_summary)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Mart 3: `claims_analysis`

        Settlement funnel: out of all claims filed, how many were paid?
        What's the average processing time? Is it within SLA?
        """
    )
    return


@app.cell
def _(BASE, policy, spark):
    from pyspark.sql import functions as F

    claims = spark.read.format("delta").load(str(BASE / "silver/claims"))

    claims_analysis = (
        claims.join(
            policy.select("policy_id", "product_code"), on="policy_id", how="left"
        )
        .groupBy(F.col("product_code"), F.col("status"))
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount").alias("total_claimed"),
            F.sum("settled_amount").alias("total_settled"),
            F.avg("processing_days").alias("avg_processing_days"),
        )
        .withColumn(
            "settlement_ratio",
            F.when(
                F.col("total_claimed") > 0,
                F.round(F.col("total_settled") / F.col("total_claimed"), 4),
            ),
        )
        .orderBy("product_code", "status")
    )

    print(f"claims_analysis mart: {claims_analysis.count()} rows")
    claims_analysis.show(truncate=False)
    return (claims, claims_analysis)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Writing Gold as Delta + Time Travel

        Because Gold is Delta, you can time-travel to yesterday's mart values —
        useful when a business user says "the numbers changed, what was it yesterday?"
        """
    )
    return


@app.cell
def _(BASE, voc_daily):
    from delta.tables import DeltaTable

    gold_path = str(BASE / "gold/voc_daily")
    voc_daily.write.format("delta").mode("overwrite").save(gold_path)

    # Simulate next-day Gold run with different numbers
    import random
    from pyspark.sql import functions as F

    updated = voc_daily.withColumn("total_voc", F.col("total_voc") + 5)
    updated.write.format("delta").mode("overwrite").save(gold_path)

    print("Gold voc_daily — current version vs yesterday:")
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    print("Current (v1):")
    spark.read.format("delta").option("versionAsOf", 1).load(gold_path).show(3)
    print("Yesterday (v0):")
    spark.read.format("delta").option("versionAsOf", 0).load(gold_path).show(3)
    return (gold_path, updated)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Summary

        | Mart | Answers |
        |------|---------|
        | `voc_daily` | How many complaints today? Resolution rate? Avg time? |
        | `policy_summary` | How is the portfolio distributed? Premium by product? |
        | `claims_analysis` | Settlement rates? Processing time within SLA? |

        **The key shift**: instead of Gold being a notebook that does everything
        (ingest → clean → aggregate → format), Gold is *just* aggregations
        over a stable Silver contract. Bronze and Silver do their jobs so Gold can
        focus on business logic alone.

        This makes each layer independently testable and independently deployable.
        """
    )
    return


if __name__ == "__main__":
    app.run()
