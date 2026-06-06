"""Cost Attribution Dashboard — DBU spend by team, job, and product."""

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="full", app_title="Cost Attribution")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Cost Attribution Dashboard
        **Source**: `system.billing.usage`

        Answers: which team, which job, which product consumed the most DBU?
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    DATA_ROOT = Path(__file__).parent.parent / "data" / "system"

    builder = (
        SparkSession.builder.appName("cost-attribution")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    usage = spark.read.format("delta").load(str(DATA_ROOT / "billing" / "usage"))
    jobs = spark.read.format("delta").load(str(DATA_ROOT / "lakeflow" / "jobs"))

    job_names = jobs.groupBy("job_id").agg(F.last("name").alias("job_name"))
    return (
        DATA_ROOT,
        F,
        Path,
        builder,
        configure_spark_with_delta_pip,
        job_names,
        jobs,
        spark,
        usage,
    )


@app.cell
def _(F, mo, usage):
    by_team = (
        usage.groupBy(
            F.col("custom_tags_team").alias("team"),
            F.col("custom_tags_environment").alias("environment"),
            "billing_origin_product",
        )
        .agg(
            F.round(F.sum("usage_quantity"), 2).alias("total_dbu"),
            F.countDistinct("cluster_id").alias("cluster_count"),
            F.countDistinct("job_id").alias("job_count"),
        )
        .orderBy(F.col("total_dbu").desc())
        .toPandas()
    )

    mo.md("## DBU by Team × Product")
    return (by_team,)


@app.cell
def _(by_team, mo):
    mo.ui.table(by_team, selection=None)
    return


@app.cell
def _(F, job_names, mo, usage):
    by_job = (
        usage.filter(F.col("billing_origin_product") == "JOBS")
        .groupBy("job_id")
        .agg(
            F.round(F.sum("usage_quantity"), 2).alias("total_dbu"),
            F.countDistinct("job_run_id").alias("run_count"),
            F.round(
                F.sum("usage_quantity") / F.countDistinct("job_run_id"), 2
            ).alias("avg_dbu_per_run"),
        )
        .join(job_names, "job_id", "left")
        .select("job_name", "total_dbu", "run_count", "avg_dbu_per_run")
        .orderBy(F.col("total_dbu").desc())
        .limit(20)
        .toPandas()
    )

    mo.md("## Top 20 Jobs by DBU Consumption")
    return (by_job,)


@app.cell
def _(by_job, mo):
    mo.ui.table(by_job, selection=None)
    return


@app.cell
def _(F, mo, usage):
    daily = (
        usage.groupBy("usage_date", "billing_origin_product")
        .agg(F.round(F.sum("usage_quantity"), 2).alias("daily_dbu"))
        .orderBy("usage_date", F.col("daily_dbu").desc())
        .toPandas()
    )

    mo.md("## Daily DBU Trend by Product")
    return (daily,)


@app.cell
def _(daily, mo):
    mo.ui.table(daily, selection=None)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        **Next**: open `03_query_profiler.py` to see slow queries and spill offenders.
        """
    )
    return


if __name__ == "__main__":
    app.run()
