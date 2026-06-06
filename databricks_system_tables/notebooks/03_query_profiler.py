"""Query Profiler — slow queries, spill offenders, cache hit ratios."""

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="full", app_title="Query Profiler")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Query Profiler
        **Source**: `system.query.history`

        Surfaces the queries that hurt most: slowest, most memory pressure (spill),
        worst cache utilization. Maps back to Pain Point 6 — code quality.
        """
    )
    return


@app.cell
def _():
    from pathlib import Path

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    DATA_ROOT = Path(__file__).parent.parent / "data" / "system"

    builder = (
        SparkSession.builder.appName("query-profiler")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    history = spark.read.format("delta").load(str(DATA_ROOT / "query" / "history"))
    finished = history.filter(F.col("execution_status") == "FINISHED")
    return (
        DATA_ROOT,
        F,
        Path,
        Window,
        builder,
        configure_spark_with_delta_pip,
        finished,
        history,
        spark,
    )


@app.cell
def _(F, Window, finished, mo):
    p95_threshold = finished.agg(
        F.percentile_approx("total_duration_ms", 0.95).alias("p95")
    ).collect()[0]["p95"]

    slow_queries = (
        finished.filter(F.col("total_duration_ms") >= p95_threshold)
        .select(
            "statement_id",
            "executed_by",
            "client_application",
            F.round(F.col("total_duration_ms") / 60000.0, 2).alias("total_min"),
            F.round(F.col("execution_duration_ms") / 60000.0, 2).alias("exec_min"),
            F.round(F.col("compilation_duration_ms") / 1000.0, 1).alias("compile_sec"),
            F.round(F.col("spilled_local_bytes") / 1e9, 2).alias("spill_gb"),
            F.round(F.col("read_bytes") / 1e9, 2).alias("read_gb"),
            "read_io_cache_percent",
            "from_result_cache",
            "job_id",
            "notebook_id",
            "start_time",
        )
        .orderBy(F.col("total_min").desc())
        .limit(50)
        .toPandas()
    )

    mo.md(
        f"## Slow Queries (above P95 = {round(p95_threshold/60000, 1)} min)\n"
        f"**{len(slow_queries)} queries** above threshold"
    )
    return p95_threshold, slow_queries


@app.cell
def _(mo, slow_queries):
    mo.ui.table(slow_queries, selection=None)
    return


@app.cell
def _(F, finished, mo):
    spill_offenders = (
        finished.filter(F.col("spilled_local_bytes") > 1e9)
        .select(
            "statement_id",
            "executed_by",
            F.round(F.col("spilled_local_bytes") / 1e9, 1).alias("spill_gb"),
            F.round(F.col("shuffle_read_bytes") / 1e9, 1).alias("shuffle_gb"),
            F.round(F.col("total_duration_ms") / 60000.0, 2).alias("total_min"),
            F.round(F.col("read_bytes") / 1e9, 2).alias("read_gb"),
            "client_application",
            "warehouse_id",
            "job_id",
            "notebook_id",
            "start_time",
        )
        .orderBy(F.col("spill_gb").desc())
        .limit(30)
        .toPandas()
    )

    mo.md("## Spill Offenders (> 1 GB disk spill)")
    return (spill_offenders,)


@app.cell
def _(mo, spill_offenders):
    mo.ui.table(spill_offenders, selection=None)
    return


@app.cell
def _(F, finished, mo):
    cache_stats = (
        finished.filter(F.col("warehouse_id").isNotNull())
        .groupBy("warehouse_id")
        .agg(
            F.count("*").alias("query_count"),
            F.round(F.avg("read_io_cache_percent"), 1).alias("avg_io_cache_pct"),
            F.round(
                F.sum(F.when(F.col("from_result_cache"), 1).otherwise(0)) * 100.0
                / F.count("*"),
                1,
            ).alias("result_cache_hit_pct"),
            F.round(F.sum("read_bytes") / 1e12, 3).alias("total_read_tb"),
            F.round(F.avg("total_duration_ms") / 1000.0, 1).alias("avg_duration_sec"),
        )
        .orderBy(F.col("query_count").desc())
        .toPandas()
    )

    mo.md("## Cache Effectiveness by Warehouse")
    return (cache_stats,)


@app.cell
def _(cache_stats, mo):
    mo.ui.table(cache_stats, selection=None)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        **Next**: open `04_lineage_explorer.py` to trace data flow across medallion layers.
        """
    )
    return


if __name__ == "__main__":
    app.run()
