"""Lineage Explorer — upstream/downstream impact, medallion validation, catalog isolation."""

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="full", app_title="Lineage Explorer")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Lineage Explorer
        **Source**: `system.access.table_lineage`

        Three lenses:
        1. **Downstream impact** — what breaks if table X changes?
        2. **Medallion validation** — does Gold ever bypass Silver and read Bronze directly?
        3. **Catalog isolation** — do VOCD jobs ever read from VOCP?
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
        SparkSession.builder.appName("lineage-explorer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    lineage = spark.read.format("delta").load(str(DATA_ROOT / "access" / "table_lineage"))
    return DATA_ROOT, F, Path, builder, configure_spark_with_delta_pip, lineage, spark


@app.cell
def _(mo):
    source_table = mo.ui.text(
        value="VOCP.silver.policy_clean",
        label="Source table (downstream impact)",
        placeholder="catalog.schema.table",
    )
    source_table
    return (source_table,)


@app.cell
def _(F, lineage, mo, source_table):
    downstream = (
        lineage.filter(
            (F.col("source_table_full_name") == source_table.value)
            & F.col("target_table_full_name").isNotNull()
        )
        .groupBy(
            "target_table_full_name",
            "target_type",
            "entity_type",
            "entity_id",
            "created_by",
        )
        .agg(
            F.max("event_time").alias("last_seen"),
            F.count("*").alias("event_count"),
        )
        .orderBy(F.col("last_seen").desc())
        .toPandas()
    )

    mo.md(
        f"## Downstream Impact: `{source_table.value}`\n"
        f"**{len(downstream)} downstream tables/entities** read from this table"
    )
    return (downstream,)


@app.cell
def _(downstream, mo):
    mo.ui.table(downstream, selection=None)
    return


@app.cell
def _(F, lineage, mo):
    violations = (
        lineage.filter(
            F.col("source_table_full_name").like("%.bronze.%")
            & F.col("target_table_full_name").like("%.gold.%")
        )
        .select(
            "source_table_full_name",
            "target_table_full_name",
            "entity_type",
            "entity_id",
            "created_by",
            "event_time",
        )
        .orderBy(F.col("event_time").desc())
        .toPandas()
    )

    status = "🔴 VIOLATIONS FOUND" if len(violations) > 0 else "✅ Clean — no Gold→Bronze shortcuts"
    mo.md(
        f"""
        ## Medallion Validation — Gold Bypassing Silver?
        *Pain Point 3*: Gold should only read from Silver.

        **{status}**
        ({len(violations)} rows — should be 0)
        """
    )
    return status, violations


@app.cell
def _(mo, violations):
    mo.ui.table(violations, selection=None)
    return


@app.cell
def _(F, lineage, mo):
    catalog_violations = (
        lineage.filter(
            (
                F.col("source_table_full_name").like("VOCP.%")
                & F.col("target_table_full_name").like("VOCD.%")
            )
            | (
                F.col("source_table_full_name").like("VOCD.%")
                & F.col("target_table_full_name").like("VOCP.%")
            )
        )
        .withColumn(
            "violation_direction",
            F.when(
                F.col("source_table_full_name").like("VOCP.%"),
                F.lit("prod_to_dev"),
            ).otherwise(F.lit("dev_to_prod")),
        )
        .select(
            "source_table_full_name",
            "target_table_full_name",
            "violation_direction",
            "entity_type",
            "entity_id",
            "created_by",
            "event_time",
        )
        .orderBy(F.col("event_time").desc())
        .toPandas()
    )

    cat_status = (
        "🔴 ISOLATION BREACH" if len(catalog_violations) > 0
        else "✅ Clean — no cross-catalog data flow"
    )
    mo.md(
        f"""
        ## Catalog Isolation — VOCP ↔ VOCD
        *Pain Point 5*: VOCP (prod) and VOCD (dev) must not share data.

        **{cat_status}**
        ({len(catalog_violations)} rows — should be 0)
        """
    )
    return cat_status, catalog_violations


@app.cell
def _(catalog_violations, mo):
    mo.ui.table(catalog_violations, selection=None)
    return


@app.cell
def _(F, lineage, mo):
    flow_summary = (
        lineage.groupBy(
            "source_table_catalog",
            "source_table_schema",
            "target_table_catalog",
            "target_table_schema",
            "entity_type",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("entity_id").alias("distinct_entities"),
            F.max("event_time").alias("last_seen"),
        )
        .orderBy(F.col("event_count").desc())
        .toPandas()
    )

    mo.md("## All Lineage Flow Patterns")
    return (flow_summary,)


@app.cell
def _(flow_summary, mo):
    mo.ui.table(flow_summary, selection=None)
    return


if __name__ == "__main__":
    app.run()
