"""Silver layer transforms: dedup, type cast, PII masking.

Silver's contract:
    INPUT:  Raw Delta tables in bronze — schema matches the ODS source exactly,
            may contain duplicates, raw strings, and sensitive PII.
    OUTPUT: Clean Delta tables — deduplicated, typed, PII-masked,
            ready for analytical joins without business logic applied.

What Silver does NOT do:
    - Business aggregations (that's Gold)
    - Cross-table joins for mart shapes (that's Gold)
    - SLA / KPI calculations (that's Gold)

Why this boundary matters:
    When Gold breaks (it will), you can fix Gold without re-running the
    expensive Bronze ingestion. Silver is a stable contract between the
    raw data world and the analytical world.

Config-driven engine:
    All table-specific logic (casts, PII columns, PK, derived columns) lives in
    config/table_registry.yaml. Adding a new table = one YAML block, no Python changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from lakehouse.config.registry import TableSpec
    from lakehouse.config.settings import Settings
    from pyspark.sql import DataFrame, SparkSession


def dedup_by_latest(df: "DataFrame", pk: str, ts_col: str = "_ingest_ts") -> "DataFrame":
    """Keep the most recently ingested row per primary key.

    This is SCD Type 1 (overwrite): we don't keep history, just the latest.
    For SCD Type 2 (keep history), use a MERGE with row validity dates instead.
    """
    w = Window.partitionBy(pk).orderBy(F.col(ts_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def mask_rrn(df: "DataFrame", col_name: str = "rrn_masked") -> "DataFrame":
    """Ensure the back 7 digits of a Resident Registration Number are masked.

    개인정보보호법 (Personal Information Protection Act) requires that RRNs
    are not stored or processed in plaintext beyond the front 6 digits.
    Input is expected to already be masked from the ODS, but we enforce it here
    as a defense-in-depth measure.

    Format: YYMMDD-GXXXXXX  →  YYMMDD-*******
    """
    return df.withColumn(
        col_name,
        F.regexp_replace(F.col(col_name), r"(\d{6})-\d{7}", r"$1-*******"),
    )


def mask_phone(df: "DataFrame", col_name: str = "phone_masked") -> "DataFrame":
    """Mask middle digits of Korean phone numbers: 010-1234-5678 → 010-****-5678."""
    return df.withColumn(
        col_name,
        F.regexp_replace(F.col(col_name), r"(\d{3})-\d{4}-(\d{4})", r"$1-****-$2"),
    )


_PII_MASKERS = {
    "mask_rrn": mask_rrn,
    "mask_phone": mask_phone,
}


def transform_from_spec(df: "DataFrame", spec: "TableSpec") -> "DataFrame":
    """Generic Silver transform driven entirely by TableSpec config.

    Order: cast → dedup → PII mask → derived columns.
    Cast first so derived column SQL expressions see correctly typed inputs.
    """
    for col_name, type_str in spec.cast_types.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(type_str))

    df = dedup_by_latest(df, pk=spec.pk, ts_col=spec.dedup_ts_col)

    for col_name, masker_name in spec.pii.items():
        if col_name in df.columns and masker_name in _PII_MASKERS:
            df = _PII_MASKERS[masker_name](df, col_name=col_name)

    for col_name, sql_expr in spec.derived_columns.items():
        df = df.withColumn(col_name, F.expr(sql_expr))

    return df


def transform_table(
    spark: "SparkSession",
    settings: "Settings",
    spec: "TableSpec",
) -> int:
    """Read from bronze Delta, transform via spec, write to silver Delta.

    Returns the number of rows written.
    """
    bronze_df = spark.read.format("delta").load(settings.bronze_path(spec.channel, spec.name))
    silver_df = transform_from_spec(bronze_df, spec)

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(settings.silver_path(spec.channel, spec.name))
    )

    return silver_df.count()
