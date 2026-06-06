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
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from lakehouse.config.settings import Settings
    from pyspark.sql import DataFrame, SparkSession

TABLES = ["customer", "policy", "claims", "voc"]



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



def transform_customer(df: "DataFrame") -> "DataFrame":
    return (
        df.select(
            F.col("customer_id").cast("string"),
            F.col("name").cast("string"),
            F.col("birth_date").cast("date"),
            F.col("gender").cast("string"),
            F.col("channel").cast("string"),
            F.col("rrn_masked").cast("string"),
            F.col("phone_masked").cast("string"),
            F.col("email").cast("string"),
            F.col("created_at").cast("timestamp"),
            F.col("updated_at").cast("timestamp"),
            F.col("_ingest_ts"),
            F.col("_dt"),
        )
        .pipe(dedup_by_latest, pk="customer_id")
        .pipe(mask_rrn)
        .pipe(mask_phone)
    )


def transform_policy(df: "DataFrame") -> "DataFrame":
    return (
        df.select(
            F.col("policy_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("product_code").cast("string"),
            F.col("product_name").cast("string"),
            F.col("start_date").cast("date"),
            F.col("end_date").cast("date"),
            F.col("premium").cast("double"),
            F.col("status").cast("string"),
            F.col("created_at").cast("timestamp"),
            F.col("updated_at").cast("timestamp"),
            F.col("_ingest_ts"),
            F.col("_dt"),
        )
        .pipe(dedup_by_latest, pk="policy_id")
        .withColumn(
            "policy_age_days",
            F.datediff(F.coalesce(F.col("end_date"), F.current_date()), F.col("start_date")),
        )
    )


def transform_claims(df: "DataFrame") -> "DataFrame":
    return (
        df.select(
            F.col("claim_id").cast("string"),
            F.col("policy_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("claim_date").cast("date"),
            F.col("claim_amount").cast("double"),
            F.col("settled_amount").cast("double"),
            F.col("status").cast("string"),
            F.col("claim_type").cast("string"),
            F.col("processed_at").cast("timestamp"),
            F.col("created_at").cast("timestamp"),
            F.col("updated_at").cast("timestamp"),
            F.col("_ingest_ts"),
            F.col("_dt"),
        )
        .pipe(dedup_by_latest, pk="claim_id")
        .withColumn(
            "processing_days",
            F.when(
                F.col("processed_at").isNotNull(),
                F.datediff(F.col("processed_at").cast("date"), F.col("claim_date")),
            ),
        )
    )


def transform_voc(df: "DataFrame") -> "DataFrame":
    return (
        df.select(
            F.col("voc_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("complaint_type").cast("string"),
            F.col("channel").cast("string"),
            F.col("priority").cast("string"),
            F.col("status").cast("string"),
            F.col("created_at").cast("timestamp"),
            F.col("resolved_at").cast("timestamp"),
            F.col("_ingest_ts"),
            F.col("_dt"),
        )
        .pipe(dedup_by_latest, pk="voc_id")
        .withColumn(
            "resolution_hours",
            F.when(
                F.col("resolved_at").isNotNull(),
                (
                    F.unix_timestamp(F.col("resolved_at"))
                    - F.unix_timestamp(F.col("created_at"))
                )
                / 3600,
            ),
        )
    )


_TRANSFORMS = {
    "customer": transform_customer,
    "policy": transform_policy,
    "claims": transform_claims,
    "voc": transform_voc,
}


def transform_table(
    spark: "SparkSession",
    settings: "Settings",
    table: str,
) -> int:
    """Read from bronze Delta, transform, write to silver Delta.

    Returns the number of rows written.
    """
    if table not in _TRANSFORMS:
        raise ValueError(f"No transform defined for table: {table!r}")

    bronze_df = spark.read.format("delta").load(settings.bronze_path(table))
    silver_df = _TRANSFORMS[table](bronze_df)
    silver_path = settings.silver_path(table)

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    return silver_df.count()
