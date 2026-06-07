"""Gold layer: business-ready mart aggregations.

Gold's contract:
    INPUT:  Clean Silver Delta tables.
    OUTPUT: Denormalized, query-ready mart tables — shaped for BI tools,
            dashboards, and downstream ML feature stores.

What Gold owns:
    - Business aggregations (KPIs, SLAs, funnel metrics)
    - Cross-table joins for mart shapes
    - Window functions for trend / ranking
    - Any field that requires business rule interpretation

Why Gold is "heavy":
    Gold intentionally centralizes the business logic so that BI tools
    can run simple SELECTs without re-implementing rules. The trade-off
    is that Gold jobs take longer and touch more data — that's expected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from lakehouse.config.settings import Settings
    from pyspark.sql import SparkSession


def build_voc_daily(spark: "SparkSession", settings: "Settings") -> int:
    """Daily VOC summary: complaint counts, resolution rate, avg resolution time."""
    voc = spark.read.format("delta").load(settings.silver_path("chan2", "voc"))
    customer = spark.read.format("delta").load(settings.silver_path("chan1", "customer"))

    mart = (
        voc.join(
            customer.select("customer_id", F.col("channel").alias("acq_channel")),
            on="customer_id",
            how="left",
        )
        .groupBy(
            F.to_date(F.col("created_at")).alias("date"),
            F.col("complaint_type"),
            F.col("acq_channel").alias("channel"),
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

    mart.write.format("delta").mode("overwrite").save(settings.gold_path("voc_daily"))
    return mart.count()


def build_policy_summary(spark: "SparkSession", settings: "Settings") -> int:
    """Policy portfolio summary: active counts, premium by product and channel."""
    policy = spark.read.format("delta").load(settings.silver_path("chan1", "policy"))
    customer = spark.read.format("delta").load(settings.silver_path("chan1", "customer"))

    mart = (
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

    mart.write.format("delta").mode("overwrite").save(settings.gold_path("policy_summary"))
    return mart.count()


def build_claims_analysis(spark: "SparkSession", settings: "Settings") -> int:
    """Claims funnel: settlement rates, processing time, amount by product."""
    claims = spark.read.format("delta").load(settings.silver_path("chan1", "claims"))
    policy = spark.read.format("delta").load(settings.silver_path("chan1", "policy"))

    mart = (
        claims.join(
            policy.select("policy_id", "product_code"),
            on="policy_id",
            how="left",
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
            ).otherwise(None),
        )
        .orderBy("product_code", "status")
    )

    mart.write.format("delta").mode("overwrite").save(settings.gold_path("claims_analysis"))
    return mart.count()


_MART_BUILDERS = {
    "voc_daily": build_voc_daily,
    "policy_summary": build_policy_summary,
    "claims_analysis": build_claims_analysis,
}


def build_mart(spark: "SparkSession", settings: "Settings", mart: str) -> int:
    if mart not in _MART_BUILDERS:
        raise ValueError(f"Unknown mart: {mart!r}. Available: {list(_MART_BUILDERS)}")
    return _MART_BUILDERS[mart](spark, settings)
