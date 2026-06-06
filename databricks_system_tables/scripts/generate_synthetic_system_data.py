"""
Generates synthetic Databricks system table data as local Delta tables.

Populates:
  ./data/system/lakeflow/job_run_timeline
  ./data/system/lakeflow/jobs
  ./data/system/billing/usage
  ./data/system/query/history
  ./data/system/access/table_lineage

Run:  uv run python scripts/generate_synthetic_system_data.py
Then: uv run marimo edit notebooks/01_ops_dashboard.py
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

fake = Faker("ko_KR")
random.seed(42)

DATA_DIR = Path(__file__).parent.parent / "data" / "system"

ACCOUNT_ID = "abc-insurance-account-01"
WORKSPACE_ID = "1234567890"

TEAMS = ["actuarial", "claims", "platform", "finance", "underwriting"]
JOB_NAMES = [
    "ingest_bronze_policy",
    "ingest_bronze_claims",
    "transform_silver_policy",
    "transform_silver_claims",
    "build_gold_loss_ratio",
    "build_gold_voc_daily",
    "build_gold_policy_summary",
    "export_actuarial_report",
    "sync_ods_snapshot",
    "validate_medallion_contracts",
]
SKU_NAMES = [
    "PREMIUM_JOBS_COMPUTE",
    "STANDARD_ALL_PURPOSE_COMPUTE",
    "ENTERPRISE_SQL_PRO_COMPUTE",
]
RESULT_STATES = ["SUCCEEDED"] * 85 + ["FAILED"] * 8 + ["TIMED_OUT"] * 4 + ["ERROR"] * 3
TERMINATION_CODES = {
    "SUCCEEDED": "SUCCESS",
    "FAILED": "DRIVER_ERROR",
    "TIMED_OUT": "TIMEOUT",
    "ERROR": "CLUSTER_ERROR",
}
TABLE_NAMES = [
    ("VOCP", "bronze", "raw_policy"),
    ("VOCP", "bronze", "raw_claims"),
    ("VOCP", "silver", "policy_clean"),
    ("VOCP", "silver", "claims_clean"),
    ("VOCP", "gold", "loss_ratio_daily"),
    ("VOCP", "gold", "policy_summary"),
    ("VOCP", "gold", "voc_daily"),
    ("VOCD", "bronze", "raw_policy"),
    ("VOCD", "silver", "policy_clean"),
    ("VOCD", "gold", "loss_ratio_daily"),
]
ENTITY_TYPES = ["JOB", "NOTEBOOK", "PIPELINE"]


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("system-table-generator")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def rand_ts(days_ago_max: int = 90, days_ago_min: int = 0) -> datetime:
    now = datetime.now(timezone.utc)
    offset = timedelta(
        days=random.uniform(days_ago_min, days_ago_max),
        hours=random.uniform(0, 23),
        minutes=random.uniform(0, 59),
    )
    return now - offset


def generate_jobs(n: int = 10) -> list[dict]:
    rows = []
    for i, name in enumerate(JOB_NAMES[:n]):
        job_id = str(100_000 + i)
        created_at = rand_ts(180, 90)
        rows.append(
            {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "job_id": job_id,
                "name": name,
                "creator_user_name": fake.email(),
                "run_as_user_name": f"svc-{name.split('_')[0]}@insurance.co.kr",
                "change_time": created_at,
                "delete_time": None,
                "trigger_type": random.choice(["CRON", "TABLE", "CONTINUOUS"]),
                "paused": False,
            }
        )
    return rows


def generate_job_run_timeline(jobs: list[dict], n_runs: int = 500) -> list[dict]:
    rows = []
    for _ in range(n_runs):
        job = random.choice(jobs)
        result_state = random.choice(RESULT_STATES)
        start = rand_ts(30, 0)
        setup_s = random.randint(30, 420) if random.random() < 0.3 else random.randint(5, 30)
        exec_s = random.randint(60, 7200)
        run_duration_s = setup_s + exec_s + random.randint(5, 30)
        end = start + timedelta(seconds=run_duration_s)
        rows.append(
            {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "job_id": job["job_id"],
                "run_id": str(uuid.uuid4().int)[:12],
                "period_start_time": start,
                "period_end_time": end,
                "result_state": result_state,
                "termination_code": TERMINATION_CODES[result_state],
                "trigger_type": job["trigger_type"],
                "run_type": "JOB_RUN",
                "run_name": job["name"],
                "run_duration_seconds": run_duration_s,
                "setup_duration_seconds": setup_s,
                "queue_duration_seconds": random.randint(0, 60),
                "execution_duration_seconds": exec_s,
                "cleanup_duration_seconds": random.randint(5, 30),
            }
        )
    return rows


def generate_billing_usage(jobs: list[dict], n_records: int = 2000) -> list[dict]:
    rows = []
    for _ in range(n_records):
        job = random.choice(jobs)
        team = random.choice(TEAMS)
        usage_date = (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 89))).date()
        dbu = round(random.uniform(0.1, 50.0), 4)
        rows.append(
            {
                "record_id": str(uuid.uuid4()),
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "sku_name": random.choice(SKU_NAMES),
                "cloud": "AZURE",
                "usage_date": usage_date,
                "usage_start_time": datetime.combine(usage_date, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                ),
                "usage_end_time": datetime.combine(usage_date, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )
                + timedelta(hours=1),
                "usage_unit": "DBU",
                "usage_quantity": dbu,
                "billing_origin_product": random.choice(
                    ["JOBS", "ALL_PURPOSE", "SQL", "DLT"]
                ),
                "record_type": "ORIGINAL",
                "custom_tags_team": team,
                "custom_tags_project": job["name"].replace("_", "-"),
                "custom_tags_environment": random.choice(["prod", "dev"]),
                "job_id": job["job_id"],
                "job_run_id": str(uuid.uuid4().int)[:12],
                "cluster_id": f"cluster-{random.randint(1000, 9999)}",
                "run_as": job["run_as_user_name"],
            }
        )
    return rows


def generate_query_history(jobs: list[dict], n_records: int = 1000) -> list[dict]:
    rows = []
    for _ in range(n_records):
        job = random.choice(jobs)
        start = rand_ts(30, 0)
        compile_ms = random.randint(100, 5000)
        exec_ms = random.randint(500, 10_800_000)
        total_ms = compile_ms + exec_ms + random.randint(50, 500)
        read_bytes = random.randint(1_000_000, 5_000_000_000_000)
        spill = random.randint(0, 200_000_000_000) if random.random() < 0.2 else 0
        rows.append(
            {
                "statement_id": str(uuid.uuid4()),
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "executed_by": job["run_as_user_name"],
                "execution_status": random.choice(["FINISHED"] * 9 + ["FAILED"]),
                "statement_type": random.choice(["SELECT", "INSERT", "MERGE", "CREATE"]),
                "start_time": start,
                "end_time": start + timedelta(milliseconds=total_ms),
                "total_duration_ms": total_ms,
                "compilation_duration_ms": compile_ms,
                "execution_duration_ms": exec_ms,
                "waiting_for_compute_duration_ms": random.randint(0, 60_000),
                "waiting_at_capacity_duration_ms": random.randint(0, 10_000),
                "read_bytes": read_bytes,
                "produced_rows": random.randint(0, 10_000_000),
                "read_rows": random.randint(1000, 100_000_000),
                "spilled_local_bytes": spill,
                "shuffle_read_bytes": random.randint(0, 1_000_000_000),
                "read_partitions": random.randint(1, 500),
                "pruned_files": random.randint(0, 2000),
                "read_io_cache_percent": random.randint(0, 100),
                "from_result_cache": random.random() < 0.15,
                "client_application": random.choice(
                    ["Databricks SQL Editor", "Power BI", "Tableau", "databricks-sdk"]
                ),
                "warehouse_id": f"wh-{random.randint(100, 999)}",
                "job_id": job["job_id"],
                "notebook_id": f"nb-{random.randint(10000, 99999)}",
                "statement_text": f"SELECT * FROM {random.choice(['VOCP', 'VOCD'])}.{random.choice(['bronze', 'silver', 'gold'])}.sample_table LIMIT 1000",
            }
        )
    return rows


def generate_table_lineage(jobs: list[dict], n_records: int = 800) -> list[dict]:
    rows = []
    lineage_pairs = [
        (("VOCP", "bronze", "raw_policy"), ("VOCP", "silver", "policy_clean")),
        (("VOCP", "bronze", "raw_claims"), ("VOCP", "silver", "claims_clean")),
        (("VOCP", "silver", "policy_clean"), ("VOCP", "gold", "loss_ratio_daily")),
        (("VOCP", "silver", "claims_clean"), ("VOCP", "gold", "loss_ratio_daily")),
        (("VOCP", "silver", "policy_clean"), ("VOCP", "gold", "policy_summary")),
        (("VOCD", "bronze", "raw_policy"), ("VOCD", "silver", "policy_clean")),
        (("VOCD", "silver", "policy_clean"), ("VOCD", "gold", "loss_ratio_daily")),
        (("VOCP", "bronze", "raw_policy"), ("VOCD", "silver", "policy_clean")),  # violation
    ]
    for _ in range(n_records):
        src, tgt = random.choice(lineage_pairs)
        job = random.choice(jobs)
        event_time = rand_ts(30, 0)
        rows.append(
            {
                "account_id": ACCOUNT_ID,
                "workspace_id": WORKSPACE_ID,
                "source_table_full_name": f"{src[0]}.{src[1]}.{src[2]}",
                "source_table_catalog": src[0],
                "source_table_schema": src[1],
                "source_table_name": src[2],
                "source_type": "TABLE",
                "target_table_full_name": f"{tgt[0]}.{tgt[1]}.{tgt[2]}",
                "target_table_catalog": tgt[0],
                "target_table_schema": tgt[1],
                "target_table_name": tgt[2],
                "target_type": "TABLE",
                "entity_type": random.choice(ENTITY_TYPES),
                "entity_id": job["job_id"],
                "created_by": job["run_as_user_name"],
                "event_time": event_time,
                "event_date": event_time.date(),
                "record_id": str(uuid.uuid4()),
                "event_id": str(uuid.uuid4()),
                "statement_id": str(uuid.uuid4()),
                "direct_access": random.random() < 0.8,
            }
        )
    return rows


def write_delta(spark: SparkSession, rows: list[dict], path: str) -> None:
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").save(path)
    print(f"  wrote {df.count()} rows → {path}")


def main() -> None:
    print("Starting synthetic system table generation...")
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    jobs = generate_jobs()
    print(f"Generated {len(jobs)} jobs")

    print("Writing system.lakeflow.jobs ...")
    write_delta(spark, jobs, str(DATA_DIR / "lakeflow" / "jobs"))

    print("Writing system.lakeflow.job_run_timeline ...")
    run_timeline = generate_job_run_timeline(jobs, n_runs=500)
    write_delta(spark, run_timeline, str(DATA_DIR / "lakeflow" / "job_run_timeline"))

    print("Writing system.billing.usage ...")
    usage = generate_billing_usage(jobs, n_records=2000)
    write_delta(spark, usage, str(DATA_DIR / "billing" / "usage"))

    print("Writing system.query.history ...")
    query_hist = generate_query_history(jobs, n_records=1000)
    write_delta(spark, query_hist, str(DATA_DIR / "query" / "history"))

    print("Writing system.access.table_lineage ...")
    lineage = generate_table_lineage(jobs, n_records=800)
    write_delta(spark, lineage, str(DATA_DIR / "access" / "table_lineage"))

    print("\nDone. Local Delta tables written to ./data/system/")
    print("Run: uv run marimo edit notebooks/01_ops_dashboard.py")

    spark.stop()


if __name__ == "__main__":
    main()
