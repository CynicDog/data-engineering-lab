"""Synthetic Korean insurance data generator.

Simulates what ADF does in the real stack:
    On-premise ODS (Oracle/MSSQL) → SHIR → ADF → ADLS landing zone (Parquet)

This script writes Parquet files to the MinIO landing zone in the same
structure, so the bronze_dag.py can ingest them as if they came from ADF.

Usage:
    # Generate data for today (default)
    uv run scripts/generate_source_data.py

    # Generate data for a specific date
    uv run scripts/generate_source_data.py --dt 2024-03-15

    # Generate multiple days (simulate historical backfill)
    uv run scripts/generate_source_data.py --days 7

    # Generate for both daily AND hourly schedules (demonstrates overlap bug)
    uv run scripts/generate_source_data.py --demo-overlap

    # Point at a different MinIO endpoint (e.g., running locally)
    uv run scripts/generate_source_data.py --endpoint-url http://localhost:9030
"""

from __future__ import annotations

import argparse
import io
import os
import random
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from faker import Faker

fake = Faker("ko_KR")
fake_en = Faker("en_US")
random.seed(42)

BUCKET = os.environ.get("LAKE_BUCKET", "lakehouse")
ENDPOINT = os.environ.get("LAKE_S3_ENDPOINT", "http://localhost:9030")
ACCESS_KEY = os.environ.get("LAKE_S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("LAKE_S3_SECRET_KEY", "minioadmin")

PRODUCT_CODES = ["LIFE", "HEALTH", "AUTO", "FIRE"]
PRODUCT_NAMES = {
    "LIFE": "종신보험",
    "HEALTH": "건강보험",
    "AUTO": "자동차보험",
    "FIRE": "화재보험",
}
CHANNELS = ["설계사", "온라인", "방카슈랑스", "다이렉트"]
CLAIM_STATUSES = ["접수", "심사중", "지급완료", "부지급"]
CLAIM_TYPES = ["입원", "통원", "수술", "사망", "장해", "재물손해"]
VOC_COMPLAINT_TYPES = ["보험료", "보험금", "상품", "서비스", "기타"]
VOC_CHANNELS = ["전화", "온라인", "방문", "우편"]
VOC_STATUSES = ["open", "in_progress", "resolved", "closed"]
POLICY_STATUSES = ["active", "active", "active", "lapsed", "cancelled"]  # Weighted


def _make_rrn(birth: date, gender: str) -> str:
    """Generate a masked RRN. Real RRNs are never generated — only masked form."""
    gender_digit = "1" if gender == "M" else "2"
    front = birth.strftime("%y%m%d")
    return f"{front}-{gender_digit}*******"


def generate_customers(n: int, base_date: date) -> pd.DataFrame:
    rows = []
    for i in range(n):
        cid = f"C{base_date.strftime('%Y%m%d')}{i:05d}"
        birth = fake.date_of_birth(minimum_age=20, maximum_age=75)
        gender = random.choice(["M", "F"])
        rows.append(
            {
                "customer_id": cid,
                "name": fake.name(),
                "rrn_masked": _make_rrn(birth, gender),
                "birth_date": birth.isoformat(),
                "gender": gender,
                "channel": random.choice(CHANNELS),
                "phone_masked": f"010-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
                "email": fake_en.email(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def generate_policies(customers_df: pd.DataFrame, base_date: date) -> pd.DataFrame:
    rows = []
    for _, customer in customers_df.iterrows():
        n_policies = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
        for j in range(n_policies):
            pid = f"P{base_date.strftime('%Y%m%d')}{customer['customer_id'][1:]}{j}"
            product = random.choice(PRODUCT_CODES)
            start = base_date - timedelta(days=random.randint(30, 3650))
            end = start + timedelta(days=random.randint(365, 3650))
            rows.append(
                {
                    "policy_id": pid,
                    "customer_id": customer["customer_id"],
                    "product_code": product,
                    "product_name": PRODUCT_NAMES[product],
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "premium": round(random.uniform(50000, 2000000), 2),
                    "status": random.choice(POLICY_STATUSES),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
    return pd.DataFrame(rows)


def generate_claims(policies_df: pd.DataFrame, base_date: date) -> pd.DataFrame:
    rows = []
    active = policies_df[policies_df["status"] == "active"].sample(
        frac=0.15, random_state=42
    )
    for _, policy in active.iterrows():
        claim_amount = round(random.uniform(100000, 50000000), 2)
        status = random.choice(CLAIM_STATUSES)
        settled = round(claim_amount * random.uniform(0.5, 1.0), 2) if status == "지급완료" else None
        processed_at = (
            datetime.now(timezone.utc).isoformat() if status in ("지급완료", "부지급") else None
        )
        rows.append(
            {
                "claim_id": f"CL{base_date.strftime('%Y%m%d')}{policy['policy_id'][1:]}",
                "policy_id": policy["policy_id"],
                "customer_id": policy["customer_id"],
                "claim_date": base_date.isoformat(),
                "claim_amount": claim_amount,
                "settled_amount": settled,
                "status": status,
                "claim_type": random.choice(CLAIM_TYPES),
                "processed_at": processed_at,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["claim_id", "policy_id", "customer_id", "claim_date",
                 "claim_amount", "settled_amount", "status", "claim_type",
                 "processed_at", "created_at", "updated_at"]
    )


def generate_voc(customers_df: pd.DataFrame, base_date: date) -> pd.DataFrame:
    n = max(1, len(customers_df) // 10)
    sample = customers_df.sample(n=min(n, len(customers_df)), random_state=42)
    rows = []
    for _, customer in sample.iterrows():
        status = random.choice(VOC_STATUSES)
        resolved_at = (
            datetime.now(timezone.utc).isoformat()
            if status in ("resolved", "closed")
            else None
        )
        rows.append(
            {
                "voc_id": f"V{base_date.strftime('%Y%m%d')}{customer['customer_id'][1:]}",
                "customer_id": customer["customer_id"],
                "complaint_type": random.choice(VOC_COMPLAINT_TYPES),
                "channel": random.choice(VOC_CHANNELS),
                "description": fake.sentence(nb_words=12),
                "priority": random.choices(["high", "normal", "low"], weights=[10, 70, 20])[0],
                "status": status,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "resolved_at": resolved_at,
                "resolution_note": fake.sentence() if resolved_at else None,
            }
        )
    return pd.DataFrame(rows)


def write_parquet_to_minio(
    s3: Any,
    df: pd.DataFrame,
    table: str,
    dt: str,
    schedule_type: str = "daily",
) -> str:
    key = f"landing/{table}/dt={dt}/part.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{BUCKET}/{key}"


def write_status_txt_antipattern(
    s3: Any,
    tables: list[str],
    dt: str,
    schedule_type: str,
) -> str:
    """Write a status.txt file — the anti-pattern this lab shows how to replace.

    Demonstrates the problem: daily and hourly schedules write to the same
    folder with the same filename pattern, causing overwrites and lost tracking.
    """
    lines = "\n".join(f"{t}|{dt}|landing/{t}/dt={dt}/part.parquet" for t in tables)
    key = f"control/status_{schedule_type}.txt"
    s3.put_object(Bucket=BUCKET, Key=key, Body=lines.encode())
    print(f"  [anti-pattern] Wrote status file: s3://{BUCKET}/{key}")
    return f"s3://{BUCKET}/{key}"


def generate_for_date(
    s3: Any,
    dt: date,
    n_customers: int = 100,
    schedule_type: str = "daily",
    write_status_txt: bool = False,
) -> None:
    dt_str = dt.isoformat()
    print(f"\nGenerating {schedule_type} data for {dt_str} ({n_customers} customers)...")

    customers = generate_customers(n_customers, dt)
    policies = generate_policies(customers, dt)
    claims = generate_claims(policies, dt)
    voc = generate_voc(customers, dt)

    tables = {
        "customer": customers,
        "policy": policies,
        "claims": claims,
        "voc": voc,
    }

    for table, df in tables.items():
        path = write_parquet_to_minio(s3, df, table, dt_str, schedule_type)
        print(f"  {table}: {len(df):,} rows → {path}")

    if write_status_txt:
        write_status_txt_antipattern(s3, list(tables.keys()), dt_str, schedule_type)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", default=date.today().isoformat(), help="Date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=1, help="Generate N days ending at --dt")
    parser.add_argument("--customers", type=int, default=100, help="Customers per day")
    parser.add_argument(
        "--demo-overlap",
        action="store_true",
        help="Write BOTH daily and hourly status files to the same key, "
             "demonstrating the status.txt overlap bug.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=ENDPOINT,
        help=f"MinIO endpoint URL (default: {ENDPOINT})",
    )
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint_url,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    end_dt = date.fromisoformat(args.dt)
    dates = [end_dt - timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    for dt in dates:
        generate_for_date(
            s3,
            dt,
            n_customers=args.customers,
            schedule_type="daily",
            write_status_txt=args.demo_overlap,
        )
        if args.demo_overlap:
            print("\n  [DEMO] Now writing hourly data for the same date...")
            print("  [DEMO] This overwrites the daily status.txt — THIS IS THE BUG.")
            generate_for_date(
                s3,
                dt,
                n_customers=10,
                schedule_type="hourly",
                write_status_txt=True,
            )
            print("\n  [DEMO] The daily status.txt is now GONE — overwritten by hourly.")
            print("  [DEMO] Bronze pipeline reads the hourly file → misses 90 customers.")
            print("  [DEMO] There's no trace that the daily file ever existed.")
            print("  [DEMO] See notebooks/02_bronze_antipatterns.py for the Delta audit fix.")

    print("\nDone.")


if __name__ == "__main__":
    main()
