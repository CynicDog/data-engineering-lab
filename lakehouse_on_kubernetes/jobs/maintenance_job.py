"""Delta table maintenance Spark job — runs OPTIMIZE then VACUUM on one table path.

Each Airflow maintenance task submits one instance of this job per table. Keeping
maintenance per-table (not one big job for all tables) means a failed OPTIMIZE on
one table doesn't block VACUUM on others, and Spark resource usage stays bounded.

    python maintenance_job.py --path s3a://lakehouse/bronze/chan1/customer
    python maintenance_job.py --path s3a://lakehouse/silver/chan1/policy --vacuum-hours 168

Why OPTIMIZE matters:
    Every Delta write (including replaceWhere partition overwrites) creates new
    Parquet files. Over time, a table accumulates many small files — the "small
    file problem". A full-table scan ends up opening thousands of files instead of
    a few large ones, each incurring an S3 LIST + open round-trip. OPTIMIZE
    compacts them into fewer, larger files per partition. Queries on the table
    after OPTIMIZE can be 2-10× faster depending on how fragmented it was.

Why VACUUM matters:
    Delta's ACID model works by keeping old file versions around for time travel
    and concurrent reads. VACUUM deletes files that are no longer referenced by
    any table version newer than the retention window. Without VACUUM, S3 storage
    costs and LIST overhead grow without bound.

Default retention: 168 hours (7 days) — safe for daily pipelines that may
backfill up to a week. Reduce to 24h only if storage cost is a concern and no
time travel queries span more than a day.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, "/opt/lakehouse/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _bootstrap import get_cluster_spark  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, help="s3a:// path to the Delta table")
    parser.add_argument(
        "--vacuum-hours",
        type=int,
        default=168,
        help="Retention window for VACUUM in hours (default: 168 = 7 days)",
    )
    args = parser.parse_args()

    spark = get_cluster_spark(f"maintenance_{args.path.rstrip('/').split('/')[-1]}")
    try:
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(spark, args.path)

        print(f"OPTIMIZE {args.path}", flush=True)
        dt.optimize().executeCompaction()

        print(f"VACUUM {args.path} RETAIN {args.vacuum_hours} HOURS", flush=True)
        # retention_hours is a float; DeltaTable.vacuum() accepts hours as float.
        dt.vacuum(args.vacuum_hours)

        print(f"MAINTENANCE_DONE path={args.path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
