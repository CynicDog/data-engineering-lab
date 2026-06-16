"""Bronze ingestion Spark job — runs as a SparkApplication driver pod.

One invocation = one (channel, table) for one date. Airflow's bronze DAG
submits one SparkApplication per (channel, table), each running this file with
the matching arguments. The actual ingest logic is the unchanged
`ingest_table_with_audit` from the shared lakehouse package.

    python bronze_job.py --channel chan1 --table customer --dt 2024-01-15 \
        --schedule-type daily
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# Make imports robust regardless of how Spark sets PYTHONPATH in the pod:
# the job's own dir (for _bootstrap) and the package root (for lakehouse.*).
sys.path.insert(0, "/opt/lakehouse/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _bootstrap import get_cluster_spark  # noqa: E402

from lakehouse.bronze.ingest import ingest_table_with_audit  # noqa: E402
from lakehouse.config.settings import load_settings  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--dt", required=True, help="ISO date, e.g. 2024-01-15")
    parser.add_argument("--schedule-type", default="daily")
    args = parser.parse_args()

    settings = load_settings()
    spark = get_cluster_spark(f"bronze_{args.channel}_{args.table}_{args.dt}")
    try:
        result = ingest_table_with_audit(
            spark,
            settings,
            args.channel,
            args.table,
            args.dt,
            schedule_type=args.schedule_type,
        )
        # Surface the summary in the driver log for kubectl logs / Airflow.
        print(f"BRONZE_RESULT {json.dumps(result)}", flush=True)
    finally:
        spark.stop()

    if result.get("status") != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
