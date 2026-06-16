"""Gold mart Spark job — runs as a SparkApplication driver pod.

Per-mart granularity: Airflow's gold DAG submits one SparkApplication per mart,
each running this file. The mart logic is the unchanged `build_mart` dispatcher
(voc_daily / policy_summary / claims_analysis).

    python gold_job.py --mart voc_daily
"""

from __future__ import annotations

import argparse
import os
import sys

# Make imports robust regardless of how Spark sets PYTHONPATH in the pod.
sys.path.insert(0, "/opt/lakehouse/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _bootstrap import get_cluster_spark  # noqa: E402

from lakehouse.config.settings import load_settings  # noqa: E402
from lakehouse.gold.mart import build_mart  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mart", required=True)
    args = parser.parse_args()

    settings = load_settings()
    spark = get_cluster_spark(f"gold_{args.mart}")
    try:
        rows = build_mart(spark, settings, args.mart)
        print(f"GOLD_RESULT {args.mart} rows={rows}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
