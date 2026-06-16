"""Silver transform Spark job — runs as a SparkApplication driver pod.

Per-table granularity: Airflow's silver DAG submits one SparkApplication per
(channel, table), each running this file. The transform logic is the unchanged
`transform_table` (cast -> dedup -> PII mask -> derived columns), driven by the
TableSpec resolved from the shared table_registry.yaml.

    python silver_job.py --channel chan1 --table customer
"""

from __future__ import annotations

import argparse
import os
import sys

# Make imports robust regardless of how Spark sets PYTHONPATH in the pod.
sys.path.insert(0, "/opt/lakehouse/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _bootstrap import get_cluster_spark  # noqa: E402

from lakehouse.config.registry import load_registry  # noqa: E402
from lakehouse.config.settings import load_settings  # noqa: E402
from lakehouse.silver.transform import transform_table  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    settings = load_settings()
    registry = load_registry()
    spec = registry.table_spec(args.channel, args.table)

    spark = get_cluster_spark(f"silver_{args.channel}_{args.table}")
    try:
        rows = transform_table(spark, settings, spec)
        print(f"SILVER_RESULT {args.channel}.{args.table} rows={rows}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
