"""Delta table maintenance DAG — weekly OPTIMIZE + VACUUM across all medallion tables.

Every Delta write (replaceWhere, overwrite, append) creates new Parquet data files.
Without compaction, a table that runs daily accumulates O(days × partitions) small
files. Each small file is a separate S3 LIST + open syscall — scan performance
degrades linearly with file count even when the total data size is unchanged.

OPTIMIZE compacts files per partition. VACUUM removes files no longer referenced
by any table version within the retention window.

Design choices:
- One SparkApplication per table: a failed compaction on one table does not block
  others, and resource usage stays predictable.
- Weekly cadence: daily pipelines add ~1 file set per run; weekly compaction keeps
  the small-file ratio manageable without excessive compute cost.
- 168-hour VACUUM retention (7 days): safe for backfill pipelines that may replay
  up to one week of history.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

sys.path.insert(0, os.path.dirname(__file__))

from _spark_app import NAMESPACE, render_maintenance
from lakehouse.config.registry import load_registry
from lakehouse.config.settings import load_settings

registry = load_registry()
_settings = load_settings()

_ALL_TABLES: list[tuple[str, str]] = []  # (name_suffix, s3a_path)

for ch in registry.channels:
    for t in ch.tables:
        _ALL_TABLES.append((f"bronze-{ch.name}-{t.name}", _settings.bronze_path(ch.name, t.name)))
        _ALL_TABLES.append((f"silver-{ch.name}-{t.name}", _settings.silver_path(ch.name, t.name)))

for mart in registry.marts:
    _ALL_TABLES.append((f"gold-{mart}", _settings.gold_path(mart)))


def _dag():
    from airflow.sdk import dag

    @dag(
        dag_id="lakehouse_maintenance",
        schedule="@weekly",
        catchup=False,
        max_active_runs=1,
        tags=["maintenance", "delta", "optimize", "vacuum", "spark-on-k8s"],
        doc_md="""
## Lakehouse Maintenance (Spark-on-Kubernetes)

Weekly OPTIMIZE + VACUUM across all bronze, silver, and gold Delta tables.

- **OPTIMIZE**: compacts small Parquet files per partition.
- **VACUUM**: deletes unreferenced old file versions beyond the 7-day retention window.

One `SparkKubernetesOperator` per table — failures are isolated per table.
""",
    )
    def lakehouse_maintenance():
        for suffix, path in _ALL_TABLES:
            SparkKubernetesOperator(
                task_id=f"maintain_{suffix.replace('-', '_')}",
                namespace=NAMESPACE,
                application_file=render_maintenance(suffix, path, profile="small"),
                kubernetes_conn_id=None,
                in_cluster=True,
                get_logs=True,
                do_xcom_push=False,
                retries=1,
                execution_timeout=timedelta(minutes=60),
            )

    return lakehouse_maintenance()


dag = _dag()
