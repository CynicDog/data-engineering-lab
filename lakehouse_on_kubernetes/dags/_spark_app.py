"""Render SparkApplication (CRD) manifests for the medallion Spark jobs.

## Scalable-by-design resource model

Jobs select a named profile ("small" / "medium" / "large") rather than specifying
raw memory and core counts. The profile drives:

  1. Spark-level memory/cores  (what Spark tells its JVM and task scheduler)
  2. Kubernetes resource requests/limits  (what K8s uses for pod scheduling and
     eviction; separate from Spark's view)
  3. Dynamic allocation bounds  (min/max executors; replaces the old fixed
     `instances: 2`)
  4. Shuffle partition count  (AQE coalesces downward, so setting this at the
     profile level is safe — the right number is data-volume-dependent, not
     job-type-dependent)

Running "small" today and "medium" tomorrow requires one argument change at the
call site, not a YAML edit across every SparkApplication manifest.

## Dynamic allocation

Static executor counts (`instances: N`) are replaced with dynamic allocation via
the Spark Operator's native `dynamicAllocation` spec field. The Operator injects
the corresponding `spark.dynamicAllocation.*` configs automatically. Shuffle
tracking (`spark.dynamicAllocation.shuffleTracking.enabled`) replaces the legacy
external shuffle service DaemonSet: executors are not deallocated while they hold
live shuffle data, removing the dependency on a stateful sidecar.

## Kubernetes resource requests vs. Spark memory

Spark's `memory` field is what the JVM heap is told it has. The Kubernetes
scheduler needs `requests` (for placement) and `limits` (for eviction) on the
pod. These are distinct:

  Spark memory:       heap size the JVM sees
  memoryOverhead:     off-heap (JVM itself, GC, native libs) — added on top
  Total pod memory =  Spark memory + memoryOverhead

  K8s request:        used for pod placement (should be ≈ total pod memory)
  K8s limit:          hard ceiling (pod OOMs/evicts if exceeded)

Setting requests without limits risks unbounded consumption; setting limits
without requests causes mis-scheduling. Both are set here.

## Executor topology spread

Executors carry a topology spread constraint
(`whenUnsatisfiable: ScheduleAnyway`) that distributes them across nodes when
multiple nodes are available. On a single-node lab this is a no-op; on a
multi-node production cluster it prevents all executors landing on one node,
which would defeat the purpose of having more than one node.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import yaml


class _NoAliasDumper(yaml.SafeDumper):
    """Emit fully-expanded YAML — no &anchors/*aliases for shared dicts."""

    def ignore_aliases(self, data):
        return True


SPARK_IMAGE = "lakehouse/spark:dev"
SPARK_VERSION = "3.5.3"
NAMESPACE = "lakehouse"
S3_ENDPOINT = "http://minio.lakehouse.svc.cluster.local:9000"

ProfileName = Literal["small", "medium", "large"]


@dataclass(frozen=True)
class SparkProfile:
    """All resource parameters for one job size class.

    Attributes that start with ``driver_`` / ``executor_`` map to the
    corresponding fields in the SparkApplication CRD. ``k8s_`` prefixed fields
    map to the Kubernetes pod resource request/limit, which the Operator
    translates into the pod spec.
    """

    # Spark-level memory (heap, visible to Spark's memory manager)
    driver_memory: str
    executor_memory: str
    # JVM off-heap overhead — GC, native memory, direct buffers.
    # Total pod memory ≈ {memory} + {memory_overhead}.
    driver_memory_overhead: str
    executor_memory_overhead: str
    # Spark logical core count (task parallelism within the executor)
    driver_cores: int
    executor_cores: int
    # Kubernetes CPU request (for scheduler placement) and limit (hard cap).
    # Using millicores ("200m") keeps the values proportionate across profiles.
    driver_core_request: str
    driver_core_limit: str
    executor_core_request: str
    executor_core_limit: str
    # Dynamic allocation executor bounds.
    # `initial_executors` is what Spark requests at job start before it has
    # observed the actual workload. AQE + dynamic allocation adjust from there.
    min_executors: int
    initial_executors: int
    max_executors: int
    # Initial shuffle partition count. AQE coalescePartitions shrinks this at
    # runtime if actual shuffle output is small, so setting it generously is
    # safe — cost is only incurred if Spark actually uses the partitions.
    shuffle_partitions: int


PROFILES: dict[ProfileName, SparkProfile] = {
    # Lab / local Kind cluster. Fits comfortably on one 8GB node alongside the
    # Airflow control plane. Dynamic allocation scales 1–4 executors.
    "small": SparkProfile(
        driver_memory="512m",
        executor_memory="512m",
        driver_memory_overhead="128m",
        executor_memory_overhead="128m",
        driver_cores=1,
        executor_cores=1,
        driver_core_request="200m",
        driver_core_limit="1000m",
        executor_core_request="200m",
        executor_core_limit="1000m",
        min_executors=1,
        initial_executors=2,
        max_executors=4,
        shuffle_partitions=4,
    ),
    # Mid-size production tables. Typical daily aggregation on tens of millions
    # of rows. Dynamic allocation scales 2–20 executors.
    "medium": SparkProfile(
        driver_memory="2g",
        executor_memory="4g",
        driver_memory_overhead="512m",
        executor_memory_overhead="512m",
        driver_cores=1,
        executor_cores=2,
        driver_core_request="500m",
        driver_core_limit="2000m",
        executor_core_request="1000m",
        executor_core_limit="2000m",
        min_executors=2,
        initial_executors=4,
        max_executors=20,
        shuffle_partitions=200,
    ),
    # Full-history recomputation, backfill, or heavy feature store jobs.
    # Dynamic allocation scales 4–50 executors.
    "large": SparkProfile(
        driver_memory="4g",
        executor_memory="8g",
        driver_memory_overhead="1g",
        executor_memory_overhead="1g",
        driver_cores=2,
        executor_cores=4,
        driver_core_request="1000m",
        driver_core_limit="4000m",
        executor_core_request="2000m",
        executor_core_limit="4000m",
        min_executors=4,
        initial_executors=8,
        max_executors=50,
        shuffle_partitions=400,
    ),
}

# sparkConf shared by every layer and profile — Delta + S3A + AQE wiring.
# shuffle_partitions is profile-specific and overridden in render().
_BASE_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # AQE: declared explicitly so intent is visible in the Spark UI and
    # thresholds can be tuned per profile without touching this base config.
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Dynamic allocation: shuffle tracking replaces the external shuffle service
    # DaemonSet. Executors are not released while they hold live shuffle blocks.
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.timeout": "1h",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    ),
}

_SECRET_ENV = [
    {"name": "AWS_ACCESS_KEY_ID",
     "valueFrom": {"secretKeyRef": {"name": "minio-creds", "key": "LAKE_S3_ACCESS_KEY"}}},
    {"name": "AWS_SECRET_ACCESS_KEY",
     "valueFrom": {"secretKeyRef": {"name": "minio-creds", "key": "LAKE_S3_SECRET_KEY"}}},
    {"name": "LAKE_S3_ACCESS_KEY",
     "valueFrom": {"secretKeyRef": {"name": "minio-creds", "key": "LAKE_S3_ACCESS_KEY"}}},
    {"name": "LAKE_S3_SECRET_KEY",
     "valueFrom": {"secretKeyRef": {"name": "minio-creds", "key": "LAKE_S3_SECRET_KEY"}}},
    {"name": "LAKE_S3_ENDPOINT", "value": S3_ENDPOINT},
    {"name": "LAKE_BUCKET", "value": "lakehouse"},
    {"name": "LAKEHOUSE_PROFILE", "value": "dev"},
]

_CONFIG_VOLUME = [{"name": "lakehouse-config", "configMap": {"name": "lakehouse-config"}}]
_CONFIG_MOUNT = [{"name": "lakehouse-config", "mountPath": "/opt/lakehouse/config"}]

# Topology spread for executor pods.
# whenUnsatisfiable: ScheduleAnyway → constraint is a soft preference, not a hard
# requirement. On a single-node lab the spread cannot be satisfied and Kubernetes
# schedules all executors on the one available node without error. On a multi-node
# production cluster the scheduler distributes executors across nodes automatically,
# preventing all compute from landing on a single node.
_EXECUTOR_TOPOLOGY_SPREAD = [
    {
        "maxSkew": 1,
        "topologyKey": "kubernetes.io/hostname",
        "whenUnsatisfiable": "ScheduleAnyway",
        "labelSelector": {
            "matchLabels": {"spark-role": "executor"},
        },
    }
]

_RUN_DT = "(dag_run.logical_date or dag_run.run_after)"
DS = "{{ " + _RUN_DT + '.strftime("%Y-%m-%d") }}'
DS_NODASH = "{{ " + _RUN_DT + '.strftime("%Y%m%d") }}'


def _dns_safe(value: str) -> str:
    return value.replace("_", "-").lower()


def render(
    name: str,
    job_file: str,
    arguments: list[str],
    profile: ProfileName = "small",
) -> str:
    """Return a SparkApplication manifest as a YAML string.

    Args:
        name:      RFC-1123-safe name including a per-run suffix so re-runs
                   don't collide on existing SparkApplication objects.
        job_file:  Entrypoint basename under /opt/lakehouse/jobs/.
        arguments: Passed through to the job main().
        profile:   Resource class — "small", "medium", or "large". Controls
                   memory, cores, K8s requests/limits, and dynamic allocation
                   bounds. Defaults to "small" (lab-safe).
    """
    p = PROFILES[profile]

    spark_conf = dict(_BASE_SPARK_CONF)
    spark_conf["spark.sql.shuffle.partitions"] = str(p.shuffle_partitions)

    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local:///opt/lakehouse/jobs/{job_file}",
            "arguments": arguments,
            "sparkVersion": SPARK_VERSION,
            "restartPolicy": {"type": "Never"},
            "sparkConf": spark_conf,
            "volumes": _CONFIG_VOLUME,
            # Dynamic allocation: the Operator injects spark.dynamicAllocation.*
            # configs from these fields. `instances` is intentionally absent —
            # executor count is managed by Spark's dynamic allocator at runtime.
            "dynamicAllocation": {
                "enabled": True,
                "initialExecutors": p.initial_executors,
                "minExecutors": p.min_executors,
                "maxExecutors": p.max_executors,
                "shuffleTrackingTimeout": 3600,  # seconds
            },
            "driver": {
                "cores": p.driver_cores,
                "memory": p.driver_memory,
                "memoryOverhead": p.driver_memory_overhead,
                # Kubernetes CPU request (placement) and limit (enforcement).
                # Distinct from Spark's `cores` (task-slot count).
                "coreRequest": p.driver_core_request,
                "coreLimit": p.driver_core_limit,
                "serviceAccount": "spark",
                "env": list(_SECRET_ENV),
                "volumeMounts": _CONFIG_MOUNT,
            },
            "executor": {
                "cores": p.executor_cores,
                "memory": p.executor_memory,
                "memoryOverhead": p.executor_memory_overhead,
                "coreRequest": p.executor_core_request,
                "coreLimit": p.executor_core_limit,
                "env": list(_SECRET_ENV),
                "volumeMounts": _CONFIG_MOUNT,
                # Spread executors across nodes when multiple nodes exist.
                # No-op on single-node lab; effective in multi-node production.
                "podTemplateSpec": {
                    "spec": {
                        "topologySpreadConstraints": _EXECUTOR_TOPOLOGY_SPREAD,
                    }
                },
            },
        },
    }
    return yaml.dump(manifest, Dumper=_NoAliasDumper, sort_keys=False)


def render_bronze(
    channel: str,
    table: str,
    dt: str = DS,
    profile: ProfileName = "small",
) -> str:
    name = f"bronze-{_dns_safe(channel)}-{_dns_safe(table)}-{DS_NODASH}"
    return render(
        name,
        "bronze_job.py",
        ["--channel", channel, "--table", table, "--dt", dt, "--schedule-type", "daily"],
        profile=profile,
    )


def render_silver(
    channel: str,
    table: str,
    profile: ProfileName = "small",
) -> str:
    name = f"silver-{_dns_safe(channel)}-{_dns_safe(table)}-{DS_NODASH}"
    return render(name, "silver_job.py", ["--channel", channel, "--table", table], profile=profile)


def render_gold(mart: str, profile: ProfileName = "small") -> str:
    name = f"gold-{_dns_safe(mart)}-{DS_NODASH}"
    return render(name, "gold_job.py", ["--mart", mart], profile=profile)


def render_maintenance(name_suffix: str, table_path: str, profile: ProfileName = "small") -> str:
    name = f"maint-{_dns_safe(name_suffix)}-{DS_NODASH}"
    return render(
        name,
        "maintenance_job.py",
        ["--path", table_path, "--vacuum-hours", "168"],
        profile=profile,
    )
