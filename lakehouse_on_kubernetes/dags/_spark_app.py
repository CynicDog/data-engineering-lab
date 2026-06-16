"""Render SparkApplication (CRD) manifests for the medallion Spark jobs.

Each Airflow task submits one SparkApplication to the Spark Operator. Rather
than keep three near-identical YAML files, we build the manifest from one base
in Python and `yaml.safe_dump` it to a string. `SparkKubernetesOperator`
templates `application_file`, so embedded Jinja macros like ``{{ ds }}`` /
``{{ ds_nodash }}`` are resolved at task runtime.

Design notes:
* Delta extensions + S3A endpoint config live in ``sparkConf`` (the
  cluster-mode home for them) — the job code does NOT set them.
* MinIO credentials are NOT inlined (they would leak into Airflow logs).
  They are injected as env vars from the ``minio-creds`` Secret, and S3A reads
  them via ``EnvironmentVariableCredentialsProvider`` (AWS_ACCESS_KEY_ID /
  AWS_SECRET_ACCESS_KEY). ``LAKE_*`` env vars feed ``load_settings()``.
* The lakehouse-config ConfigMap is mounted at /opt/lakehouse/config in driver
  and executors so ``parents[3]`` config resolution finds dev.yaml +
  table_registry.yaml.
"""

from __future__ import annotations

import yaml


class _NoAliasDumper(yaml.SafeDumper):
    """Emit fully-expanded YAML — no &anchors/*aliases for shared dicts."""

    def ignore_aliases(self, data):  # noqa: D401
        return True


# Must match images/spark/Dockerfile tag and helm/airflow image config.
SPARK_IMAGE = "lakehouse/spark:dev"
SPARK_VERSION = "3.5.3"
NAMESPACE = "lakehouse"
# In-cluster MinIO S3 endpoint (matches lakehouse/config/dev.yaml).
S3_ENDPOINT = "http://minio.lakehouse.svc.cluster.local:9000"

# sparkConf shared by every layer — Delta + S3A wiring.
_BASE_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.shuffle.partitions": "4",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    # Read AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the pod env (injected
    # from the minio-creds Secret). NOTE: this class lives in the AWS SDK
    # (com.amazonaws.auth.*) — there is no org.apache.hadoop.fs.s3a.* variant.
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    ),
}

# Env injected into BOTH driver and executor pods.
# AWS_* feed S3A's EnvironmentVariableCredentialsProvider; LAKE_*/LAKEHOUSE_*
# feed lakehouse.config.load_settings() (env overrides the yaml profile).
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


# Run-date macros for the SparkApplication name and the `--dt` argument.
#
# Only `dag_run` is guaranteed in the template context: on an Airflow 3 manual run
# the built-in `ds`/`ds_nodash` and the bare `logical_date` var are undefined, so
# referencing them raises. `dag_run.logical_date` is None on manual runs and set on
# scheduled ones, and `dag_run.run_after` is always present — so this expression
# yields the scheduled date when there is one and the trigger time otherwise.
#
# strftime is double-quoted because yaml.dump serializes these macros as
# single-quoted YAML scalars, which would double any inner single quote and break
# the Jinja. Double quotes survive serialization unchanged.
_RUN_DT = "(dag_run.logical_date or dag_run.run_after)"
DS = "{{ " + _RUN_DT + '.strftime("%Y-%m-%d") }}'
DS_NODASH = "{{ " + _RUN_DT + '.strftime("%Y%m%d") }}'


def _dns_safe(value: str) -> str:
    """Make a fragment safe for a SparkApplication metadata.name (RFC 1123)."""
    return value.replace("_", "-").lower()


def render(name: str, job_file: str, arguments: list[str]) -> str:
    """Return a SparkApplication manifest as a YAML string.

    `name` should already include a per-run suffix (e.g. ``...-{{ ds_nodash }}``)
    so re-runs don't collide. `job_file` is the entrypoint basename in
    /opt/lakehouse/jobs. `arguments` are passed through to the job.
    """
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
            "sparkConf": dict(_BASE_SPARK_CONF),
            "volumes": _CONFIG_VOLUME,
            "driver": {
                "cores": 1,
                "memory": "512m",
                "serviceAccount": "spark",
                "env": list(_SECRET_ENV),
                "volumeMounts": _CONFIG_MOUNT,
            },
            "executor": {
                "cores": 1,
                "instances": 2,
                "memory": "512m",
                "env": list(_SECRET_ENV),
                "volumeMounts": _CONFIG_MOUNT,
            },
        },
    }
    return yaml.dump(manifest, Dumper=_NoAliasDumper, sort_keys=False)


def render_bronze(channel: str, table: str, dt: str = DS) -> str:
    name = f"bronze-{_dns_safe(channel)}-{_dns_safe(table)}-{DS_NODASH}"
    return render(
        name,
        "bronze_job.py",
        ["--channel", channel, "--table", table, "--dt", dt, "--schedule-type", "daily"],
    )


def render_silver(channel: str, table: str) -> str:
    name = f"silver-{_dns_safe(channel)}-{_dns_safe(table)}-{DS_NODASH}"
    return render(name, "silver_job.py", ["--channel", channel, "--table", table])


def render_gold(mart: str) -> str:
    name = f"gold-{_dns_safe(mart)}-{DS_NODASH}"
    return render(name, "gold_job.py", ["--mart", mart])
