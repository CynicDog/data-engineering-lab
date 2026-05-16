# airflow_cloud_native

A hands-on lab for orchestrating cloud-native data pipelines with **Apache Airflow 3.x**, exploring three different architectures:

| Sub-project | Pattern | Idea in one line |
|---|---|---|
| [`lambda/`](./lambda) | **Lambda architecture** | Two physical layers — a slow, correct batch path and a fast, approximate speed path — reconciled at query time. |
| [`kappa/`](./kappa) | **Kappa architecture** | One physical layer — a single stream pipeline that you *replay from the log* whenever you need a new view. |
| [`medallion_lakehouse/`](./medallion_lakehouse) | **Medallion (Bronze/Silver/Gold)** | Three layered, contract-bounded zones on object storage — raw landings, conformed entities, business marts — driven by dbt + DuckDB. |

Each sub-project is **fully self-contained**: its own `docker-compose.yaml`, its own Airflow stack, its own `pyproject.toml`. You can bring one up without the others.

## Shared shape

All three stacks share the same backbone:

- **Airflow 3.x** (api-server + scheduler + dag-processor + triggerer; ± celery worker) on Postgres
- **MinIO** as the S3-compatible object store for Bronze/Silver/Gold (or Delta) data
- **Simple Auth Manager** with `admin` / `admin` seeded via `config/simple_auth_manager_passwords.json`

The streaming stacks (`lambda/`, `kappa/`) add **Redpanda** + Redis; the lakehouse stack adds **dbt-duckdb** for SQL-first transforms.

Python compute is intentionally lightweight — no Spark cluster — using **Polars**, **PyArrow**, **delta-rs** (`deltalake`), and **DuckDB** so the data layer is a single pip install away.

## Why separate projects?

Running them side-by-side as one mega-stack would obscure where each architecture differs. Keeping them physically separate makes the comparison tactile:

- Open `lambda/dags/` — *batch* DAG + *speed* consumer + *serving merge*.
- Open `kappa/dags/` — one stream DAG + a *replay* DAG. No batch/speed distinction.
- Open `medallion_lakehouse/dags/` — *ingest* + *silver* + *gold* + *data quality*, with dbt models driving Silver and Gold (one `dbt build --select tag:<layer>` per DAG).

The compose stacks use different host ports so you can run them side-by-side if you want.

| Service | Lambda | Kappa | Medallion |
|---|---|---|---|
| Airflow UI | 8080 | 8090 | 8100 |
| Redpanda broker | 19092 | 29092 | — |
| Redpanda Console | 8081 | 8091 | — |
| MinIO console | 9001 | 9011 | 9021 |
| Postgres | 5432 | 5442 | 5452 |

## Getting started

```bash
cd lambda    # or: cd kappa, cd medallion_lakehouse
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up airflow-init   # one-time DB migrate
docker compose up -d
```

Then open the relevant Airflow UI (`admin` / `admin`) and unpause the DAGs.

See each sub-project's `README.md` for the actual pipeline walkthrough, testing instructions, and cleanup steps.

---

# Airflow Cheat Sheet

A reference for the Airflow 3.x concepts and components used across all three sub-projects, with pointers back to the actual code where each one shows up.

## Core data model

| Concept | What it is | Where it lives |
|---|---|---|
| **DAG** | A directed-acyclic graph definition — *what* should run and in what order. Python module under `dags/`. | `kappa/dags/stream_pipeline_dag.py` |
| **Task** | A node in the DAG — *one unit of work*. Defined via the `@task` decorator or a classic Operator. | `BashOperator(task_id="build", ...)` in `silver_dag.py` |
| **DagRun** | One *instance* of executing a DAG — bound to a `logical_date` and a `data_interval_[start,end)` window. | Row in `dag_run` table; visible in the UI per scheduled tick. |
| **TaskInstance** | One execution of one task within one DagRun. Has its own state, try_number, log, XCom outputs. | Row in `task_instance` table. |
| **XCom** | Cross-Communication — small key/value payloads tasks publish for downstream tasks to read. The `@task` return value is implicitly pushed. | `xcom` table. Use `ti.xcom_pull(...)` to read. |

The mental model: a **DAG** is the recipe, a **DagRun** is one execution of the recipe, **TaskInstances** are the steps inside that execution.

## The processes that make it run

An Airflow 3.x stack is **not** a monolith — it's a handful of independent processes coordinating through Postgres. The three lab stacks all spin up the same set (medallion drops the worker because it uses LocalExecutor):

| Process | Job | Where you see it |
|---|---|---|
| **api-server** | The web UI + the REST + execution API. Task subprocesses report state here over HTTP. Replaces 2.x's "webserver". | Port 8080 (lambda) / 8090 (kappa) / 8100 (medallion). |
| **scheduler** | Watches the DB for DagRuns that need to start, decides which TaskInstances to schedule, hands them to the executor. | One per stack. Health: `localhost:8974/health` inside the container. |
| **dag-processor** | Continuously parses the `dags/` folder, validates DAGs, writes/updates DAG records in the metadata DB. (In 2.x this was inside the scheduler.) | A separate container in all three stacks. |
| **triggerer** | Hosts **deferrable** operators — runs lightweight async waits (e.g. "wake me when this S3 object exists") on a single process, so 10,000 wait-for-X tasks don't tie up 10,000 worker slots. | Present in all three stacks; idle until you use a deferrable operator. |
| **worker** (Celery only) | Pulls tasks off the Redis queue and runs them. | `airflow-worker` in lambda/kappa. Medallion has none — LocalExecutor runs tasks inside the scheduler container. |

> Why split them? Each one scales independently. You can run 1 api-server + 5 schedulers + 50 workers. You can also redeploy the api-server without restarting in-flight tasks.

## Executors — *who actually runs the task*

| Executor | How tasks run | Use it when | Used in |
|---|---|---|---|
| **LocalExecutor** | Scheduler forks subprocesses on the same host. No external queue. | Dev, small stacks. | `medallion_lakehouse/` |
| **CeleryExecutor** | Tasks enqueued in Redis (or RabbitMQ); workers consume and run them on other hosts. | Multi-host, horizontally scalable. | `lambda/`, `kappa/` |
| **KubernetesExecutor** | Each task spawns its own pod, dies after. | When you want pod-level isolation / unique resource limits per task. | Not used in this lab. |
| **CeleryKubernetesExecutor** | Hybrid — most tasks via Celery, a subset via Kubernetes (using `executor_config`). | Mixed workloads. | Not used here. |

The executor is set via `AIRFLOW__CORE__EXECUTOR`. Switching is a one-line env change *for the same DAG code*.

## Scheduling — *when DagRuns are created*

DAGs declare `schedule=...` on the `@dag` decorator. Airflow 3 accepts five very different things in that slot:

| Schedule kind | Example | Semantics |
|---|---|---|
| **Cron string** | `schedule="@hourly"` or `"0 0 * * *"` | Classic cron tick. |
| **`timedelta`** | `schedule=timedelta(minutes=1)` | Fixed interval from `start_date`. |
| **Presets** | `"@daily"`, `"@hourly"`, `"@once"`, `None` | Aliases / disable scheduling. |
| **Asset list** (data-aware) | `schedule=[Asset("s3://lakehouse/silver")]` | Fire on **any** of the listed assets emitting a new event. |
| **Asset expression** | `schedule=AssetAll([a, b])` or `AssetAny([a, b])` | AND / OR combinations. |

`schedule=None` means *manual trigger only* (used in `kappa/dags/replay_dag.py`).

### Data interval — the most common gotcha

When the scheduler creates a DagRun, it computes a *closed past window*:

```
data_interval_start ──────── data_interval_end ──────── logical_date / run_after
```

- **`data_interval_end`** is the *end of the window the run is responsible for*, **not** wall-clock now. For an `@hourly` DAG firing at 13:05, `data_interval_end = 13:00`, `data_interval_start = 12:00`. The DAG is processing *the previous hour*.
- **`logical_date`** is Airflow 3's name for what 2.x called `execution_date` — the moment the DagRun is logically anchored to. For most schedules it equals `data_interval_end`.

These are injected as **keyword arguments** into your task function if you declare them:

```python
@task
def process(data_interval_end: datetime, data_interval_start: datetime = None):
    ...
```

We use this in `lambda/dags/batch_layer_dag.py` and `medallion_lakehouse/dags/bronze_dag.py`.

## Triggering — *what actually fires a DagRun*

Five distinct triggering mechanisms in Airflow 3, and they're commonly confused:

| Trigger source | What it does | Example |
|---|---|---|
| **The scheduler clock** | Cron / `timedelta` ticks. | Every stack. |
| **Manual trigger** | `airflow dags trigger <dag_id>` or the UI "Trigger DAG" button. Required when `schedule=None`. | `kappa/replay_pipeline`. |
| **Asset event** | A task with `outlets=[...]` succeeds → an "asset event" gets recorded → any DAG with `schedule=[that asset]` gets a fresh DagRun queued. | `silver_models` and `gold_models` in medallion. |
| **Sensors / deferrable operators** | A *task* (not a DAG trigger) that blocks waiting for an external condition — file appearing, time of day, another DAG's state. Deferrable variants release the worker slot. | None in this lab; this is where the triggerer process earns its keep. |
| **TriggerDagRunOperator** | One DAG explicitly fires another DAG (as a task). | None in this lab — we prefer asset-driven cross-DAG dependencies. |

## Assets (formerly Datasets in 2.x)

An **Asset** is a named handle on a piece of external state. In Airflow 3 it's the canonical way to express cross-DAG dependencies and to decouple "I produced X" from "I depend on X".

```python
from airflow.sdk.definitions.asset import Asset

SILVER_ASSET = Asset("s3://lakehouse/silver")

# Producer: declare an outlet on a task
@task(outlets=[SILVER_ASSET])
def build(): ...

# Consumer: schedule a DAG on the asset
@dag(schedule=[SILVER_ASSET])
def gold_models(): ...
```

Key properties:
- **The Asset URI is just an identifier**, not a data channel. Airflow doesn't read or move data — your task reads from MinIO/Postgres/wherever directly. The Asset is purely the *signal*.
- **OR semantics** for a list: `schedule=[a, b, c]` fires when *any* one emits. For AND use `AssetAll([a, b, c])`.
- **URI normalizers** can validate by scheme — e.g. `postgres://` is rejected unless the path is `<host>/<db>/<schema>/<table>`. We hit that early in this lab.

The Airflow UI's **Assets** view gives you a cross-DAG lineage graph driven entirely by these declarations.

## Trigger rules — *when this task should run, given upstream state*

By default a task runs when *all* upstream tasks have succeeded. Override with `trigger_rule=` on the task:

| Trigger rule | Run when upstream is… | Use it for |
|---|---|---|
| `all_success` (default) | all succeeded | normal happy path |
| `all_failed` | all failed | "on failure" notifier tasks |
| `all_done` | all finished, success or fail | always-run cleanup |
| `one_success` | at least one succeeded | branch-merging fan-in |
| `one_failed` | at least one failed | fast-failure alerting |
| `none_failed` | all succeeded or skipped | tolerate skipped branches |
| `none_failed_min_one_success` | none failed AND ≥1 succeeded | branch-merge that tolerates skip |
| `always` | unconditionally | rare — debugging only |

## Task context — variables Airflow injects

Declared as kwargs on your task function or accessed via `kwargs["..."]` / `get_current_context()`:

| Variable | Meaning |
|---|---|
| `data_interval_start`, `data_interval_end` | The window this run owns (see above). |
| `logical_date` | The DagRun's logical timestamp (= `data_interval_end` for most schedules). |
| `run_id` | Unique per DagRun, e.g. `scheduled__2026-05-16T13:00:00+00:00`. |
| `dag_run` | The full `DagRun` object — `.conf` for manual-trigger params. |
| `task_instance` (`ti`) | The TaskInstance for this run. Use `ti.xcom_pull(...)`. |
| `params` | Per-DAG `params={...}` declared on `@dag`. |

Manual trigger with config (used by `kappa/replay_pipeline`):

```bash
airflow dags trigger replay_pipeline --conf '{"since": "2026-01-01T00:00:00Z"}'
```

Inside the task: `dag_run.conf["since"]` or via `params` if declared with a default.

## DAG-level config

```python
@dag(
    dag_id="batch_layer",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=1),
    catchup=False,                # don't backfill missed runs since start_date
    max_active_runs=1,            # at most one DagRun in flight (prevents stampede)
    tags=["lambda", "batch"],
    default_args={"retries": 0, "retry_delay": timedelta(minutes=2)},
)
def batch_layer(): ...
```

- `catchup=True` makes Airflow create DagRuns for every missed interval since `start_date` — useful for replayable batch, dangerous for live streams.
- `max_active_runs=1` is what keeps minutely DAGs from piling up if one tick is slow.
- `default_args` applies to every task unless the task overrides.

## Task-level config

```python
@task(
    task_id="ingest_orders",
    outlets=[BRONZE_ORDERS_ASSET],
    retries=2,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(hours=1),
    pool="bronze_ingest",         # rate-limit concurrent tasks of this kind
    queue="celery_default",       # which Celery queue to route to
    execution_timeout=timedelta(minutes=10),
)
def ingest(...): ...
```

Pools are useful for protecting downstream systems — e.g. `pool="warehouse_writes"` with `slots=4` caps concurrent writes regardless of executor parallelism.

## TaskFlow API vs classic Operators

Two ways to write the same task. Both used in this lab.

```python
# TaskFlow — Python-native, return value becomes XCom automatically.
@task
def process(data_interval_end: datetime) -> dict:
    return run_batch(window_end=data_interval_end)

# Classic Operator — pre-built for shelling out, SQL, S3, k8s, etc.
BashOperator(
    task_id="build",
    bash_command="cd /opt/airflow/dbt_project && dbt build --select tag:silver",
    outlets=[SILVER_ASSET],
)
```

Rule of thumb: **TaskFlow** for Python work, **Operators** for "use this pre-built integration".

## What changed in Airflow 3 (vs 2.x)

If you're cross-referencing tutorials or stackoverflow answers, watch out for these:

| 2.x | 3.x |
|---|---|
| `airflow.decorators.dag/task` | `airflow.sdk` (`from airflow.sdk import dag, task`) |
| `webserver` process | `api-server` |
| `Dataset` | `Asset` |
| `execution_date` | `logical_date` (data_interval_end usually matches) |
| `airflow users create` | Removed — auth managers are pluggable. Simple Auth Manager (default) reads `simple_auth_manager_passwords.json`. FAB Auth Manager re-adds CLI user CRUD. |
| Tasks talk directly to the metadata DB | Tasks talk to the **api-server** over HTTP via the **Task Execution API**. Requires `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` and a shared `AIRFLOW__API_AUTH__JWT_SECRET`. |
| `AIRFLOW__API__AUTH_BACKENDS` | No-op under auth-manager model (harmless to leave). |

## Common pitfalls (we hit all of these)

1. **Task says queued, executor says failed.** Worker can't reach the api-server, or its JWT doesn't match. Check `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` and `AIRFLOW__API_AUTH__JWT_SECRET` are set on *every* Airflow service and identical across them.
2. **`KeyError: 'getpwuid(): uid not found: 501'`.** You overrode `entrypoint: /bin/bash` on `airflow-init` and bypassed the image's `/etc/passwd` patching. Use `command: db migrate` and let the default entrypoint run.
3. **`Postgres URI must contain database, schema, and table`.** The postgres provider's Asset URI normalizer is strict. Use `postgres://<host>/<db>/<schema>/<table>` — four path segments, not two.
4. **`Read-only file system: simple_auth_manager_passwords.json.generated`.** Don't mount that file `:ro` — Simple Auth Manager opens it in `a+` mode on every boot.
5. **`SyntaxError: cannot insert multiple commands into a prepared statement`.** psycopg3 rejects multi-statement SQL via `conn.execute()`. Split DDL + DML into separate `execute()` calls.
6. **`DeltaProtocolError: Atomic rename requires a LockClient for S3 backends`.** delta-rs refuses to write to S3 without DynamoDB locking. For single-writer setups, opt out with `AWS_S3_ALLOW_UNSAFE_RENAME=true` in `storage_options`.
7. **`DAG state changed externally`.** The worker died mid-task — usually OOM or import error before our task code ran. Check `docker compose logs airflow-worker` for the actual stack trace.

## CLI cheatsheet (run inside any airflow container)

```bash
# Inspect
airflow dags list
airflow dags details <dag_id>
airflow tasks list <dag_id>

# Manually fire a single DagRun (the fastest dev loop — no scheduler wait)
airflow dags test <dag_id>
airflow dags test <dag_id> --conf '{"key": "value"}'

# Run one task inside an existing DagRun
airflow tasks test <dag_id> <task_id> <logical_date>

# Pause / unpause
airflow dags pause <dag_id>
airflow dags unpause <dag_id>

# Metadata DB
airflow db migrate
airflow db check
airflow db shell

# Connections / variables / pools
airflow connections list
airflow variables list
airflow pools list
```

## Where logs and state live

| Thing | Location |
|---|---|
| Task logs | `logs/dag_id=<X>/run_id=<Y>/task_id=<Z>/attempt=<N>.log` (mounted as a volume) |
| DAG parse errors | `dag-processor` container logs (`docker compose logs airflow-dag-processor`) |
| Scheduler decisions | `airflow-scheduler` logs |
| Metadata (DagRuns, TIs, XComs, Assets, ...) | Postgres `airflow` DB |
| Asset events | `asset_event` table |
| Simple-auth-manager users | `/opt/airflow/simple_auth_manager_passwords.json.generated` inside the container |

