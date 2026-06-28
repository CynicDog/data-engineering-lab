# Design Principles: Spark + Airflow in Production

These are declarative principles for how Spark and Airflow should work together in a well-designed data platform. They are not prescriptions about hardware — a lab running on a single laptop and a production fleet running on hundreds of nodes should both satisfy them.

---

## Orchestration (Airflow)

**Airflow owns scheduling, not execution.**  
Airflow decides *when* a job runs and *what* depends on what. It does not run Spark code in-process, does not hold data in memory, and does not make execution decisions that belong inside Spark. The moment a task submits a SparkApplication CRD, Airflow's job is done.

**Every task dependency should be grounded in a real data dependency.**  
A downstream task should only trigger because its upstream *data* is fresh — not because a clock fired, not because a sibling task in the same DAG finished, not because all jobs in the previous layer ran. Each consumer knows which specific tables it reads; its schedule should be expressed in terms of exactly those assets, no more.

**Asset scheduling is the correct level of abstraction.**  
Cross-DAG dependencies belong to the data model (what tables exist, what reads from what), not to DAG-level polling or time offsets. An asset-based schedule is a contract: this DAG runs when these specific data artifacts are ready. Sensors and `ExternalTaskSensor` are workarounds for the absence of this contract.

**Failure visibility is part of the design.**  
Every production task should have an `execution_timeout` and an `on_failure_callback`. A pipeline that fails silently is not a pipeline — it is a liability. SLA windows should reflect real business requirements (when do downstream consumers need this data?), not arbitrary time buffers.

**Task granularity should match retry granularity.**  
The right task boundary is the smallest unit you would want to retry independently. If a silver transform for one table fails, you should be able to rerun only that table, not the entire layer. If three gold marts are independent, they should be three independent tasks (or DAGs), not one task that builds all three.

---

## Distributed Compute (Spark)

**Spark's job is to distribute computation, not to be a subprocess.**  
Running Spark in `local[N]` mode (in-process, on the Airflow worker) forfeits everything Spark promises: fault tolerance, executor isolation, independent scaling. Production Spark runs in cluster mode, with driver and executors on separate nodes, and communicates back only a result — never raw data.

**Declare your optimization intent explicitly.**  
Adaptive Query Execution (AQE) is on by default, but defaults are invisible. Explicitly enabling `spark.sql.adaptive.coalescePartitions.enabled` and `spark.sql.adaptive.skewJoin.enabled` signals intent, enables per-environment tuning, and makes the query plans observable in the Spark UI. Don't rely on behavior you haven't declared.

**Broadcast small tables explicitly.**  
When one side of a join is small and bounded (a dimension table, a lookup table, a reference dataset), use `F.broadcast()`. The sort-merge join Spark falls back to when the optimizer miscalculates the size is a full shuffle of *both* sides — the most expensive join possible. Explicit broadcast is a statement of domain knowledge that the optimizer cannot derive from statistics alone.

**Cache when a computation feeds both an action and a write.**  
A Spark transformation plan is lazy. If you call `.count()` after `.write()`, you evaluate the entire plan twice — reading from storage twice, recomputing every transformation twice. The correct pattern is: `.cache()` the result, then `.count()`, then `.write()`, then `.unpersist()`. One evaluation, two consumers.

**Write strategies should match the update semantics of the data.**  
A full `mode("overwrite")` is correct for a complete replacement (e.g., a gold mart that aggregates all history on every run). A partition-aware `replaceWhere` is correct for idempotent date-level rewrites (e.g., bronze ingestion). A `MERGE INTO` is correct for upsert semantics (e.g., a silver table that tracks the current state of records with a primary key). Choosing the wrong strategy either wastes compute (full overwrite of historical data that didn't change) or corrupts state (overwriting records that should be updated-in-place).

**Delta tables require active maintenance.**  
Every write to a Delta table creates new Parquet files. Without periodic `OPTIMIZE`, a table that is written daily accumulates small files faster than scans can read them. Without `VACUUM`, old file versions consume storage indefinitely. Maintenance is not optional cleanup — it is part of the write lifecycle and should be automated as a scheduled DAG.

---

## The Boundary Between Them

**Airflow should not know Spark internals. Spark should not know Airflow internals.**  
The coupling point is a job submission (a SparkApplication CRD, a spark-submit call, an API request to a job service). On the Airflow side: task ID, timeout, retry count, asset outlets. On the Spark side: job arguments, resource config, business logic. Neither side reaches into the other.

**Retries belong at the right layer.**  
Airflow retries a *task* — the full job submission and wait cycle. Spark retries a *task stage* — individual executor-level failures within a running job. These are separate concerns operating at different granularities. Airflow's retry handles job-level failures (driver crash, image pull failure, API timeout). Spark's retry handles executor-level failures (OOM, network partition, shuffle fetch error). Configure both independently.

**Resource decisions are Spark's responsibility, not Airflow's.**  
Airflow submits a job. How many executors to use, how much memory to allocate per executor, whether to scale dynamically based on data volume — these belong in the SparkApplication spec or in Spark's own adaptive machinery (dynamic allocation, AQE). Hardcoding executor count in Airflow DAG parameters couples the orchestration layer to infrastructure decisions that should be opaque to it.

---

## On Scalability

Scalability is a property of the design, not the hardware. A system designed to be scalable today, running on small resources, becomes a production system by increasing resources — not by changing its design.

**The job profile pattern** is the central mechanism. DAG code selects a named profile (`"small"`, `"medium"`, `"large"`); the profile drives memory, cores, Kubernetes resource requests/limits, and dynamic allocation bounds. When a table outgrows the small profile, one argument changes at the call site. Nothing else changes — not the DAG logic, not the Spark job code, not the Kubernetes manifests.

**Dynamic allocation** decouples executor count from the job definition. Instead of declaring `instances: 2` (a static fact about the job that ages poorly as data volume changes), the job declares `minExecutors` and `maxExecutors` (bounds within which Spark self-adjusts based on actual task queue depth). A job that processes 1,000 rows and a job that processes 100 million rows can share the same profile definition — the allocator picks the right executor count at runtime.

**Kubernetes resource requests and limits** are distinct from Spark's memory view. Spark's `memory` field is the JVM heap. `memoryOverhead` is the off-heap cost (GC, native buffers, the JVM itself). The Kubernetes pod's total memory consumption is their sum. Resource requests tell the Kubernetes scheduler how much memory to reserve for placement; limits tell the kernel the hard ceiling. Setting both correctly is what prevents executor pods from being evicted mid-shuffle because they consumed more than the node thought was allocated to them.

**Namespace ResourceQuota** bounds total consumption across all pods. Without it, a misconfigured profile or a runaway job can exhaust node memory and evict control-plane pods (the Airflow scheduler, the Spark Operator). The quota ensures that one bad job cannot bring down the platform.

**Executor topology spread** is a soft scheduling preference that distributes executor pods across nodes when multiple nodes are available. On a single-node lab it is a no-op. On a multi-node cluster it prevents all executors from landing on one node — which would make horizontal scaling meaningless.

## On Hardware Constraints

Running on a laptop with 8GB and a single Kind node is a valid environment for proving architectural correctness. The design patterns above — profile-driven sizing, dynamic allocation, explicit AQE, broadcast hints, cache-then-write, topology spread — are all active in this environment, and all are the same patterns that Netflix, Uber, and Pinterest run in production. The behavior scales with data volume; the design does not need to change.
