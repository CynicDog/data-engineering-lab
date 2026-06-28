# Uber: Operating at 750,000 Tasks Per Day

## Context

Uber's data platform processes data for real-time dispatch, pricing, fraud detection, and driver/rider analytics across hundreds of cities simultaneously. By 2020, their Airflow deployment had grown to 200,000 pipelines running 450,000 DAG runs and 750,000 task executions per day. At that scale, Airflow itself became a distributed systems problem.

Uber's response was **Piper**: a heavily forked, internally maintained Airflow distribution with custom schedulers, execution backends, and a no-code pipeline authoring layer. They also built **USCS** (Uber Spark Compute Service) as the abstraction layer between orchestration and Spark execution, and **DataCentral** as the observability and cost attribution platform sitting above everything.

## Scale

- 200,000 active pipelines
- 450,000 DAG runs per day
- 750,000 task executions per day
- Multi-region deployment across Hadoop and Kubernetes clusters

## Spark + Airflow Integration

### Piper: Airflow at Uber's scale

Piper is what Airflow looks like after years of production hardening at extreme scale. Key divergences from upstream Airflow:

**Distributed scheduler**: Uber replaced Airflow's single-process scheduler with a distributed scheduler that partitions DAG ownership across multiple scheduler instances. This removes the single point of failure and the parsing bottleneck that appears when Airflow is running tens of thousands of DAGs.

**No-code pipeline authoring**: Piper includes a UI for non-engineers to define pipelines by connecting pre-built operator templates. Engineers define the operators; analysts and data scientists wire them together without writing Python. This dramatically expanded who could create data pipelines at Uber without corresponding growth in the engineering team.

**Event-driven triggers**: In addition to cron-based scheduling, Piper supports event-based triggers — a pipeline can fire when a Kafka message arrives, when a data asset is updated, or when another pipeline completes. This moves the platform toward a streaming-native model even for batch jobs.

### USCS: The abstraction between orchestration and Spark

Uber Spark Compute Service (USCS) is the routing layer between Piper and actual Spark execution. Piper tasks do not call `spark-submit` or interact with Kubernetes directly. They call USCS's API.

USCS then:
1. Selects the appropriate compute backend (Hadoop YARN or Kubernetes, depending on job type, queue depth, and cost)
2. Handles queue management and fair scheduling across teams
3. Enforces resource quotas per team
4. Manages job lifecycle (submission, monitoring, retry-on-OOM, cancellation)

From Piper's perspective, a Spark job is an API call with a payload. The infrastructure beneath it is opaque. This means Uber can migrate from Hadoop to Kubernetes (which they did, progressively) without rewriting any DAGs.

### DataCentral: Observability and cost attribution

DataCentral is Uber's observability platform for the data platform. It sits above Piper and USCS and provides:

- **Per-job cost attribution**: each job is tagged with the team and product that owns it. Cost per team per day is reported. Teams have budgets; exceeding budget triggers alerts.
- **Resource utilization tracking**: driver memory usage, executor OOM events, shuffle spill rates, and task skew are tracked per job over time. Teams can see whether their jobs are getting slower, using more memory, or hitting skew more often.
- **Anomaly detection**: if a job that normally takes 5 minutes suddenly takes 45 minutes, DataCentral flags it before the downstream SLA is breached.

The insight is that observability is not a dashboard added on top of a working system — it is a first-class design requirement. Without DataCentral, Uber would not know which teams are causing cluster hotspots, which jobs have latent OOM problems, or which pipelines are trending toward SLA breach.

### Hudi as the table format

Uber created **Apache Hudi** (Hadoop Upserts Deletes and Incrementals) specifically to solve the upsert problem at scale. Before Hudi, updating a record in a Parquet-based data lake meant rewriting the entire partition that contained it. At Uber's scale (billions of records per day with frequent updates), this was prohibitively expensive.

Hudi introduced two storage types:
- **Copy-on-Write (CoW)**: rewrites files on every update — fast reads, expensive writes
- **Merge-on-Read (MoR)**: writes deltas inline, merges on read — fast writes, slightly slower reads

Uber's use case: driver location history, trip records, and fraud signals all require frequent upserts. Hudi's MoR storage allows those upserts to land quickly, with periodic compaction (equivalent to Delta's OPTIMIZE) that merges the delta files into the base files.

The Airflow → Hudi integration: each pipeline that writes to a Hudi table has a downstream `hudi_compact` task that triggers compaction when the delta file ratio crosses a threshold. Compaction is not on a fixed schedule — it is triggered by data volume.

## Lessons for this project

- **At sufficient scale, the scheduler is an infrastructure problem** — Airflow's single-process scheduler hits limits at thousands of DAGs. The solution is not to switch orchestrators immediately; it is to monitor scheduler lag and act before it becomes a user-visible problem.
- **Abstraction layers pay off over time** — USCS let Uber migrate from Hadoop to Kubernetes without touching any of the thousands of DAGs that submit through it. This is the long-run payoff of keeping orchestration and execution separate.
- **Upserts are a first-class concern** — Hudi was built because full-partition overwrites don't scale for high-update tables. Delta's MERGE INTO addresses the same problem. Design your write strategy for the update semantics of your data, not for your current data volume.
- **Cost attribution drives better engineering** — when teams see their own compute costs in real time, they optimize. DataCentral's per-team cost visibility changed how engineers wrote Spark jobs at Uber.

## References

- *No Code Workflow Orchestrator for Building Batch & Streaming Pipelines at Scale*, Uber Engineering Blog. https://eng.uber.com/no-code-workflow-orchestrator/
- *Managing Uber's Data Workflows at Scale*, Uber Engineering Blog. https://www.uber.com/blog/managing-data-workflows-at-scale/
- *Making Apache Spark Effortless for All of Uber (USCS)*, Uber Engineering Blog. https://www.uber.com/blog/uscs-apache-spark/
- *DataCentral: Uber's Big Data Observability and Chargeback Platform*, Uber Engineering Blog. https://www.uber.com/en-IN/blog/datacentral-ubers-observability-and-chargeback-platform/
- Apache Hudi project (created at Uber): https://hudi.apache.org
