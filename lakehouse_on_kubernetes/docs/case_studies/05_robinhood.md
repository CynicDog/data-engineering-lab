# Robinhood: Multi-Cluster Airflow and the Job Management Service

## Context

Robinhood's data platform powers real-time trading analytics, regulatory reporting, fraud detection, and customer-facing portfolio metrics. Their infrastructure needs are unusual in financial services: they must meet strict data latency SLAs (regulatory reports have hard deadlines), process event streams during market hours (high burst, predictable schedule), and operate at a compliance level that requires data lineage and auditability for every pipeline.

Their data platform has evolved through two major phases. V1 was Spark on Hadoop with Airflow on top. V2 introduced a **Job Management Service (JMS)** between Airflow and the compute layer, enabling multi-backend routing and a migration from Hadoop to Kubernetes without rewriting any DAGs.

## Scale

- 15 separate Airflow clusters (different teams, environments, and compliance boundaries)
- 4,000+ active data pipelines
- Multi-backend: Hadoop YARN and Kubernetes, routed transparently by JMS
- Financial services compliance requirements: full lineage, auditability, and data retention

## Spark + Airflow Integration

### 15 Airflow clusters: why not one?

The natural instinct is to centralize: one Airflow cluster, one place to look. Robinhood went the other direction. Their 15 clusters exist because:

- **Compliance boundaries**: pipelines that handle regulated financial data (e.g., trading records, customer PII) run in isolated clusters with stricter access controls and audit logging. Mixing them with general analytics pipelines creates compliance surface area.
- **Team autonomy**: different engineering teams have different upgrade cadences, plugin requirements, and oncall rotations. A shared cluster means shared blast radius.
- **Environment isolation**: dev, staging, and production each need their own scheduler state. Sharing Airflow state across environments creates subtle bugs where production pipelines see dev metadata.

Robinhood manages the coordination overhead through standardized DAG templates, shared operator libraries, and cross-cluster dependency tracking (a task in cluster A can trigger a check in cluster B via an API).

### The Job Management Service

JMS is Robinhood's version of the abstraction layer between orchestration and compute. Airflow tasks call JMS's API to submit a job. JMS then:

1. **Routes to the appropriate backend**: Hadoop for legacy jobs with external shuffle service dependencies; Kubernetes for new jobs that use shuffle tracking.
2. **Applies resource profiles**: like Pinterest's Archer, JMS maintains per-team and per-job-class resource profiles. DAG authors specify a profile name, not specific memory values.
3. **Handles job lifecycle monitoring**: JMS polls the backend and updates Airflow via callback when the job terminates. This means the Airflow task slot is held only during submission, not during the entire job execution.
4. **Enforces quotas**: each team has a compute budget enforced at the JMS layer. A team that exceeds its quota gets queued, not rejected — JMS holds the submission until capacity frees up.

The payoff: when Robinhood migrated from Hadoop to Kubernetes, existing DAGs did not change. JMS's routing logic was updated; every pipeline that went through JMS automatically ran on the new backend.

### Airflow scaling: per-cluster configuration

Each of Robinhood's 15 clusters is tuned independently:

- **KubernetesExecutor**: all worker pods run in Kubernetes. No long-lived worker processes; each task gets a clean pod.
- **Concurrency limits**: set per cluster based on the compute capacity behind JMS's queue for that cluster. Market-hours pipelines get higher parallelism limits than overnight batch.
- **Remote logging**: task logs are shipped to S3 immediately. Workers are ephemeral; logs must survive worker pod termination.

### Data lineage as a compliance requirement

Unlike companies where lineage is a nice-to-have for debugging, Robinhood treats data lineage as a regulatory requirement. Every pipeline that touches trading data must be able to answer: which source records contributed to this output? What version of the transformation logic ran? When did it run?

Their implementation: each task emits XCom metadata (source table, target table, job ID, row counts, transformation version). A lineage service consumes this metadata and builds a queryable graph. Auditors can trace any output record back to its raw source.

This connects directly to the design of `bronze/audit.py` in this project: the audit log exists for the same reason. At Robinhood's compliance level, that log would also include job IDs, source record hashes, and transformation code versions — not just row counts.

### Shuffle tracking configuration

Robinhood's Kubernetes deployment uses dynamic allocation with shuffle tracking:

```
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.shuffleTracking.enabled = true
spark.dynamicAllocation.shuffleTracking.timeout = 24h
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 50
```

No external shuffle service DaemonSet. Executor deallocation is blocked until shuffle data is consumed, with a 24-hour timeout to catch pipelines that finish shuffles but don't release executors promptly.

## Lessons for this project

- **Multiple Airflow clusters are sometimes the right answer** — the complexity of managing 15 clusters is real, but so is the compliance and team-autonomy benefit. Start with one; know when to split.
- **JMS/Archer/Frederator is a pattern, not a product** — the abstraction layer between orchestration and compute appears at every company that reaches sufficient scale. Building it (even simply) early pays off during infrastructure migrations.
- **Lineage is built in, not bolted on** — the audit log in this project is the seed of a lineage system. The design is right; the next step is making it queryable and cross-pipeline.
- **KubernetesExecutor is production-correct** — Robinhood runs KubernetesExecutor in production. No long-lived workers, no shared state between tasks. Each task's environment is deterministic.

## References

- *Why Robinhood Uses Airflow*, Robinhood Engineering Blog. https://medium.com/robinhood-engineering/why-robinhood-uses-airflow-aed13a9a90c8
- *Upgrading & Scaling Airflow at Robinhood*, Robinhood Newsroom. https://robinhood.com/us/en/newsroom/upgrading-scaling-airflow-at-robinhood/
- *Enhancing Efficiency: Robinhood's Batch Processing Platform*, Robinhood Newsroom. https://robinhood.com/us/en/newsroom/robinhoods-batch-processing-platform/
- *Airflow in Action: Robinhood*, Astronomer Blog. https://www.astronomer.io/blog/airflow-in-action-robinhood/
