# Pinterest: Spinner, Archer, and the 96% OOM Reduction

## Context

Pinterest runs one of the largest image and recommendation systems in the world, with petabytes of behavioral data flowing through their platform daily. Their data platform powers content ranking, ad targeting, and creator analytics. By 2024, they had completed a major migration: from a Hadoop-based Spark cluster to Spark-on-Kubernetes running on AWS EKS, orchestrated through a custom Airflow-based system called **Spinner** that submits jobs through a submission service called **Archer**.

Their most publicized outcome from this migration: a **96% reduction in Spark OOM failures** through better observability and per-job memory management.

## Scale

- Thousands of Spark jobs running daily
- AWS EKS as the Kubernetes substrate
- Apache YuniKorn as the batch scheduler (gang scheduling, fair queuing)
- 96% reduction in OOM failures after migration

## Spark + Airflow Integration

### Spinner + Archer: the two-tier model

Pinterest follows the same abstraction pattern as Uber (Piper + USCS) but with a different implementation:

**Spinner** is an Airflow-based orchestration layer. Engineers define DAGs in Spinner using pre-built operators. Spinner handles scheduling, dependency management, SLA tracking, and failure alerting. It does not know or care about Kubernetes.

**Archer** is the Spark job submission service. Archer receives a job submission request from Spinner (via an HTTP API call), translates it into a SparkApplication CRD, submits it to the Kubernetes API, and monitors the job lifecycle. Archer handles:
- Backend selection (different EKS node pools for different workload classes)
- Pod template application (light/standard/heavy resource profiles)
- Retry-on-OOM with memory bump: if a job fails with an OOM error, Archer automatically retries with a higher memory allocation rather than propagating the failure to Spinner
- Shuffle tracking configuration (Spark 3.0+ dynamic allocation without external shuffle service)

The separation means Spinner's operators are stable even as Archer's Kubernetes configuration evolves. Engineers writing pipelines in Spinner never specify executor memory or node pool affinity — those are Archer's concerns.

### Solving the OOM problem

The 96% OOM reduction came from three changes working together:

**1. Per-job observability via Fluent Bit**

Pinterest deployed Fluent Bit as a DaemonSet on every Kubernetes node. Fluent Bit collects stdout/stderr from every Spark executor pod and forwards it to S3 and CloudWatch. This means that when an executor dies with an OOM error, the error appears in CloudWatch within seconds — not buried in Kubernetes event logs that expire after an hour.

Before Fluent Bit: engineers found out about OOM failures from downstream SLA alerts (the pipeline didn't finish). After Fluent Bit: engineers saw OOM patterns in CloudWatch dashboards before they became SLA breaches.

**2. Automatic retry with memory bump**

Archer implements an OOM retry policy at the submission layer: if a SparkApplication terminates with an exit code indicating OOM, Archer automatically resubmits the job with executor memory multiplied by 1.5 (up to a configured maximum). The retry is transparent to Spinner — from Spinner's perspective, the task eventually succeeded.

This eliminates a class of failures that previously required manual intervention: the engineer would investigate the OOM, adjust the memory config in the DAG, and re-trigger the pipeline. Now Archer handles this automatically for predictable cases.

**3. Workload class profiles**

Archer maintains three job profiles:
- **Light**: 2 executors, 4GB each — small dimension tables, metadata jobs
- **Standard**: 8 executors, 8GB each — daily behavioral aggregations
- **Heavy**: 32 executors, 16GB each — full-history recomputation, feature store backfills

DAG authors specify a profile (not specific memory/executor values). Archer maps the profile to the appropriate resource request and node pool affinity. When cluster capacity changes, Archer's profiles are updated once — not per-DAG.

### YuniKorn: batch scheduling on Kubernetes

Standard Kubernetes scheduling is not designed for batch workloads. It does not understand gang scheduling (all executors for a job must start together or not at all) or fair queuing across multiple teams. Pinterest chose **Apache YuniKorn** as an alternative scheduler that runs alongside the Kubernetes default scheduler.

YuniKorn provides:
- **Gang scheduling**: if a job requests 32 executors, YuniKorn holds the driver until all 32 can be allocated simultaneously. This prevents partial-start deadlocks where the driver sits waiting for executors that are blocked by other drivers.
- **Hierarchical fair queuing**: each team gets a queue with a guaranteed share of cluster capacity. Burst usage above the quota is allowed when capacity is available but doesn't starve other teams.
- **Preemption**: low-priority jobs can be preempted to make room for high-priority ones within the same team's quota.

### Dynamic allocation without external shuffle service

Pinterest uses Spark's dynamic allocation with shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled = true`), which was introduced in Spark 3.0. Before shuffle tracking, dynamic allocation required an external shuffle service running as a DaemonSet on every node — a stateful, complex piece of infrastructure. Shuffle tracking eliminates this requirement by tracking which executors hold live shuffle data and preventing their deallocation until the shuffle is consumed.

Pinterest's configuration:
```
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 100
spark.dynamicAllocation.shuffleTracking.enabled = true
spark.dynamicAllocation.shuffleTracking.timeout = 24h
```

## Lessons for this project

- **Observability first, optimization second** — Pinterest did not reduce OOM failures by guessing at memory configurations. They reduced OOM failures by making failures visible (Fluent Bit → CloudWatch) and then building automated responses (retry with bump). The sequence matters.
- **The abstraction layer (Archer) enables automatic remediation** — a retry-with-memory-bump policy is only possible because Archer sits between orchestration and Kubernetes. If Spinner talked to Kubernetes directly, this logic would have to live in every DAG.
- **Dynamic allocation with shuffle tracking eliminates a class of infrastructure complexity** — the external shuffle service DaemonSet is gone. For new Kubernetes deployments, start with shuffle tracking.
- **Profiles > explicit resource values** — DAG authors specifying explicit executor memory is the wrong level of abstraction. Job profiles owned by the platform team and referenced by name in DAGs is the right level.

## References

- *From Hadoop to Kubernetes: Pinterest's Scalable Spark Architecture on AWS EKS*, InfoQ, 2025. https://www.infoq.com/news/2025/07/pinterest-spark-kubernetes/
- *Inside Pinterest's Custom Spark Job Logging and Monitoring on Amazon EKS Using Fluent Bit*, AWS Blog. https://aws.amazon.com/blogs/containers/inside-pinterests-custom-spark-job-logging-and-monitoring-on-amazon-eks-using-aws-for-fluent-bit-amazon-s3-and-adot/
- *Spinner: Pinterest's Workflow Platform*, Pinterest Engineering Blog. https://medium.com/pinterest-engineering/spinner-pinterests-workflow-platform-c5bbe190ba5
- *Pinterest Reduces Spark OOM Failures by 96%*, InfoQ, 2026. https://www.infoq.com/news/2026/04/pinterest-spark-oom-reduction/
- Apache YuniKorn project: https://yunikorn.apache.org
