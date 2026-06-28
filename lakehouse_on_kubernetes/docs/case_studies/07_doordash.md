# DoorDash: Multi-Cluster Airflow and the Orchestration Frederator

## Context

DoorDash's data platform supports real-time logistics, dynamic pricing, restaurant analytics, and dasher (driver) performance systems. Their data engineering challenges are characteristic of a marketplace: three-sided (consumer, merchant, dasher), event-driven, and latency-sensitive. The data from a single delivery touches dozens of pipelines: order analytics, fraud signals, routing optimization feedback, and payment reconciliation.

DoorDash runs Airflow at scale across multiple clusters and has built an internal coordination layer called the **Orchestration Frederator** that manages cross-cluster dependencies and provides a unified view of pipeline state.

## Scale

- Multiple Airflow clusters (separated by team, environment, and data domain)
- SparkKubernetesOperator as the standard for Spark submission
- Cross-cluster pipeline dependencies managed by Orchestration Frederator

## Spark + Airflow Integration

### Airflow on Kubernetes: the operational model

DoorDash runs Airflow with KubernetesExecutor, following the same pattern as Robinhood and other mature deployments: each task gets a dedicated worker pod, ephemeral and isolated. Their infrastructure decisions:

- **Remote logging**: task logs are written to S3 immediately. Worker pod termination does not lose task history.
- **Shared DAG storage**: DAG files are stored in a shared volume (or synced via a git-sync sidecar) so all scheduler and worker pods see the same DAG definitions.
- **Resource limits per task**: KubernetesExecutor allows specifying CPU and memory limits per task. DoorDash uses this to prevent individual tasks from monopolizing node resources — a poorly-written task cannot starve other tasks running on the same node.

### SparkKubernetesOperator as the standard

DoorDash standardized on `SparkKubernetesOperator` for all Spark workloads. Their operator configuration:

- `in_cluster=True`: worker pods run in the same cluster as the Spark Operator; no separate Kubernetes connection needed
- `get_logs=True`: driver logs are streamed back to Airflow and stored with the task instance
- `do_xcom_push=True` for jobs that return metrics (row counts, data quality scores) to downstream tasks via XCom

Their SparkApplication specs follow a template library maintained by the platform team. Individual teams customize job-specific parameters (entry point, arguments, resource profile) but do not modify the base spec. This is the same design as this project's `_spark_app.py`: a central render function, not per-job YAML files.

### The Orchestration Frederator: cross-cluster dependency management

At DoorDash, different teams own different Airflow clusters. A pipeline owned by the merchant analytics team may depend on output from a pipeline owned by the orders team — which runs in a different cluster. Managing these cross-cluster dependencies without a coordination layer would require polling, flag files, or time offsets — all fragile.

The Frederator is DoorDash's solution:

- **Unified DAG graph**: the Frederator maintains a global view of all DAGs across all clusters and their declared inter-cluster dependencies. Engineers can visualize the full dependency chain even when it spans multiple clusters.
- **Cross-cluster triggers**: when a pipeline in cluster A completes successfully, the Frederator notifies cluster B to release the downstream task that was waiting. No polling; no flag files.
- **SLA enforcement across clusters**: an SLA is evaluated at the Frederator level, not the cluster level. If a chain spans three clusters, the SLA timer starts when the first task fires and ends when the last task completes — regardless of cluster boundaries.
- **Failure propagation**: if a task in cluster A fails, the Frederator can propagate the failure to dependent tasks in cluster B, preventing those tasks from running against stale input.

### Data lineage via XCom

DoorDash uses Airflow's XCom mechanism extensively for data lineage:

Each Spark task returns a metadata dict via XCom:
```python
{
    "source_tables": ["silver.orders", "silver.merchants"],
    "output_table": "gold.order_metrics",
    "row_count": 142830,
    "job_id": "spark-abc123",
    "run_id": "{{ run_id }}",
    "written_at": "2024-03-15T08:32:11Z"
}
```

A lineage task downstream of every gold task reads this XCom and writes it to a lineage store. The lineage store answers queries like: "which source tables contributed to this dashboard metric?" and "which jobs wrote to this table in the last 7 days?"

This is not just for debugging. For regulatory queries ("show me all data sources that contributed to this customer's credit decision"), having this lineage queryable without manual investigation is the difference between a 30-minute compliance response and a 3-week audit.

### Task granularity: one task per table

DoorDash follows the one-task-per-table granularity model, not one-task-per-layer. Their rationale:

- **Retry at the right granularity**: if the `merchant_metrics` transformation fails, they want to retry exactly that task, not all of gold.
- **SLA per table**: different tables have different downstream consumers with different latency requirements. A 30-minute SLA for the real-time fraud score table and a 4-hour SLA for the weekly merchant report are different constraints that cannot be expressed at the layer level.
- **Parallel execution**: independent tables in the same layer can run simultaneously. One-task-per-layer forces sequential execution within a layer.

DoorDash avoids DAG explosion (the risk of this approach at large scale) by grouping tables into logical pipeline files rather than one DAG per table. Each DAG covers a business domain; tasks within the DAG cover the individual tables for that domain.

## Lessons for this project

- **Cross-cluster dependency management is a real problem at scale** — the Frederator exists because Airflow's `ExternalTaskSensor` is polling-based (expensive and slow) and does not provide a unified view. At single-cluster scale, the problem doesn't appear; it appears when the platform grows past one team's cluster.
- **XCom for lineage metadata** — `do_xcom_push=True` with structured metadata is a lightweight lineage solution. It doesn't require a separate lineage service to start; the data is in Airflow's database and can be queried directly.
- **One task per table matches retry granularity to failure granularity** — this project already follows this pattern. It is correct.
- **Resource limits per task via KubernetesExecutor** — the `airflow-values.yaml` in this project sets concurrency limits at the DAG level. At higher scale, per-task resource limits in the worker pod spec prevent individual tasks from crowding out others.

## References

- *How to Run Apache Airflow on Kubernetes at Scale*, DoorDash Engineering Blog. https://doordash.engineering/2021/09/28/how-to-run-apache-airflow-on-kubernetes-at-scale/
- *Airflow in Action: How DoorDash Scaled for Data and ML Engineering*, Astronomer Blog. https://www.astronomer.io/blog/airflow-in-action-doordash/
