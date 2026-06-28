# Case Studies: Spark + Airflow in Production

Companies that built their own data platforms — no Databricks, no heavy SaaS delegation.

| Company | Orchestrator | Spark Submission | Table Format | Scale |
|---|---|---|---|---|
| [Airbnb](./01_airbnb.md) | Airflow (original creators) | In-cluster Spark | Iceberg / Hive | 35B events/day |
| [Netflix](./02_netflix.md) | Maestro (custom) | Spark on K8s | Iceberg | 300M+ users |
| [Uber](./03_uber.md) | Piper (Airflow fork) | Hadoop → K8s via USCS | Hudi | 200k pipelines, 750k tasks/day |
| [Pinterest](./04_pinterest.md) | Spinner (Airflow-based) | Archer → Spark Operator | Delta / Parquet | 96% OOM reduction |
| [Robinhood](./05_robinhood.md) | Airflow + JMS | Multi-backend K8s | Parquet / Delta | 4k+ pipelines, 15 clusters |
| [Shopify](./06_shopify.md) | Airflow + Starscream | KubernetesExecutor | Iceberg | 10k DAGs, 400 concurrent tasks |
| [DoorDash](./07_doordash.md) | Airflow + Frederator | SparkKubernetesOperator | Delta | Multi-cluster |

## Cross-cutting themes

1. **Abstraction between orchestrator and executor** — every company at sufficient scale introduced a service layer between Airflow and Spark (JMS, Archer, Frederator). Airflow submits to the service; the service routes to infrastructure.
2. **SparkKubernetesOperator is the convergence point** — companies still on Hadoop are migrating; new platforms start here.
3. **Asset/event-driven scheduling is emerging** — most still use time-based cron, but Airbnb (Dataset scheduling) and Uber (event-driven triggers) are moving toward data-event-driven pipelines.
4. **Per-table task granularity** — most companies create one task per logical table or mart, not one task per layer. Retry at the right granularity.
5. **Observability is not optional** — Pinterest (Fluent Bit → S3 → CloudWatch), Uber (DataCentral chargeback platform), Netflix (Maestro UI) all invest heavily in making pipeline state visible.
