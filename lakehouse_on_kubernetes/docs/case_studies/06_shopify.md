# Shopify: 10,000 DAGs and the Starscream PySpark Platform

## Context

Shopify's data platform serves one of the most event-dense e-commerce environments in the world. Peak events (Black Friday, Cyber Monday) generate order-of-magnitude traffic spikes that stress every layer of the data infrastructure. Their platform must be both scalable enough for peak load and cost-efficient enough for the 363 other days of the year.

Shopify runs Airflow at significant scale — 10,000+ DAGs, 400 concurrent tasks at peak, 150,000+ DAG runs per day — with a custom internal framework called **Starscream** that abstracts PySpark job definition and submission for their engineering teams.

## Scale

- 10,000+ active Airflow DAGs
- 400 concurrent tasks at peak (Black Friday scale)
- 150,000+ DAG runs per day
- Black Friday peak: order-of-magnitude traffic above baseline

## Spark + Airflow Integration

### Starscream: PySpark without the configuration overhead

Starscream is Shopify's internal platform for defining and running PySpark jobs. It is not a scheduler replacement (Airflow does that) — it is a higher-level interface for writing Spark jobs that automatically handles:

- **Resource configuration**: Starscream applies team-based and job-class-based resource profiles. Engineers write transformation logic; the platform decides executor memory, core count, and Kubernetes node affinity.
- **Schema enforcement**: every Starscream job declares its input and output schemas. The framework validates that the written data matches the declared schema before marking the task as successful. A schema drift (an upstream change that drops a column) fails at the schema check, not downstream in a BI dashboard.
- **Dependency injection**: Starscream jobs declare their input tables by logical name. The framework resolves the physical path (S3 path, partition spec, version) at runtime. Engineers do not hard-code S3 paths in job code.
- **Automatic retry on transient failures**: Starscream wraps the Spark job execution and applies retry logic for specific transient error types (S3 throttling, Kubernetes API server timeouts) that the job author should not need to handle.

The result: a Shopify engineer writing a new transformation writes a Python class with `input_tables`, `output_table`, and `transform(df)` — not a SparkApplication YAML and a Kubernetes RBAC spec.

### Airflow at 10,000 DAGs: operational realities

Shopify's Airflow deployment revealed several scaling limits that are worth understanding:

**DAG parsing time**: Airflow's scheduler re-parses all DAG files on a short interval. At 10,000 DAGs, parsing time on a single scheduler process becomes a bottleneck. Shopify addressed this by:
- Moving to Airflow's `min_file_process_interval` configuration to control parsing frequency
- Ensuring DAG files do not perform expensive operations (database queries, S3 reads) at module import time — these happen during every parse cycle, not just at task runtime
- Using `DagBag` pre-compilation and lazy loading where possible

**Scheduler single point of failure**: with 10,000 DAGs and 150k daily runs, the Airflow scheduler becoming unavailable stops all pipeline progress. Shopify runs the scheduler in high-availability mode (multiple scheduler processes with a distributed lock) to survive single scheduler pod failures.

**Task queuing**: KubernetesExecutor creates one pod per task. At 400 concurrent tasks, that's 400 simultaneous pod creation requests to the Kubernetes API server. Shopify rate-limits task submission and uses Kubernetes resource quotas to prevent the executor from exhausting cluster capacity during peak periods.

### Schema contracts between layers

Shopify treats the schema at each medallion layer as a contract. The schema is:

1. **Declared explicitly** in the table definition (not inferred from data)
2. **Validated on write** — Starscream checks that written data matches the declared schema
3. **Versioned** — schema changes go through a review process, and pipelines that read the table are notified of breaking changes before they land

This is the production version of the `AUDIT_SCHEMA` defined in `bronze/audit.py` of this project. In Shopify's system, every table — not just the audit log — has a declared schema that is enforced on every write.

### Reliability engineering for Black Friday

Black Friday is a known stress event, which means Shopify can prepare for it specifically:

- **Pre-scaling**: cluster capacity is pre-warmed before the peak window. Cold-start delays (Kubernetes node provisioning) are eliminated by keeping nodes warm.
- **Priority queuing**: critical pipelines (real-time GMV reporting, fraud detection feeds) get dedicated resource queues that are protected from burst usage by lower-priority analytical jobs.
- **Circuit breakers**: if a data source (e.g., the order events stream) is degraded, the pipelines that read from it are paused rather than failing noisily. Downstream consumers receive a "source degraded" signal rather than corrupted data.

## Lessons for this project

- **Schema enforcement at the write boundary is worth the engineering investment** — Starscream's schema check catches upstream breaking changes before they reach downstream consumers. The cost of enforcing schemas is lower than the cost of debugging data corruption in a production BI dashboard.
- **DAG parsing is a runtime cost** — expensive operations in DAG module scope (file reads, database queries, network calls) run on every scheduler parse cycle. The `load_registry()` call in this project's DAGs is acceptable because it reads a local file; a call to a remote API would not be.
- **Peak events require peak-specific preparation** — the design that works at median load may not work at 10× load. Know your peak patterns and design your resource allocation strategy around them, not around your average.
- **Starscream's dependency injection model** — engineers declaring tables by logical name (not S3 path) is the same principle as this project's `settings.silver_path()` method. Abstract physical paths away from business logic.

## References

- *How to Reliably Scale Your Data Platform for High Volumes*, Shopify Engineering Blog. https://shopify.engineering/reliably-scale-data-platform
- *Airflow Summit 2024: Powering Next-Gen Analytics Platforms & Data Infrastructure*, Medium. https://medium.com/@dev.studio.ua/airflow-summit-2024-powering-next-gen-analytics-platforms-data-infrastructure-9852482358ed
