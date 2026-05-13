# Medallion Architecture on Databricks: Silver Scope & Layer Orchestration

A working memo on two open design questions for our enterprise data platform:

1. What belongs in the Silver layer, and how 1:1 it should stay with Bronze.
2. How to trigger each layer so Silver does not run ahead of Bronze, and Gold does not run ahead of Silver, at enterprise scale.

## Context and constraints

We are standardizing on Databricks with Delta Lake as the lakehouse format. The platform serves multiple business domains, has SLA-bound downstream consumers (BI, ML, regulatory reporting), and must scale to hundreds of pipelines maintained by multiple teams. "Enterprise scalable" here means three things in practice:

- **Operational scalability** — onboarding new sources and new marts must not require redesigning the DAG.
- **Organizational scalability** — domain teams should own their slice of Silver/Gold without coordinating every release.
- **Cost and runtime scalability** — incremental processing, not full refreshes; idempotent runs; clear SLAs per table.

## Question 1: What goes in Silver?

### The two extremes

**Strict 1:1 Silver (Bronze-shaped, just cleansed).**
Each Bronze table maps to exactly one Silver table. Silver does deduplication, type casting, PII handling, schema enforcement, late-arriving-data handling — but no joins, no business logic, no conformed dimensions.

- Pros: Trivial lineage. Easy to reprocess. Source-system-shaped, so SMEs from the source domain can still recognize it. Cheap to maintain.
- Cons: Gold inherits every conforming join, every dimension resolution, every cross-source rule. Gold becomes a monolith of business logic + mart shaping. Two Gold marts that both need "customer enriched with account hierarchy" will each rebuild it, drifting over time.

**Heavy Silver (pre-joined, business-ready entities).**
Silver tables represent conformed business entities (Customer, Account, Transaction) already joined across sources, with surrogate keys and SCD2 history.

- Pros: Gold is thin — mostly aggregations and presentation shaping. Reuse across marts is high.
- Cons: Silver becomes the bottleneck. One source schema change ripples into many Silver tables. Ownership gets muddy (who owns "Customer" when five source systems feed it?). Reprocessing is expensive.

### What we recommend: a two-tier Silver

The pattern that scales in practice is to split Silver into two sub-layers, both still in the Silver medallion but with different responsibilities. Databricks documentation and most large Delta deployments converge on something like this.

**Silver-Cleansed (sometimes called Silver-Standardized).**
Strict 1:1 with Bronze. One table per source table. Responsibilities:

- Schema enforcement and type normalization
- Deduplication, idempotency keys
- PII masking / tokenization where required at rest
- Late-arriving-data and CDC merge (MERGE INTO on a Delta key)
- Soft-delete handling, tombstones
- Data quality expectations (DLT expectations or equivalent) — quarantine bad rows, don't drop silently
- Source-system-shaped column names retained (or renamed only by a strict convention)

This tier is owned by the **source-aligned team** (the team closest to the producing system).

**Silver-Conformed (sometimes called Silver-Integrated or Silver+).**
Cross-source entities. One table per business concept, not per source. Responsibilities:

- Conformed dimensions with surrogate keys
- SCD2 history where the business needs it
- Cross-source joins that are *stable business definitions*, not mart-specific shaping
- Reference data resolution (currency, geography, calendar)
- Entity resolution / master-data joins

This tier is owned by the **domain team** (Customer domain, Finance domain, etc.), not the source teams.

**Gold** then becomes what it should be: mart-shaped, consumer-specific aggregates and denormalized views built on Silver-Conformed. A mart-specific join that only one report needs lives in Gold, not Silver-Conformed.

### How to decide where a join belongs

A useful test: would *more than one* downstream mart want this exact join, with the exact same business semantics? If yes, it belongs in Silver-Conformed. If no, it belongs in Gold. The cost of getting this wrong is asymmetric — a wrongly-placed Silver join creates platform-wide coupling; a wrongly-placed Gold join just gets refactored later when a second consumer appears.

### What Silver should never do

- Business-specific filters ("active customers as defined by the marketing team")
- Mart-specific aggregations
- Presentation formatting (currency symbols, display names)
- Time-bucketing chosen by a specific report

These all belong in Gold or in the semantic layer above it.

## Question 2: How to trigger each layer

The intuition in the question is exactly right: Silver cannot blindly run on a schedule that hopes Bronze finished, and Gold cannot run hoping Silver finished. At enterprise scale, time-based chaining breaks the first time a source is late.

### The three trigger models, and when each fits

**1. Schedule-based ("run Silver at 02:00").**
Simple, but fragile. Works only for tightly-controlled batch sources where Bronze ingestion has a guaranteed completion time. Do not use as the primary mechanism for an enterprise platform — use it only as a *floor* (e.g., "run no earlier than 02:00") combined with one of the below.

**2. Dependency-based (DAG / Workflow orchestration).**
Each task declares its upstream tasks. The orchestrator runs Silver-Cleansed only after the relevant Bronze task succeeds, and Gold only after its Silver dependencies succeed.

On Databricks the two practical options:

- **Databricks Workflows (Jobs)** with multi-task jobs and task dependencies. Good for explicit, hand-authored DAGs. Scales to hundreds of tasks per job and can fan out.
- **Delta Live Tables / Lakeflow Declarative Pipelines.** You declare tables and their `LIVE` dependencies; DLT computes the DAG and runs them in correct order with built-in expectations, retries, and incremental processing. This is the most enterprise-scalable option for *within-pipeline* dependencies because the DAG is derived from code, not maintained separately.

**3. Event-driven (data arrival triggers next layer).**
Bronze ingestion via Auto Loader (file notifications from cloud storage). When new files land, Auto Loader picks them up. Downstream layers can be triggered by:

- Delta change-data-feed (CDF) on the upstream table — Silver reads only changed rows since its last commit version.
- File-arrival triggers on Databricks Workflows (`trigger: file_arrival`).
- Continuous DLT pipelines (streaming mode), where Silver and Gold are streaming Delta reads from upstream.

Event-driven scales best for high-frequency or unpredictable arrival patterns, but it is more operationally complex.

### What we recommend: a hybrid pattern

For an enterprise platform with mixed source cadences (some batch, some streaming, some event-driven), a single orchestration model rarely fits everything. The pattern that holds up:

**Within a domain pipeline: DLT / Lakeflow declarative.**
Bronze → Silver-Cleansed → Silver-Conformed → Gold within a single business domain are expressed as a declarative pipeline. DLT computes the DAG, handles incremental processing via streaming tables or materialized views, and runs the layers in the correct order with no scheduler glue. This eliminates 80% of the "did Silver wait for Bronze?" problem because the dependency is in the code.

**Across pipelines: Workflows or an external orchestrator.**
Cross-domain dependencies (e.g., the Finance Gold mart needs Customer Silver-Conformed from the Customer domain) are wired with Databricks Workflows task dependencies, or with Airflow / Dagster if you already run one platform-wide. The orchestrator triggers a downstream pipeline only after the upstream pipeline run that produced the needed Delta version has succeeded.

**For SLA enforcement: data-aware triggers.**
The downstream pipeline triggers on the *Delta table version* it depends on, not on the clock. Two mechanisms:

- **Trigger on file arrival** for Bronze ingestion entry points.
- **Trigger on Delta table change** (poll the table's commit history, or use change-data-feed) for cross-pipeline dependencies. Lakeflow supports table-update triggers; Airflow has Databricks sensors that do the same.

This means Silver runs *because Bronze produced a new version*, not because the clock ticked. Gold runs *because Silver produced a new version*. If Bronze is late, the chain naturally waits.

### Idempotency and reprocessing

None of the above matters if a re-run corrupts state. Enforce:

- Every Silver and Gold transformation reads from the upstream Delta table using a commit version or timestamp, and writes via `MERGE INTO` keyed on a stable business key.
- Streaming reads checkpoint per table, per pipeline.
- Backfills are explicit jobs that target a date range and write with the same `MERGE INTO` semantics — re-running them is safe.
- Every table has a documented owner, SLA, and freshness expectation registered in Unity Catalog.

### Failure handling

At enterprise scale, partial failures are the common case, not the exception. Two rules:

- **Quarantine, don't fail the pipeline, on data-quality breaches.** DLT expectations with `EXPECT ... ON VIOLATION DROP ROW` (or `QUARANTINE`) keep the pipeline running while flagging bad rows for review. A whole-pipeline failure because one row violated a check will train teams to disable checks.
- **Fail loudly on schema or contract breaches.** A source schema change should stop the pipeline and page the source-aligned team — never auto-evolve into Silver.

## Summary recommendations

- **Silver scope:** split Silver into Silver-Cleansed (1:1 with Bronze, source-aligned ownership) and Silver-Conformed (business entities, domain-aligned ownership). Put cross-source joins in Silver-Conformed only if more than one mart will use them with identical semantics. Mart-specific logic stays in Gold.
- **Triggering:** within a pipeline, use DLT / Lakeflow declarative pipelines so the DAG is derived from code. Across pipelines, use data-aware triggers (file arrival, Delta table updates) via Databricks Workflows or an external orchestrator. Avoid clock-based chaining as the primary mechanism.
- **Non-negotiables for enterprise scale:** idempotent MERGEs everywhere, Unity Catalog ownership and lineage on every table, expectations with quarantine semantics, and explicit per-table SLAs.

## Open questions to resolve next

- Do we standardize on DLT / Lakeflow for all new pipelines, or allow plain Workflows + notebooks for teams not ready?
- Which team owns Silver-Conformed for cross-domain entities like Customer (source-team federation vs. a central data-platform team)?
- What is our backfill SLA — how fast must we be able to reprocess 90 days of Gold after a Silver fix?
- Do we adopt an external orchestrator (Airflow / Dagster) for cross-pipeline dependencies, or stay fully on Databricks Workflows?
