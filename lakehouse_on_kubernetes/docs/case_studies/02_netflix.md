# Netflix: Maestro and the Case for a Custom Orchestrator

## Context

Netflix runs one of the most demanding data platforms in the world — 300 million+ users, petabytes of interaction data per day, and a recommendation engine that must re-train daily on all of it. By 2022, Airflow at Netflix had grown to the point where its scheduling latency, DAG parsing overhead, and limited workflow expressiveness were becoming constraints. Netflix's answer was **Maestro**: a fully custom workflow orchestrator built in-house.

Maestro is not a replacement for Airflow in philosophy — it still follows DAG-based dependency modeling, SLA enforcement, and event-driven triggers. It is a replacement in implementation, optimized for Netflix's specific scale requirements.

## Scale

- 300 million+ subscribers
- Petabytes of data processed daily
- Continuous recommendation model retraining
- Hundreds of Spark, Flink, and Hive jobs per day

## Spark + Airflow (Maestro) Integration

### Maestro: what changed and what stayed the same

Maestro preserves the core ideas of Airflow: pipelines are DAGs, tasks have dependencies, SLAs are enforced, failures are observable. What it changes is the execution model:

- **Scalable scheduler**: Maestro's scheduler is distributed. Airflow's scheduler is a single process that can become a bottleneck when parsing thousands of DAGs concurrently.
- **Workflow-as-config**: Workflows in Maestro can be defined in JSON/YAML in addition to Python, lowering the barrier for non-engineer contributors to define pipelines.
- **Deep integration with Netflix infrastructure**: Maestro natively understands Netflix's internal container platform (Titus), cost attribution, and lineage tracking — things that required custom Airflow plugins before.

The principle is the same: orchestrator submits work, execution engine runs it. What changed is the interface and the scalability of the submission layer.

### Iceberg as the table format

Netflix standardized on **Apache Iceberg** rather than Delta Lake. Their motivation: Iceberg is a fully open specification with no vendor control, and Netflix has a long history of contributing to (and depending on) open-source formats they can fork if needed. Delta Lake's governance model (Databricks stewardship) was a factor in their decision.

The operational impact on their Spark jobs:

- **MERGE INTO for upserts**: all slowly-changing dimension tables use `MERGE INTO` (Iceberg's equivalent of Delta MERGE). No full-table overwrites for tables with historical data.
- **Time travel for backfills**: when a pipeline bug corrupts data, Netflix uses Iceberg's snapshot history to restore a clean state and replay from a known good version, rather than re-ingesting from the source system.
- **Predicate pushdown via partition spec**: Iceberg's partition spec is separate from the physical layout, so partitioning strategy can evolve without rewriting historical data. Netflix uses this to change partition granularity (daily → hourly) as data volumes grow.

### Data quality between layers

Netflix does not trust that upstream data is correct. Every major pipeline stage includes an assertion job before downstream consumers run:

- Row count within expected range
- Null rate below threshold on critical columns
- Join cardinality sanity checks (unexpected fan-out means a bad join key)
- Statistical distribution checks for recommendation feature tables

If an assertion fails, the downstream job does not run and the on-call engineer is paged via PagerDuty. This is treated as a production incident, not a data engineering ticket.

### AQE and shuffle tuning

Netflix runs Spark 3.x with AQE fully enabled and tuned:

- `spark.sql.adaptive.coalescePartitions.enabled = true` — prevents post-shuffle stages from spawning hundreds of near-empty tasks when data is smaller than expected
- `spark.sql.adaptive.skewJoin.enabled = true` — critical for recommendation pipelines where a small number of items (blockbuster titles) appear in a disproportionate fraction of viewing records
- Explicit `broadcast()` hints on all dimension tables (user metadata, item metadata) to prevent accidental sort-merge joins as table sizes change

## Lessons for this project

- **Custom orchestrators emerge from scale** — Airflow is the right tool until it isn't. Maestro exists because Netflix grew past Airflow's scheduler throughput, not because Airflow's design was wrong.
- **MERGE INTO over overwrite** — for any table with a primary key and historical data, MERGE is the correct write strategy. Full overwrite is only appropriate when the table is a complete recomputation (an aggregated mart with no per-key history).
- **Iceberg and Delta are both valid choices** — the important thing is committing to one ACID format and using its features (time travel, schema evolution, MERGE) rather than treating it as a fancy Parquet writer.
- **Time travel as an operational tool** — building pipelines with ACID table formats means you have a recovery path when bugs corrupt data. Design for recoverability.

## References

- *How Netflix Orchestrates Millions of Workflow Jobs with Maestro*, ByteByteGo Newsletter. https://blog.bytebytego.com/p/how-netflix-orchestrates-millions
- *Meson: Workflow Orchestration for Netflix Recommendations*, Netflix TechBlog. https://netflixtechblog.com/meson-workflow-orchestration-for-netflix-recommendations-fc932625c1d9
- *Engineering the AI Factory: Inside Netflix's AI Infrastructure*, Vamsi Talks Tech. https://www.vamsitalkstech.com/ai/industry-spotlight-engineering-the-ai-factory-inside-netflixs-ai-infrastructure-part-3/
- *Inside Netflix's Data Pipelines: Scaling for 300 Million Users*, Medium. https://medium.com/@yassinwdinana/inside-netflixs-data-pipelines-scaling-for-300-million-users-950e168c67c1
