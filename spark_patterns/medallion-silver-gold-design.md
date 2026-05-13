# Medallion: Silver Scope and Layer Triggering

Two open design questions for our Databricks medallion build. We are committed to a three-layer model (Bronze, Silver, Gold) with no sub-tiers, so each layer's role must absorb the full responsibility implied by that constraint.

This note frames the trade-offs and lands on a recommendation for each.

## Question 1: What belongs in Silver?

### The tension

Two intuitions pull in opposite directions:

1. **Silver as 1:1 cleansed Bronze.** Each Bronze table maps to exactly one Silver table. Silver only performs deduplication, type casting, null handling, PII masking, schema enforcement, and CDC application. No joins, no integration.
2. **Silver as conformed business entities.** Silver consolidates multi-source records into canonical entities (e.g., one `customer`, one `account`, one `policy`) with conformed keys and dimensions. Joins and integration happen here.

If Silver stays 1:1, Gold inherits *all* integration logic on top of its aggregation logic. Gold becomes the only place where cross-source semantics are resolved, which is the failure mode the medallion pattern was originally designed to avoid.

### Option A: Silver as 1:1 cleansed Bronze

Pros:
- Trivial lineage: one Bronze table, one Silver table.
- Deterministic and easy to reprocess. A Bronze backfill rebuilds exactly the matching Silver table.
- No fan-in dependencies inside Silver, which simplifies orchestration.
- Source-system schema drift is contained at the Silver layer per source.

Cons:
- Every Gold mart must redo the same joins, deduplications across sources, and conformance work. Integration logic gets duplicated across marts and drifts over time.
- No single source of truth for business entities. "What is a customer?" gets answered N times in N Gold tables.
- Gold becomes the entire enterprise data model plus aggregation, which is too much for one layer in a three-layer architecture.
- Data quality contracts have to live at the Gold boundary, far from where the messiness originates.

### Option B: Silver as conformed business entities

Pros:
- One canonical definition per business entity. Conformed dimensions exist here, not in each mart.
- Gold shrinks to its proper role: business-specific aggregations, marts, feature tables, and reporting shapes.
- Cross-source quality issues surface at Silver, where engineering owns them, rather than leaking into analyst-owned Gold queries.
- Aligns with the Databricks reference definition of the medallion pattern (Silver = cleansed *and* conformed).

Cons:
- Silver builds have fan-in: a Silver entity table may depend on several Bronze tables, which complicates orchestration (see Question 2).
- The Silver/Gold boundary needs an explicit rule, otherwise aggregations creep into Silver.
- Reprocessing a single Bronze source no longer cleanly rebuilds one Silver table; it triggers re-conformance.

### Recommendation

**Adopt Option B with an explicit boundary rule.** In a three-layer architecture, Silver has to carry conformance, otherwise Gold collapses under combined integration plus aggregation duties.

Boundary rule to enforce in code review:

- **Silver tables** are entity- or event-grain, conformed across sources, with stable business keys. No business-specific aggregations. No mart-shaped pivots. Slowly-changing dimension handling lives here.
- **Gold tables** are mart-shaped: aggregations, denormalized reporting tables, feature tables, KPI tables. They consume Silver and never read Bronze directly.

Because we cannot introduce Silver sub-tiers, the discipline is to keep Silver narrowly scoped to "conformed entities and events" and resist the temptation to add intermediate staging tables inside Silver. If a transformation feels like it needs an intermediate, it usually belongs in a CTE or a temporary view inside the same Silver job, not a new table.

## Question 2: How is each layer triggered?

Bronze cannot precede source arrival; Silver cannot precede Bronze; Gold cannot precede Silver. The orchestration model has to enforce this without timing assumptions.

Databricks native trigger types available to us:

- **Schedule** (cron-style).
- **File Arrival** (fires when objects land in a watched path).
- **Table Update** (fires when an upstream Delta table commits a new version).
- *Continuous is excluded by policy.*

We can also write custom trigger logic, e.g., a job that polls upstream commit timestamps or sentinel tables before running.

### Option A: Scheduled cascade

Each layer is on a schedule with a time offset (Bronze 02:00, Silver 03:00, Gold 04:00). Simple, predictable, and easy to reason about, but fragile: if Bronze is late or fails, Silver runs against stale data and Gold inherits the gap silently. This pattern only works when source arrival times are tightly bounded and SLAs allow a wide buffer.

### Option B: Event-driven cascade

Bronze is triggered by File Arrival from the source landing zone. Silver is triggered by Table Update on its Bronze parents. Gold is triggered by Table Update on its Silver parents. Each layer fires only after upstream data is actually committed.

This is clean for the 1:1 case, but Question 1 lands on conformed Silver, which means Silver tables have multiple Bronze parents. A naive Table Update trigger fires Silver on *every* Bronze commit, leading to redundant runs and partial-conformance reads (Silver computes against Bronze A's new data and Bronze B's older snapshot).

### Option C: Workflow-orchestrated DAG

A Databricks Workflow (Job) declares the full Bronze to Silver to Gold DAG as tasks with explicit dependencies. The Workflow itself is started by one trigger at the top (File Arrival on the landing zone, or Schedule). Internal ordering is the orchestrator's responsibility, not the trigger system's.

For conformed Silver this is the natural fit: a Silver task lists all its Bronze parents as upstream tasks, and the orchestrator only starts Silver when *all* parents finish. The same holds for Gold.

### Option D: Custom programmatic trigger

A controller job that queries Delta commit history or a sentinel table, evaluates "is the full upstream set ready and consistent," and then dispatches downstream layers. Maximum flexibility but maximum operational surface area. Justified only when native primitives cannot express the readiness condition.

### Recommendation

**Workflow-orchestrated DAG (Option C) as the default, with File Arrival as the top-of-DAG trigger for streaming-style sources and Schedule for batch-window sources.**

Concrete pattern:

- **Bronze ingestion jobs** are leaf tasks in the Workflow. For continuously arriving sources, the Workflow is started by **File Arrival** on the landing zone path. For batch sources with known windows (nightly extracts, vendor drops), the Workflow is started by **Schedule**.
- **Silver tasks** declare all relevant Bronze ingestion tasks as upstream dependencies. They run only when every parent succeeds, which gives us atomic conformance reads.
- **Gold tasks** declare their Silver parents as upstream dependencies. Same atomicity guarantee.
- **Table Update triggers** are reserved for cases where a downstream consumer lives in a *separate* Workflow (cross-pipeline propagation) and we genuinely want event-driven decoupling. Inside one logical pipeline, dependencies belong in the DAG, not in trigger rules.
- **Custom programmatic triggers** are reserved for readiness conditions Workflows cannot express, such as "wait for all of N partner files where N is dynamic" or "only run if quality gate X passed in a separate system."

This gives us enterprise-grade ordering guarantees without inventing our own controller, keeps the dependency graph visible in one place (the Workflow definition), and preserves File Arrival's responsiveness at the boundary where it matters most: source-to-Bronze.

## Summary

- Silver carries conformance, not just cleansing. Gold is marts, not integration.
- Orchestration lives in a Databricks Workflow DAG. Triggers fire the top of the DAG; intra-pipeline ordering is enforced by task dependencies, not by trigger fan-out.
