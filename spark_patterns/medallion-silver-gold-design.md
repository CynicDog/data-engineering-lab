# Medallion: Silver Scope and Layer Triggering

Two design questions for our Databricks medallion build, committed to strict three layers (Bronze, Silver, Gold) with no sub-tiers. Each layer must absorb the full responsibility implied by that constraint, and orchestration must hold up at enterprise scale — hundreds of tables, dozens of pipelines, multiple owning teams — without degenerating into per-pipeline hard-coding.

## Question 1: What belongs in Silver?

### The tension

1. **Silver as 1:1 cleansed Bronze.** Each Bronze table maps to exactly one Silver table. Silver does deduplication, type casting, null handling, PII masking, schema enforcement, and CDC application. No joins, no integration.
2. **Silver as conformed business entities.** Silver consolidates multi-source records into canonical entities (one `customer`, one `account`, one `policy`) with conformed keys and dimensions. Joins and integration happen here.

If Silver stays 1:1, Gold inherits *all* integration logic on top of its aggregation logic. Gold becomes the only place where cross-source semantics are resolved — which is the exact failure mode the medallion pattern was designed to avoid.

### Concrete example: the `customer` entity

Three Bronze sources contribute to one logical customer:

- `bronze.crm_customer` — CRM system, owns name, contact, segment.
- `bronze.billing_account_holder` — billing system, owns billing address and dunning state.
- `bronze.identity_user` — identity provider, owns auth identifiers, MFA state.

Under **Option A**, three Silver tables mirror three Bronze tables. Every Gold mart that needs "customer" rejoins all three. The Risk mart joins them one way (treating identity as authoritative for the natural key), the Finance mart joins them another way (treating billing as authoritative because billing addresses are the legal record). They disagree about how many customers exist. This is the textbook conformed-dimension failure mode.

Under **Option B**, `silver.customer` is one entity with a single business key, a documented resolution rule for which source wins on each attribute, SCD-2 history, and a quality contract. Every Gold mart joins to this one table.

### Option A: Silver as 1:1 cleansed Bronze

Pros:
- Trivial lineage: one Bronze, one Silver.
- Backfills are local: rebuilding Bronze X rebuilds Silver X.
- No fan-in inside Silver — each Silver job has exactly one upstream.
- Source-system schema drift is contained per source.

Cons:
- Every Gold mart redoes the same joins, deduplications across sources, and conformance work.
- No single source of truth for business entities. "What is a customer?" is answered N times in N Gold tables, with drift.
- Gold becomes the entire enterprise data model plus aggregation — too much weight for one layer in a three-layer architecture.
- Data quality contracts live at the Gold boundary, far from where the messiness originates. Bad source data leaks through Silver untouched.

### Option B: Silver as conformed business entities

Pros:
- One canonical definition per business entity. Conformed dimensions exist here, not in each mart.
- Gold shrinks to its proper role: business-specific aggregations, marts, feature tables, reporting shapes.
- Cross-source quality issues surface at Silver, where the platform team owns them, instead of leaking into analyst-owned Gold queries.
- Aligns with the Databricks reference definition of medallion (Silver = cleansed *and* conformed).

Cons:
- Silver builds have fan-in. A Silver entity may depend on several Bronze tables, which complicates ordering (Question 2).
- The Silver/Gold boundary needs an explicit rule, or aggregations creep into Silver.
- Reprocessing a single Bronze source no longer cleanly rebuilds one Silver table; it triggers re-conformance for every Silver entity that source feeds.

### Recommendation

**Adopt Option B with an explicit boundary rule.** In three layers, Silver has to carry conformance. The boundary rule we enforce in review:

- **Silver tables** are entity- or event-grain, conformed across sources, with stable business keys. SCD-2 history lives here. No business-specific aggregations. No mart-shaped pivots. No KPIs.
- **Gold tables** are mart-shaped: aggregations, denormalized reporting tables, feature tables, KPI tables. They consume Silver and never read Bronze directly.

Because we cannot introduce Silver sub-tiers, the discipline is to keep Silver narrowly scoped to "conformed entities and events" and resist staging tables inside Silver. If a transformation needs an intermediate result, it belongs in a CTE or a temporary view inside the same Silver job, not in a new published table.

## Question 2: How is each layer triggered?

The hard constraint: Bronze cannot precede source arrival, Silver cannot precede Bronze, Gold cannot precede Silver — and at scale we cannot enforce this with timing assumptions or with per-pipeline DAG configuration. Databricks Workflow DAGs would force dependencies to be re-declared inside every job spec, which is hard-coding wearing an orchestration costume: every new Gold mart edits a Workflow, every cross-team dependency requires coordinating two Workflows, and there is no global view of "what depends on what" outside the union of all job definitions. That is the failure mode we are designing against.

Available primitives:

- **Schedule** (cron). Useful for the boundary and for periodic recomputes. Useless for expressing "wait for upstream."
- **File Arrival.** Useful at exactly one place: the source-to-Bronze boundary.
- **Table Update.** Fires on every Delta commit of a watched table. Fan-out is fine; readiness semantics are not — a single commit ≠ "all upstream data for this logical window is ready."
- **Custom programmatic trigger.** Required for the fan-in/readiness logic Native triggers cannot express.

The two trigger types that *do* something useful at scale are File Arrival (one-shot, at the source boundary) and a generic readiness query that we own.

### Why scheduled cascades fail

Bronze at 02:00, Silver at 03:00, Gold at 04:00. Works on the demo. In production: source delivery slips by 40 minutes, Silver runs against partial Bronze, Gold runs against partial Silver, downstream consumers receive a quietly-wrong dashboard. Failures cascade silently because nothing in the system says "this run was based on incomplete inputs."

### Why naive Table Update chains fail

`silver.customer` watches three Bronze tables and fires on each commit. When `bronze.crm_customer` commits first, Silver fires immediately and reads stale snapshots of `bronze.billing_account_holder` and `bronze.identity_user`. Then those commit, Silver fires twice more. We get three runs per logical batch, two of which are wrong, and the "correct" one is only correct by accident of ordering. There is no construct in Table Update triggers for "fire when all of [A, B, C] have committed for the same logical window."

### Why per-pipeline Workflow DAGs fail at scale

A 200-table platform with 30 marts and 60 source feeds produces dozens of Workflows, each carrying its own copy of the dependency graph. The system's dependency truth is the union of those job definitions, which means:

- Adding a new Gold mart that consumes an existing Silver entity is an edit to a Workflow, reviewed by whoever owns that Workflow. Cross-team friction.
- The same Silver entity appears as an upstream in many Workflows. Renaming it is an N-job edit.
- "What is blocking Gold table X?" requires reading a job UI, not querying a system.
- Backfills require re-running the right Workflow with the right parameters — a manual, error-prone operation.
- No single place answers "are all Silver entities fresh for business date 2026-05-13?"

This is the "no better than hard code" failure. Workflows remain useful as **compute executors** (they run notebooks/wheels reliably with retries and cluster reuse), but they are the wrong place to encode cross-table dependency truth.

### Recommendation: declarative registry plus a thin readiness controller

Encode dependencies once, in data, not in job specs. Drive the cascade from a generic controller that reads the registry, asks "what is eligible to run now?", and dispatches Databricks Jobs. Use native triggers minimally and at well-chosen seams.

Three pieces:

#### 1. Dataset registry (declarative)

A Delta table — call it `_platform.dataset_registry` — with one row per published table at any layer:

```
dataset_id          STRING   -- e.g. "silver.customer"
layer               STRING   -- "bronze" | "silver" | "gold"
upstream_ids        ARRAY<STRING>  -- e.g. ["bronze.crm_customer", "bronze.billing_account_holder", "bronze.identity_user"]
window_grain        STRING   -- "business_date" | "hourly" | "event_date" | "none"
trigger_kind        STRING   -- "file_arrival" | "upstream_ready" | "schedule" | "manual"
trigger_config      MAP<STRING,STRING>  -- landing path, cron, etc.
job_id              STRING   -- Databricks Job ID to dispatch
quality_gate_id     STRING   -- reference to expectation rule set
freshness_sla_min   INT      -- minutes; observability uses this
owner_team          STRING
```

The registry is the single source of dependency truth. Adding a Gold mart means inserting one row, not editing a Workflow. Renaming a Silver entity is a registry change with a migration. Cross-team consumption is declared by Team B pointing `upstream_ids` at Team A's dataset — no edit to Team A's job.

#### 2. Run ledger (operational state)

A Delta table — `_platform.run_ledger` — append-only, one row per completed run of a dataset for a logical window:

```
dataset_id           STRING
logical_window       STRING   -- e.g. "2026-05-13" or "2026-05-13T14"
status               STRING   -- "succeeded" | "failed" | "quality_failed"
source_versions      MAP<STRING,BIGINT>  -- upstream dataset_id → Delta version read
delta_commit_version BIGINT   -- this dataset's commit produced by the run
quality_summary      STRUCT<passed:INT, failed:INT, warnings:INT>
started_at           TIMESTAMP
finished_at          TIMESTAMP
run_id               STRING
```

The ledger answers every operational question with SQL: freshness, lineage of a specific run, which window of which dataset is blocking which downstream, what version of Bronze a given Gold table actually consumed.

#### 3. Thin readiness controller

One generic job per layer family (Bronze, Silver, Gold). Each runs the same logic:

```python
def dispatch_ready_datasets(layer: str, now: datetime) -> None:
    candidates = registry.where(layer == target_layer
                                and trigger_kind == "upstream_ready")
    for ds in candidates:
        for window in pending_windows(ds, now):           # windows not in ledger yet
            upstream_states = ledger.latest_status(ds.upstream_ids, window)
            if all(s.status == "succeeded" for s in upstream_states):
                if ds.quality_gate_id:
                    if any(s.quality_summary.failed > 0 for s in upstream_states):
                        skip_with_reason(ds, window, "upstream_quality_failed")
                        continue
                jobs_api.run_now(ds.job_id, params={
                    "dataset_id": ds.dataset_id,
                    "logical_window": window,
                    "source_versions": {s.dataset_id: s.delta_commit_version
                                        for s in upstream_states},
                })
```

The dispatched job, on success, writes a row to the ledger with the source_versions it actually consumed. That row is the event that lets the next layer fire.

### How native triggers wire in (deliberately minimal)

- **File Arrival** triggers Bronze ingestion jobs at the source-to-platform boundary. This is the only place File Arrival is used, and it is the right tool there.
- **Schedule** triggers the readiness controllers themselves (every 1–5 minutes). The controller is cheap: a few SQL queries against the registry and ledger. This is the cron we accept, because it is not encoding business dependencies — only "wake up and ask the registry what's ready."
- **Table Update** is used in exactly one place: a trigger on `_platform.run_ledger`. Any ledger commit wakes the next-layer controller immediately, so the steady-state cascade is event-driven, not poll-driven, while the scheduled tick is a safety net for missed events.
- **Custom programmatic** lives inside the controller. That is where readiness logic, dynamic fan-in, and quality gating happen.

There are no per-pipeline DAGs. Databricks Workflows still execute work (one Workflow per dataset, parameterized by `logical_window`) — they just don't encode dependencies between datasets.

### How this handles real pain points

**Late-arriving Bronze data.** A correction to `bronze.crm_customer` for business_date `2026-05-10` writes a new ledger row for that window. The Silver controller, on its next tick (or on the ledger Table Update event), sees a new "succeeded" row for window `2026-05-10` whose upstream state is now newer, and dispatches `silver.customer` for that window. Gold cascades automatically. No human re-runs a DAG.

**Multi-source conformance race.** `silver.customer` declares three upstreams and a `business_date` window grain. The controller will not dispatch it for window W until all three upstreams have a `succeeded` ledger row for window W. No partial-snapshot reads.

**Cross-team consumption.** Team B's `gold.risk_segment_daily` declares `silver.customer` as an upstream by inserting one registry row in Team B's repository. Team A makes no change. Renaming `silver.customer` to `silver.party` is a registry update with a deprecation window — searchable in one place.

**Backfill.** Operator writes ledger entries marking the upstreams as ready for a historical window range, or invokes a backfill helper that re-runs ingestion for that range. Controllers dispatch the cascade window-by-window. Idempotent because dataset jobs are partition-overwrite keyed on `logical_window`.

**Quality gate.** A run that fails expectations writes a ledger row with `status = quality_failed`. Downstream controllers treat that as "upstream not ready" and refuse to dispatch. The dashboard goes stale loudly instead of silently propagating bad data. Policy lives in one place (the controller), not sprinkled across N Workflow specs.

**Observability.** "Are all Gold marts fresh for 2026-05-13?" is one SQL query against the ledger. "What's blocking `gold.finance_dashboard`?" is one recursive CTE walking the registry against the ledger. The UI for this is a Databricks SQL dashboard, not the Jobs page.

### Anti-patterns this design refuses

- Encoding inter-dataset dependencies inside Workflow specs.
- Using Schedule offsets to imply ordering ("Silver runs an hour after Bronze").
- Using Table Update triggers directly on data tables for fan-in. Table Update is fine on the *ledger* (a single, semantically-rich table), not on raw upstreams.
- Inventing Silver sub-tiers to avoid fan-in. Fan-in is solved by the readiness controller, not by adding layers.
- Putting business KPIs in Silver because "Gold is too slow to update." If Gold is slow, fix the controller or the job, do not blur the boundary.

### Trade-offs we accept

This pattern adds a platform component we have to build and maintain: the registry schema, the ledger schema, the controller job, and a small SDK for ingestion jobs to write ledger rows on success. That is real operational tax. The payoff is that adding the 201st dataset is the same effort as adding the 50th — declarative insert into the registry plus one parameterized Workflow — and the global dependency graph is queryable, not splattered across a folder of Workflow JSON.

A team that has ten pipelines and one squad should use Workflow DAGs and not build this. A team that already feels the pain of cross-Workflow coordination and per-pipeline edits should build it before that pain compounds into rewrites.

## Summary

- Silver carries conformance, not just cleansing. Gold is marts, not integration. The boundary is enforced in review, not by adding sub-tiers.
- Dependencies live in a `_platform.dataset_registry` Delta table. Operational state lives in a `_platform.run_ledger` Delta table. A thin generic controller per layer dispatches what is ready.
- Native triggers are used at exactly three seams: File Arrival for source-to-Bronze, Table Update on the ledger for steady-state cascades, Schedule for the controller heartbeat. Everything else is one SQL query against the registry and ledger.
- Workflows remain the compute executor. They are not the dependency truth.
