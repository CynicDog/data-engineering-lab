# Medallion Architecture — Layer Contracts

## The Problem with "Bronze = copy, Gold = everything else"

If Silver has no clear job, Bronze and Gold expand to fill the vacuum:
- Bronze does some cleaning "just to be safe"
- Gold does the cleaning Bronze missed, then aggregates, then formats for BI
- When something breaks, you don't know which layer to fix

The result: every pipeline is a black box. Fixing Gold requires understanding
Bronze. Re-running Bronze invalidates Gold. Testing any single layer is impossible.


## The Layer Contracts

### Bronze — Fidelity Layer

**One sentence**: Bronze is a faithful replica of the source, in a queryable format.

**Does:**
- Convert source format (Parquet from ADF, JSON from Kafka) to Delta
- Add system metadata: `_ingest_ts`, `_dt` (partition key)
- Record every ingestion event in the audit log
- Preserve the source schema exactly — no transformations

**Does NOT:**
- Deduplicate rows
- Cast types (except the literal format conversion, e.g., parquet string → Delta string)
- Mask PII
- Apply business logic

**Contract guarantee:**
> If you have the source data and the Bronze pipeline, you can always rebuild Silver.
> Bronze is the recovery point.

**How to trigger:**
- Scheduled (daily/hourly via Airflow cron)
- Or event-driven (Autoloader on file arrival in Databricks)

### Silver — Clean Data API Layer

**One sentence**: Silver is the deduplicated, typed, PII-safe, structurally stable
representation of each source entity.

**Does:**
- Deduplicate rows (window function, keep latest by `_ingest_ts`)
- Cast columns to correct types (`string → date`, `string → double`)
- Mask PII (RRN back 7 digits, phone middle 4 digits)
- Standardize code values (trim whitespace, normalize case)
- Compute derived structural columns (`policy_age_days`, `processing_days`)

**Does NOT:**
- Cross-table joins (that's Gold)
- Business aggregations (that's Gold)
- Apply business rule interpretation (that's Gold)
- Filter rows that "shouldn't" be there by business rules (that's Gold)

**Contract guarantee:**
> Silver tables are the stable API that every downstream consumer (Gold, ML, BI)
> can build on. If a Silver table breaks, all downstream breaks. Silver is therefore
> the highest-quality, most-tested layer.

**How to trigger:**
- Asset-triggered by Bronze in Airflow (no check files)
- Or sensor-triggered in Databricks Workflows

### Gold — Business Logic Layer

**One sentence**: Gold is the query-ready, denormalized, business-rule-applied
representation of business metrics.

**Does:**
- Cross-table joins (policy + customer + claims)
- Business aggregations (KPIs, SLAs, funnel metrics)
- Window functions for trends and rankings
- Any calculation that requires business rule knowledge
- Format data for specific BI tool consumption

**Does NOT:**
- Data quality fixes (that's Silver/Bronze)
- PII exposure (Silver guarantees masking before Gold sees the data)
- Raw source data storage (that's Bronze)

**Contract guarantee:**
> Gold tables can be dropped and rebuilt at any time from Silver.
> Rebuilding Gold should never require re-ingesting Bronze.

**How to trigger:**
- Asset-triggered by Silver in Airflow
- Or triggered by a Databricks Workflows dependency on the Silver job


## The Current File-Based Trigger Chain (Anti-Pattern)

```
Bronze completes
    → writes bronze_done_{table}.flag to s3://lakehouse/control/
Silver polls for all four flag files
    → when all four exist, processes Silver
    → writes silver_done.flag
Gold polls for silver_done.flag
    → when found, processes Gold
    → writes gold_complete_{date}.flag
```

**Problems:**
1. What if a flag file isn't written (job error mid-run)?
   → Silver never triggers. No alert. Data silently stale.
2. What if a flag file from yesterday's run is still there?
   → Silver may process today's Bronze against yesterday's Silver.
3. How do you see the pipeline state?
   → Storage explorer + `ls` on S3 + compute startup.


## The Asset-Based Trigger Chain (This Lab)

```
Bronze ingest_customer task completes
    → emits Asset("s3://lakehouse/bronze/customer")
    → Airflow records this event in its metadata DB

transform_silver DAG schedule=[all BRONZE_ASSETS]
    → triggered automatically when ANY bronze asset is updated
    → no flag files, no polling

build_gold DAG schedule=[SILVER_ASSET]
    → triggered when silver asset is updated

Pipeline state visible in Airflow UI
    → task logs, run history, retry counts
    → no compute startup required to inspect state
```


## Migrating from File-Based to Asset-Based Triggers

This is a migration that can be done incrementally:

**Phase 1**: Keep existing Databricks Workflows. Add Airflow Asset emission
alongside flag file writes (dual-signal). Verify Airflow sees all runs.

**Phase 2**: Move Silver to be Airflow-triggered (reading from audit log
instead of checking flag files). Keep Gold on Databricks Workflow.

**Phase 3**: Move Gold trigger to Airflow Asset. Remove all flag file writes.

**Phase 4**: Remove legacy trigger code from Databricks notebooks.

This gives you a safe rollback at each phase.
