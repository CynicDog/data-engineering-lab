# CI/CD Boundary — The ESM Argument

## The Core Problem

Your ESM approval process was designed for *application deployments*: code
changes that go through dev → test → UAT → prod with human reviews at each gate.

That's the right process for code changes.

The problem is that your current setup forces *operational state changes*
(enabling a paused pipeline, changing a job's schedule, adjusting a cluster size)
through the same approval cycle. This is not a process problem — it's a categorization
problem. You're using the wrong tool for the wrong class of change.


## The Two Classes of Change

### Class 1: Deploy-Time Changes (MUST go through ESM)

These change what the pipeline *is*:
- Modifying a DAG's task logic
- Updating a Python package's behavior
- Changing a SQL query in a mart
- Adding a new data source
- Schema changes to existing Delta tables

**Why ESM is correct here:**
These changes affect data correctness. A wrong Gold mart query produces wrong
KPIs reported to management. UAT and human review are appropriate safety gates.

### Class 2: Run-Time Control (SHOULD NOT go through ESM)

These change what the pipeline *is doing right now*:
- Pausing/unpausing a production job
- Enabling/disabling a feature flag
- Adjusting a retry count
- Triggering a backfill for a date range
- Changing a cluster's autoscale min/max

**Why ESM is wrong here:**
An operator needs to be able to pause a broken pipeline *now*, not after
3 days of approvals. These are operational decisions, not code deployments.
The blast radius of a wrong operational decision is usually recoverable
(restart the job). The blast radius of not being able to stop a runaway
pipeline is not.


## What to Tell Your Compliance Team

> "We want to separate *application code changes* (which go through ESM)
> from *operational state management* (which should be managed by designated
> operators via the platform API).
>
> The change: specific operational actions — pause/unpause a pipeline,
> trigger a backfill, enable/disable a feature flag — will be executed
> by the data engineering team via the Databricks Jobs API or Airflow API,
> using service accounts with scoped permissions.
>
> Every such action is logged by the platform (Databricks audit log /
> Airflow task log) with timestamp, actor, and action. This audit trail
> is actually *better* visibility than the current ESM process, which only
> records approvals, not actual actions.
>
> Code changes (new features, bug fixes, schema changes) continue to
> go through ESM exactly as they do today."


## Practical Implementation

### Mechanism 1: Airflow Pause/Unpause API

```bash
# Pause a DAG (no ESM required — operational action)
curl -X PATCH http://airflow:8080/api/v1/dags/ingest_bronze \
  -H "Content-Type: application/json" \
  -d '{"is_paused": true}' \
  -u admin:admin

# Unpause
curl -X PATCH http://airflow:8080/api/v1/dags/ingest_bronze \
  -H "Content-Type: application/json" \
  -d '{"is_paused": false}' \
  -u admin:admin
```

This is equivalent to the Databricks Jobs API `update` endpoint.

### Mechanism 2: Feature Flags in Delta

Store operational flags in a Delta table. Pipeline code reads the flag before
proceeding. Operators change the flag via a simple DML — no deployment needed.

```sql
-- Control table (deployed once via ESM, then managed operationally)
CREATE TABLE IF NOT EXISTS control.pipeline_flags (
    pipeline_name STRING,
    flag_name     STRING,
    flag_value    STRING,
    updated_at    TIMESTAMP,
    updated_by    STRING
) USING DELTA;

-- Enable a feature (operational action, no ESM)
INSERT INTO control.pipeline_flags
VALUES ('ingest_bronze', 'enabled', 'true', current_timestamp(), 'ops-team');
```

In pipeline code:
```python
def is_pipeline_enabled(spark, settings, pipeline_name: str) -> bool:
    flags = (spark.read.format("delta")
             .load(settings.control_path("pipeline_flags"))
             .filter(f"pipeline_name = '{pipeline_name}'")
             .filter("flag_name = 'enabled'"))
    if flags.isEmpty():
        return True  # default enabled
    return flags.first()["flag_value"] == "true"
```

### Mechanism 3: Databricks Jobs API (for existing Databricks setup)

```bash
# Pause a Databricks job (no ESM)
databricks jobs update --json '{"job_id": 12345, "new_settings": {"schedule": {"pause_status": "PAUSED"}}}'

# Trigger a backfill run for a specific date
databricks jobs run-now --job-id 12345 --job-parameters '{"dt": "2024-03-01"}'
```


## The Git Branch Policy vs. Two Environments

Your current setup:
```
master ──── feature/* (checkout from master)
              ↓ (PR → merge)
         development ──── DEV CI/CD pipeline
              ↓ (PR → merge)
           release ──── TEST CI/CD + ESM UAT + PROD CI/CD
              ↓ (PR → merge)
            master (read-only, latest state)
```

With only two Databricks environments (DEV and PROD), the TEST/UAT step
requires the DEV environment to act as staging.

**Recommendation:**
Accept that DEV environment serves double duty: development AND pre-prod UAT.
In practice this means:

1. `development` branch → DEV Databricks workspace (developer testing)
2. UAT testing also happens on DEV workspace against `release` branch
3. After UAT sign-off, PROD pipeline deploys from `release` to PROD workspace
4. `release` → `master` merge marks production deployment complete

The key is to deploy Databricks assets (notebooks, jobs, libraries) via
Azure DevOps pipelines keyed to branches, not to manually manage them.
Use `databricks bundle deploy` (Databricks Asset Bundles) or Terraform
to make deployments reproducible.


## Production Proposal

### The Real Problem at Your Company

To enable a paused production pipeline, you currently need to:
- Create an ESM story
- Create a feature branch
- Make a code change (or fake one)
- PR → development → DEV pipeline
- PR → release → TEST-CI/CD → UAT team (days of waiting)
- Team leader approval
- PROD pipeline
- PR → master

This is the full lifecycle for flipping a `pause_status` from `PAUSED` to `UNPAUSED`.
This is not a governance problem. This is a categorization problem.

### The Argument to Raise with ESM / Compliance

The core distinction you need your compliance team to accept:

> A **deploy-time change** modifies the artifact — the code, the schema, the logic.
> It requires review and approval because it changes what the system does.
>
> A **run-time control** operates the artifact — starting it, stopping it,
> triggering a backfill. It does not change what the system does.
> It is equivalent to a DBA restarting a database service or an operator
> opening a pipeline valve. It is governed by role, not by approval cycle.

Present this table to your team leader and the ESM / compliance stakeholders:

| Action | Class | Proposed Path |
|---|---|---|
| New notebook logic | Deploy-time | ESM — no change |
| SQL query change in mart | Deploy-time | ESM — no change |
| New Delta table schema | Deploy-time | ESM — no change |
| New ADF pipeline activity | Deploy-time | ESM — no change |
| Unpause a deployed job | Run-time | Databricks Jobs API — operator action |
| Trigger a backfill run | Run-time | Databricks Jobs API — operator action |
| Re-queue a FAILED ingestion row | Run-time | SQL UPDATE on ingestion_log — operator action |
| Change cluster auto-terminate setting | Run-time | Databricks UI / API — cloud admin action |
| Add a new cluster to a pool | Infrastructure | Cloud admin action — separate from ESM scope |

**Your talking point:**
> "We are not asking to bypass ESM for code changes. We are asking to define
> a separate operational procedure for runtime control of already-approved
> deployed artifacts. Every operational action will be logged by Databricks
> audit logs — timestamp, actor, action — which is better traceability than
> ESM approvals, which only record that approval happened, not what was
> actually done."

### Implementation

**Deploy all Databricks Jobs as PAUSED by default via DABs:**
```yaml
resources:
  jobs:
    bronze_ingest:
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "Asia/Seoul"
        pause_status: PAUSED
```

The job is deployed through the full ESM cycle, arrives in PROD as `PAUSED`.
No data runs until an operator explicitly unpauses it. This satisfies the
compliance requirement that "nothing happens in PROD without approval" —
the deployment (code) was approved; the operator then opens the valve.

**Operator unpause (no ESM, logged by Databricks audit):**
```bash
databricks jobs update --json '{
  "job_id": 12345,
  "new_settings": {
    "schedule": { "pause_status": "UNPAUSED" }
  }
}'
```

Or via the Databricks UI: Jobs → select job → Edit schedule → Unpause.
Both actions appear in the Databricks audit log under `jobs.update`.

**Backfill trigger (no ESM, operational action):**
```bash
databricks jobs run-now \
  --job-id 12345 \
  --job-parameters '{"dt": "2024-03-01", "schedule_type": "DAILY"}'
```

### Mapping to Your Git Branch Policy

The DABs targets map directly to your two-environment structure:

```yaml
targets:
  dev:
    workspace:
      host: https://adb-dev-xxxx.azuredatabricks.net
    mode: development
    variables:
      voc_catalog: VOCD

  prod:
    workspace:
      host: https://adb-prod-yyyy.azuredatabricks.net
    mode: production
    variables:
      voc_catalog: VOCP
```

AzDevOps pipeline for `development` branch deploys `--target dev`.
AzDevOps pipeline for `release` branch deploys `--target prod`.
The `master` branch remains read-only — no deploy target needed.

### Operations Runbook (Present to Compliance as Evidence)

Create a short Operations Runbook (separate from ESM, owned by data engineering):

1. **Unpause a production job** — who is authorized (data engineering team), how
   (Databricks Jobs API or UI), what is logged (Databricks audit log), who reviews
   (team leader weekly audit of run-time actions)
2. **Trigger a backfill** — same authorization model, parameters documented
3. **Re-queue a failed ingestion** — SQL UPDATE on `ingestion_log`, documented
4. **Emergency pipeline halt** — `pause_status: PAUSED` via API, escalation path

This runbook demonstrates that run-time operations are governed — just not by
the ESM cycle designed for code deployments.
