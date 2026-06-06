# Pain Points Catalog

A structured breakdown of the six pain points and the patterns that address each one.

| # | Pain Point | Root Cause | Pattern in This Lab | Deep Dive |
|---|-----------|------------|---------------------|-----------|
| 1 | Compute cold start (6–7 min) | Per-job VM provisioning, no warm pool | Always-on Spark inside Airflow container | [02_compute_strategy.md](02_compute_strategy.md) |
| 2 | Fragile ADF→Bronze pipeline | File-based IPC (status.txt); daily/hourly overlap | Delta audit table replaces status file | [03_ingestion_pipeline.md](03_ingestion_pipeline.md) |
| 3 | Unclear medallion responsibilities | No deliberate layer contract; file-based triggers | Airflow Asset graph; clear Bronze/Silver/Gold contract | [04_medallion_design.md](04_medallion_design.md) |
| 4 | CI/CD ESM mismatch | No distinction between deploy-time and run-time control | API-driven operational control; feature flags in Delta | [05_cicd_boundary.md](05_cicd_boundary.md) |
| 5 | VOCP/VOCD catalog naming chaos | No config abstraction; env branching in source code | Profile-based `Settings` dataclass | [06_environment_management.md](06_environment_management.md) |
| 6 | Notebook-first, no code quality | Cultural gap; no tooling for test/lint/format | Python package + uv + ruff + pytest | [07_code_quality.md](07_code_quality.md) |


## Pain Point 1 — Compute Cold Start

**What you have:**
Job Clusters spin up from scratch for each run. Azure VMs take 6–7 minutes to
provision. As both a user and an administrator, this is unacceptable for production
latency and wasteful as a cloud resource pattern.

**Root cause:**
The "Job Cluster per run" model is designed for large-scale batch isolation, not
interactive or latency-sensitive workloads. It treats every run as a throwaway.

**Where it hurts most:**
- Interactive data exploration (user waits 7 minutes to run a single cell)
- Small frequent jobs (a 5-minute job spends more time starting than running)
- Operations (pausing/unpausing a pipeline to test requires a full cluster start)

→ See [02_compute_strategy.md](02_compute_strategy.md) for the full solution map.


## Pain Point 2 — Fragile Ingestion Pipeline

**What you have:**
ADF writes a status text file to ADLS. Databricks reads it, parses lines, processes
parquets referenced in the file, then deletes the file. Daily and hourly schedules
write to the same path/extension, causing one to silently overwrite the other.

**Root cause:**
Using a file as an inter-process communication (IPC) mechanism. Files on blob storage
have no concurrency guarantees, no schema, no history, and no query interface.

**Symptoms:**
- Silent data loss when hourly overwrites daily status file
- "The file doesn't exist" errors with no recovery path
- Debugging requires compute startup just to check if a file is there

→ See [03_ingestion_pipeline.md](03_ingestion_pipeline.md) for the Delta audit table solution.


## Pain Point 3 — Unclear Medallion Architecture

**What you have:**
Bronze = parquet-to-delta copy. Gold = everything else (heavy mart queries).
Silver has no clear definition. Layer triggers are file-based (`check_file`,
`gold_trigger.flag`), creating a chain of fragile signals.

**Root cause:**
No deliberate design decision about what each layer owns. The medallion architecture
was adopted as a folder structure, not as a data contract.

**Symptoms:**
- Gold notebooks contain business logic AND data quality AND aggregations
- Fixing a Gold bug sometimes requires re-running Bronze
- No answer to "why is Silver even here?"

→ See [04_medallion_design.md](04_medallion_design.md) for the layer contracts.


## Pain Point 4 — CI/CD ESM Mismatch

**What you have:**
Every change — including enabling a paused production pipeline — goes through the
full ESM approval cycle (feature branch → dev → test → UAT → team leader → prod).
This makes operational decisions (on/off switches) as expensive as code deployments.

**Root cause:**
No distinction between *deploy-time changes* (code that needs review and approval)
and *run-time control* (operational state that an operator should manage directly).

**Symptoms:**
- Enabling a paused job in production requires days of approval
- Can't react quickly to data quality incidents
- Developers and operators have the same permissions model

→ See [05_cicd_boundary.md](05_cicd_boundary.md) for the argument to make and the patterns to implement.


## Pain Point 5 — Environment/Catalog Chaos

**What you have:**
Unity Catalog names are unique across an Azure tenant, forcing `VOCP` (prod) and
`VOCD` (dev) as separate catalogs. Every piece of code that references a catalog
name must branch on environment, creating a maintenance burden that grows with
every new catalog, table, and pipeline.

**Root cause:**
No configuration abstraction layer. Catalog names are treated as code constants
rather than environment-injected values.

**Symptoms:**
- `if env == "prod": catalog = "VOCP" else: catalog = "VOCD"` scattered everywhere
- Adding a new catalog means updating dozens of source files
- Profile differences are invisible unless you diff across environments

→ See [06_environment_management.md](06_environment_management.md) for the `Settings` pattern.


## Pain Point 6 — Code Quality

**What you have:**
Pipelines written exclusively in Databricks notebooks. Multiple assignment without
justification. Poor or absent comments. No understanding of when to use side effects
vs functional style. Repetitive code. Hardcoded values. No tests, no linting, no
formatter.

**Root cause:**
Notebook-first culture. Databricks notebooks are excellent for exploration but
terrible for production pipeline management. The tooling gap (no easy pytest, no
ruff, no proper imports) reinforces bad habits.

**Symptoms:**
- Can't run `pytest` against pipeline code
- `git diff` on a notebook shows JSON diffs, not code diffs
- Refactoring is scary because there are no tests

→ See [07_code_quality.md](07_code_quality.md) for the package structure and tooling setup.
