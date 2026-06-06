# Compute Strategy — Solving the Cold Start Problem

## The Databricks Compute Model

Databricks offers three compute types, each with a different latency/cost trade-off:

| Type | Cold Start | Cost | Best For |
|------|-----------|------|----------|
| **Job Cluster** | 5–10 min | Per-minute billing, terminates after job | Scheduled batch jobs where latency doesn't matter |
| **All-Purpose Cluster** | 0 min (warm) | Continuous billing while running | Interactive notebooks, ad-hoc exploration |
| **SQL Warehouse** | < 30 sec (serverless) or 2–3 min (classic) | Per-DBU, auto-suspends | BI queries, SQL analytics |
| **Cluster Pool** | 30–60 sec (pre-warmed VMs) | Pool instances billed while idle | Job clusters that need faster start |

In the 망분리 context, serverless SQL Warehouse is likely blocked by SaaS restrictions.
Classic SQL Warehouse and Cluster Pool are available since they use dedicated VMs.


## The Right Tool for Each Workload

### Interactive Exploration
**Problem**: A data analyst wants to run a quick query on production data.
They create a Job Cluster → wait 7 minutes → run one query → cluster terminates.

**Solution options** (pick one based on your security posture):
1. **All-Purpose Cluster (shared)**: Always-on cluster available to all analysts.
   Cost: continuous billing. Acceptable for small teams with predictable usage.
2. **Classic SQL Warehouse**: 2–3 min start, auto-suspends after inactivity.
   Best for SQL-only analytics. Doesn't require notebook privileges.
3. **Cluster Pool**: Pre-warmed VMs reduce Job Cluster start to ~60 sec.
   Good if you can't have shared always-on clusters for security reasons.

**The argument to make internally:**
An analyst who waits 7 minutes will either stop exploring data (lost insight)
or keep the cluster running permanently (cost blowout). Neither is acceptable.
An All-Purpose Cluster or SQL Warehouse at fixed cost is cheaper than wasted
analyst hours and zombie clusters.

### Scheduled Batch Pipelines
**Problem**: Production pipelines use Job Clusters and take 7 minutes to start.

**Solution**: This is actually acceptable — with the right design:
- Batch pipelines should not be latency-sensitive
- A 7-minute start is fine if the job itself runs for 30+ minutes
- Design pipelines to run less frequently (daily, not hourly) where possible
- Use Cluster Pools to cut start time to ~60 sec if truly needed

**The mistake**: Using Job Clusters for pipelines that run every 15 minutes.
Each run costs 7 minutes of startup overhead. Redesign to run hourly with 
incremental processing instead.

### Ad-Hoc Debugging
**Problem**: A pipeline fails. Engineer opens a Job Cluster to investigate.
7-minute wait. Files queries. Cluster terminates. Opens another. Repeat.

**Solution**: Keep a small All-Purpose Cluster (1–2 nodes) running during
business hours specifically for debugging. The cost is predictable and small
compared to the productivity loss of repeated cold starts.


## Why This Lab Uses Always-On Spark

In this lab, PySpark runs *inside* the Airflow container. The JVM is warm.
There's no cold start for DAG tasks.

This directly mirrors the **Cluster Pool / All-Purpose Cluster** pattern:
compute is pre-provisioned and available, not spun up per-job.

The lab teaches Delta Lake patterns that work regardless of which compute
model you use. The principles don't change when you move back to Databricks;
only the `SparkSession.builder.master(...)` call changes.

```python
# Lab (always-on Spark in container)
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Databricks Job Cluster (cold start, 7 min)
# SparkSession is provided automatically — no .master() needed
spark = SparkSession.getActiveSession()

# Databricks All-Purpose Cluster (warm, 0 min)
# Same as above — SparkSession is already running
spark = SparkSession.getActiveSession()
```


## Recommendations for Your Setup

1. **Provision a shared All-Purpose Cluster for analysts** (2–4 nodes, auto-terminate after 4h inactivity). Single-sign-on via Azure AD. Budget: fixed monthly.

2. **Use Cluster Pools for production Job Clusters** to cut start time from 7 min to ~60 sec. The pool holds idle instances between jobs.

3. **Move simple SQL analytics to SQL Warehouse** (Classic, not Serverless since that's blocked). Auto-suspend after 30 minutes.

4. **Redesign high-frequency small jobs** to be batched. A job that runs every 15 minutes should probably run hourly with Structured Streaming or micro-batch instead.

5. **Document the compute model in your runbook**: which pipelines use which compute type, what the expected start time is, and who is responsible for monitoring zombie clusters.


## Production Proposal

### The Real Problem at Your Company

You have two Azure Databricks workspaces (DEV and PROD, separate subscriptions).
Serverless compute is blocked by 망분리 — no SaaS endpoint can reach outside the
corporate network boundary. Every Job Cluster spins cold Azure VMs: 6-7 minutes.
As a cloud administrator, you're also watching VMs appear, linger, and disappear
at random, with no predictable cost pattern and no easy way to explain it to
finance or your team leader.

The lab solution (always-on Spark in an Airflow container) teaches you the principle:
pre-provisioned compute beats cold-start compute every time. The production answer
is **Databricks Cluster Pools**.

### Solution: Cluster Pools

A Cluster Pool is a set of pre-warmed Azure VMs sitting idle, managed by Databricks.
When a Job Cluster or All-Purpose Cluster is configured to use the pool, it grabs an
idle VM instead of provisioning from scratch. Cold start drops from 6-7 minutes to
roughly 30-90 seconds.

Pool VMs are idle — they are not running Spark, not running a driver, not billing
DBU. They are just reserved Azure VMs. As a cloud admin, this is a clean mental model:
one pool with a defined minimum and maximum, instead of unpredictable VM churn.

**Configure the pool in `databricks.yml` (Databricks Asset Bundles):**
```yaml
resources:
  instance_pools:
    batch_pool:
      instance_pool_name: insurance-batch-pool
      node_type_id: Standard_DS3_v2
      min_idle_instances: 1
      max_capacity: 8
      idle_instance_autotermination_minutes: 20
```

**Attach a Job Cluster to the pool:**
```yaml
resources:
  jobs:
    bronze_ingest:
      job_clusters:
        - job_cluster_key: bronze_cluster
          new_cluster:
            instance_pool_id: ${resources.instance_pools.batch_pool.id}
            spark_version: 15.4.x-scala2.12
            num_workers: 2
```

### Workload Map for Your Environment

| Workload | Compute | Cold Start | Notes |
|---|---|---|---|
| Interactive exploration (analyst) | All-Purpose Cluster attached to pool | ~60 sec (from pool) | Auto-terminate after 30 min idle |
| Bronze/Silver/Gold batch ETL | Job Cluster attached to pool | ~60 sec (from pool) | Pool has min 1 idle VM |
| Ad-hoc debugging | Same All-Purpose Cluster | 0 sec (already warm) | Don't spin a new cluster to debug |
| SQL analytics (if not blocked) | Classic SQL Warehouse | 2-3 min | Not serverless — dedicated VM |

### What to Tell the Cloud Team

Cluster Pools are not always-running compute — they are a VM reservation with
auto-release. The cost model is: pool idle instances are billed at VM rate only
(no Databricks DBU). This is significantly cheaper than a running All-Purpose Cluster.
Present pool cost (VM reservation) vs. current cost (ad-hoc VMs + 7-min idle time
during startup) to make the financial argument.

### As Cloud Administrator

Define pool lifecycle in your governance runbook:
- DEV pool: min 1 idle, max 4, auto-terminates idle instances after 20 min
- PROD pool: min 2 idle (SLA on start time), max 10, monitored via Azure Monitor
- Tag all pool VMs with `project=data-platform`, `env=dev|prod` for cost attribution
- Set up an Azure Cost Management budget alert on the tag — the first signal that
  a pool is misconfigured and holding too many idle VMs
