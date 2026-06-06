# Security & Audit

## What Problem This Solves

A production job was re-enabled at 03:00 on a Sunday without going through ESM. It
shouldn't have been — but someone did it, and now you need to prove to your compliance
team what happened, who did it, and from where.

`system.access.audit` is an immutable, platform-managed log of every action taken
against your Databricks workspace: every API call, every UI click, every permission
change, every cluster start, every secret access. It is the compliance evidence that
ESM is designed to create — but system tables give it to you automatically, for free,
with SQL access.


## `system.access.audit`

| Column | Type | Meaning |
|---|---|---|
| `account_id` | string | Account |
| `workspace_id` | string | Workspace (0 for account-level events) |
| `event_time` | timestamp | When the event occurred (UTC) |
| `event_date` | date | Partition column — always filter on this |
| `user_identity` | struct | `{email, subjectName}` — who performed the action |
| `source_ip_address` | string | IP address of the request |
| `user_agent` | string | HTTP client (Terraform, SDK, browser, etc.) |
| `service_name` | string | Which Databricks service (clusters, jobs, secrets, etc.) |
| `action_name` | string | What happened (create, update, delete, get, run, etc.) |
| `request_params` | map | Parameters sent with the request |
| `response` | struct | `{statusCode, errorMessage, result}` |
| `audit_level` | string | `WORKSPACE_LEVEL` or `ACCOUNT_LEVEL` |
| `session_id` | string | Session identifier |
| `identity_metadata` | struct | `{run_by, run_as}` — distinguishes who triggered vs who executed |

**Retention**: 365 days. Workspace-level events are regional (stored in the workspace
region). Account-level events (metastore changes, account admin actions) are global.


## Key `service_name` + `action_name` Combinations

| What you want to audit | service_name | action_name |
|---|---|---|
| Job created / modified | `jobs` | `create`, `update`, `reset` |
| Job enabled / disabled | `jobs` | `update` (check `request_params.new_settings.schedule.pause_status`) |
| Job run triggered manually | `jobs` | `runNow` |
| Cluster created | `clusters` | `create` |
| Cluster terminated | `clusters` | `delete` |
| Cluster policy changed | `clusterPolicies` | `create`, `edit` |
| Secret read | `secrets` | `getSecret` |
| Table permission granted | `unityCatalog` | `updatePermissions` |
| Catalog permission granted | `unityCatalog` | `updatePermissions` |
| User added to workspace | `accounts` | `addPrincipalToGroup`, `updateUser` |
| Service principal created | `accounts` | `createServicePrincipal` |
| Workspace config changed | `workspace` | `workspaceConfEdit` |


## Production Query Patterns

### Who re-enabled that job at 03:00?

```sql
SELECT
    event_time,
    user_identity.email             AS user,
    source_ip_address,
    user_agent,
    request_params['job_id']        AS job_id,
    request_params['new_settings']  AS new_settings_json,
    response.statusCode
FROM system.access.audit
WHERE service_name = 'jobs'
  AND action_name = 'update'
  AND event_date = '${date}'
  AND event_time BETWEEN '${date} 02:00:00' AND '${date} 04:00:00'
ORDER BY event_time;
```

### All permission changes in the last 30 days

```sql
SELECT
    event_time,
    user_identity.email             AS changed_by,
    request_params['securable_type']    AS object_type,
    request_params['securable_full_name'] AS object_name,
    request_params['changes']          AS permission_changes,
    source_ip_address
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
  AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### Secret access audit

Who read which secrets, and when:

```sql
SELECT
    event_time,
    user_identity.email             AS user,
    request_params['scope']         AS secret_scope,
    request_params['key']           AS secret_key,
    source_ip_address,
    response.statusCode
FROM system.access.audit
WHERE service_name = 'secrets'
  AND action_name = 'getSecret'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### Terraform vs manual changes

Separate infrastructure-as-code changes (managed) from ad-hoc changes (potentially
unmanaged):

```sql
SELECT
    DATE_TRUNC('day', event_time)   AS day,
    CASE
        WHEN user_agent LIKE '%Terraform%' THEN 'terraform'
        WHEN user_agent LIKE '%databricks-sdk%' THEN 'sdk'
        WHEN user_agent LIKE '%Mozilla%' THEN 'ui'
        ELSE 'other'
    END                             AS change_source,
    service_name,
    action_name,
    COUNT(*)                        AS event_count
FROM system.access.audit
WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND service_name IN ('jobs', 'clusters', 'clusterPolicies', 'unityCatalog')
  AND action_name IN ('create', 'update', 'edit', 'delete', 'reset')
GROUP BY day, change_source, service_name, action_name
ORDER BY day DESC, event_count DESC;
```


## Connecting to Pain Point 4 (CI/CD ESM Mismatch)

The ESM approval process exists to create an audit trail of changes to production.
But it treats every change — including enabling a paused job — as a deployment event.

`system.access.audit` gives you the same audit trail automatically:
- Every job enable/disable is recorded with user, timestamp, and IP
- Every cluster config change is immutably logged
- Every permission grant is captured

This is the compliance evidence your auditors need, without routing operational
changes through a 3-day approval cycle.

The argument to make to your compliance team:
> "ESM was designed to prove that production changes are authorized and traceable.
> `system.access.audit` provides that traceability automatically, in real time,
> with 365-day retention, for every action — not just the ones we remember to
> route through ESM. We should restrict ESM to deploy-time changes (code, schema,
> permissions) and rely on system tables for operational audit."

→ See [`07_pain_points_addressed.md`](07_pain_points_addressed.md) for the full analysis.
