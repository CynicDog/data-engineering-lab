# Environment Management — The Catalog Naming Problem

## The VOCP/VOCD Problem

Unity Catalog names are unique across an Azure tenant. You have one tenant,
two Databricks workspaces (DEV subscription, PROD subscription), and you
want a catalog called `VOC` in both. You can't. So you create `VOCD` and `VOCP`.

Now every piece of code that references `VOC` must know whether it's running
in dev or prod:

```python
# What happens without a config layer
if env == "prod":
    catalog = "VOCP"
elif env == "dev":
    catalog = "VOCD"

spark.sql(f"SELECT * FROM {catalog}.bronze.voc")
```

This pattern scales terribly:
- You have MLCRP, MLVOCP, MLIWT, MLSQP → four catalogs, each with dev/prod variants
- Eight catalog names to manage, branching in every notebook and job
- Adding a new catalog means updating every place that branches on env
- A typo in `MLVOCD` vs `MLVOCP` causes a silent wrong-environment read


## The Fix: Profile-Based Configuration

Instead of branching on environment inside code, inject the environment-specific
values as configuration:

```python
# What this lab implements in src/lakehouse/config/settings.py
@dataclass(frozen=True)
class Settings:
    env: str
    catalog: str  # "VOCD" or "VOCP" — injected, never branched on in code

def load_settings(profile: str | None = None) -> Settings:
    profile = profile or os.environ.get("LAKEHOUSE_PROFILE", "dev")
    with open(f"config/{profile}.yaml") as f:
        data = yaml.safe_load(f)
    return Settings(**data)
```

```yaml
# config/dev.yaml
env: dev
catalog: VOCD

# config/prod.yaml
env: prod
catalog: VOCP
```

```python
# In any pipeline code — no branching, no if/else
settings = load_settings()
spark.sql(f"SELECT * FROM {settings.catalog}.bronze.voc")
```

The catalog name is configuration, not code.


## Extending to Multiple Catalogs

For MLCRP, MLVOCP, MLIWT, MLSQP (mapping to on-premise channel databases):

```yaml
# config/dev.yaml
env: dev
catalogs:
  crp: MLCRPD
  voc: MLVOCD
  iwt: MLIWTD
  sqp: MLSQPD

# config/prod.yaml
env: prod
catalogs:
  crp: MLCRPP
  voc: MLVOCP
  iwt: MLIWTP
  sqp: MLSQPP
```

```python
@dataclass(frozen=True)
class Settings:
    env: str
    catalogs: dict[str, str]  # logical name → actual UC catalog name

settings = load_settings()
crp_catalog = settings.catalogs["crp"]  # "MLCRPD" in dev, "MLCRPP" in prod
spark.sql(f"SELECT * FROM {crp_catalog}.bronze.policy")
```

One place to change when you add a catalog. Zero code branching.


## Environment Variable Override

In production, sensitive values (credentials, connection strings) should not
live in YAML files. Use environment variables with the YAML as a non-secret
default:

```python
def load_settings(profile=None):
    profile = profile or os.environ.get("LAKEHOUSE_PROFILE", "dev")
    with open(f"config/{profile}.yaml") as f:
        data = yaml.safe_load(f)
    return Settings(
        env=data["env"],
        catalog=data["catalog"],
        # Env vars override YAML values
        s3_access_key=os.environ.get("LAKE_S3_ACCESS_KEY", data["s3_access_key"]),
        s3_secret_key=os.environ.get("LAKE_S3_SECRET_KEY", data["s3_secret_key"]),
    )
```

In Azure Databricks, store secrets in Azure Key Vault and reference them
via `dbutils.secrets.get()`. In Airflow, use Airflow Connections/Variables.


## The Databricks Asset Bundle Approach

For Databricks-specific environments, Databricks Asset Bundles (DABs) support
target environments natively:

```yaml
# databricks.yml
bundle:
  name: voc_platform

targets:
  dev:
    workspace:
      host: https://adb-xxx.azuredatabricks.net
    variables:
      catalog: VOCD

  prod:
    workspace:
      host: https://adb-yyy.azuredatabricks.net
    variables:
      catalog: VOCP

resources:
  jobs:
    bronze_ingest:
      name: "Bronze Ingest [${var.catalog}]"
      tasks:
        - task_key: ingest_customer
          notebook_task:
            notebook_path: ./notebooks/bronze_ingest
            base_parameters:
              catalog: ${var.catalog}
```

Deploy to dev: `databricks bundle deploy --target dev`
Deploy to prod: `databricks bundle deploy --target prod`

This is the "infrastructure as code" answer to VOCP/VOCD. The bundle knows
which catalog to use per target. No branching in notebook code.


## Summary

| Approach | Code branching | Maintainability |
|----------|---------------|-----------------|
| Hardcoded catalog names | Every file | Nightmare at scale |
| `if env == "prod":` branching | Every file | Marginally better |
| Profile YAML + Settings dataclass | Zero | Easy to add catalogs |
| Databricks Asset Bundles | Zero (in notebooks) | Best for Databricks-native |

Use the `Settings` dataclass pattern immediately (no Databricks dependency,
works in any Python code). Add Asset Bundles when you formalize your
Databricks deployment pipeline.


## Production Proposal

### The Real Problem at Your Company

You have one Azure tenant. Two Databricks workspaces (DEV subscription, PROD
subscription). Unity Catalog names are unique across the tenant. You cannot
call a catalog `VOC` in both — so DEV gets `VOCD` and PROD gets `VOCP`.
Same pattern repeats for every channel database:

| Logical | DEV | PROD |
|---|---|---|
| VOC | VOCD | VOCP |
| MLCRP | MLCRPD | MLCRPP |
| MLVOC | MLVOCD | MLVOCP |
| MLIWT | MLIWTD | MLIWTP |
| MLSQP | MLSQPD | MLSQPP |

Eight catalog names. Every notebook, every SQL query, every Python file that
references a catalog must somehow know which environment it is in. The current
implementation branches on environment inside the code. You need 팀장 approval to
add a new catalog because it touches dozens of files.

The lab teaches the `Settings` dataclass pattern. In production, the cleaner
answer is **Databricks Asset Bundles (DABs) variable substitution at deploy time**.

### Solution: DABs Variables — Catalog Name Injected, Never Branched

Define all catalog names as DABs variables with per-target values:

```yaml
bundle:
  name: insurance-data-platform

variables:
  voc_catalog:
    description: "VOC channel catalog"
  mlcrp_catalog:
    description: "MLCRP channel catalog"
  mlvoc_catalog:
    description: "MLVOC channel catalog"
  mliwt_catalog:
    description: "MLIWT channel catalog"
  mlsqp_catalog:
    description: "MLSQP channel catalog"

targets:
  dev:
    workspace:
      host: https://adb-dev-xxxx.azuredatabricks.net
    variables:
      voc_catalog: VOCD
      mlcrp_catalog: MLCRPD
      mlvoc_catalog: MLVOCD
      mliwt_catalog: MLIWTD
      mlsqp_catalog: MLSQPD

  prod:
    workspace:
      host: https://adb-prod-yyyy.azuredatabricks.net
    variables:
      voc_catalog: VOCP
      mlcrp_catalog: MLCRPP
      mlvoc_catalog: MLVOCP
      mliwt_catalog: MLIWTP
      mlsqp_catalog: MLSQPP
```

**Inject catalog names as job parameters at deploy time:**
```yaml
resources:
  jobs:
    mlcrp_bronze:
      name: "MLCRP Bronze [${var.mlcrp_catalog}]"
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/bronze_ingest
            base_parameters:
              catalog: ${var.mlcrp_catalog}
              voc_catalog: ${var.voc_catalog}
```

When `databricks bundle deploy --target dev` runs in AzDevOps, the `catalog`
parameter is set to `MLCRPD`. When `--target prod` runs, it is set to `MLCRPP`.
The parameter is baked into the deployed Job definition — no branching at runtime.

### Python Code: Zero Environment Branching

```python
# In the notebook — reads the parameter that DABs already set correctly
catalog = dbutils.widgets.get("catalog")
voc_catalog = dbutils.widgets.get("voc_catalog")

spark.sql(f"SELECT * FROM {catalog}.bronze.policy")
spark.sql(f"SELECT * FROM {voc_catalog}.bronze.voc")
```

Or from a Python module using the `Settings` wrapper:
```python
@dataclass(frozen=True)
class Settings:
    catalog: str
    voc_catalog: str

    @classmethod
    def from_widgets(cls, dbutils) -> "Settings":
        return cls(
            catalog=dbutils.widgets.get("catalog"),
            voc_catalog=dbutils.widgets.get("voc_catalog"),
        )
```

Zero `if env == "prod"` branching. Zero profile YAML at runtime.
The deploy pipeline is the single source of truth for which catalog is which.

### Adding a New Catalog

Before: update every Python file that branches on environment.
After: add one variable to `databricks.yml`:

```yaml
variables:
  mlnew_catalog:
    description: "MLNEW channel catalog"

targets:
  dev:
    variables:
      mlnew_catalog: MLNEWD
  prod:
    variables:
      mlnew_catalog: MLNEWP
```

Add it as a parameter to the relevant jobs. Done. No other files need to change.

### Secret Management

Catalog names are not secrets — they can live in `databricks.yml` and be committed
to the repository. Credentials and connection strings are different. For those, use
Azure Key Vault backed secrets in Databricks:

```python
storage_account_key = dbutils.secrets.get(
    scope="kv-data-platform",
    key="adls-storage-account-key"
)
```

Never put credentials in `databricks.yml` or profile YAML. The Key Vault scope
is configured once per workspace by the cloud admin — no per-notebook credential
management needed.
