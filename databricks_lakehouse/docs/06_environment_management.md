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
