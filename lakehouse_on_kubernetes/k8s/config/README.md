# k8s/config — Secret + ConfigMaps as static manifests

Applied declaratively by `up.sh` with `kubectl apply -f k8s/config/`.

| file | committed? | contents |
|------|-----------|----------|
| `minio-creds.yaml` | yes | S3 / MinIO credentials Secret (lab plaintext) |
| `lakehouse-config.generated.yaml` | no (generated) | ConfigMap: `dev.yaml`, `prod.yaml`, `table_registry.yaml` → mounted at `/opt/lakehouse/config` |
| `pg-init.generated.yaml` | no (generated) | ConfigMap: `01_init_ods.sql` → Postgres `/docker-entrypoint-initdb.d` |

The two `*.generated.yaml` files are rendered by `scripts/sync-lakehouse.sh`
(`kubectl create configmap --dry-run=client -o yaml`, fully offline) from the
single-source files in `lakehouse/`. They wrap synced content
(`table_registry.yaml`, `init_ods.sql`), so they are generated rather than
hand-edited to avoid drift — but the result is a real, inspectable manifest.

Run `make sync` (or `make up`, which calls it) to (re)generate them, then
`cat k8s/config/lakehouse-config.generated.yaml` to inspect.
