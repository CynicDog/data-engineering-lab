# lakehouse_on_kubernetes — one entrypoint for the whole lifecycle.
CLUSTER ?= lakehouse
NS      ?= lakehouse
KUBECTL := kubectl --context kind-$(CLUSTER)

.PHONY: help sync build up seed trigger status logs ui down clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

sync: ## Copy the lakehouse package + config from databricks_lakehouse
	./scripts/sync-lakehouse.sh

build: ## Build the Airflow + Spark images and load them into Kind
	./scripts/build-images.sh

up: ## Create the cluster and deploy the full stack
	./scripts/up.sh

seed: ## Populate the MinIO landing zone with synthetic data
	./scripts/seed.sh

trigger: ## Unpause + trigger the ingest_bronze DAG (chains silver -> gold via assets)
	$(KUBECTL) exec deploy/airflow-scheduler -n $(NS) -- \
	  bash -lc 'airflow dags unpause ingest_bronze && \
	            airflow dags unpause transform_silver && \
	            airflow dags unpause build_gold && \
	            airflow dags trigger ingest_bronze'

status: ## Show pods + SparkApplications
	@echo "=== pods ===";            $(KUBECTL) get pods -n $(NS)
	@echo "=== sparkapplications ==="; $(KUBECTL) get sparkapplications -n $(NS) 2>/dev/null || true

logs: ## Tail logs of the most recent Spark driver pod (APP=<name-substr> to filter)
	@pod=$$($(KUBECTL) get pods -n $(NS) -l spark-role=driver \
	  --sort-by=.metadata.creationTimestamp -o name | tail -1); \
	  echo "driver: $$pod"; $(KUBECTL) logs -f -n $(NS) $$pod

ui: ## Fallback UI access via port-forward (if NodePort isn't reachable)
	@svc=$$($(KUBECTL) get svc -n $(NS) -o name | grep -E 'api-server|webserver' | head -1); \
	  echo "port-forwarding $$svc -> http://localhost:8110"; \
	  $(KUBECTL) port-forward -n $(NS) $$svc 8110:8080

down: ## Tear everything down (deletes the Kind cluster + all data)
	./scripts/down.sh

clean: ## Remove synced-in artifacts from this project
	rm -rf lakehouse/src lakehouse/config/table_registry.yaml \
	       lakehouse/generate_source_data.py lakehouse/init_ods.sql
