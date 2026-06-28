# Airbnb: The Company That Built Airflow

## Context

Airbnb created Apache Airflow in 2014 out of necessity. Their data infrastructure was built on a growing tangle of cron jobs and brittle scripts with no unified view of pipeline state, no dependency modeling, and no retry logic. Airflow was the answer: a programmable DAG-based scheduler where pipelines were code, not configuration.

By the time Airflow was open-sourced (2015) and donated to the Apache Software Foundation (2019), Airbnb had already built an opinionated production data platform on top of it — one that shaped many of the patterns the wider industry now follows.

## Scale

- 35 billion events per day at peak
- 12,000+ business metrics defined in the Minerva metric store
- Petabyte-scale data warehouse across S3 + Hive + Presto

## Spark + Airflow Integration

### Orchestration design

Airbnb's Airflow usage is deep and opinionated. Each DAG represents a logical data product (a table, a metric, a feature set), not a technical process. Tasks within a DAG represent steps in that product's production: ingest, transform, validate, publish.

Their landmark internal framework, **Minerva** (the metric store), extends this model: metric definitions are versioned YAML files that generate DAGs automatically, similar to the registry-driven dynamic task generation in this project. The idea is that engineers define *what* the metric is (its SQL logic, its dimensions, its SLAs) and the framework generates the pipeline that produces it.

### The data quality gate pattern

One of Airbnb's most influential contributions to the industry is the systematic placement of data quality assertion tasks between pipeline layers. Before a table is promoted from bronze to silver, or from silver to gold, a validation task runs first:

```
ingest_raw → validate_raw → transform_silver → validate_silver → build_gold
```

If `validate_silver` fails — because row counts dropped more than expected, because a join produced unexpected nulls, because a critical column went null — the downstream task does not run. The data product does not publish a corrupted metric. The on-call engineer gets paged, not the analytics consumer.

This pattern is implemented with **Great Expectations** in some teams, and with custom Spark-based assertion jobs in others. The mechanics differ; the placement in the DAG is invariant.

### Asset-based scheduling

Airbnb moved away from time-offset dependencies (`execution_date + timedelta(hours=2)`) toward event-driven dependencies. In their model, a downstream DAG is triggered when its upstream *data* is ready — not when a clock says it should be ready. This maps directly to Airflow 3's `Asset` scheduling model and is one of the design directions that Airbnb engineers contributed to the Airflow project.

### SLA contracts per table

Every table that Airbnb's data platform owns carries an explicit SLA: a time by which the table must be fresh. SLA violations are paged. SLA windows are set based on business need (e.g., "executive dashboards must reflect last night's data by 8 AM") and are tracked per-DAG in Airflow's SLA monitoring.

## Lessons for this project

- **Data quality gates are not optional** — the audit log in `bronze/audit.py` is a start, but a silver validation task that checks row counts, null rates, and referential integrity before gold runs is the production pattern.
- **Registry-driven DAG generation** — the `table_registry.yaml` approach in this project directly mirrors Airbnb's Minerva model. It is the right design.
- **SLA definitions belong in the DAG** — every task in this project's DAGs should have an `execution_timeout` and ideally an `on_failure_callback` that notifies a real channel.

## References

- Maxime Beauchemin, *Airflow: a workflow management platform*, Airbnb Engineering Blog, 2015. https://medium.com/airbnb-engineering/airflow-a-workflow-management-platform-46318b977fd8
- *Data Infrastructure at Airbnb*, Airbnb Engineering Blog. https://medium.com/airbnb-engineering/data-infrastructure-at-airbnb-8adfb34f169c
- Airflow GitHub repository (donated to Apache): https://github.com/apache/airflow
