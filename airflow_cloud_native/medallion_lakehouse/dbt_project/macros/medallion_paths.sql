{# Per-model location helpers — called from inside `{{ config(...) }}` blocks
   in each model file so `this` is resolved against that model's render context.
   `this.name` is not defined when the project-level config renders. #}

{% macro silver_location() -%}
  s3://{{ env_var('LAKE_BUCKET', 'lakehouse') }}/silver/{{ this.name }}.parquet
{%- endmacro %}

{% macro gold_location() -%}
  s3://{{ env_var('LAKE_BUCKET', 'lakehouse') }}/gold/{{ this.name }}.parquet
{%- endmacro %}
