# duck-hatchery

DuckDB experiments with Python clients, in marimo notebooks.

DuckDB is an in-process analytical (OLAP) database — think "SQLite for analytics."
There is no server to run: it lives inside your Python process and can query pandas /
Polars / Arrow frames and Parquet / CSV files directly, in place.

## Requirements

- Python 3.13
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync
```

## Run

```bash
uv run marimo edit notebooks/00_duckdb_in_process_analytics.py
```
