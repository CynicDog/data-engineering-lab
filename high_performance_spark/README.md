# high-performance-spark

PySpark experiments in marimo notebooks.

## Requirements

- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- Java 17+ (required by Spark 4.x)

## Setup

```bash
uv sync
```

## Run

Entry script:

```bash
uv run main.py
```

Marimo notebooks:

```bash
uv run marimo edit notebooks/00_spark_setup.py
```
