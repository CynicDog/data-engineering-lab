# hdfs-hatchery

The Hadoop Distributed Filesystem (HDFS), hands-on, in a marimo notebook.

A `docker compose` stack stands up a real (if tiny) HDFS cluster — one namenode
and three datanodes — and the notebook drives it: uploading files, inspecting
blocks and replicas, killing a datanode to watch the cluster route around it,
and reading straight from the namenode's REST API. Nothing here is simulated.

## Requirements

- Python 3.13
- [uv](https://docs.astral.sh/uv/)
- [Docker](https://docs.docker.com/get-docker/) with Compose

## Setup

```bash
uv sync
docker compose up -d
```

Give the cluster a few seconds to leave safe mode before running the notebook
(it self-checks for this).

## Run

```bash
uv run marimo edit notebooks/00_hdfs_concepts.py
```

## Tear down

```bash
docker compose down -v
```

The `-v` also drops the namenode/datanode volumes, so the next `up` starts
from a freshly formatted, empty filesystem.
