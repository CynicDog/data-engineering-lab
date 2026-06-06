"""Config-driven table registry — single source of truth for all channels and tables.

Adding a new table: one YAML block in config/table_registry.yaml.
No Python changes. No DAG changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parents[3]  # .../databricks_lakehouse/


@dataclass
class TableSpec:
    name: str
    channel: str
    pk: str
    dedup_ts_col: str
    schedule_type: str
    cast_types: dict[str, str] = field(default_factory=dict)
    pii: dict[str, str] = field(default_factory=dict)
    derived_columns: dict[str, str] = field(default_factory=dict)


@dataclass
class ChannelSpec:
    name: str
    tables: list[TableSpec]


@dataclass
class Registry:
    channels: list[ChannelSpec]
    marts: list[str]

    def all_tables(self) -> list[TableSpec]:
        return [t for ch in self.channels for t in ch.tables]

    def tables_for_channel(self, channel: str) -> list[TableSpec]:
        for ch in self.channels:
            if ch.name == channel:
                return ch.tables
        return []

    def table_spec(self, channel: str, table: str) -> TableSpec:
        for spec in self.tables_for_channel(channel):
            if spec.name == table:
                return spec
        raise KeyError(f"No table {table!r} in channel {channel!r}")


def load_registry(path: Path | None = None) -> Registry:
    path = path or _REPO_ROOT / "config" / "table_registry.yaml"
    with open(path) as f:
        data = yaml.safe_load(f)

    channels = [
        ChannelSpec(
            name=ch_name,
            tables=[
                TableSpec(
                    name=t_name,
                    channel=ch_name,
                    pk=t_data["pk"],
                    dedup_ts_col=t_data["dedup_ts_col"],
                    schedule_type=t_data["schedule_type"],
                    cast_types=t_data.get("cast_types") or {},
                    pii=t_data.get("pii") or {},
                    derived_columns=t_data.get("derived_columns") or {},
                )
                for t_name, t_data in ch_data["tables"].items()
            ],
        )
        for ch_name, ch_data in data["channels"].items()
    ]
    return Registry(channels=channels, marts=data.get("marts") or [])
