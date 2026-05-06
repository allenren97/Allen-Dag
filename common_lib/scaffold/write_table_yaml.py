"""Write one ``<table>.yaml`` per intake row into ``<dag>/table/``."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


YAML_FIELDS = (
    "engine",
    "connection_id_import",
    "connection_id_export",
    "database",
    "schema",
    "table",
    "predicate",
)


def write_table_yaml(dag_dir: Path, row: dict[str, Any]) -> Path:
    """Serialize the connector-relevant fields of ``row`` to YAML."""
    table_dir = dag_dir / "table"
    table_dir.mkdir(parents=True, exist_ok=True)
    out_path = table_dir / f"{row['table']}.yaml"
    payload = {key: row.get(key) for key in YAML_FIELDS}
    out_path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return out_path
