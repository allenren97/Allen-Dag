"""Extract task: read a table YAML and import to a local parquet file."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import (
    DB2ImportConnector,
    MSSQLImportConnector,
)


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def extract(yaml_path: str) -> str:
    """Instantiate the right import connector for the given YAML and run it."""
    cfg = _load_cfg(yaml_path)
    context = get_current_context()
    engine = str(cfg.get("engine") or "").lower().strip()
    landing_partition_prefix = str(context.get("dag").dag_id) if context.get("dag") else None

    if engine == "mssql":
        importer = MSSQLImportConnector(
            connection_id_import=cfg["connection_id_import"],
            database=cfg["database"],
            table=cfg["table"],
            predicate=cfg.get("predicate"),
            landing_partition_prefix=landing_partition_prefix,
        )
    elif engine == "db2":
        importer = DB2ImportConnector(
            connection_id_import=cfg["connection_id_import"],
            database=cfg["database"],
            table=cfg["table"],
            predicate=cfg.get("predicate"),
            landing_partition_prefix=landing_partition_prefix,
        )
    else:
        raise AirflowException(
            f"Unsupported engine {engine!r} in {yaml_path}"
        )
    return importer.to_parquet(**context)
