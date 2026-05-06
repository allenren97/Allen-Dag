"""Extract task: read a table YAML and import to a local parquet file."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import IMPORT_CONNECTORS


UPSTREAM_TASKS: list[str] = []


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def extract(yaml_path: str, upstream_task_ids: dict[str, str]) -> str:
    """Look up the import connector by ``engine`` and run it.

    The connector class is resolved from ``IMPORT_CONNECTORS`` (auto-registered
    by every ``BaseConnector`` subclass that sets a non-empty ``ENGINE``),
    so adding a new source database does not require editing this file.
    """
    del upstream_task_ids
    cfg = _load_cfg(yaml_path)
    context = get_current_context()
    engine = str(cfg.get("engine") or "").strip().lower()

    connector_cls = IMPORT_CONNECTORS.get(engine)
    if connector_cls is None:
        raise AirflowException(
            f"Unsupported engine {engine!r} in {yaml_path}; "
            f"known engines: {sorted(IMPORT_CONNECTORS)}"
        )

    landing_partition_prefix = (
        str(context.get("dag").dag_id) if context.get("dag") else None
    )
    importer = connector_cls(
        connection_id_import=cfg["connection_id_import"],
        database=cfg["database"],
        table=cfg["table"],
        predicate=cfg.get("predicate"),
        landing_partition_prefix=landing_partition_prefix,
    )
    return importer.to_parquet(**context)
