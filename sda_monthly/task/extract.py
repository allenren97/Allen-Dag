"""Extract task: read a table YAML and import to a local parquet file."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import IMPORT_CONNECTORS


UPSTREAM_TASKS: list[str] = []

logger = logging.getLogger(__name__)


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def extract(yaml_path: str, upstream_task_ids: dict[str, str]) -> str:
    """Look up the import connector by ``engine`` and run it.

    The connector class is resolved from ``IMPORT_CONNECTORS`` (auto-registered
    by every ``BaseImportConnector`` subclass that sets a non-empty ``ENGINE``),
    so adding a new source database does not require editing this file.
    """
    # TaskGroup always passes upstream IDs; extract is the chain root — nothing upstream.
    _ = upstream_task_ids
    cfg = _load_cfg(yaml_path)
    context = get_current_context()
    engine = str(cfg.get("engine") or "").strip().lower()

    connector_cls = IMPORT_CONNECTORS.get(engine)
    if connector_cls is None:
        raise AirflowException(
            f"Unsupported engine {engine!r} in {yaml_path}; "
            f"known engines: {sorted(IMPORT_CONNECTORS)}"
        )

    logger.info(
        "Building %s for %s.%s.%s (predicate=%r)",
        connector_cls.__name__,
        cfg["database"], cfg["schema"], cfg["table"], cfg.get("predicate"),
    )
    importer = connector_cls(
        connection_id_import=cfg["connection_id_import"],
        database=cfg["database"],
        schema=cfg["schema"],
        table=cfg["table"],
        predicate=cfg.get("predicate"),
    )
    return importer.to_parquet(**context)
