"""Upload task: pull the parquet from XCom and push it to the export target."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import EXPORT_CONNECTORS


UPSTREAM_TASKS: list[str] = ["extract"]

logger = logging.getLogger(__name__)


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def _resolve_export_class(cfg: dict[str, Any], yaml_path: str) -> type:
    explicit = str(cfg.get("export_engine") or "").strip().lower()
    if explicit:
        cls = EXPORT_CONNECTORS.get(explicit)
        if cls is None:
            raise AirflowException(
                f"Unknown export_engine {explicit!r} in {yaml_path}; "
                f"known: {sorted(EXPORT_CONNECTORS)}"
            )
        return cls
    if len(EXPORT_CONNECTORS) == 1:
        # Exactly one exporter is registered; YAML may omit `export_engine`.
        for export_cls in EXPORT_CONNECTORS.values():
            return export_cls
    if not EXPORT_CONNECTORS:
        raise AirflowException("No export connectors registered; cannot upload.")
    raise AirflowException(
        f"Multiple export connectors registered ({sorted(EXPORT_CONNECTORS)}); "
        f"add `export_engine:` to {yaml_path}."
    )


def upload(yaml_path: str, upstream_task_ids: dict[str, str]) -> str:
    cfg = _load_cfg(yaml_path)
    context = get_current_context()
    ti = context["ti"]
    extract_task_id = upstream_task_ids["extract"]
    parquet_path = ti.xcom_pull(task_ids=extract_task_id)
    if not parquet_path:
        raise AirflowException(
            f"No parquet path returned by {extract_task_id}; nothing to upload."
        )

    dag = context.get("dag")
    # Azure container names cannot contain `_`; DAG ids often use underscores.
    container_name = str(getattr(dag, "dag_id", None) or "manual").replace("_", "-")

    export_cls = _resolve_export_class(cfg, yaml_path)
    logger.info(
        "Building %s for %s.%s.%s -> container=%r",
        export_cls.__name__,
        cfg["database"], cfg["schema"], cfg["table"], container_name,
    )
    exporter = export_cls(
        connection_id_export=cfg["connection_id_export"],
        database=cfg["database"],
        schema=cfg["schema"],
        table=cfg["table"],
        container_name=container_name,
    )
    return exporter.upload(local_parquet_path=parquet_path, **context)
