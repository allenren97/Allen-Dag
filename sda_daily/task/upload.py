"""Upload task: pull the parquet from XCom and push it to Azure Blob Storage."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import AzureExportConnector


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def upload(yaml_path: str, extract_task_id: str) -> str:
    """Pull parquet path from the matching extract task and upload to blob."""
    cfg = _load_cfg(yaml_path)
    context = get_current_context()
    ti = context["ti"]
    parquet_path = ti.xcom_pull(task_ids=extract_task_id)
    if not parquet_path:
        raise AirflowException(
            f"No parquet path returned by {extract_task_id}; nothing to upload."
        )

    dag = context.get("dag")
    container_name = getattr(dag, "dag_id", None) or "manual"

    exporter = AzureExportConnector(
        connection_id_export=cfg["connection_id_export"],
        database=cfg["database"],
        table=cfg["table"],
        container_name=container_name,
        delete_local=True,
    )
    return exporter.upload(local_parquet_path=parquet_path, **context)
