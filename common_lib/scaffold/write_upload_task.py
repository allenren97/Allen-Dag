"""Write the per-DAG ``task/upload.py`` that reads a YAML and runs the export.

This template is *export-engine-agnostic*. It imports
``EXPORT_CONNECTORS`` (a ``{export_name: connector_class}`` registry built
by ``common_lib/connector_class/__init__.py``) and dispatches by the YAML's
optional ``export_engine:`` field. When only one export connector is
registered (the current state), the YAML can omit ``export_engine:`` and
the task will use the single registered connector. When more than one
exists, ``export_engine:`` must be set in the YAML.

Adding support for a new export target is therefore purely additive: drop
a new ``common_lib/connector_class/<target>.py`` file with
``class XYZExportConnector(BaseConnector): EXPORT = "<target>"`` and the
next DAG run picks it up. This template never needs to change.
"""
from __future__ import annotations

from pathlib import Path


_TEMPLATE = '''\
"""Upload task: pull the parquet from XCom and push it to the export target."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

from common_lib.connector_class import EXPORT_CONNECTORS


UPSTREAM_TASKS: list[str] = ["extract"]


def _load_cfg(yaml_path: str) -> dict[str, Any]:
    with Path(yaml_path).open("r") as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict):
        raise AirflowException(f"YAML at {yaml_path} did not parse to a mapping")
    return cfg


def _resolve_export_class(cfg: dict[str, Any], yaml_path: str) -> type:
    """Pick the export connector class for this YAML.

    * If the YAML sets ``export_engine: <name>``, use that one.
    * Otherwise, if exactly one connector is registered, use it.
    * Otherwise, fail with a clear error listing the candidates.
    """
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
        return next(iter(EXPORT_CONNECTORS.values()))
    if not EXPORT_CONNECTORS:
        raise AirflowException(
            "No export connectors registered; cannot upload."
        )
    raise AirflowException(
        f"Multiple export connectors registered "
        f"({sorted(EXPORT_CONNECTORS)}); add `export_engine:` to {yaml_path}."
    )


def upload(yaml_path: str, upstream_task_ids: dict[str, str]) -> str:
    """Pull the parquet path from the matching extract task and run the export.

    The export connector class is resolved from ``EXPORT_CONNECTORS`` (auto-
    registered by every ``BaseConnector`` subclass that sets a non-empty
    ``EXPORT``), so adding a new export target does not require editing this
    file.
    """
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
    container_name = getattr(dag, "dag_id", None) or "manual"

    export_cls = _resolve_export_class(cfg, yaml_path)
    exporter = export_cls(
        connection_id_export=cfg["connection_id_export"],
        database=cfg["database"],
        table=cfg["table"],
        container_name=container_name,
        delete_local=True,
    )
    return exporter.upload(local_parquet_path=parquet_path, **context)
'''


def write_upload_task(dag_dir: Path) -> Path:
    task_dir = dag_dir / "task"
    task_dir.mkdir(parents=True, exist_ok=True)
    out_path = task_dir / "upload.py"
    out_path.write_text(_TEMPLATE)
    return out_path
