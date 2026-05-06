"""Build the ``extract -> upload`` TaskGroup for a single table YAML.

Each generated DAG folder owns its own ``task/extract.py`` and
``task/upload.py``. This builder loads those modules by file path so that the
DAG-level ``dag.py`` stays a one-line loop and the per-DAG task files remain
the obvious, editable entrypoint when someone is reading a single DAG folder.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def _load_module_from_path(module_name: str, path: Path) -> ModuleType:
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name!r} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_table_taskgroup(yaml_path: Path) -> TaskGroup:
    """
    Given ``<dag>/table/<table>.yaml``, build a TaskGroup named ``<table>``
    containing ``extract`` and ``upload`` PythonOperators wired in series.
    """
    yaml_path = Path(yaml_path)
    table = yaml_path.stem
    dag_dir = yaml_path.parent.parent
    task_dir = dag_dir / "task"

    extract_mod = _load_module_from_path(
        f"_dag_task_{dag_dir.name}_extract",
        task_dir / "extract.py",
    )
    upload_mod = _load_module_from_path(
        f"_dag_task_{dag_dir.name}_upload",
        task_dir / "upload.py",
    )

    with TaskGroup(group_id=table) as group:
        extract_op = PythonOperator(
            task_id="extract",
            python_callable=extract_mod.extract,
            op_kwargs={"yaml_path": str(yaml_path)},
        )
        upload_op = PythonOperator(
            task_id="upload",
            python_callable=upload_mod.upload,
            op_kwargs={
                "yaml_path": str(yaml_path),
                "extract_task_id": extract_op.task_id,
            },
        )
        extract_op >> upload_op

    return group
