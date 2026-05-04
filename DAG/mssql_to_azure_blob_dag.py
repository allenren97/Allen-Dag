from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# connector_class/ lives one level above this DAG folder; make it importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, get_current_context

from connector_class.db2 import DB2ImportConnector
from connector_class.mssql import MSSQLImportConnector
from connector_class.wasb import AzureExportConnector
from mock_dataset import business_line1_monthly, business_line1_weekly

logger = logging.getLogger(__name__)

default_args = {
    "owner": "allen",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _safe_task_suffix(cfg: dict[str, Any], index: int) -> str:
    raw = str(cfg.get("name") or cfg.get("table") or f"table_{index}")
    safe = re.sub(r"[^0-9A-Za-z._-]+", "_", raw).strip("._-") or f"idx_{index}"
    return safe


def extract_to_parquet_callable(
    cfg: dict[str, Any],
    landing_partition_prefix: str,
) -> str:
    context = get_current_context()
    engine = str(cfg.get("engine", "")).lower().strip()
    logger.info(
        "Extract start engine=%s for %s.%s.%s predicate=%r",
        engine,
        cfg["database"],
        cfg["schema"],
        cfg["table"],
        cfg.get("predicate"),
    )
    try:
        if engine == "mssql":
            importer = MSSQLImportConnector(
                connection_id_import=cfg["connection_id_import"],
                database=cfg["database"],
                schema=cfg["schema"],
                table=cfg["table"],
                predicate=cfg.get("predicate"),
                landing_partition_prefix=landing_partition_prefix,
            )
        elif engine == "db2":
            importer = DB2ImportConnector(
                connection_id_import=cfg["connection_id_import"],
                database=cfg["database"],
                schema=cfg["schema"],
                table=cfg["table"],
                predicate=cfg.get("predicate"),
                landing_partition_prefix=landing_partition_prefix,
            )
        else:
            raise AirflowException(
                f"Unsupported engine {cfg.get('engine')!r} for "
                f"table {cfg.get('name')!r}"
            )
        local_parquet_path = importer.to_parquet(**context)
    except AirflowException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in extract task")
        raise AirflowException(f"Extract task crashed: {exc}") from exc

    logger.info("Extract succeeded; parquet at %s", local_parquet_path)
    return local_parquet_path


def upload_to_azure_blob_callable(
    cfg: dict[str, Any],
    extract_task_id: str,
) -> str:
    context = get_current_context()
    ti = context["ti"]
    parquet_path = ti.xcom_pull(task_ids=extract_task_id)
    if not parquet_path:
        raise AirflowException("No parquet path from extract; nothing to upload.")

    dag = context.get("dag")
    dag_id_val = getattr(dag, "dag_id", None) or "manual"

    logger.info(
        "Upload start %s -> blob (table=%s)",
        parquet_path,
        cfg["table"],
    )
    try:
        exporter = AzureExportConnector(
            connection_id_export=cfg["connection_id_export"],
            database=cfg["database"],
            schema=cfg["schema"],
            table=cfg["table"],
            container_name=dag_id_val,
            delete_local=True,
        )
        blob_uri = exporter.upload(
            local_parquet_path=parquet_path,
            **context,
        )
    except AirflowException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in upload task")
        raise AirflowException(f"Upload task crashed: {exc}") from exc

    logger.info("Upload succeeded: %s", blob_uri)
    return blob_uri


def _build_parallel_blob_dag(
    dag_id: str,
    schedule: str,
    table_configs: list[dict[str, Any]],
    landing_partition_prefix: str,
) -> DAG:
    """
    Classic DAG: ``with DAG(...)`` plus one ``PythonOperator`` extract and one
    ``PythonOperator`` upload per table config; branches run in parallel.
    """
    with DAG(
        dag_id=dag_id,
        description=(
            f"Parallel ETL: {len(table_configs)} tables ({dag_id}) -> Azure Blob"
        ),
        start_date=datetime(2026, 1, 1),
        schedule=schedule,
        catchup=False,
        default_args=default_args,
        tags=[dag_id, "azure_blob", "etl", "parallel"],
        max_active_runs=1,
    ) as dag:
        for i, cfg in enumerate(table_configs):
            suffix = _safe_task_suffix(cfg, i)
            extract_task_id = f"extract_{suffix}"
            extract = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_to_parquet_callable,
                op_kwargs={
                    "cfg": cfg,
                    "landing_partition_prefix": landing_partition_prefix,
                },
            )
            upload = PythonOperator(
                task_id=f"upload_{suffix}",
                python_callable=upload_to_azure_blob_callable,
                op_kwargs={
                    "cfg": cfg,
                    "extract_task_id": extract_task_id,
                },
            )
            extract >> upload

    return dag


business_line1_monthly = _build_parallel_blob_dag(
    dag_id="business_line1_monthly",
    schedule="@monthly",
    table_configs=business_line1_monthly,
    landing_partition_prefix="business_line1_monthly",
)

business_line1_weekly = _build_parallel_blob_dag(
    dag_id="business_line1_weekly",
    schedule="@weekly",
    table_configs=business_line1_weekly,
    landing_partition_prefix="business_line1_weekly",
)
