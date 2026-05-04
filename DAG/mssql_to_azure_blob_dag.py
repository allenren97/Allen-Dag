from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

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
    Per table: ``extract_{i}`` -> ``export_{i}`` (XCom parquet path). All
    branches are independent and run in parallel within the DAG.
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
            extract_task_id = f"extract_{i}"
            extract = PythonOperator(
                task_id=extract_task_id,
                python_callable=extract_to_parquet_callable,
                op_kwargs={
                    "cfg": cfg,
                    "landing_partition_prefix": landing_partition_prefix,
                },
            )
            export = PythonOperator(
                task_id=f"export_{i}",
                python_callable=upload_to_azure_blob_callable,
                op_kwargs={
                    "cfg": cfg,
                    "extract_task_id": extract_task_id,
                },
            )
            extract >> export

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
