from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# connector_class/ lives one level above this DAG folder; make it importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

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


def _build_parallel_blob_dag(
    dag_id: str,
    schedule: str,
    table_configs: list[dict[str, Any]],
    landing_partition_prefix: str,
):
    """
    Airflow 2.10 TaskFlow + dynamic task mapping: one mapped extract and one
    mapped upload; map indices align so three tables run in parallel.
    """

    @dag(
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
    )
    def _parallel_etl():
        @task(task_id="extract_to_parquet")
        def extract_to_parquet(cfg: dict[str, Any]) -> str:
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

        @task(task_id="upload_to_azure_blob")
        def upload_to_azure_blob(cfg: dict[str, Any], parquet_path: str) -> str:
            context = get_current_context()
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

        parquet_paths = extract_to_parquet.expand(cfg=table_configs)
        upload_to_azure_blob.expand(cfg=table_configs, parquet_path=parquet_paths)

    return _parallel_etl()


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
