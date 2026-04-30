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

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from connector_class.mssql import MSSQLImportConnector
from connector_class.wasb import AzureExportConnector
from mock_dataset import DAG_CONFIG

logger = logging.getLogger(__name__)

EXTRACT_TASK_ID = "extract_to_parquet"
UPLOAD_TASK_ID = "upload_to_azure_blob"

default_args = {
    "owner": "allen",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _extract_to_parquet(**context: Any) -> str:
    """Pull data from MSSQL and persist it as parquet in the landing zone."""
    cfg = DAG_CONFIG
    logger.info(
        "Starting extract for %s.%s.%s with predicate=%r",
        cfg["database"],
        cfg["schema"],
        cfg["table"],
        cfg.get("predicate"),
    )

    try:
        importer = MSSQLImportConnector(
            connection_id_import=cfg["connection_id_import"],
            database=cfg["database"],
            schema=cfg["schema"],
            table=cfg["table"],
            predicate=cfg.get("predicate"),
        )
        local_parquet_path = importer.to_parquet(**context)
    except AirflowException:
        # Already a typed Airflow error from the connector; let it bubble up.
        raise
    except Exception as exc:
        logger.exception("Unexpected error in extract task")
        raise AirflowException(f"Extract task crashed: {exc}") from exc

    logger.info("Extract task succeeded; parquet at %s", local_parquet_path)
    return local_parquet_path  # auto-pushed to XCom as 'return_value'


def _upload_to_azure_blob(**context: Any) -> str:
    """Pull the parquet path from XCom and upload it to Azure Blob Storage."""
    cfg = DAG_CONFIG
    ti = context["ti"]
    local_parquet_path = ti.xcom_pull(task_ids=EXTRACT_TASK_ID)

    if not local_parquet_path:
        raise AirflowException(
            f"No XCom value from task '{EXTRACT_TASK_ID}'; nothing to upload."
        )

    logger.info(
        "Starting upload of %s for table %s to Azure Blob",
        local_parquet_path,
        cfg["table"],
    )

    dag_id = getattr(context.get("dag"), "dag_id", None) or cfg["name"]

    try:
        exporter = AzureExportConnector(
            connection_id_export=cfg["connection_id_export"],
            database=cfg["database"],
            schema=cfg["schema"],
            table=cfg["table"],
            container_name=dag_id,
            delete_local=True,
        )
        blob_uri = exporter.upload(
            local_parquet_path=local_parquet_path,
            **context,
        )
    except AirflowException:
        raise
    except Exception as exc:
        logger.exception("Unexpected error in upload task")
        raise AirflowException(f"Upload task crashed: {exc}") from exc

    logger.info("Upload task succeeded: %s", blob_uri)
    return blob_uri


with DAG(
    dag_id=DAG_CONFIG["name"],
    description=(
        f"ETL pipeline: {DAG_CONFIG['engine']} "
        f"({DAG_CONFIG['database']}.{DAG_CONFIG['schema']}.{DAG_CONFIG['table']}) "
        f"-> Azure Blob Storage"
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=[DAG_CONFIG["engine"], "azure_blob", "etl"],
    max_active_runs=1,
) as dag:

    extract = PythonOperator(
        task_id=EXTRACT_TASK_ID,
        python_callable=_extract_to_parquet,
    )

    upload = PythonOperator(
        task_id=UPLOAD_TASK_ID,
        python_callable=_upload_to_azure_blob,
    )

    extract >> upload
