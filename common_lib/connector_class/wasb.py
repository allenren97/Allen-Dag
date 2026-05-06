from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

from .base import BaseConnector


class AzureExportConnector(BaseConnector):
    """
    Upload a local parquet file produced by the import step into Azure Blob
    Storage via ``WasbHook``.

    The connection ``connection_id_export`` must already exist in Airflow with
    type ``wasb``.
    """

    EXPORT = "azure_blob"

    DEFAULT_CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "raw")

    def __init__(
        self,
        connection_id_export: str,
        database: str,
        table: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        delete_local: bool = False,
    ) -> None:
        super().__init__(
            connection_id=connection_id_export,
            database=database,
            table=table,
        )
        self.container_name = (container_name or self.DEFAULT_CONTAINER).strip()
        if not self.container_name:
            raise ValueError("container_name must be a non-empty string")
        self.overwrite = overwrite
        self.delete_local = delete_local

    def _get_hook(self):
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        except ImportError as exc:
            raise AirflowException(
                "apache-airflow-providers-microsoft-azure is not installed; "
                "cannot create WasbHook."
            ) from exc

        try:
            return WasbHook(wasb_conn_id=self.connection_id)
        except Exception as exc:
            raise AirflowException(
                f"Failed to instantiate WasbHook for connection "
                f"'{self.connection_id}': {exc}"
            ) from exc

    def _get_dag_id(self, **context: Any) -> str:
        dag = context.get("dag")
        if dag is not None and getattr(dag, "dag_id", None):
            return str(dag.dag_id)
        return str(context.get("dag_id") or "manual")

    def _build_blob_name(self, local_path: Path, **context: Any) -> str:
        dag_id = self._get_dag_id(**context)
        return "/".join([dag_id, self.database, self.table, local_path.name])

    def _ensure_container(self, hook) -> None:
        try:
            hook.create_container(container_name=self.container_name)
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "containeralreadyexists" in msg:
                return
            raise

    def upload(self, local_parquet_path: str, **context: Any) -> str:
        """
        Upload ``local_parquet_path`` to Azure Blob Storage and return the
        ``wasb://container/blob`` URI of the uploaded object.

        Raises ``AirflowException`` if the local file is missing, empty, or
        the upload fails.
        """
        if not local_parquet_path:
            raise AirflowException(
                "No local_parquet_path was provided to AzureExportConnector.upload"
            )

        local = Path(local_parquet_path)
        if not local.exists():
            raise AirflowException(
                f"Local parquet file does not exist: {local}"
            )
        if not local.is_file():
            raise AirflowException(
                f"Local parquet path is not a regular file: {local}"
            )
        size = local.stat().st_size
        if size == 0:
            raise AirflowException(f"Local parquet file is empty: {local}")

        hook = self._get_hook()
        self._ensure_container(hook)
        blob_name = self._build_blob_name(local, **context)
        blob_uri = f"wasb://{self.container_name}/{blob_name}"

        self.logger.info(
            "Uploading %s (%d bytes) -> %s (overwrite=%s)",
            local,
            size,
            blob_uri,
            self.overwrite,
        )
        try:
            hook.load_file(
                file_path=str(local),
                container_name=self.container_name,
                blob_name=blob_name,
                overwrite=self.overwrite,
            )
        except Exception as exc:
            self.logger.exception("Azure Blob upload failed for %s", blob_uri)
            raise AirflowException(
                f"Failed to upload {local} to {blob_uri}: {exc}"
            ) from exc

        self.logger.info("Upload complete: %s", blob_uri)

        if self.delete_local:
            try:
                local.unlink()
                self.logger.info("Removed local file %s after upload", local)
            except OSError as cleanup_exc:
                self.logger.warning(
                    "Could not remove local file %s after upload: %s",
                    local,
                    cleanup_exc,
                )

        return blob_uri
