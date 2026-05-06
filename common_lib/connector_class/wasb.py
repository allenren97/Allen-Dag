"""Azure Blob Storage export connector.

Upload a local parquet file produced by an import step into Azure Blob
Storage via ``WasbHook``. The Airflow connection ``connection_id_export``
must already exist with type ``wasb``.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

from .base import BaseConnector


class AzureExportConnector(BaseConnector):

    EXPORT = "azure_blob"

    DEFAULT_CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "blob")

    def __init__(
        self,
        connection_id_export: str,
        database: str,
        schema: str,
        table: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
    ) -> None:
        super().__init__(
            connection_id=connection_id_export,
            database=database,
            schema=schema,
            table=table,
        )
        self.container_name = (container_name or self.DEFAULT_CONTAINER).strip()
        if not self.container_name:
            raise ValueError("container_name must be a non-empty string")
        self.overwrite = overwrite

    def _get_hook(self):
        # Lazy import so the package stays importable when the Azure
        # provider isn't installed.
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        self.logger.info(
            "Creating WasbHook for connection %r", self.connection_id,
        )
        return WasbHook(wasb_conn_id=self.connection_id)

    def _build_blob_name(self, local_path: Path, **context: Any) -> str:
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "manual"
        return "/".join(
            [dag_id, self.database, self.schema, self.table, local_path.name]
        )

    def _ensure_container(self, hook) -> None:
        # Narrow try/except: swallow only "already exists" — any other
        # error must propagate so credential / permission issues surface.
        try:
            hook.create_container(container_name=self.container_name)
        except Exception as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "containeralreadyexists" in msg:
                self.logger.debug(
                    "Container %r already exists; reusing.", self.container_name,
                )
                return
            raise

    def upload(self, local_parquet_path: str, **context: Any) -> str:
        """Upload ``local_parquet_path`` and return the ``wasb://...`` URI."""
        if not local_parquet_path:
            raise AirflowException(
                "No local_parquet_path was provided to AzureExportConnector.upload"
            )

        local = Path(local_parquet_path)
        if not local.is_file():
            raise AirflowException(
                f"Local parquet file is missing or not a regular file: {local}"
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
            local, size, blob_uri, self.overwrite,
        )
        hook.load_file(
            file_path=str(local),
            container_name=self.container_name,
            blob_name=blob_name,
            overwrite=self.overwrite,
        )
        self.logger.info("Upload complete: %s", blob_uri)
        return blob_uri
