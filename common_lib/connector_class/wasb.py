"""Azure Blob Storage export connector.

Upload a local parquet file produced by an import step into Azure Blob
Storage via ``WasbHook``. The Airflow connection ``connection_id_export``
must already exist with type ``wasb``.

Engine-agnostic orchestration (file validation, blob-name layout,
``delete_local``, logging) lives in :class:`BaseExportConnector`. This
class only owns the wasb-specific bits: how to get a hook, how to ensure
the container exists, how to build a ``wasb://...`` URI, and how to call
``hook.load_file``.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .bases import BaseExportConnector


class AzureExportConnector(BaseExportConnector):

    EXPORT = "azure_blob"

    DEFAULT_CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "blob")

    def _get_hook(self):
        # Lazy import so the package stays importable when the Azure
        # provider isn't installed in the current environment.
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        self.logger.info(
            "Creating WasbHook for connection %r", self.connection_id,
        )
        return WasbHook(wasb_conn_id=self.connection_id)

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

    def _build_target_uri(self, blob_name: str) -> str:
        return f"wasb://{self.container_name}/{blob_name}"

    def _upload_to_target(
        self, local: Path, blob_name: str, **context: Any
    ) -> None:
        hook = self._get_hook()
        self._ensure_container(hook)
        hook.load_file(
            file_path=str(local),
            container_name=self.container_name,
            blob_name=blob_name,
            overwrite=self.overwrite,
        )
