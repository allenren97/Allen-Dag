"""Base class for connectors that upload a local parquet file to a remote target."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

from .connector import BaseConnector


class BaseExportConnector(BaseConnector):
    """Base for connectors that upload a local parquet file to a remote store.

    ``upload(local_parquet_path, **context)`` is the template-method entry
    point and runs the same orchestration for every target: validate the
    local file → derive the destination key → call the engine-specific
    upload → log → optional local cleanup.

    Subclasses must set ``EXPORT`` and implement two abstract hooks:

    * ``_build_target_uri(blob_name)`` — human-readable destination URI
      used for logging and as the return value of ``upload``.
    * ``_upload_to_target(local, blob_name, **context)`` — the actual
      provider-specific transfer (e.g. ``WasbHook.load_file``,
      ``S3Hook.load_file``).

    Subclasses may override ``DEFAULT_CONTAINER`` (the fallback used when
    no ``container_name`` is passed to the constructor) and may override
    ``_build_blob_name`` if they want a different remote key layout (the
    default mirrors the on-disk landing layout).
    """

    DEFAULT_CONTAINER: str = ""

    def __init__(
        self,
        connection_id_export: str,
        database: str,
        schema: str,
        table: str,
        container_name: Optional[str] = None,
        overwrite: bool = True,
        delete_local: bool = False,
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
        self.delete_local = delete_local

    def _build_blob_name(self, local_path: Path, **context: Any) -> str:
        """Default remote key layout (override per export if needed)."""
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "manual"
        return "/".join(
            [dag_id, self.database, self.schema, self.table, local_path.name]
        )

    def _build_target_uri(self, blob_name: str) -> str:
        """Human-readable destination URI (e.g. ``wasb://...`` or ``s3://...``)."""
        raise NotImplementedError

    def _upload_to_target(
        self, local: Path, blob_name: str, **context: Any
    ) -> None:
        """Provider-specific transfer (subclasses override)."""
        raise NotImplementedError

    def upload(self, local_parquet_path: str, **context: Any) -> str:
        """Validate, upload, log, optionally delete-local; return the URI."""
        if not local_parquet_path:
            raise AirflowException(
                f"No local_parquet_path was provided to {type(self).__name__}.upload"
            )

        local = Path(local_parquet_path)
        if not local.is_file():
            raise AirflowException(
                f"Local parquet file is missing or not a regular file: {local}"
            )
        size = local.stat().st_size
        if size == 0:
            raise AirflowException(f"Local parquet file is empty: {local}")

        blob_name = self._build_blob_name(local, **context)
        uri = self._build_target_uri(blob_name)

        self.logger.info(
            "Uploading %s (%d bytes) -> %s (overwrite=%s)",
            local, size, uri, self.overwrite,
        )
        self._upload_to_target(local, blob_name, **context)
        self.logger.info("Upload complete: %s", uri)

        if self.delete_local:
            try:
                local.unlink()
                self.logger.info("Deleted local file %s after upload", local)
            except OSError as exc:
                self.logger.warning(
                    "Could not delete local file %s after upload: %s", local, exc,
                )

        return uri
