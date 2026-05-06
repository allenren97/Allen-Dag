"""Connector base classes.

``BaseConnector``        — shared by every import / export connector.
``BaseImportConnector``  — intermediate base for connectors that fetch a
                           DataFrame from a source and write it to parquet
                           (MSSQL, DB2, future Oracle / Postgres / …).

Each concrete import connector only has to declare ``ENGINE`` and override
``_build_query`` + ``_fetch_dataframe``. Path resolution, parquet writing,
tmp-file cleanup, and step-by-step logging all live here.
"""
from __future__ import annotations

import logging
import os
from abc import ABC
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from airflow.exceptions import AirflowException


class BaseConnector(ABC):
    """Common identifiers + logger for every connector (import or export)."""

    def __init__(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str,
    ) -> None:
        missing = [
            name
            for name, value in (
                ("connection_id", connection_id),
                ("database", database),
                ("schema", schema),
                ("table", table),
            )
            if not value or not str(value).strip()
        ]
        if missing:
            raise ValueError(
                f"BaseConnector is missing required argument(s): {', '.join(missing)}"
            )

        self.connection_id = connection_id
        self.database = database
        self.schema = schema
        self.table = table
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def full_table_name(self) -> str:
        """Fully-qualified table name, e.g. ``Database.Schema.Table``."""
        return f"{self.database}.{self.schema}.{self.table}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"connection_id={self.connection_id!r}, "
            f"full_table_name={self.full_table_name!r})"
        )


class BaseImportConnector(BaseConnector):
    """Base for connectors that fetch a DataFrame and write it to parquet.

    Subclasses set ``ENGINE`` and implement ``_build_query`` plus
    ``_fetch_dataframe``. ``to_parquet`` is the public entry point and runs
    the same orchestration for every engine: build query → fetch → write
    parquet to a tmp file → atomic rename → return path.

    Output path layout (override ``LANDING_DIR`` via the
    ``AIRFLOW_LANDING_DIR`` env var for local development)::

        <LANDING_DIR>/<dag_id>/<run_date>/<database>_<schema>/<table>_<ts>.parquet
    """

    LANDING_DIR = Path(os.environ.get("AIRFLOW_LANDING_DIR", "/bns/rrap/data"))

    def __init__(
        self,
        connection_id_import: str,
        database: str,
        schema: str,
        table: str,
        predicate: Optional[str] = None,
    ) -> None:
        super().__init__(
            connection_id=connection_id_import,
            database=database,
            schema=schema,
            table=table,
        )
        self.predicate = predicate.strip() if predicate else None

    def _build_query(self) -> str:
        """Engine-specific SELECT (subclasses override for quoting rules)."""
        raise NotImplementedError

    def _fetch_dataframe(self) -> pd.DataFrame:
        """Engine-specific DataFrame fetch (subclasses override)."""
        raise NotImplementedError

    def _resolve_output_path(self, **context: Any) -> Path:
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "manual"
        run_date = context.get("ds") or "manual"
        ts_nodash = context.get("ts_nodash") or context.get("run_id") or "manual"

        file_path = (
            self.LANDING_DIR
            / dag_id
            / run_date
            / f"{self.database}_{self.schema}"
            / f"{self.table}_{ts_nodash}.parquet"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        return file_path

    def to_parquet(self, **context: Any) -> str:
        """Run the engine-specific fetch and write the result to parquet.

        If the query returns 0 rows an empty parquet file containing only
        the column schema (no data rows) is still written and uploaded.
        """
        out_path = self._resolve_output_path(**context)
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        self.logger.info("Resolved output path for %s -> %s", self.full_table_name, out_path)

        df = self._fetch_dataframe()
        if df is None:
            raise AirflowException(
                f"{type(self).__name__} returned None for {self.full_table_name}; "
                f"cannot infer schema to write a parquet file."
            )
        if df.empty:
            self.logger.warning(
                "Query returned 0 rows for %s (predicate=%r); writing empty parquet "
                "with %d-column schema only.",
                self.full_table_name, self.predicate, len(df.columns),
            )
        else:
            self.logger.info(
                "Fetched %d rows / %d columns from %s",
                len(df), len(df.columns), self.full_table_name,
            )

        try:
            df.to_parquet(tmp_path, engine="pyarrow", index=False)
            tmp_path.replace(out_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        size_bytes = out_path.stat().st_size
        self.logger.info(
            "Wrote %s (%d bytes) for %s", out_path, size_bytes, self.full_table_name,
        )
        return str(out_path)
