from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

from .base import BaseConnector


class MSSQLImportConnector(BaseConnector):
    """
    Pull rows from a Microsoft SQL Server table into a local parquet file.

    The connector relies on ``MsSqlHook`` from the Airflow MSSQL provider
    package, so the connection ``connection_id_import`` must already exist
    in Airflow with type ``mssql``.
    """

    ENGINE = "mssql"

    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    LANDING_DIR = Path(
        os.environ.get("AIRFLOW_LANDING_DIR", _PROJECT_ROOT / "data")
    )

    def __init__(
        self,
        connection_id_import: str,
        database: str,
        table: str,
        predicate: Optional[str] = None,
        landing_partition_prefix: Optional[str] = None,
    ) -> None:
        super().__init__(
            connection_id=connection_id_import,
            database=database,
            table=table,
        )
        self.predicate = predicate.strip() if predicate else None
        self.landing_partition_prefix = (
            landing_partition_prefix.strip() if landing_partition_prefix else None
        )

    def _build_query(self) -> str:
        query = f"SELECT * FROM [{self.table}]"
        if self.predicate:
            query += f" WHERE {self.predicate}"
        return query

    def _get_hook(self):
        try:
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
        except ImportError as exc:
            raise AirflowException(
                "apache-airflow-providers-microsoft-mssql is not installed; "
                "cannot create MsSqlHook."
            ) from exc

        try:
            return MsSqlHook(
                mssql_conn_id=self.connection_id,
                schema=self.database,
            )
        except Exception as exc:
            raise AirflowException(
                f"Failed to instantiate MsSqlHook for connection "
                f"'{self.connection_id}': {exc}"
            ) from exc

    def _resolve_output_path(self, **context: Any) -> Path:
        ts_nodash = context.get("ts_nodash") or context.get("run_id") or "manual"
        if self.landing_partition_prefix:
            ds = context.get("ds") or "manual"
            out_dir = (
                self.LANDING_DIR
                / self.landing_partition_prefix
                / ds
                / self.database
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            return out_dir / f"{self.table}_{ts_nodash}.parquet"
        self.LANDING_DIR.mkdir(parents=True, exist_ok=True)
        return self.LANDING_DIR / f"{self.table}_{ts_nodash}.parquet"

    def to_parquet(self, **context: Any) -> str:
        """
        Run the SELECT, write the result to a parquet file in the landing dir,
        and return the absolute path.

        If the query returns 0 rows, an empty parquet file containing only the
        column schema (no data rows) is still written and uploaded downstream.

        Raises ``AirflowException`` on connection errors, query errors, or
        filesystem failures.
        """
        hook = self._get_hook()
        query = self._build_query()
        out_path = self._resolve_output_path(**context)
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

        self.logger.info("Running MSSQL query on %s: %s", self.fqtn, query)
        try:
            df = hook.get_pandas_df(sql=query)
        except Exception as exc:
            self.logger.exception("MSSQL query failed for %s", self.fqtn)
            raise AirflowException(
                f"MSSQL query failed for {self.fqtn}: {exc}"
            ) from exc

        if df is None:
            raise AirflowException(
                f"MSSQL hook returned None for {self.fqtn}; cannot infer "
                f"schema to write a parquet file."
            )

        if df.empty:
            self.logger.warning(
                "Query returned 0 rows for %s (predicate=%r); writing empty "
                "parquet with %d-column schema only.",
                self.fqtn,
                self.predicate,
                len(df.columns),
            )
        else:
            self.logger.info(
                "Fetched %d rows / %d columns from %s; writing parquet to %s",
                len(df),
                len(df.columns),
                self.fqtn,
                out_path,
            )

        try:
            df.to_parquet(tmp_path, engine="pyarrow", index=False)
            tmp_path.replace(out_path)
        except Exception as exc:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError as cleanup_exc:
                    self.logger.warning(
                        "Could not remove partial parquet %s: %s",
                        tmp_path,
                        cleanup_exc,
                    )
            raise AirflowException(
                f"Failed to write parquet for {self.fqtn} at {out_path}: {exc}"
            ) from exc

        size_bytes = out_path.stat().st_size
        self.logger.info(
            "Wrote %s (%d bytes) for %s", out_path, size_bytes, self.fqtn
        )
        return str(out_path)
