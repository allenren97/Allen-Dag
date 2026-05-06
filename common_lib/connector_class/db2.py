from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from airflow.exceptions import AirflowException

from .base import BaseConnector


class DB2ImportConnector(BaseConnector):
    """
    Pull rows from an IBM Db2 table into a local parquet file.

    Uses ``Db2Hook`` from an installed Airflow DB2 provider (Apache IBM
    provider when available, otherwise ``airflow-provider-ibm-db2``). The
    Airflow connection ``connection_id_import`` must exist (conn type per
    your provider, commonly ``db2`` / ``Db2``).
    """

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
        query = f'SELECT * FROM "{self.table}"'
        if self.predicate:
            query += f" WHERE {self.predicate}"
        return query

    def _get_hook(self):
        Db2Hook = None
        try:
            from airflow.providers.ibm.hooks.db2 import Db2Hook as _ApacheDb2Hook

            Db2Hook = _ApacheDb2Hook
        except ImportError:
            try:
                from airflow_provider_ibm_db2.hooks.db2 import (
                    Db2Hook as _CommunityDb2Hook,
                )

                Db2Hook = _CommunityDb2Hook
            except ImportError as exc:
                raise AirflowException(
                    "No Db2Hook implementation found. Install a DB2 Airflow provider "
                    "(e.g. `pip install airflow-provider-ibm-db2`) or add the Apache "
                    "IBM provider compatible with your Airflow version."
                ) from exc

        try:
            return Db2Hook(db2_conn_id=self.connection_id)
        except Exception as exc:
            raise AirflowException(
                f"Failed to instantiate Db2Hook for connection "
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
        hook = self._get_hook()
        query = self._build_query()
        out_path = self._resolve_output_path(**context)
        tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

        self.logger.info("Running DB2 query on %s: %s", self.fqtn, query)
        try:
            conn = hook.get_conn()
            try:
                df = pd.read_sql(query, conn)
            finally:
                conn.close()
        except Exception as exc:
            self.logger.exception("DB2 query failed for %s", self.fqtn)
            raise AirflowException(
                f"DB2 query failed for {self.fqtn}: {exc}"
            ) from exc

        if df is None:
            raise AirflowException(
                f"DB2 read returned None for {self.fqtn}; cannot infer "
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
