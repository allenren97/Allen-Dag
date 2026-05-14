"""Base class for connectors that fetch a DataFrame and write it to parquet."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from airflow.exceptions import AirflowException

from .connector import BaseConnector


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
            # BaseConnector names it `connection_id` because each instance only speaks
            # to one Airflow Connection; import ctors rename the arg for readability.
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
        # Prefer real Airflow TaskInstance context (`dag`), fall back when running stubbed/manual.
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "manual"
        run_date = context.get("ds") or "manual"
        ts_nodash = context.get("ts_nodash") or context.get("run_id") or "manual"

        # database_schema as a directory avoids two different schemas clashing on disk
        # when tables share the same name.
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
            # Empty schema cannot be inferred from None — distinguishes "lazy subclass bug"
            # from intentional 0-row result (handled below via df.empty).
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
            # Atomic replace: concurrent readers either see old parquet or complete new file.
            tmp_path.replace(out_path)
        finally:
            # tmp file must not linger — next run otherwise fails or reads half-written bytes.
            if tmp_path.exists():
                tmp_path.unlink()

        size_bytes = out_path.stat().st_size
        self.logger.info(
            "Wrote %s (%d bytes) for %s", out_path, size_bytes, self.full_table_name,
        )
        return str(out_path)
