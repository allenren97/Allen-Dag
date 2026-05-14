"""MSSQL import connector.

Pull rows from a Microsoft SQL Server table into a local parquet file.
Relies on ``MsSqlHook`` from ``apache-airflow-providers-microsoft-mssql``.
The Airflow connection ``connection_id_import`` must already exist with
type ``mssql``.
"""
from __future__ import annotations

import pandas as pd

from .bases import BaseImportConnector


class MSSQLImportConnector(BaseImportConnector):

    ENGINE = "mssql"

    def _build_query(self) -> str:
        # Bracket quoting is T-SQL-specific; avoids reserved words breaking unquoted identifiers.
        query = f"SELECT * FROM [{self.database}].[{self.schema}].[{self.table}]"
        if self.predicate:
            query += f" WHERE {self.predicate}"
        return query

    def _get_hook(self):
        # Lazy import so the package can still be imported in environments
        # where the MSSQL provider isn't installed; failure here is a clear
        # ImportError naming the missing provider.
        from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

        self.logger.info(
            "Creating MsSqlHook for connection %r", self.connection_id,
        )
        return MsSqlHook(mssql_conn_id=self.connection_id)

    def _fetch_dataframe(self) -> pd.DataFrame:
        # Delegates pooling / cursor details to MsSqlHook so we stay thin — only orchestration differs per engine inside BaseImportConnector.
        hook = self._get_hook()
        query = self._build_query()
        self.logger.info(
            "Running MSSQL query on %s: %s", self.full_table_name, query,
        )
        return hook.get_pandas_df(sql=query)
