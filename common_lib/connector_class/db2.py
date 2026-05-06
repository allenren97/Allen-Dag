"""DB2 import connector.

Pull rows from an IBM Db2 table into a local parquet file using the raw
``ibm_db`` driver. The Airflow connection ``connection_id_import`` is
read via ``BaseHook`` and turned into a Db2 connection string at run
time.
"""
from __future__ import annotations

import pandas as pd

from .bases import BaseImportConnector


class DB2ImportConnector(BaseImportConnector):

    ENGINE = "db2"

    def _build_query(self) -> str:
        query = f"SELECT * FROM {self.schema}.{self.table}"
        if self.predicate:
            query += f" WHERE {self.predicate}"
        return query

    def _open_connection(self):
        # Lazy imports: ``ibm_db`` is an optional native dep, so importing
        # it here keeps the package importable in environments without it.
        import ibm_db
        from airflow.hooks.base import BaseHook

        airflow_conn = BaseHook.get_connection(self.connection_id)
        conn_str = (
            f"DATABASE={airflow_conn.schema};"
            f"HOSTNAME={airflow_conn.host};"
            f"PORT={airflow_conn.port};"
            f"PROTOCOL=TCPIP;"
            f"UID={airflow_conn.login};"
            f"PWD={airflow_conn.password};"
        )
        self.logger.info(
            "Opening DB2 connection to %s:%s database=%s as %s",
            airflow_conn.host, airflow_conn.port,
            airflow_conn.schema, airflow_conn.login,
        )
        return ibm_db.connect(conn_str, "", "")

    def _fetch_dataframe(self) -> pd.DataFrame:
        import ibm_db

        query = self._build_query()
        self.logger.info(
            "Running DB2 query on %s: %s", self.full_table_name, query,
        )

        conn = self._open_connection()
        try:
            stmt = ibm_db.exec_immediate(conn, query)
            rows: list[dict] = []
            row = ibm_db.fetch_assoc(stmt)
            while row:
                rows.append(row)
                row = ibm_db.fetch_assoc(stmt)
        finally:
            ibm_db.close(conn)

        return pd.DataFrame(data=rows)
