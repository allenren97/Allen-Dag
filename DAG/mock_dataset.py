from __future__ import annotations

DAG_CONFIG: dict = {
    "name": "mssql_customers_to_azure_blob",
    "engine": "mssql",
    "connection_id_import": "mssql_default",
    "connection_id_export": "wasb_default",
    "database": "AdventureWorks",
    "schema": "Sales",
    "table": "Customers",
    "predicate": "ModifiedDate >= '2026-01-01'",
}
