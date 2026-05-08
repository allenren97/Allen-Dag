# Import connector framework — summary, to-do, definition of done

## Summary

Build a pluggable import-connector framework that pulls rows from a source database into a local parquet file, with engine-agnostic orchestration (output path, atomic write, logging, empty-result handling) centralized in `BaseImportConnector`. Adding a new source database is a **one-file** change: add `<engine>.py` under `common_lib/connector_class/` that subclasses `BaseImportConnector`, sets `ENGINE`, and implements `_build_query` and `_fetch_dataframe`. Package auto-discovery registers it in `IMPORT_CONNECTORS`; generated `task/extract.py` uses the registry without further edits.

The shipped set includes MSSQL (`MsSqlHook`) and DB2 (raw `ibm_db`, optional). Modules whose deps are missing are logged and skipped so the package still imports.

See [IMPORT_CONNECTORS.md](IMPORT_CONNECTORS.md) for the full design.

## To-do

- [ ] `bases/connector.py` — `BaseConnector`: identifiers, logger, validation (`ValueError` listing all missing fields).
- [ ] `bases/import_connector.py` — `BaseImportConnector`:
  - [ ] `LANDING_DIR` from `AIRFLOW_LANDING_DIR`.
  - [ ] ctor `(connection_id_import, database, schema, table, predicate=None)`.
  - [ ] `_resolve_output_path(**context)`, `to_parquet(**context)` (tmp + atomic rename, pyarrow).
  - [ ] Zero rows → schema-only parquet + warning; `_fetch_dataframe` returning `None` → `AirflowException`.
- [ ] `mssql.py` — `ENGINE = "mssql"`, MSSQL quoting, lazy `MsSqlHook`.
- [ ] `db2.py` — `ENGINE = "db2"`, lazy `ibm_db` + `BaseHook`.
- [ ] `__init__.py` — autoload sibling modules; build `IMPORT_CONNECTORS`; duplicates → `RuntimeError`; import failures → warning + skip.
- [ ] `requirements.txt` — pin `pandas`, `pyarrow`, MSSQL provider; DB2/native deps optional/unpinned.

## Definition of done

- [ ] `from common_lib.connector_class import IMPORT_CONNECTORS` succeeds when MSSQL provider is installed; DB2 registers only when `ibm_db` (and related) is installed; missing DB2 does not break import.
- [ ] Keys in `IMPORT_CONNECTORS` are lowercase engine names (`ENGINE`).
- [ ] Omitting required ctor fields raises `ValueError` naming every missing field.
- [ ] `to_parquet(**ctx)` lands under `<LANDING_DIR>/<dag_id>/<run_date>/<database>_<schema>/<table>_<ts>.parquet`, returns path string; no orphaned `*.parquet.tmp`.
- [ ] Empty query result yields valid schema-only parquet and does not abort the DAG.
- [ ] Duplicate `ENGINE` across classes fails at registry build with a clear error.
- [ ] Broken optional module skipped with warning and omitted from registry.
- [ ] Adding a hypothetical `oracle.py` with valid `ENGINE` + hooks appears in `IMPORT_CONNECTORS` with no edits to `__init__.py` scaffolding or DAG task files.
