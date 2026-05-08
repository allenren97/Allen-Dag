# Import connectors

Import connectors fetch rows from a source database and write them to a
local parquet file. The work is split across three layers so adding a
new source database is a one-file change.

```text
common_lib/connector_class/
├── __init__.py                # auto-discovers concrete connectors → IMPORT_CONNECTORS / EXPORT_CONNECTORS
├── bases/
│   ├── connector.py           #   BaseConnector — root identity layer
│   └── import_connector.py    #   BaseImportConnector — template-method to_parquet
├── mssql.py                   # MSSQLImportConnector (ENGINE = "mssql")
└── db2.py                     # DB2ImportConnector   (ENGINE = "db2")
```

## Class hierarchy

### `BaseConnector` — `bases/connector.py`

Root identity layer shared by every connector (import **and** export).
Owns the four identifiers every connector needs and the per-instance
logger:

| Attribute / property | Purpose |
| --- | --- |
| `connection_id` | Airflow connection id |
| `database`, `schema`, `table` | Source identifiers |
| `full_table_name` | `f"{database}.{schema}.{table}"`, used in every log line |
| `logger` | `logging.getLogger(self.__class__.__name__)` |

Constructor validates that all four identifiers are non-empty and
raises `ValueError` listing **every** missing field at once (not just
the first one) — easier to fix bad YAMLs in one pass.

Concrete connectors should not subclass `BaseConnector` directly;
subclass `BaseImportConnector` (this file) or `BaseExportConnector`
(see [EXPORT_CONNECTORS.md](EXPORT_CONNECTORS.md)) instead.

### `BaseImportConnector` — `bases/import_connector.py`

Engine-agnostic orchestration. `to_parquet(**context)` is the public
entry point and is **never overridden** — it runs the same flow for
every engine:

1. Resolve the output path under
   `<AIRFLOW_LANDING_DIR>/<dag_id>/<run_date>/<database>_<schema>/<table>_<ts>.parquet`
   and `mkdir -p` its parent.
2. Call the engine-specific `_fetch_dataframe()`.
3. Write the DataFrame to a `*.parquet.tmp` file via pyarrow.
4. Atomic-rename to the final path; clean up the tmp file on any error.
5. Log row count, byte size, and the resolved path.
6. Return the path string for XCom.

`AIRFLOW_LANDING_DIR` defaults to `/bns/rrap/data`; override it with
the env var for local development (e.g. `export AIRFLOW_LANDING_DIR="$(pwd)/data"`).

**Empty result sets are not an error.** If the query returns 0 rows the
connector still writes a parquet with the column schema only and logs a
warning. The downstream upload always has a file to ship.

Constructor signature is fixed:

```python
__init__(self, connection_id_import, database, schema, table, predicate=None)
```

If a future engine needs extra knobs (Oracle service-name, DB2 protocol,
…), prefer reading them from a class-scoped env var so this signature
stays uniform across every import connector.

### Concrete connectors — one file per engine

Each declares `ENGINE = "<name>"` and overrides exactly two methods:

| Method | Override responsibility |
| --- | --- |
| `_build_query(self) -> str` | Engine-specific quoting (e.g. MSSQL uses `[db].[schema].[table]`, DB2 uses `schema.table`). Append `WHERE <predicate>` when `self.predicate` is set. |
| `_fetch_dataframe(self) -> pd.DataFrame` | Open the connection, run the query, return a DataFrame. |

**Provider hooks are imported lazily** inside `_fetch_dataframe` (and
any helpers it calls) — the package keeps loading even when an
optional native dep (`ibm_db`, the MSSQL provider, etc.) isn't
installed in the current environment.

Initial set:

| File | Class | `ENGINE` | Underlying client |
| --- | --- | --- | --- |
| `mssql.py` | `MSSQLImportConnector` | `mssql` | `MsSqlHook.get_pandas_df` (apache-airflow-providers-microsoft-mssql) |
| `db2.py` | `DB2ImportConnector` | `db2` | Raw `ibm_db` driver, connection string built from `BaseHook.get_connection` |

## Auto-discovery / registry

`__init__.py` walks every concrete sibling module at import time
(skipping `bases/` and any module whose optional dep is missing), then
walks every subclass of `BaseConnector` and builds the registry keyed
by `ENGINE`:

```python
IMPORT_CONNECTORS: dict[str, type[BaseImportConnector]]
# {"mssql": MSSQLImportConnector, "db2": DB2ImportConnector, ...}
```

Duplicate `ENGINE` values raise at import time. A failed module is
logged with `Skipping connector module 'xyz' — ImportError: …` and
skipped — the package still loads with a usable subset.

The generated `task/extract.py` does
`IMPORT_CONNECTORS[cfg["engine"]](...)`, so a new engine becomes
visible to every existing DAG without edits to `__init__.py`, the
scaffold templates, or any generated DAG file.

## Adding a new import connector

1. Drop a new file alongside `mssql.py` named `<engine>.py` (e.g.
   `oracle.py`).
2. Subclass `BaseImportConnector`, set `ENGINE = "<engine>"`, and
   implement `_build_query` and `_fetch_dataframe`. Import the
   provider hook lazily inside `_fetch_dataframe`.
3. Use it from any spreadsheet row by setting `engine: <engine>` and
   pointing `connection_id_import` at the matching Airflow connection.

No edits to `__init__.py`, scaffold templates, existing `extract.py`
files, or generated DAG files are needed. See the root [README.md](../../README.md#adding-a-new-import-connector)
for a worked Oracle example.

## Required contract — quick reference

| Element | Value |
| --- | --- |
| Base class | Subclass of `BaseImportConnector` |
| Class attribute | `ENGINE: str` — non-empty, unique, lowercase |
| Methods to override | `_build_query`, `_fetch_dataframe` |
| Methods inherited (do **not** override) | `to_parquet`, `_resolve_output_path`, `__init__` |
| Constructor | `__init__(connection_id_import, database, schema, table, predicate=None)` |
