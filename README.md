# Allen Dag — Spreadsheet-driven Airflow ETL

This repo turns a single business-intake spreadsheet (`Business Requirement.xlsx`)
into a fleet of Airflow DAGs that pull tables from various source databases,
write them to local Parquet, and upload them to a cloud blob store. The
ingestion shape is **per row**: one row in the spreadsheet = one table = one
TaskGroup inside the DAG that owns its `(business_line, cadence)` pair.

The codebase is intentionally split into two layers:

- **`common_lib/`** — generic, reusable. Holds connector classes, the
  scaffolder, the TaskGroup builder, and the intake reader.
- **`<business_line>_<cadence>/`** — generated, DAG-specific. Each folder
  is owned by one DAG; the only thing inside it that you typically edit by
  hand are the per-table YAMLs.

Adding a new source database, a new export target, or a new task type is
designed to be **purely additive** — you drop one file in the right place
and nothing else changes.

---

## Table of contents

1. [Repository layout](#repository-layout)
2. [End-to-end flow](#end-to-end-flow)
3. [Setup](#setup)
4. [Generating DAG folders from the spreadsheet](#generating-dag-folders-from-the-spreadsheet)
   - [Sync scenarios](#sync-scenarios)
5. [Running the DAGs in Airflow](#running-the-dags-in-airflow)
6. [Per-table YAML schema](#per-table-yaml-schema)
7. [Adding a new import connector](#adding-a-new-import-connector)
8. [Adding a new export connector](#adding-a-new-export-connector)
9. [Adding a new task type (e.g. validate)](#adding-a-new-task-type-eg-validate)
10. [Configuration & environment variables](#configuration--environment-variables)
11. [Troubleshooting](#troubleshooting)

---

## Repository layout

```text
.
├── Business Requirement.xlsx          # the intake spreadsheet (one row per table)
├── requirements.txt
├── README.md
├── common_lib/                        # generic, reusable code
│   ├── connector_class/
│   │   ├── bases/                     # base classes (one per file)
│   │   │   ├── connector.py           #   BaseConnector — root identity layer
│   │   │   ├── import_connector.py    #   BaseImportConnector — template-method to_parquet
│   │   │   └── export_connector.py    #   BaseExportConnector — template-method upload
│   │   ├── mssql.py                   # MSSQLImportConnector (ENGINE = "mssql")
│   │   ├── db2.py                     # DB2ImportConnector   (ENGINE = "db2")
│   │   ├── wasb.py                    # AzureExportConnector (EXPORT = "azure_blob")
│   │   └── __init__.py                # auto-discovers concrete connectors →
│   │                                  #   IMPORT_CONNECTORS / EXPORT_CONNECTORS
│   │                                  # (skips bases/ subpackage and any modules
│   │                                  #  whose optional deps are missing)
│   ├── intake/
│   │   ├── read_excel.py              # parse the xlsx into normalized row dicts
│   │   └── group_by_dag.py            # bucket rows by (business_line, cadence)
│   ├── scaffold/
│   │   ├── write_dag_file.py          # writes <dag>/dag.py
│   │   ├── write_table_yaml.py        # writes <dag>/table/<table>.yaml
│   │   ├── write_extract_task.py      # writes <dag>/task/extract.py (engine-agnostic)
│   │   └── write_upload_task.py       # writes <dag>/task/upload.py  (export-agnostic)
│   ├── tasks/
│   │   └── build_table_taskgroup.py   # discovers task/*.py, reads UPSTREAM_TASKS,
│   │                                  # builds the TaskGroup at DAG-parse time
│   └── create_dag.py                  # CLI entrypoint: xlsx → scaffold DAG folders
└── sda_daily/                         # one generated folder per (business_line, cadence)
    ├── dag.py                         # one-liner: loop over table/*.yaml → TaskGroup
    ├── table/
    │   ├── TM_DIM.yaml                # one YAML per table (config for that table)
    │   └── FileInfo.yaml
    └── task/
        ├── extract.py                 # generic; dispatches via IMPORT_CONNECTORS[engine]
        └── upload.py                  # generic; dispatches via EXPORT_CONNECTORS[…]
```

`sda_monthly/` and `rrap_weekly/` follow the same shape.

---

## End-to-end flow

```text
┌──────────────────────────────┐
│  Business Requirement.xlsx   │  one row per table
└──────────────┬───────────────┘
               │  python -m common_lib.create_dag
               ▼
┌──────────────────────────────┐
│  <business_line>_<cadence>/  │  scaffolded folder (created if absent, skipped if present)
│   ├── dag.py                 │  ← writes itself once, then is parsed by Airflow
│   ├── table/<table>.yaml     │  ← one per row in the spreadsheet
│   └── task/{extract,upload}.py
└──────────────┬───────────────┘
               │  Airflow scheduler imports dag.py
               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  DAG (id = folder name, e.g. "sda_daily", schedule = "@daily")       │
│                                                                      │
│   for yaml_path in table/*.yaml:                                     │
│       build_table_taskgroup(yaml_path)   ← from common_lib.tasks    │
│                                                                      │
│   each TaskGroup has shape  [upstream...] >> extract >> [downstream]│
│   wired from each task module's UPSTREAM_TASKS list                  │
└──────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       │  triggered by Airflow
                                                       ▼
                                            ┌────────────────────┐
                                            │  extract task      │  IMPORT_CONNECTORS[engine]
                                            │  └─ to_parquet()   │  → /data/.../<table>.parquet
                                            └────────┬───────────┘
                                                     │  XCom: parquet path
                                                     ▼
                                            ┌────────────────────┐
                                            │  upload task       │  EXPORT_CONNECTORS[…]
                                            │  └─ upload()       │  → wasb://<container>/<blob>
                                            └────────────────────┘
```

### Phase 1 — scaffolding (one-shot, run by a developer)

`common_lib/create_dag.py` is invoked from the CLI:

1. `read_excel.read_intake_rows()` parses the xlsx; each row is normalized
   into a dict keyed by header.
2. `group_by_dag.group_by_dag()` buckets rows by
   `<business_line>_<cadence>` (the slug becomes the DAG id).
3. For each bucket whose folder does **not** already exist:
   - `mkdir <dag_id>/`
   - one `<dag_id>/table/<table>.yaml` per row (`write_table_yaml`)
   - `<dag_id>/task/extract.py` and `upload.py` from the scaffold templates
   - `<dag_id>/dag.py` (cadence → `@daily` / `@weekly` / `@monthly`)
4. For each bucket whose folder **already exists**:
   - new rows drop in the corresponding `<table>.yaml` next to the
     existing ones;
   - rows whose connector-relevant fields (`engine`,
     `connection_id_*`, `database`, `schema`, `predicate`) drifted
     away from the spreadsheet have their `<table>.yaml`
     **rewritten** so the YAML matches the spreadsheet again;
   - rows that are no longer in the spreadsheet have their
     `<table>.yaml` **deleted** so the group stays in sync;
   - the hand-tweakable scaffold pieces (`dag.py`, `task/*.py`) are
     never overwritten.
5. Any DAG folder on disk whose `(business_line, cadence)` group has
   **disappeared from the spreadsheet** is treated as cancelled: the
   whole folder (`dag.py`, `table/`, `task/`) is removed so Airflow
   stops parsing the DAG on its next scan.

A DAG folder is identified by the presence of a top-level `dag.py`,
so siblings like `common_lib/`, `.git/`, and any local data folders
are never touched. To regenerate a DAG folder from scratch, `rm -rf`
it and re-run.

The spreadsheet is the sole source of truth for the contents of each
`<table>.yaml`: edits to those YAMLs are **not** supported — change
the spreadsheet and re-run. Use `--dry-run` to preview the changes
before applying them.

### Phase 2 — DAG parse (every time Airflow scans the folder)

Each `<dag_id>/dag.py` is a tiny shim:

```python
with DAG(dag_id=HERE.name, schedule="@daily", ...):
    for yaml_path in sorted((HERE / "table").glob("*.yaml")):
        build_table_taskgroup(yaml_path)
```

`build_table_taskgroup(yaml_path)` (in `common_lib/tasks/`):

1. Lists every `*.py` in the sibling `task/` folder.
2. For each one, loads the module and reads its module-level
   `UPSTREAM_TASKS: list[str]`.
3. Topologically sorts the tasks.
4. Creates a `PythonOperator` per task in dependency order, passing
   `op_kwargs={"yaml_path": ..., "upstream_task_ids": {...}}`.
5. Wires `>>` between them based on each task's `UPSTREAM_TASKS`.

The shape produced today is `extract >> upload`, because `extract.py`
declares `UPSTREAM_TASKS = []` and `upload.py` declares
`UPSTREAM_TASKS = ["extract"]`. To grow the graph, see
[Adding a new task type](#adding-a-new-task-type-eg-validate).

### Phase 3 — DAG run (each scheduled trigger)

`task/extract.py` (engine-agnostic):

```python
from common_lib.connector_class import IMPORT_CONNECTORS

def extract(yaml_path, upstream_task_ids):
    cfg = _load_cfg(yaml_path)
    cls = IMPORT_CONNECTORS[cfg["engine"]]   # mssql -> MSSQLImportConnector, db2 -> DB2…
    importer = cls(
        connection_id_import=cfg["connection_id_import"],
        database=cfg["database"],
        schema=cfg["schema"],
        table=cfg["table"],
        predicate=cfg.get("predicate"),
    )
    return importer.to_parquet(**context)    # returns absolute parquet path
```

`task/upload.py` (export-agnostic):

```python
from common_lib.connector_class import EXPORT_CONNECTORS

def upload(yaml_path, upstream_task_ids):
    cfg = _load_cfg(yaml_path)
    parquet_path = ti.xcom_pull(task_ids=upstream_task_ids["extract"])
    cls = EXPORT_CONNECTORS[cfg.get("export_engine") or only_one()]
    exporter = cls(
        connection_id_export=cfg["connection_id_export"],
        database=cfg["database"],
        schema=cfg["schema"],
        table=cfg["table"],
        container_name=dag_id,
    )
    return exporter.upload(local_parquet_path=parquet_path, **context)
```

Both connector classes declare a class attribute:

- `ENGINE = "mssql"` / `"db2"` / … on every import connector
- `EXPORT = "azure_blob"` / `"s3"` / … on every export connector

`common_lib/connector_class/__init__.py` walks every subclass of
`BaseConnector` at import time and builds two registries:

```python
IMPORT_CONNECTORS = {"mssql": MSSQLImportConnector, "db2": DB2ImportConnector, …}
EXPORT_CONNECTORS = {"azure_blob": AzureExportConnector, …}
```

If a sibling module fails to import (typically because an optional native
dep — e.g. `ibm_db` — isn't installed in the current environment), the
auto-loader logs a warning and skips it. The package still loads with a
usable subset of connectors; only YAMLs targeting the missing engine
will fail at run time, with a clear "Unsupported engine" message.

This is what makes "dropping a new connector file" a self-contained change.

### Class hierarchy

Each base class lives in its own file under `connector_class/bases/`:

```text
BaseConnector (ABC)                 connection_id, database, schema, table,
│   bases/connector.py              full_table_name, logger
│
├── BaseImportConnector             LANDING_DIR, predicate
│   │   bases/import_connector.py   _resolve_output_path(...)
│   │                               to_parquet(**context)        ← template method
│   │                               _build_query()                ← override per engine
│   │                               _fetch_dataframe()            ← override per engine
│   │
│   ├── MSSQLImportConnector        ENGINE = "mssql"           (mssql.py)
│   └── DB2ImportConnector          ENGINE = "db2"             (db2.py)
│
└── BaseExportConnector             DEFAULT_CONTAINER, container_name,
    │   bases/export_connector.py   overwrite, delete_local
    │                               _build_blob_name(local, **context)
    │                               upload(local_parquet_path, **context)  ← template method
    │                               _build_target_uri(blob_name)            ← override per engine
    │                               _upload_to_target(local, blob_name, **context) ← override
    │
    └── AzureExportConnector        EXPORT = "azure_blob"     (wasb.py)
```

`to_parquet` lives in `BaseImportConnector` and runs the same orchestration
for every engine: build query → fetch DataFrame → write parquet to a tmp
file → atomic rename → return path. Per-engine subclasses only have to
know how to *quote* their identifiers (`_build_query`) and how to *fetch*
(`_fetch_dataframe`).

`upload` lives in `BaseExportConnector` and runs the same orchestration
for every target: validate the local file → derive the blob name → call
the engine-specific transfer → log → optional `delete_local`. Per-target
subclasses only have to implement how to *describe* the destination
(`_build_target_uri`) and how to *transfer* the bytes
(`_upload_to_target`).

---

## Setup

```bash
cd "<path to this repo>"

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Pinned dependencies (see `requirements.txt`):

- `apache-airflow==2.10.5`
- `apache-airflow-providers-microsoft-mssql==3.9.2` — provides `MsSqlHook`
- `apache-airflow-providers-microsoft-azure==10.5.1` — provides `WasbHook`
- `pandas==2.2.3`
- `pyarrow==17.0.0`
- `PyYAML==6.0.2`
- `openpyxl==3.1.5` — `.xlsx` backend used by pandas in the intake reader

A DB2 import connector is included but not pinned by default; install
`airflow-provider-ibm-db2` (or the Apache `airflow-providers-ibm` package)
when you need it.

---

## Generating DAG folders from the spreadsheet

```bash
python -m common_lib.create_dag --intake "Business Requirement.xlsx" --repo-root .
```

CLI flags (all optional; defaults shown):

| Flag | Default | Meaning |
| --- | --- | --- |
| `--intake` | `Business Requirement.xlsx` | Path to the intake xlsx |
| `--repo-root` | `.` | Folder where DAG folders are created |
| `--dry-run` | _off_ | Print the planned changes without modifying the filesystem. |

Output is one line per DAG id with one of these statuses:

| Status | Meaning |
| --- | --- |
| `created` | The DAG folder did not exist; everything was scaffolded. |
| `added: <t1>, <t2>` | New table YAMLs were dropped into an existing group. |
| `updated: <t1>, <t2>` | Existing table YAMLs whose connector-relevant fields drifted from the spreadsheet were rewritten to match. |
| `removed: <t1>, <t2>` | Table YAMLs that no longer match a spreadsheet row were deleted from an existing group. |
| `added: <…>; updated: <…>; removed: <…>` | Any combination of the above happened in the same group on this run. |
| `cancelled` | The whole `(business_line, cadence)` group is gone from the spreadsheet; the DAG folder was removed. |
| `skipped` | The DAG folder was already in sync with the spreadsheet; nothing was changed. |

Typical no-op output:

```text
  rrap_weekly  skipped
  sda_daily    skipped
  sda_monthly  skipped
```

### Sync scenarios

The full set of cases the scaffolder handles, organized by what changed
in the spreadsheet between two runs.

#### Per-row scenarios (a single row in the spreadsheet)

| Change in spreadsheet | What happens on disk | Status |
| --- | --- | --- |
| New row in a brand-new `(business_line, cadence)` group | The whole DAG folder is scaffolded: `dag.py`, `task/extract.py`, `task/upload.py`, and `table/<table>.yaml` | `created` |
| New row in an already-existing group | A new `<table>.yaml` is dropped next to the existing ones | `added: <table>` |
| Row's connector fields change (`engine`, `connection_id_import`, `connection_id_export`, `database`, `schema`, `predicate`) | The matching `<table>.yaml` is rewritten to match the spreadsheet | `updated: <table>` |
| Row's `table` value changes (rename) | Treated as delete + add: old `<table>.yaml` is deleted, new one is written | `added: <new>; removed: <old>` |
| Row's `business_line` or `cadence` changes (row migrates between groups) | Old group loses the YAML; new group gains it. If the move makes the old group empty, the old folder is cancelled. | `removed: <table>` in old group + `added: <table>` (or `created`) in new group, plus `cancelled` for the old group if it ends up empty |
| Row deleted from spreadsheet | The matching `<table>.yaml` is deleted | `removed: <table>` |

#### Per-group scenarios (a whole `(business_line, cadence)` bucket)

| Change in spreadsheet | What happens on disk | Status |
| --- | --- | --- |
| New group appears | New DAG folder scaffolded end-to-end | `created` |
| Group entirely disappears from the spreadsheet | The whole DAG folder (`dag.py`, `table/`, `task/`) is removed so Airflow stops parsing it | `cancelled` |
| Group still present, no changes | Nothing happens | `skipped` |
| Group has any combination of additions, content edits, and removals | Verbs are joined with `"; "` | e.g. `added: A; updated: B; removed: C` |

#### Whole-spreadsheet scenarios

| Situation | Behavior |
| --- | --- |
| Empty intake but DAG folders still exist on disk | Every existing DAG folder is `cancelled` |
| Empty intake and no DAG folders on disk | Prints `Intake is empty; no DAGs to generate.` |
| Two rows share the same `(business_line, cadence, table)` triple | Run is rejected with `ValueError: Duplicate intake row for dag_id='…', table='…'` before any filesystem change |
| Required column missing from the spreadsheet header (`business_line`, `cadence`, `engine`, `connection_id_import`, `connection_id_export`, `database`, `schema`, `table`) | Run is rejected with `ValueError: Intake header is missing required column(s): […]` |

#### CLI / safety scenarios

| Action | Behavior |
| --- | --- |
| `python -m common_lib.create_dag --intake … --repo-root …` | Applies all the syncs above |
| `--dry-run` | Same status report, but no `mkdir` / `write` / `unlink` / `rmtree` happens; useful for previewing destructive `cancelled` runs |
| Sibling folders without a top-level `dag.py` (e.g. `common_lib/`, `.git/`, `data/`) | Never touched — the cancellation pass only considers folders that contain a `dag.py` |
| Hand-edit to any generated YAML | Out of contract — the next run will overwrite it back to whatever the spreadsheet says |
| Hand-edit to a generated `dag.py` or `task/*.py` in a surviving group | Preserved — those files are never overwritten in groups that survive |
| `dag.py` / `task/*.py` in a `cancelled` group | Removed along with the rest of the folder |

### Required spreadsheet columns

The first sheet must have these columns (case-sensitive header):

| Column | Meaning |
| --- | --- |
| `business_line` | First half of the DAG id, e.g. `sda` |
| `cadence` | Second half of the DAG id; one of `daily`, `weekly`, `monthly` |
| `engine` | Source engine name; must match a registered `ENGINE` in `IMPORT_CONNECTORS` |
| `connection_id_import` | Airflow connection id for the import side |
| `connection_id_export` | Airflow connection id for the export side |
| `database` | Source database / catalog name |
| `schema` | Source schema name (combined with database/table for the FQN and used in queries) |
| `table` | Source table name |

Optional columns (may be blank):

- `predicate` — SQL fragment appended after `WHERE` in the import query

If a required column is missing the intake reader raises with a clear
error listing which columns are absent.

---

## Running the DAGs in Airflow

Quickest local setup:

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="$(pwd)"

airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname a --lastname a --role Admin --email a@a.com

airflow standalone
```

Then open the URL Airflow prints (typically `http://localhost:8080`) and:

1. **Admin → Connections** — add the connection ids your YAMLs reference,
   e.g. `mssql_default` (type `mssql`) and `wasb_default` (type `wasb`).
2. Unpause `sda_daily` / `sda_monthly` / `rrap_weekly` and trigger a run.

To trigger from the CLI without using the UI:

```bash
airflow dags trigger sda_daily
```

---

## Per-table YAML schema

Each `<dag>/table/<table>.yaml` is a flat mapping:

```yaml
engine: db2                    # required — must match a key in IMPORT_CONNECTORS
connection_id_import: db2_conn
connection_id_export: azure-sas
database: database             # required — passed straight through to the connector
schema: dbo                    # required — combined into the fully-qualified table name
table: TM_DIM                  # required
predicate: null                # optional — SQL fragment appended after WHERE

# Optional. Only needed when more than one export connector is registered.
# Must match a key in EXPORT_CONNECTORS.
# export_engine: azure_blob
```

The `database`, `schema`, `table` triple is what every connector receives.
The fully-qualified table name (`<database>.<schema>.<table>`) shows up in
all the connector logs so you can grep a run by table name.

`extract.py` reads `engine` and dispatches via `IMPORT_CONNECTORS[engine]`.
`upload.py` reads the optional `export_engine`; when only one export
connector exists, the field is inferred and may be omitted.

---

## Adding a new import connector

Goal: support a new source database (e.g. Oracle, Postgres, Snowflake)
without editing any scaffold templates or generated DAG files.

`BaseImportConnector` already does all of the orchestration: identifier
validation, output-path resolution, parquet write with tmp-file cleanup,
empty-result handling, and step-by-step INFO logging. A new connector
only has to plug in two engine-specific things — *how to quote the
identifiers* and *how to fetch the DataFrame*.

### Step 1 — create the import connector class

Drop a new file in `common_lib/connector_class/`. Filename convention:
`<engine>.py`. Example for Oracle:

```python
# common_lib/connector_class/oracle.py
from __future__ import annotations

import pandas as pd

from .base import BaseImportConnector


class OracleImportConnector(BaseImportConnector):

    ENGINE = "oracle"

    def _build_query(self) -> str:
        query = f'SELECT * FROM "{self.schema}"."{self.table}"'
        if self.predicate:
            query += f" WHERE {self.predicate}"
        return query

    def _fetch_dataframe(self) -> pd.DataFrame:
        # Lazy import: keeps the package importable when the Oracle
        # provider isn't installed in the current environment.
        from airflow.providers.oracle.hooks.oracle import OracleHook

        hook = OracleHook(oracle_conn_id=self.connection_id)
        query = self._build_query()
        self.logger.info(
            "Running Oracle query on %s: %s", self.full_table_name, query,
        )
        return hook.get_pandas_df(sql=query)
```

That is the *complete* connector. `to_parquet`, the landing-dir tree,
the tmp-file write/atomic-rename, the row-count log, the "0 rows = empty
parquet still written" behavior — all inherited from
`BaseImportConnector`.

### Step 2 — that's it

The next time Python imports `common_lib.connector_class`,
`_autoload_submodules()` picks up `oracle.py` automatically and
`OracleImportConnector` is visible as `IMPORT_CONNECTORS["oracle"]`.
You'll see this line in the logs at DAG parse time:

```text
INFO common_lib.connector_class: Connector registries built: imports=['db2', 'mssql', 'oracle'], exports=['azure_blob']
```

### Step 3 — use it

In any new spreadsheet row (or directly in a `*.yaml`), set:

```yaml
engine: oracle
connection_id_import: my_oracle_conn
database: SALES
schema: APP
table: ORDERS
```

The generated `task/extract.py` picks it up via the registry. **No
edits to `__init__.py`, scaffold templates, existing extract.py files,
or DAG files are needed.**

### Required contract for an import connector

| Element | Value |
| --- | --- |
| Base class | Subclass of `BaseImportConnector` (in `common_lib/connector_class/base.py`) |
| Class attribute | `ENGINE: str` — non-empty, unique, lowercase. Duplicates raise at import time. |
| Methods to override | `_build_query(self) -> str` and `_fetch_dataframe(self) -> pandas.DataFrame` |
| Methods inherited (do **not** override) | `to_parquet(**context)`, `_resolve_output_path(**context)`, the `__init__` signature |

The constructor signature is fixed by `BaseImportConnector`:

```python
__init__(self, connection_id_import, database, schema, table, predicate=None)
```

If a future engine needs extra knobs (e.g. an Oracle service-name or a
DB2 protocol override), prefer reading them from an env var at class
scope so the constructor signature stays uniform across all import
connectors.

---

## Adding a new export connector

Goal: support a new export target (S3, GCS, NFS, …) without editing any
scaffold templates or generated DAG files.

`BaseExportConnector` already does all of the orchestration: validate the
local parquet file, derive a blob name from
`<dag_id>/<database>/<schema>/<table>/<filename>`, log start/end of the
upload, and optionally delete the local file after a successful upload.
A new export only has to plug in two engine-specific things — *how to
describe the destination* and *how to transfer the bytes*.

### Step 1 — create the export connector class

```python
# common_lib/connector_class/s3.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from .bases import BaseExportConnector


class S3ExportConnector(BaseExportConnector):

    EXPORT = "s3"

    # If you want a default bucket name, point this at an env var:
    # DEFAULT_CONTAINER = os.environ.get("S3_BUCKET", "")

    def _build_target_uri(self, blob_name: str) -> str:
        return f"s3://{self.container_name}/{blob_name}"

    def _upload_to_target(
        self, local: Path, blob_name: str, **context: Any
    ) -> None:
        # Lazy import: keeps the package importable when the AWS provider
        # isn't installed in the current environment.
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=self.connection_id)
        hook.load_file(
            filename=str(local),
            bucket_name=self.container_name,
            key=blob_name,
            replace=self.overwrite,
        )
```

That is the *complete* connector. File validation, the blob-name layout,
"local file is empty / missing" errors, the upload-start/upload-complete
log lines, and `delete_local` are all inherited from
`BaseExportConnector`.

### Step 2 — declare it per-YAML

Once a **second** export connector exists, every YAML must explicitly
choose one:

```yaml
export_engine: s3       # was inferred while only "azure_blob" existed
```

If the YAML omits `export_engine` but more than one is registered,
`upload.py` raises with a message listing the available export names.

### Required contract for an export connector

| Element | Value |
| --- | --- |
| Base class | Subclass of `BaseExportConnector` (in `common_lib/connector_class/bases/`) |
| Class attribute | `EXPORT: str` — non-empty, unique, lowercase |
| Methods to override | `_build_target_uri(blob_name) -> str` and `_upload_to_target(local, blob_name, **context) -> None` |
| Methods inherited (do **not** override unless you really need to) | `upload(local_parquet_path, **context)`, `_build_blob_name(...)`, the `__init__` signature |

The constructor signature is fixed by `BaseExportConnector`:

```python
__init__(self, connection_id_export, database, schema, table,
         container_name=None, overwrite=True, delete_local=False)
```

You're free to use `container_name` to mean whatever fits your target
("bucket", "filesystem prefix", etc.). Engine-specific knobs that aren't
in this signature should be read from environment variables — see
`AzureExportConnector` reading `AZURE_BLOB_CONTAINER`.

---

## Adding a new task type (e.g. validate)

The TaskGroup builder discovers every `*.py` in `<dag>/task/` and wires
them by reading each module's `UPSTREAM_TASKS`. So adding a new step to
the per-table TaskGroup is purely additive too.

### Example: a `validate` task that runs in parallel with `upload`

Drop a new file `<dag>/task/validate.py`:

```python
"""Validate task: row-count sanity check on the produced parquet."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context


UPSTREAM_TASKS: list[str] = ["extract"]   # ← the pointer the builder reads


def validate(yaml_path: str, upstream_task_ids: dict[str, str]) -> None:
    parquet_path = get_current_context()["ti"].xcom_pull(
        task_ids=upstream_task_ids["extract"]
    )
    if not parquet_path or not Path(parquet_path).exists():
        raise AirflowException(f"validate: parquet missing at {parquet_path!r}")
    n_rows = len(pd.read_parquet(parquet_path))
    if n_rows == 0:
        raise AirflowException("validate: 0 rows imported")
```

### What happens next

The next DAG parse will produce:

```text
extract >> upload
        >> validate          (parallel branch off extract)
```

…with no changes to `dag.py`, `build_table_taskgroup.py`, or any other
task file.

### Required contract for a task module

| Element | Value |
| --- | --- |
| Filename | `<task_id>.py` (the stem becomes the operator's `task_id`) |
| Module attribute | `UPSTREAM_TASKS: list[str]` — names of other task modules in the same group it depends on (defaults to `[]` if absent) |
| Callable | A function named the same as the file, signature `(yaml_path: str, upstream_task_ids: dict[str, str]) -> Any` |

`upstream_task_ids` is supplied by `build_table_taskgroup` and maps each
name in `UPSTREAM_TASKS` to the **fully-qualified Airflow task id**
(`"<group_id>.<name>"`). Use it for `ti.xcom_pull(task_ids=...)`.

### Caveats

- Cycles or unknown upstream names raise at DAG parse time with a clear
  message naming the offending module.
- Within a single `<dag>/task/` folder, the wiring is uniform across every
  table in that DAG — every TaskGroup gets the same set of tasks. If you
  need per-table variation, that's a future extension (the YAML could
  carry a list of enabled tasks).

---

## Configuration & environment variables

### Airflow connections

The connection ids referenced in each YAML's
`connection_id_import` / `connection_id_export` must exist under
**Admin → Connections** in the Airflow UI. Examples:

| Connection id | Type | Used for |
| --- | --- | --- |
| `mssql_default` | `mssql` | MSSQL imports |
| `db2_conn` | `db2` (provider-dependent) | DB2 imports |
| `wasb_default` / `azure-sas` | `wasb` | Azure Blob exports |

### Environment variables (all optional)

| Var | Default | Effect |
| --- | --- | --- |
| `AIRFLOW_LANDING_DIR` | `/bns/rrap/data` | Root of the parquet landing tree used by `BaseImportConnector._resolve_output_path`. Override locally to write under your repo (e.g. `export AIRFLOW_LANDING_DIR="$(pwd)/data"`). |
| `AZURE_BLOB_CONTAINER` | `blob` | Default container for `AzureExportConnector` when a caller doesn't specify one. The upload task overrides this with the DAG id. |
| `AIRFLOW_HOME` | `~/airflow` | Standard Airflow var; set to a project-local dir for self-contained dev. |

### Where parquet files land

`BaseImportConnector._resolve_output_path` writes to:

```text
<AIRFLOW_LANDING_DIR>/<dag_id>/<run_date>/<database>_<schema>/<table>_<ts_nodash>.parquet
```

For example a `sda_daily` run on 2026-05-06 producing the
`database.dbo.TM_DIM` table writes to:

```text
/bns/rrap/data/sda_daily/2026-05-06/database_dbo/TM_DIM_20260506T000000.parquet
```

The `database_schema` directory layer is what makes two tables with the
same `table` name but different schemas / databases coexist on disk.

---

## Troubleshooting

| Symptom | Likely cause / where to look |
| --- | --- |
| `ModuleNotFoundError: No module named 'common_lib'` | Run from the workspace root (the directory containing `common_lib/`). For Airflow, ensure `PYTHONPATH` or the dags-folder includes this root. |
| DAG missing in Airflow UI | Check **DAG Import Errors** in the UI; usually a missing provider package or a typo in a YAML. |
| `Intake header is missing required column(s): ['schema']` | The spreadsheet predates the `schema` column. Add a `schema` column and re-run the scaffolder. |
| `Duplicate intake row for dag_id='…', table='…'` | Two spreadsheet rows share the same `(business_line, cadence, table)` triple. Each row maps to a single YAML, so the triple must be unique — fix the spreadsheet and re-run. |
| `KeyError: 'schema'` from `extract.py`/`upload.py` | The YAML predates the `schema` field. Edit the YAML to add `schema:` (or delete the DAG folder and re-run `python -m common_lib.create_dag`). |
| `Unsupported engine 'xyz'` from `extract.py` | The YAML's `engine` is not a registered `ENGINE`. Confirm the connector file exists in `common_lib/connector_class/` and its `ENGINE` attribute matches the YAML. Check the DAG-parse log line `Connector registries built: imports=[…]` to see what's actually registered. |
| `Skipping connector module 'xyz' — ImportError: …` at startup | The auto-loader couldn't import that connector's optional native / provider dep. Install the dep, or remove the file if you don't need that engine. The package keeps loading without it. |
| `Unknown export_engine 'xyz'` from `upload.py` | Same idea on the export side: `EXPORT` attribute on the connector vs. `export_engine` field in the YAML. |
| `Multiple export connectors registered (...)` from `upload.py` | More than one export connector is registered, but a YAML omits `export_engine:`. Add the field to that YAML. |
| `Duplicate ENGINE='xyz' declared by …` at import time | Two connector classes set the same `ENGINE` (or `EXPORT`). The names must be unique. |
| `Cycle or missing upstream in task graph` at DAG parse | A `task/*.py` declares `UPSTREAM_TASKS` containing an unknown name, or the dependency graph has a cycle. The error message names the offending modules. |
| Upload says `ContainerNotFound` | `AzureExportConnector._ensure_container` calls `create_container`; the credentials in the wasb connection must allow container creation, or pre-create the container manually. |
| `BaseConnector is missing required argument(s): schema` | A YAML or caller didn't pass `schema`. The error names every missing field at once. |
| Parquet file written to the wrong place locally | Check `AIRFLOW_LANDING_DIR`; default is `/bns/rrap/data` (production NFS path). Set the env var when running locally. |
| Local file from `extract` not visible in `upload` | They ran on different workers with separate disks. Either collapse them into a single combined task or use shared storage / persistent volume. |
| `apache-airflow-providers-... is not installed` | The relevant provider isn't in `requirements.txt` for this environment. Install it and restart Airflow. |
