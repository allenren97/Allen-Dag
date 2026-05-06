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
│   │   ├── base.py                    # BaseConnector (ABC) — common ctor + logger
│   │   ├── mssql.py                   # MSSQLImportConnector (ENGINE = "mssql")
│   │   ├── db2.py                     # DB2ImportConnector   (ENGINE = "db2")
│   │   ├── wasb.py                    # AzureExportConnector (EXPORT = "azure_blob")
│   │   └── __init__.py                # auto-discovers connectors → IMPORT_CONNECTORS,
│   │                                  #                              EXPORT_CONNECTORS
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

The scaffolder is **non-destructive**: any folder that already exists is
skipped untouched. To regenerate a DAG folder, `rm -rf` it and re-run.

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
    importer = cls(connection_id_import=..., database=..., table=..., predicate=..., …)
    return importer.to_parquet(**context)    # returns absolute parquet path
```

`task/upload.py` (export-agnostic):

```python
from common_lib.connector_class import EXPORT_CONNECTORS

def upload(yaml_path, upstream_task_ids):
    cfg = _load_cfg(yaml_path)
    parquet_path = ti.xcom_pull(task_ids=upstream_task_ids["extract"])
    cls = EXPORT_CONNECTORS[cfg.get("export_engine") or only_one()]
    exporter = cls(connection_id_export=..., database=..., table=..., container_name=dag_id, …)
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

This is what makes "dropping a new connector file" a self-contained change.

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
- `openpyxl==3.1.5` — used by the intake reader

A DB2 import connector is included but not pinned by default; install
`airflow-provider-ibm-db2` (or the Apache `airflow-providers-ibm` package)
when you need it.

---

## Generating DAG folders from the spreadsheet

```bash
python -m common_lib.create_dag --intake "Business Requirement.xlsx" --repo-root .
```

CLI flags (both optional; defaults shown):

| Flag | Default | Meaning |
| --- | --- | --- |
| `--intake` | `Business Requirement.xlsx` | Path to the intake xlsx |
| `--repo-root` | `.` | Folder where DAG folders are created |

Output is one line per DAG id, marked `created` or `skipped`:

```text
  rrap_weekly  skipped
  sda_daily    skipped
  sda_monthly  skipped
```

### Required spreadsheet columns

The first sheet must have these columns (case-sensitive header):

| Column | Meaning |
| --- | --- |
| `business_line` | First half of the DAG id, e.g. `sda` |
| `cadence` | Second half of the DAG id; one of `daily`, `weekly`, `monthly` |
| `engine` | Source engine name; must match a registered `ENGINE` in `IMPORT_CONNECTORS` |
| `connection_id_import` | Airflow connection id for the import side |
| `connection_id_export` | Airflow connection id for the export side |
| `database` | Source database / schema name |
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
database: database
table: TM_DIM
predicate: null                # optional — SQL fragment appended after WHERE

# Optional. Only needed when more than one export connector is registered.
# Must match a key in EXPORT_CONNECTORS.
# export_engine: azure_blob
```

`extract.py` reads `engine` and dispatches via `IMPORT_CONNECTORS[engine]`.
`upload.py` reads the optional `export_engine`; when only one export
connector exists, the field is inferred and may be omitted.

---

## Adding a new import connector

Goal: support a new source database (e.g. Oracle, Postgres, Snowflake)
without editing any scaffold templates or generated DAG files.

### Step 1 — create the import connector class

Drop a new file in `common_lib/connector_class/`. Filename convention:
`<engine>.py`. Example for Oracle:

```python
# common_lib/connector_class/oracle.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Optional

from airflow.exceptions import AirflowException

from .base import BaseConnector


class OracleImportConnector(BaseConnector):
    """Pull rows from Oracle into a local parquet file."""

    ENGINE = "oracle"          # this is the only thing the registry needs

    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    LANDING_DIR = Path(os.environ.get("AIRFLOW_LANDING_DIR", _PROJECT_ROOT / "data"))

    def __init__(
        self,
        connection_id_import: str,
        database: str,
        table: str,
        predicate: Optional[str] = None,
        landing_partition_prefix: Optional[str] = None,
    ) -> None:
        super().__init__(connection_id=connection_id_import, database=database, table=table)
        self.predicate = predicate
        self.landing_partition_prefix = landing_partition_prefix

    def to_parquet(self, **context: Any) -> str:
        # ... build query, run hook, write parquet, return abs path ...
        return "/abs/path/to/parquet"
```

### Step 2 — that's it

The next time Python imports `common_lib.connector_class`,
`_autoload_submodules()` will pick up `oracle.py` automatically.
`OracleImportConnector` is then visible as `IMPORT_CONNECTORS["oracle"]`.

### Step 3 — use it

In any new spreadsheet row (or directly in a `*.yaml`), set:

```yaml
engine: oracle
connection_id_import: my_oracle_conn
...
```

The generated `task/extract.py` will pick it up via the registry. **No
edits to `__init__.py`, scaffold templates, existing extract.py files,
or DAG files are needed.**

### Required contract for an import connector

| Element | Value |
| --- | --- |
| Base class | Subclass of `common_lib.connector_class.base.BaseConnector` |
| Class attribute | `ENGINE: str` — non-empty, unique, lowercase. Duplicates raise at import time. |
| Constructor signature | `__init__(self, connection_id_import, database, table, predicate=None, landing_partition_prefix=None)` |
| Method | `to_parquet(self, **context) -> str` returning the absolute path of the produced parquet file |

If a future engine needs extra connector-specific config (e.g. an Oracle
service-name), prefer reading it from an env var at class scope (the way
`AzureExportConnector` does with `AZURE_BLOB_CONTAINER`) so the
constructor signature stays uniform across all import connectors.

---

## Adding a new export connector

The same pattern as imports, but with an `EXPORT` class attribute and the
`EXPORT_CONNECTORS` registry.

### Step 1 — create the export connector class

```python
# common_lib/connector_class/s3.py
from .base import BaseConnector


class S3ExportConnector(BaseConnector):
    """Upload a local parquet file to Amazon S3."""

    EXPORT = "s3"              # the only thing the registry needs

    def __init__(
        self,
        connection_id_export: str,
        database: str,
        table: str,
        container_name: str | None = None,    # interpret as bucket name internally
        delete_local: bool = False,
    ) -> None:
        super().__init__(connection_id=connection_id_export, database=database, table=table)
        self.bucket_name = container_name
        self.delete_local = delete_local

    def upload(self, local_parquet_path: str, **context) -> str:
        # ... use S3Hook, return s3://bucket/key ...
        return "s3://..."
```

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
| Base class | Subclass of `BaseConnector` |
| Class attribute | `EXPORT: str` — non-empty, unique, lowercase |
| Constructor signature | `__init__(self, connection_id_export, database, table, container_name=None, delete_local=False)` |
| Method | `upload(self, local_parquet_path: str, **context) -> str` returning the destination URI |

You're free to use `container_name` to mean whatever fits your target
("bucket", "filesystem prefix", etc.). Engine-specific knobs that aren't
in this signature should be read from environment variables.

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
| `AIRFLOW_LANDING_DIR` | `<repo_root>/data` | Where import connectors write the staged parquet |
| `AZURE_BLOB_CONTAINER` | `raw` | Default container if a YAML doesn't specify one (the upload task overrides it with the DAG id) |
| `AIRFLOW_HOME` | `~/airflow` | Standard Airflow var; set to a project-local dir for self-contained dev |

---

## Troubleshooting

| Symptom | Likely cause / where to look |
| --- | --- |
| `ModuleNotFoundError: No module named 'common_lib'` | Run from the workspace root (the directory containing `common_lib/`). For Airflow, ensure `PYTHONPATH` or the dags-folder includes this root. |
| DAG missing in Airflow UI | Check **DAG Import Errors** in the UI; usually a missing provider package or a typo in a YAML. |
| `Unsupported engine 'xyz'` from `extract.py` | The YAML's `engine` is not a registered `ENGINE`. Confirm the connector file exists in `common_lib/connector_class/` and its `ENGINE` attribute matches the YAML. |
| `Unknown export_engine 'xyz'` from `upload.py` | Same idea on the export side: `EXPORT` attribute on the connector vs. `export_engine` field in the YAML. |
| `Multiple export connectors registered (...)` from `upload.py` | More than one export connector is registered, but a YAML omits `export_engine:`. Add the field to that YAML. |
| `Duplicate ENGINE='xyz' declared by …` at import time | Two connector classes set the same `ENGINE` (or `EXPORT`). The names must be unique. |
| `Cycle or missing upstream in task graph` at DAG parse | A `task/*.py` declares `UPSTREAM_TASKS` containing an unknown name, or the dependency graph has a cycle. The error message names the offending modules. |
| Upload says `ContainerNotFound` | `AzureExportConnector` calls `create_container`; the credentials in `wasb_default` must allow container creation, or pre-create the container manually. |
| Local file from `extract` not visible in `upload` | They ran on different workers with separate disks. Either collapse them into a single combined task or use shared storage / persistent volume. |
| `apache-airflow-providers-... is not installed` | The relevant provider isn't in `requirements.txt` for this environment. Install it and restart Airflow. |
