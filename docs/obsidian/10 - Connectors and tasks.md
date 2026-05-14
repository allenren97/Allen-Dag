---
tags:
  - connectors
  - airflow
  - taskgroup
---

# Connectors + TaskGroup wiring

## `common_lib/connector_class/`

**Implementation:** `common_lib/connector_class/__init__.py` — full module copy: [[code/connector_class_init]]. After editing Python, re-run `python3 docs/obsidian/sync_code_snippets.py` from repo root ([[00 - Index]]).

- **`__init__.py`** auto-imports sibling modules (`mssql`, `db2`, `wasb`, …), collects subclasses of `BaseConnector`, builds:
  - **`IMPORT_CONNECTORS`**: `dict[str, type]` keyed by each class’s **`ENGINE`** (import).
  - **`EXPORT_CONNECTORS`**: keyed by **`EXPORT`** (export).

### How connector auto-registration works

There is no manual `register(MyConnector)` call. Registration is **import-time**:

1. **`_autoload_submodules()`** runs when the package loads. It walks **`pkgutil.iter_modules`** on `connector_class/`, **skips subpackages** (so `bases/` is not treated as a “connector file” here), and **`importlib.import_module`** each top-level sibling `*.py`. That executes `class …(BaseImportConnector)` / `BaseExportConnector`, which attaches subclasses to Python’s class graph.
2. If a sibling module **fails to import** (missing optional native lib or Airflow provider), the loader **logs a warning and skips** that module; the rest of the package still loads.
3. **`_all_subclasses(BaseConnector)`** walks **`__subclasses__()`** recursively (DFS) to find every descendant after those imports.
4. **`_build_registry("ENGINE")`** and **`_build_registry("EXPORT")`** scan those classes. For each class, it reads the attribute, **`.strip().lower()`**, and uses it as the dict key. Classes with an **empty** key are skipped (bases and abstracts). **Duplicate keys** for two different classes raise **`RuntimeError`** at import time.
5. **`IMPORT_CONNECTORS`** / **`EXPORT_CONNECTORS`** are assigned **once**, after `_autoload_submodules()`, so tasks can **`IMPORT_CONNECTORS.get(engine)`** without editing scaffold code when you add a new `connector_class/<name>.py` that sets **`ENGINE`** or **`EXPORT`**.

**Bases** (`connector_class/bases/`):

- `BaseConnector` — shared identity + logger.
- `BaseImportConnector` — `to_parquet()` template method; subclasses implement `_build_query`, `_fetch_dataframe`.
- `BaseExportConnector` — `upload()` template method; subclasses implement `_build_target_uri`, `_upload_to_target`.

Concrete engines live as **`connector_class/<engine>.py`** (e.g. MSSQL import, Azure blob export).

See repo `EXPORT_CONNECTORS.md` / README for extension notes.

## `common_lib/tasks/build_table_taskgroup.py`

**Full source:** [[code/build_table_taskgroup]]

- **`build_table_taskgroup(yaml_path: Path) -> TaskGroup`**
  - Discovers **`<dag>/task/*.py`**, loads each module, requires a **callable** named like the file stem (`extract`, `upload`, or any extra tasks you add).
  - Reads **`UPSTREAM_TASKS`** per module (list of **other task stems** in the same `task/` folder that must finish first). For each upstream name `U`, it wires **`U >> this_task`** in Airflow terms — i.e. the general pattern is **`[upstream] >> [downstream]`** repeated until the whole graph is built. Nothing treats `extract` specially; it is simply the task whose `UPSTREAM_TASKS` is empty in the default scaffold, so it sorts first.
  - **Topological sort** of that graph determines operator creation order.
  - Passes **`op_kwargs`**: `yaml_path` (string) + `upstream_task_ids` (map of logical name → operator `task_id`).

### How upstream / downstream work in the TaskGroup

- **You only declare upstream.** Each module sets **`UPSTREAM_TASKS: list[str]`** to the task **stems** it depends on (same `task/` directory). **Downstream** tasks are whoever lists **you** in their `UPSTREAM_TASKS`; the builder never asks for a separate “downstream” list.
- **Validation:** every name in `UPSTREAM_TASKS` must exist as another `*.py` stem in `task/`. Unknown names raise **`RuntimeError`**. **`_topo_sort`** orders tasks so all upstreams are scheduled before dependents; a **cycle** or dangling reference surfaces as **`RuntimeError`** from the sort.
- **Airflow edges:** for task `T` with upstreams `[U1, U2, …]`, the code does **`operators[U1] >> op`**, **`operators[U2] >> op`**, … so **`T` is downstream of each listed upstream**.
- **`upstream_task_ids`:** before creating `T`’s `PythonOperator`, the builder fills a dict **`{ "extract": "<full task_id>", … }`** using **`operators[u].task_id`** for each upstream `u`. Inside a **`TaskGroup`**, Airflow prefixes task ids; downstream code must use this map (e.g. **`ti.xcom_pull(task_ids=upstream_task_ids["extract"]`)**) instead of hard-coding `"extract"`.
- **Scope:** this graph is **per table YAML** (one `TaskGroup` per file under `table/`). **`dag.py`** does not wire **between** different table groups; parallel tables share the same **`task/`** module definitions but each group instantiates its **own** operators.

So the **relationship** between packages:

```text
create_dag  →  scaffold.*     →  files on disk under <dag_id>/
Airflow     →  dag.py          →  build_table_taskgroup
dag task    →  extract/upload →  connector_class registries
```

## XCom data flow (per table, default two-task chain)

Return values of **`PythonOperator`** callables are pushed to XCom; downstream callables receive **`upstream_task_ids`** so they can `xcom_pull` the right predecessor.

1. **`extract`** (no in-group upstream) returns **parquet path** `str`.
2. **`upload`** (downstream of `extract`) uses **`upstream_task_ids["extract"]`**, pulls XCom, returns **URI** `str`.

If you add more tasks, the same rule applies: each downstream task only knows its predecessors through **`UPSTREAM_TASKS`** + **`upstream_task_ids`**.

Back: [[00 - Index]] · [[01 - Flow from Excel to Blob]] · [[02 - DAG folder and files]]
