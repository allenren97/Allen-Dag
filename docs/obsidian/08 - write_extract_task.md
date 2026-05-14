---
tags:
  - scaffold
  - extract
---

# `write_extract_task.py`

**Full source:** [[code/write_extract_task]]

**Exports:** `write_extract_task(dag_dir: Path) -> Path`

**Writes:** `<dag_dir>/task/extract.py` (a **string template**, not Jinja — full file text embedded in the module).

## Generated module contract

- **`UPSTREAM_TASKS: list[str] = []`** — no **in-group** upstream tasks, so `extract` is the first node in the `>>` chain (see [[10 - Connectors and tasks]]).
- **`extract(yaml_path: str, upstream_task_ids: dict[str, str]) -> str`**
  - Loads YAML with `yaml.safe_load`.
  - Reads `engine` → **`IMPORT_CONNECTORS[engine]`** (from `common_lib.connector_class`).
  - Instantiates connector with `connection_id_import`, `database`, `schema`, `table`, optional `predicate`.
  - Calls **`to_parquet(**context)`** (Airflow context).
  - **Return value:** local parquet **path string** → becomes **XCom** for downstream tasks.

## Why a template file

Adding a new **import engine** is usually: new file under `connector_class/` with `ENGINE = "..."` — **no edit** to per-DAG `extract.py` once scaffolded.

## Call site

[[03 - create_dag]] only if **`task/extract.py`** does not exist — **write-once**.

Related: [[10 - Connectors and tasks]] · [[05 - Scaffold package]] · [[00 - Index]]
