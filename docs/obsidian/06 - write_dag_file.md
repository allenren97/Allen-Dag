---
tags:
  - scaffold
  - dag-py
---

# `write_dag_file.py`

**Full source:** [[code/write_dag_file]]

**Exports:** `write_dag_file(dag_dir: Path, cadence: str) -> Path`

**Writes:** `<dag_dir>/dag.py`

## Behaviour

- **`CADENCE_TO_SCHEDULE`** maps spreadsheet `cadence` (case-insensitive) to Airflow schedule:
  - `daily` → `@daily`
  - `weekly` → `@weekly`
  - `monthly` → `@monthly`
- Unknown cadence → **`ValueError`**.
- Replaces placeholder `__SCHEDULE__` in the embedded template with `repr(schedule)` (e.g. `'@weekly'`).

## What the generated `dag.py` does

1. Defines **`HERE = Path(__file__).resolve().parent`** so **`dag_id = HERE.name`** matches the folder name (your `dag_id`).
2. Builds a **`DAG`** with `start_date`, `catchup=False`, `max_active_runs=1`, tags, etc.
3. **Loops** `sorted((HERE / "table").glob("*.yaml"))` and calls **`build_table_taskgroup(yaml_path)`** from `common_lib.tasks.build_table_taskgroup` for **each** table YAML — one TaskGroup per table file.

So: **adding a new `table/foo.yaml`** automatically adds a new TaskGroup on next DAG parse — no manual edit to `dag.py` for new tables (unless you customized the loop).

## Call site

[[03 - create_dag]] calls it only when **`(dag_dir / "dag.py").exists()`** is false — **write-once**.

Related: [[02 - DAG folder and files]] · [[10 - Connectors and tasks]] · [[05 - Scaffold package]] · [[00 - Index]]
