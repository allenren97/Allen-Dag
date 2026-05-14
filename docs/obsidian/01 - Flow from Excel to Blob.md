---
tags:
  - overview
  - architecture
---

# Flow: Excel → disk → Airflow → storage

Two separate phases:

## Phase A — Generator (developer / ops, on demand)

1. Someone updates the **intake spreadsheet** (e.g. `intake_form.xlsx` or the default `Business Requirement.xlsx`).
2. You run **`python -m common_lib.create_dag`** (see [[03 - create_dag]]). Implementation: [[code/create_dag]].
3. **`read_intake_rows`** loads rows — [[code/read_excel]]. **`group_by_dag`** buckets by `dag_id` (slug from `business_line` + `cadence`) — [[code/group_by_dag]].
4. For each `dag_id`, the tool **creates or syncs** a folder under `--repo-root` (see [[02 - DAG folder and files]]).
5. **[[05 - Scaffold package]]** writers emit **once** `dag.py`, `task/extract.py`, `task/upload.py` if missing; **table YAMLs** are always kept in sync with the sheet. Source snapshots: [[code/write_dag_file]], [[code/write_extract_task]], [[code/write_upload_task]], [[code/write_table_yaml]], package [[code/scaffold_init]].

## Phase B — Airflow (scheduler / workers)

1. Airflow discovers **`dag.py`** under each `<dag_id>/` folder (however your deployment adds DAGs to Airflow).
2. **`dag.py`** loops `table/*.yaml` and calls **`build_table_taskgroup`** for each file — [[code/build_table_taskgroup]]. Each YAML gets its **own** `TaskGroup` named after the table stem. **There is no generated edge between different tables**; only tasks under that DAG’s shared **`task/`** folder form a graph per group. Details: [[10 - Connectors and tasks]].
3. **In-group upstream / downstream** (same `TaskGroup`): you only declare **upstream** in each `task/<name>.py` via **`UPSTREAM_TASKS`** (list of other task **stems** in the same `task/` folder). The builder topologically sorts tasks, then for each task wires **`operators[upstream] >> operators[this]`** in Airflow. **Downstream** is implicit (whoever lists you in `UPSTREAM_TASKS`). Each callable receives **`upstream_task_ids`**: logical name → real **`task_id`** (needed inside nested groups and for `xcom_pull`). With only **`extract`** / **`upload`**, that is **`extract >> upload`** (`extract` has `UPSTREAM_TASKS = []`; `upload` has `["extract"]`). XCom follows that chain.
4. **Connector lookup (auto-registration):** import tasks use **`IMPORT_CONNECTORS`** and export tasks use **`EXPORT_CONNECTORS`**. Those dicts are built **at import time** when `common_lib.connector_class` loads: every sibling module under `connector_class/` is imported (except subpackages like `bases/`), then every subclass of `BaseConnector` with a non-empty **`ENGINE`** (import) or **`EXPORT`** (export) is keyed into the registry. Adding a new `connector_class/<engine>.py` with the right subclass **does not require** editing `extract.py` / `upload.py`. Full walkthrough: [[10 - Connectors and tasks]] · [[code/connector_class_init]].
5. The **first** task in the in-group chain (here `extract`) resolves YAML **`engine`** against **`IMPORT_CONNECTORS`** and writes a local parquet path.
6. A **later** task (here `upload`) uses **`upstream_task_ids["extract"]`** to `xcom_pull`, resolves **`export_engine`** (or the single registered exporter) against **`EXPORT_CONNECTORS`**, pushes to blob storage, returns a **URI** string.

```mermaid
flowchart LR
  X[Excel] --> CLI[create_dag]
  CLI --> F[DAG folder]
  F --> AF[Airflow parses dag.py]
  AF --> TG[TaskGroup per table YAML]
  TG --> E[extract]
  E --> U[upload]
  U --> B[Blob / URI]
```

Related: [[00 - Index]], [[02 - DAG folder and files]].
