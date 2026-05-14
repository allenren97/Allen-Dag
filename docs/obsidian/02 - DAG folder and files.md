---
tags:
  - filesystem
  - dag-layout
---

# DAG folder structure (`<dag_id>/`)

**`dag_id`** equals the **directory name** under `--repo-root`. It is built from the spreadsheet as:

`slug(business_line) + "_" + slug(cadence)`  

(e.g. `rrap_weekly`, `retail_sda_daily`). See [[04 - Intake package]].

## Directory tree

After a full scaffold for one DAG, you should see:

```text
<dag_id>/
├── dag.py                 # Airflow DAG definition (write-once by generator)
├── table/                 # one YAML per table row in the intake group
│   ├── <table_a>.yaml
│   └── <table_b>.yaml
└── task/                  # Python task modules (write-once by generator)
    ├── extract.py
    └── upload.py
```

## What each part does

| Path | Role |
|------|------|
| `dag.py` | Declares one `DAG`; loops `table/*.yaml`; calls `build_table_taskgroup(yaml_path)` for each. Scaffold source: [[code/write_dag_file]]. |
| `table/<table>.yaml` | Connector settings for **that** table (`engine`, connection ids, `database`, `schema`, `table`, optional `predicate`, optional `export_engine`). Writer: [[code/write_table_yaml]]. |
| `task/extract.py` | Callable `extract(yaml_path, upstream_task_ids)` → returns **local parquet path** (XCom). Template: [[code/write_extract_task]]. Set module **`UPSTREAM_TASKS`** (empty for root tasks). |
| `task/upload.py` | Callable `upload(...)` → pulls extract XCom → returns **destination URI** (XCom). Template: [[code/write_upload_task]]. Typically **`UPSTREAM_TASKS = ["extract"]`**. Wiring rules: [[10 - Connectors and tasks]]. |

## What the generator overwrites vs preserves

From [[03 - create_dag]] behaviour:

- **Always synced:** every `table/<table>.yaml` is **created, updated, or deleted** to match the spreadsheet (connector-relevant columns).
- **Write-once (never overwritten if present):** `dag.py`, `task/extract.py`, `task/upload.py`. Safe to hand-edit for that DAG after first creation.
- **Whole folder removed** if the `(business_line, cadence)` group disappears from the sheet (**cancelled** DAG): only directories that look like generated DAGs (contain top-level `dag.py`) are removed from `repo_root`.

## Relationship to Airflow `dag_id`

Generated `dag.py` sets `dag_id=HERE.name`, i.e. the **folder name** matches the Airflow DAG id.

Related: [[06 - write_dag_file]], [[07 - write_table_yaml]], [[08 - write_extract_task]], [[09 - write_upload_task]] · [[00 - Index]]
