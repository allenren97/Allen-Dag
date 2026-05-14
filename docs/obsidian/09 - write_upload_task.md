---
tags:
  - scaffold
  - upload
---

# `write_upload_task.py`

**Full source:** [[code/write_upload_task]]

**Exports:** `write_upload_task(dag_dir: Path) -> Path`

**Writes:** `<dag_dir>/task/upload.py` (embedded template, same pattern as [[08 - write_extract_task]]).

## Generated module contract

- **`UPSTREAM_TASKS = ["extract"]`** — upload runs after extract in the TaskGroup.
- **`upload(yaml_path, upstream_task_ids) -> str`**
  - **`ti.xcom_pull(task_ids=upstream_task_ids["extract"])`** → parquet path from extract.
  - Resolves **`EXPORT_CONNECTORS`** (optional YAML `export_engine` if multiple exporters registered).
  - **Container name:** derived from **`dag.dag_id`** with `_` → `-` for Azure container naming rules.
  - Calls **`exporter.upload(local_parquet_path=..., **context)`**.
  - **Return value:** destination **URI** string (e.g. `wasb://...`) for logs / downstream XCom.

## Call site

[[03 - create_dag]] only if **`task/upload.py`** does not exist — **write-once**.

Related: [[10 - Connectors and tasks]] · [[01 - Flow from Excel to Blob]] · [[05 - Scaffold package]] · [[00 - Index]]
