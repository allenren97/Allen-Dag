---
tags:
  - intake
  - excel
---

# Package: `common_lib/intake/`

Turns the first worksheet of the intake **`.xlsx`** into Python structures `create_dag` can use.

## Modules

### `read_excel.py`

- **`read_intake_rows(intake_path: Path) -> list[dict[str, Any]]`**
  - Uses **`pandas.read_excel`** (`sheet_name=0`, `dtype=object`).
  - Strips column headers; validates **required** columns exist:  
    `business_line`, `cadence`, `engine`, `connection_id_import`, `connection_id_export`, `database`, `schema`, `table`.
  - **`_normalize`** trims strings; blanks / `None` / NaN → Python `None`.
  - Skips completely empty rows and rows missing **`business_line`** or **`cadence`**.

**Full source:** [[code/read_excel]]

### `group_by_dag.py`

- **`dag_id_for(row) -> str`** — `slug(business_line) + "_" + slug(cadence)` (non-alphanumeric → `_`, lowercased).
- **`group_by_dag(rows) -> dict[str, list[dict]]`**
  - Buckets rows by `dag_id`.
  - **Raises** if two rows share the same `(dag_id, table)` — each table file path must be unique.

**Full source:** [[code/group_by_dag]]

## Relationship to `create_dag`

```text
read_intake_rows(path)  →  list[dict]
        ↓
group_by_dag(rows)      →  dict[dag_id, list[dict]]
        ↓
create_dag.generate     →  mkdir, write_table_yaml, write-once scaffolds, orphan cleanup
```

Back: [[00 - Index]] · [[03 - create_dag]]
