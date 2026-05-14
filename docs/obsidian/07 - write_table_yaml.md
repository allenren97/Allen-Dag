---
tags:
  - scaffold
  - yaml
---

# `write_table_yaml.py`

**Full source:** [[code/write_table_yaml]]

**Exports:**

- **`build_table_yaml_payload(row: dict) -> dict`** — projects a full intake row down to fields that **extract/upload** care about.
- **`write_table_yaml(dag_dir: Path, row: dict) -> Path`** — writes `<dag_dir>/table/<table>.yaml`.

## YAML field order (`YAML_FIELDS`)

Written in this order (stable diffs):

1. `engine`
2. `connection_id_import`
3. `connection_id_export`
4. `database`
5. `schema`
6. `table`
7. `predicate` (optional; may be `null`)

Extra spreadsheet columns are **not** written — only churn on connector-relevant cells rewrites the file.

## Serialization

- **`yaml.safe_dump(payload, sort_keys=False)`** to disk.

## Call sites

- **`create_dag.generate`**: compares `build_table_yaml_payload(row)` to **`_read_yaml` on disk**; if different or missing → `write_table_yaml`.

Related: [[03 - create_dag]] · [[02 - DAG folder and files]] · [[05 - Scaffold package]] · [[00 - Index]]
