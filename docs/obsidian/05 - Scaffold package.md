---
tags:
  - scaffold
  - code-generation
---

# Package: `common_lib/scaffold/`

**Purpose:** Emit **boilerplate files** for each new DAG folder so every DAG shares the same patterns (TaskGroup wiring, connector registries) without copy-paste.

**Package marker:** [[code/scaffold_init]]

All writers are **imported by** [[03 - create_dag]].

## Modules (four writers)

| Module | Narrative note | Full source in vault |
|--------|----------------|----------------------|
| `write_dag_file` | [[06 - write_dag_file]] | [[code/write_dag_file]] |
| `write_table_yaml` | [[07 - write_table_yaml]] | [[code/write_table_yaml]] |
| `write_extract_task` | [[08 - write_extract_task]] | [[code/write_extract_task]] |
| `write_upload_task` | [[09 - write_upload_task]] | [[code/write_upload_task]] |

`write_table_yaml` is paired with **`build_table_yaml_payload`** in the same module — `create_dag` uses the payload for **equality** against disk before rewriting.

## Design rule

**Templates (`dag.py`, `task/*.py`) are write-once** so teams can customize a specific DAG without the next `create_dag` run clobbering edits. **YAML under `table/`** is the **source of truth** driven by the spreadsheet.

Deep dives:

- [[06 - write_dag_file]]
- [[07 - write_table_yaml]]
- [[08 - write_extract_task]]
- [[09 - write_upload_task]]

Back: [[00 - Index]] · [[02 - DAG folder and files]]
