---
tags:
  - moc
  - index
---

# ETL repo — documentation hub

Start here. This vault documents how **business intake** (spreadsheet) becomes **DAG folders**, how **Airflow** runs them, and how **shared libraries** fit together.

> **Intake file name:** The CLI default in code is `Business Requirement.xlsx`. Your team may use `intake_form.xlsx` or any path — pass `--intake` when you run [[03 - create_dag]].

## Map of content

| Topic | Note |
|--------|------|
| Big picture | [[01 - Flow from Excel to Blob]] |
| What lives under each DAG folder | [[02 - DAG folder and files]] |
| Sync engine (CLI) | [[03 - create_dag]] |
| Reading & grouping Excel rows | [[04 - Intake package]] |
| Template writers (overview) | [[05 - Scaffold package]] |
| Connectors + TaskGroup wiring | [[10 - Connectors and tasks]] |

## Embedded Python snapshots (`code/`)

Full-module copies live under **`code/`** so you can open implementation in Obsidian. Refresh from repo root after editing Python:

```bash
python3 docs/obsidian/sync_code_snippets.py
```

| Repo path | Vault note |
|-----------|------------|
| `common_lib/intake/read_excel.py` | [[code/read_excel]] |
| `common_lib/intake/group_by_dag.py` | [[code/group_by_dag]] |
| `common_lib/create_dag.py` | [[code/create_dag]] |
| `common_lib/tasks/build_table_taskgroup.py` | [[code/build_table_taskgroup]] |
| `common_lib/connector_class/__init__.py` | [[code/connector_class_init]] |
| `common_lib/scaffold/write_dag_file.py` | [[code/write_dag_file]] |
| `common_lib/scaffold/write_table_yaml.py` | [[code/write_table_yaml]] |
| `common_lib/scaffold/write_extract_task.py` | [[code/write_extract_task]] |
| `common_lib/scaffold/write_upload_task.py` | [[code/write_upload_task]] |
| `common_lib/scaffold/__init__.py` | [[code/scaffold_init]] |

Use plain wikilinks like `[[code/write_dag_file]]` (no `|alias`) so links resolve reliably.

## Scaffold modules (detail)

- [[06 - write_dag_file]]
- [[07 - write_table_yaml]]
- [[08 - write_extract_task]]
- [[09 - write_upload_task]]

Each narrative note links to the matching **`code/`** snapshot for the full source.

## Repo layout (packages)

```text
common_lib/
  create_dag.py          # CLI entry: Excel → disk sync
  intake/                # read_excel, group_by_dag
  scaffold/              # write_* templates
  connector_class/       # IMPORT_/EXPORT_ registries + engines
  tasks/
    build_table_taskgroup.py
<dag_id>/                 # one folder per (business_line, cadence), at repo root
  dag.py
  table/
  task/
```

## Quick CLI

```bash
python -m common_lib.create_dag --intake "intake_form.xlsx" --repo-root .
python -m common_lib.create_dag --intake "intake_form.xlsx" --repo-root . --dry-run
```

See [[03 - create_dag]] for flags and return semantics.
