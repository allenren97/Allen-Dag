---
tags:
  - create_dag
  - cli
---

# `common_lib/create_dag.py`

**Role:** Treat the repo (or `--repo-root`) as a **mirror** of the intake spreadsheet. When rows change, **create**, **update**, or **delete** DAG folders and `table/*.yaml` files accordingly.

**Full source:** [[code/create_dag]]

## Imports (dependency map)

| Imported from | Used for |
|---------------|----------|
| `common_lib.intake.read_excel` | `read_intake_rows` — [[code/read_excel]] |
| `common_lib.intake.group_by_dag` | `group_by_dag` — [[code/group_by_dag]] |
| `common_lib.scaffold.write_dag_file` | `write_dag_file` — [[code/write_dag_file]] |
| `common_lib.scaffold.write_extract_task` | `write_extract_task` — [[code/write_extract_task]] |
| `common_lib.scaffold.write_table_yaml` | `build_table_yaml_payload`, `write_table_yaml` — [[code/write_table_yaml]] |
| `common_lib.scaffold.write_upload_task` | `write_upload_task` — [[code/write_upload_task]] |
| `yaml` | Compare / read existing YAML on disk |

## Entry points

- **`generate(intake_path, repo_root, *, dry_run=False) -> dict[str, str]`** — per-`dag_id` status (`created`, `skipped`, `added: …`, `cancelled`, …). When `dry_run=True`, **no** filesystem writes or deletes.
- **`main()`** — argparse CLI.

## CLI

```bash
python -m common_lib.create_dag --intake "intake_form.xlsx" --repo-root .

# Preview only — same status lines, no mkdir / write / unlink / rmtree
python -m common_lib.create_dag --intake "intake_form.xlsx" --repo-root . --dry-run
```

### Flags (all optional)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--intake` | `Business Requirement.xlsx` | Path to the intake `.xlsx` |
| `--repo-root` | `.` | Folder under which `<dag_id>/` directories are created or removed |
| `--dry-run` | off | Compute statuses and print them; **no** file or directory changes. Prints `[dry-run] no changes were applied` after the table. |

Use **`--dry-run`** before a risky sync (e.g. many `cancelled` folders) to confirm which DAG ids would change.

### Output

One status line per `dag_id`. See repo **README** § “Generating DAG folders from the spreadsheet” for the full status table.

---

Back: [[00 - Index]] · [[01 - Flow from Excel to Blob]]
