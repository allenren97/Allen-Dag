# `create_dag.py` — spreadsheet → DAG-folder reconciler

`create_dag.py` is the orchestrator that turns the intake spreadsheet
into the on-disk Airflow layout. **It is the only legal way to add,
change, or remove a DAG in this repo.**

## What it does

**Input:** the intake xlsx (one row per `(business_line, cadence, table)` triple)
and a repo root.

**Output:** an updated set of `<business_line>_<cadence>/` folders,
each containing a generated `dag.py`, a `task/` folder with
`extract.py` / `upload.py`, and a `table/` folder with one
`<table>.yaml` per row. Plus a one-line-per-DAG status report on
stdout.

`generate(intake_path, repo_root, *, dry_run=False)` is the function
under the CLI:

1. **Parse** the spreadsheet (`read_intake_rows`) — required-column
   validation, blank/None normalization, skip empty rows.
2. **Bucket** rows into `(business_line, cadence) → [rows]`
   (`group_by_dag`) — rejects duplicate
   `(business_line, cadence, table)` triples with a clear
   `ValueError`.
3. **Reconcile** each surviving group's on-disk folder:
   - missing folder → scaffold everything (`created`)
   - new row → drop a `<table>.yaml` (`added: …`)
   - existing row whose connector fields drifted → rewrite the YAML
     (`updated: …`)
   - row gone → delete the `<table>.yaml` (`removed: …`)
   - generated `dag.py` and `task/*.py` are **never** overwritten in
     surviving groups.
4. **Cancel** anything stale: any folder under `repo_root` that has a
   top-level `dag.py` but whose group is no longer in the spreadsheet
   is removed wholesale (`shutil.rmtree`) so Airflow stops parsing it
   on its next scan (`cancelled`).

The "has a top-level `dag.py`" check is what keeps `common_lib/`,
`.git/`, `data/`, etc. safe from the cancellation pass.

## CLI

```bash
python -m common_lib.create_dag \
    --intake "Business Requirement.xlsx" \
    --repo-root . \
    [--dry-run]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--intake` | `Business Requirement.xlsx` | Path to the intake xlsx |
| `--repo-root` | `.` | Folder where DAG folders are created |
| `--dry-run` | _off_ | Print the planned changes without modifying the filesystem |

Typical output:

```text
  rrap_weekly  skipped
  sda_daily    updated: TM_DIM; removed: FileInfo
  sda_monthly  cancelled
```

## Status verbs

| Status | Meaning |
| --- | --- |
| `created` | The DAG folder did not exist; everything was scaffolded. |
| `added: <t1>, <t2>` | New table YAMLs were dropped into an existing group. |
| `updated: <t1>, <t2>` | Existing table YAMLs whose connector-relevant fields drifted from the spreadsheet were rewritten to match. |
| `removed: <t1>, <t2>` | Table YAMLs that no longer match a spreadsheet row were deleted from an existing group. |
| `added: <…>; updated: <…>; removed: <…>` | Any combination of the above happened in the same group on this run. |
| `cancelled` | The whole `(business_line, cadence)` group is gone from the spreadsheet; the DAG folder was removed. |
| `skipped` | The DAG folder was already in sync with the spreadsheet; nothing was changed. |

## Reconciliation scenarios

The full set of cases the scaffolder handles, organized by what
changed in the spreadsheet between two runs.

### Per-row scenarios (a single row in the spreadsheet)

| Change in spreadsheet | What happens on disk | Status |
| --- | --- | --- |
| New row in a brand-new `(business_line, cadence)` group | The whole DAG folder is scaffolded: `dag.py`, `task/extract.py`, `task/upload.py`, and `table/<table>.yaml` | `created` |
| New row in an already-existing group | A new `<table>.yaml` is dropped next to the existing ones | `added: <table>` |
| Row's connector fields change (`engine`, `connection_id_import`, `connection_id_export`, `database`, `schema`, `predicate`) | The matching `<table>.yaml` is rewritten to match the spreadsheet | `updated: <table>` |
| Row's `table` value changes (rename) | Treated as delete + add: old `<table>.yaml` is deleted, new one is written | `added: <new>; removed: <old>` |
| Row's `business_line` or `cadence` changes (row migrates between groups) | Old group loses the YAML; new group gains it. If the move makes the old group empty, the old folder is cancelled. | `removed: <table>` in old group + `added: <table>` (or `created`) in new group, plus `cancelled` for the old group if it ends up empty |
| Row deleted from spreadsheet | The matching `<table>.yaml` is deleted | `removed: <table>` |

### Per-group scenarios

| Change in spreadsheet | What happens on disk | Status |
| --- | --- | --- |
| New group appears | New DAG folder scaffolded end-to-end | `created` |
| Group entirely disappears from the spreadsheet | The whole DAG folder (`dag.py`, `table/`, `task/`) is removed | `cancelled` |
| Group still present, no changes | Nothing happens | `skipped` |
| Group has any combination of additions, content edits, and removals | Verbs are joined with `"; "` | e.g. `added: A; updated: B; removed: C` |

### Whole-spreadsheet scenarios

| Situation | Behavior |
| --- | --- |
| Empty intake but DAG folders still exist on disk | Every existing DAG folder is `cancelled` |
| Empty intake and no DAG folders on disk | Prints `Intake is empty; no DAGs to generate.` |
| Two rows share the same `(business_line, cadence, table)` triple | `ValueError: Duplicate intake row for dag_id='…', table='…'` before any filesystem change |
| Required column missing from the spreadsheet header | `ValueError: Intake header is missing required column(s): […]` |

### CLI / safety scenarios

| Action | Behavior |
| --- | --- |
| `python -m common_lib.create_dag --intake … --repo-root …` | Applies all the syncs above |
| `--dry-run` | Same status report, but no `mkdir` / write / `unlink` / `rmtree` happens; useful for previewing destructive `cancelled` runs |
| Sibling folders without a top-level `dag.py` | Never touched — the cancellation pass only considers folders that contain a `dag.py` |
| Hand-edit to any generated YAML | Out of contract — the next run will overwrite it back to whatever the spreadsheet says |
| Hand-edit to a generated `dag.py` or `task/*.py` in a surviving group | Preserved — those files are never overwritten in groups that survive |
| `dag.py` / `task/*.py` in a `cancelled` group | Removed along with the rest of the folder |

## `--dry-run` use cases

`--dry-run` runs the entire reconciliation, prints exactly what
**would** change, and performs zero filesystem mutations. Useful any
time you want to know the blast radius of a sync before committing to
it.

| Situation | Why dry-run helps |
| --- | --- |
| You just edited the spreadsheet and want to confirm only the rows you intended changed | Anything not listed as `added` / `updated` / `removed` / `cancelled` is `skipped`. |
| You're about to delete a row or a whole group | `removed` and especially `cancelled` are destructive (`shutil.rmtree`). Preview first. |
| Code review of a PR that ships a new spreadsheet | Reviewer can run dry-run to see exactly which DAG folders the PR will touch when merged. |
| CI gate enforcing "spreadsheet and on-disk state are always in sync at HEAD" | Fail the build if the report contains anything other than `skipped`. |

A simple CI gate pattern (today the script always exits 0; this wraps
it):

```bash
output=$(python -m common_lib.create_dag --intake "Business Requirement.xlsx" --dry-run)
echo "$output"
if echo "$output" | grep -Ev '^\[dry-run\]|skipped$' | grep -q .; then
  echo "spreadsheet drifted from on-disk DAGs — re-run create_dag and commit" >&2
  exit 1
fi
```

## Source-of-truth contract

The intake spreadsheet is the **only** legal mutation surface for the
contents of each `<table>.yaml`. Hand-edits to those YAMLs are not
supported — the next run will rewrite them. To change a DAG, edit the
spreadsheet and re-run.

The hand-tweakable scaffold pieces (`dag.py`, `task/*.py`) are
preserved across runs in surviving groups — they are only deleted when
the entire group is `cancelled`.
