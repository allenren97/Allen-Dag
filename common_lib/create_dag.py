"""Generate a ``<business_line>_<cadence>/`` folder for every group in the intake form.

Non-destructive and incremental: existing per-table YAMLs and the per-DAG
``dag.py`` / ``task/*.py`` files are never overwritten. Adding a brand new
``(business_line, cadence)`` row scaffolds the whole DAG folder; adding a
new table row to a group whose folder already exists drops in just that
table's YAML next to the existing ones. Run as a script:

    python -m common_lib.create_dag --intake "Business Requirement.xlsx"
"""
from __future__ import annotations

import argparse
from pathlib import Path

from common_lib.intake.read_excel import read_intake_rows
from common_lib.intake.group_by_dag import group_by_dag
from common_lib.scaffold.write_dag_file import write_dag_file
from common_lib.scaffold.write_extract_task import write_extract_task
from common_lib.scaffold.write_table_yaml import write_table_yaml
from common_lib.scaffold.write_upload_task import write_upload_task


def generate(intake_path: Path, repo_root: Path) -> dict[str, str]:
    """
    Returns ``{dag_id: status}`` where status is one of:

    * ``"created"``  — the DAG folder did not exist; everything was scaffolded.
    * ``"added: <t1>, <t2>"`` — the DAG folder already existed and one or
      more new table YAMLs were added next to the existing ones.
    * ``"skipped"``  — the DAG folder already existed and every table row
      already had its YAML on disk; nothing was changed.
    """
    rows = read_intake_rows(intake_path)
    groups = group_by_dag(rows)

    results: dict[str, str] = {}
    for dag_id, group_rows in groups.items():
        dag_dir = repo_root / dag_id
        existed = dag_dir.exists()
        dag_dir.mkdir(parents=True, exist_ok=True)

        added: list[str] = []
        for row in group_rows:
            yaml_path = dag_dir / "table" / f"{row['table']}.yaml"
            if yaml_path.exists():
                continue
            write_table_yaml(dag_dir, row)
            added.append(str(row["table"]))

        if not (dag_dir / "task" / "extract.py").exists():
            write_extract_task(dag_dir)
        if not (dag_dir / "task" / "upload.py").exists():
            write_upload_task(dag_dir)
        if not (dag_dir / "dag.py").exists():
            write_dag_file(dag_dir, group_rows[0]["cadence"])

        if not existed:
            results[dag_id] = "created"
        elif added:
            results[dag_id] = f"added: {', '.join(added)}"
        else:
            results[dag_id] = "skipped"

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--intake",
        type=Path,
        default=Path("Business Requirement.xlsx"),
        help="Path to the intake xlsx (default: %(default)s)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path("."),
        help="Folder where DAG directories are created (default: %(default)s)",
    )
    args = parser.parse_args()

    results = generate(args.intake.resolve(), args.repo_root.resolve())
    if not results:
        print("Intake is empty; no DAGs to generate.")
        return
    width = max(len(k) for k in results)
    for dag_id, status in results.items():
        print(f"  {dag_id.ljust(width)}  {status}")


if __name__ == "__main__":
    main()
