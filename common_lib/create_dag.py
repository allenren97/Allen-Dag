"""Generate a ``<business_line>_<cadence>/`` folder for every group in the intake form.

Non-destructive: any DAG folder that already exists is skipped untouched, so
adding a new ``(business_line, cadence)`` row in the spreadsheet is the only
way to scaffold a new DAG. Run as a script:

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
    Returns ``{dag_id: status}`` where status is ``"created"`` or ``"skipped"``.
    """
    rows = read_intake_rows(intake_path)
    groups = group_by_dag(rows)

    results: dict[str, str] = {}
    for dag_id, group_rows in groups.items():
        dag_dir = repo_root / dag_id
        if dag_dir.exists():
            results[dag_id] = "skipped"
            continue

        dag_dir.mkdir(parents=True)
        for row in group_rows:
            write_table_yaml(dag_dir, row)
        write_extract_task(dag_dir)
        write_upload_task(dag_dir)

        cadence = group_rows[0]["cadence"]
        write_dag_file(dag_dir, cadence)
        results[dag_id] = "created"

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
