"""Read the business intake spreadsheet into a list of normalized row dicts."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = (
    "business_line",
    "cadence",
    "engine",
    "connection_id_import",
    "connection_id_export",
    "database",
    "schema",
    "table",
)


def _normalize(value: Any) -> Any:
    """Trim strings and coerce blank / 'None' literals / NaN / NaT / NA to ``None``."""
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        v = value.strip()
        if v == "" or v.lower() == "none":
            return None
        return v
    return value


def read_intake_rows(intake_path: Path) -> list[dict[str, Any]]:
    """
    Return the data rows of the first worksheet as a list of dicts keyed by
    header. Empty rows and rows missing ``business_line`` / ``cadence`` are
    skipped. Raises ``ValueError`` if a required column is missing.
    """
    df = pd.read_excel(intake_path, sheet_name=0, dtype=object)
    df.columns = [str(c).strip() if c is not None else "" for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Intake header is missing required column(s): {missing}"
        )

    rows: list[dict[str, Any]] = []
    for raw in df.to_dict(orient="records"):
        row = {key: _normalize(value) for key, value in raw.items()}
        if all(v is None for v in row.values()):
            continue
        if not row.get("business_line") or not row.get("cadence"):
            continue
        rows.append(row)
    return rows
