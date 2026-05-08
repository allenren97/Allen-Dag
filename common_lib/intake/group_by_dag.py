"""Group intake rows into ``(business_line, cadence) -> rows`` buckets."""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any


def _slug(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", str(value).strip().lower()).strip("_")


def dag_id_for(row: dict[str, Any]) -> str:
    """``business_line`` + ``cadence`` -> ``<bl>_<cadence>`` slug."""
    return f"{_slug(row['business_line'])}_{_slug(row['cadence'])}"


def group_by_dag(
    rows: list[dict[str, Any]],
) -> "OrderedDict[str, list[dict[str, Any]]]":
    """
    Preserve first-seen order of DAG ids while bucketing the rows.

    Raises ``ValueError`` if two rows share the same
    ``(business_line, cadence, table)`` triple — every spreadsheet row
    must map to a unique YAML on disk.
    """
    groups: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
    seen: set[tuple[str, str]] = set()
    for row in rows:
        dag_id = dag_id_for(row)
        table = str(row.get("table") or "").strip()
        key = (dag_id, table)
        if key in seen:
            raise ValueError(
                f"Duplicate intake row for dag_id={dag_id!r}, table={table!r}; "
                "every (business_line, cadence, table) triple must be unique."
            )
        seen.add(key)
        groups.setdefault(dag_id, []).append(row)
    return groups
