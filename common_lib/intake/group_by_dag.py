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
    """Preserve first-seen order of DAG ids while bucketing the rows."""
    groups: "OrderedDict[str, list[dict[str, Any]]]" = OrderedDict()
    for row in rows:
        groups.setdefault(dag_id_for(row), []).append(row)
    return groups
