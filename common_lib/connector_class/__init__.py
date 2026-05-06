"""Auto-register import / export connectors via class attributes.

To add a new **import** source (e.g. Oracle), drop a new file
``common_lib/connector_class/<engine>.py`` that defines a subclass of
``BaseConnector`` with::

    class OracleImportConnector(BaseConnector):
        ENGINE = "oracle"
        ...
        def to_parquet(self, **context) -> str: ...

To add a new **export** target (e.g. S3), drop a new file with::

    class S3ExportConnector(BaseConnector):
        EXPORT = "s3"
        ...
        def upload(self, local_parquet_path: str, **context) -> str: ...

Nothing else needs to change. The two registries below are rebuilt on
import by walking every ``BaseConnector`` subclass that declares a
non-empty ``ENGINE`` (import) or ``EXPORT`` (export). The generated
``task/extract.py`` and ``task/upload.py`` look up the right class by
name, so adding a new connector never requires editing the scaffold or
any existing per-DAG task file.
"""
from __future__ import annotations

import importlib
import pkgutil

from .base import BaseConnector


def _autoload_submodules() -> None:
    """Import every sibling module so subclass registration happens at import time."""
    for _, modname, ispkg in pkgutil.iter_modules(__path__):
        if not ispkg and modname != "base":
            importlib.import_module(f"{__name__}.{modname}")


def _all_subclasses(cls: type) -> set[type]:
    seen: set[type] = set()
    stack: list[type] = [cls]
    while stack:
        c = stack.pop()
        for sub in c.__subclasses__():
            if sub not in seen:
                seen.add(sub)
                stack.append(sub)
    return seen


def _build_registry(attr: str) -> dict[str, type[BaseConnector]]:
    registry: dict[str, type[BaseConnector]] = {}
    for cls in _all_subclasses(BaseConnector):
        key = (getattr(cls, attr, "") or "").strip().lower()
        if not key:
            continue
        if key in registry and registry[key] is not cls:
            raise RuntimeError(
                f"Duplicate {attr}={key!r} declared by "
                f"{registry[key].__name__} and {cls.__name__}; "
                f"{attr.lower()} names must be unique."
            )
        registry[key] = cls
    return registry


_autoload_submodules()

IMPORT_CONNECTORS: dict[str, type[BaseConnector]] = _build_registry("ENGINE")
EXPORT_CONNECTORS: dict[str, type[BaseConnector]] = _build_registry("EXPORT")


from .db2 import DB2ImportConnector  # noqa: E402
from .mssql import MSSQLImportConnector  # noqa: E402
from .wasb import AzureExportConnector  # noqa: E402

__all__ = [
    "BaseConnector",
    "DB2ImportConnector",
    "MSSQLImportConnector",
    "AzureExportConnector",
    "IMPORT_CONNECTORS",
    "EXPORT_CONNECTORS",
]
