"""Auto-register import / export connectors via class attributes.

To add a new **import** source, drop a new file in this directory that
defines a ``BaseImportConnector`` subclass with a non-empty ``ENGINE``::

    from .base import BaseImportConnector

    class OracleImportConnector(BaseImportConnector):
        ENGINE = "oracle"
        def _build_query(self) -> str: ...
        def _fetch_dataframe(self): ...

To add a new **export** target, drop a new file with a ``BaseConnector``
subclass that declares a non-empty ``EXPORT``::

    class S3ExportConnector(BaseConnector):
        EXPORT = "s3"
        def upload(self, local_parquet_path, **context) -> str: ...

The two registries below are rebuilt on import by walking every
``BaseConnector`` subclass that declares a non-empty ``ENGINE`` (import)
or ``EXPORT`` (export). The generated ``task/extract.py`` and
``task/upload.py`` look up the right class by name, so adding a connector
never requires editing the scaffold or any existing per-DAG task file.

Optional dependencies (e.g. ``ibm_db`` for DB2): if a sibling module
fails to import because its native dependency is missing, the auto-loader
logs a warning and skips it. Only the connectors whose deps are actually
present end up in the registries.
"""
from __future__ import annotations

import importlib
import logging
import pkgutil

from .base import BaseConnector, BaseImportConnector


_logger = logging.getLogger(__name__)


def _autoload_submodules() -> None:
    """Import every sibling module so subclasses register themselves.

    Modules that fail to import (typically because an optional native /
    Airflow-provider dependency isn't installed) are logged and skipped
    so the package still loads with a usable subset of connectors.
    """
    for _, modname, ispkg in pkgutil.iter_modules(__path__):
        if ispkg or modname == "base":
            continue
        try:
            importlib.import_module(f"{__name__}.{modname}")
        except Exception as exc:
            _logger.warning(
                "Skipping connector module %r — %s: %s",
                modname, type(exc).__name__, exc,
            )


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

_logger.info(
    "Connector registries built: imports=%s, exports=%s",
    sorted(IMPORT_CONNECTORS), sorted(EXPORT_CONNECTORS),
)


__all__ = [
    "BaseConnector",
    "BaseImportConnector",
    "IMPORT_CONNECTORS",
    "EXPORT_CONNECTORS",
]
