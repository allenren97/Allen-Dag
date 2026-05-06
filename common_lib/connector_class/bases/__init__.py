"""Connector base classes.

Concrete connectors should subclass one of:

* :class:`BaseImportConnector` — fetch a DataFrame and write it to parquet.
* :class:`BaseExportConnector` — upload a local parquet file to a remote target.

:class:`BaseConnector` is the root identity layer (connection / database /
schema / table) and is rarely subclassed directly by concrete connectors.
"""
from __future__ import annotations

from .connector import BaseConnector
from .export_connector import BaseExportConnector
from .import_connector import BaseImportConnector

__all__ = [
    "BaseConnector",
    "BaseExportConnector",
    "BaseImportConnector",
]
