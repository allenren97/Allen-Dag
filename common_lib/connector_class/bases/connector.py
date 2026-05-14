"""Root connector base class — shared by every import / export connector."""
from __future__ import annotations

import logging
from abc import ABC


class BaseConnector(ABC):
    """Common identifiers + logger for every connector (import or export).

    Concrete connectors should not subclass ``BaseConnector`` directly:

    * Import connectors → subclass ``BaseImportConnector``.
    * Export connectors → subclass ``BaseExportConnector``.

    The intermediate bases own the engine-agnostic orchestration so each
    concrete class only has to plug in the engine-specific bits.
    """

    def __init__(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str,
    ) -> None:
        # Fail fast together: callers (YAML loaders) tend to omit several fields at
        # once — one exception that lists everything is nicer than four separate blows.
        missing = []
        required_pairs = (
            ("connection_id", connection_id),
            ("database", database),
            ("schema", schema),
            ("table", table),
        )
        for name, value in required_pairs:
            if not value or not str(value).strip():
                missing.append(name)
        if missing:
            raise ValueError(
                f"BaseConnector is missing required argument(s): {', '.join(missing)}"
            )

        self.connection_id = connection_id
        self.database = database
        self.schema = schema
        self.table = table
        # One logger per class name — easy to grep "MSSQLImportConnector:" in airflow logs.
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def full_table_name(self) -> str:
        """Fully-qualified table name, e.g. ``Database.Schema.Table``."""
        return f"{self.database}.{self.schema}.{self.table}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"connection_id={self.connection_id!r}, "
            f"full_table_name={self.full_table_name!r})"
        )
