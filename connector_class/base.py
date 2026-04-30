from __future__ import annotations

import logging
from abc import ABC


class BaseConnector(ABC):
    """
    Shared functionality for import/export connectors.

    Validates the common identifiers (connection_id / database / schema / table)
    once and provides a per-instance logger plus a fully-qualified table name
    helper so subclasses don't have to re-implement either.
    """

    def __init__(
        self,
        connection_id: str,
        database: str,
        schema: str,
        table: str,
    ) -> None:
        missing = [
            name
            for name, value in (
                ("connection_id", connection_id),
                ("database", database),
                ("schema", schema),
                ("table", table),
            )
            if not value or not str(value).strip()
        ]
        if missing:
            raise ValueError(
                f"BaseConnector is missing required argument(s): {', '.join(missing)}"
            )

        self.connection_id = connection_id
        self.database = database
        self.schema = schema
        self.table = table
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def fqtn(self) -> str:
        """Fully-qualified table name, e.g. AdventureWorks.Sales.Customers."""
        return f"{self.database}.{self.schema}.{self.table}"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"connection_id={self.connection_id!r}, fqtn={self.fqtn!r})"
        )
