"""Base class and shared helpers for tool generators."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from samyama_mcp.schema import GraphSchema


def to_dicts(result) -> list[dict]:
    """Convert a ``QueryResult`` (columns + records) to a list of dicts."""
    return [dict(zip(result.columns, row)) for row in result.records]


class ToolGenerator(ABC):
    """Abstract base for tool generators.

    Subclasses implement :meth:`register` which decorates closures with
    ``mcp.tool()`` and returns a list of registered tool names.
    """

    def __init__(self, client, graph: str, schema: GraphSchema):
        self.client = client
        self.graph = graph
        self.schema = schema

    @abstractmethod
    def register(self, mcp) -> list[str]:
        """Register tools on *mcp* and return their names."""

    @staticmethod
    def _json(data) -> str:
        return json.dumps(data, default=str)
