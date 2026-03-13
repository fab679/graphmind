"""Generic tools: ``cypher_query`` and ``schema_info``."""

from __future__ import annotations

import json

from samyama_mcp.escape import is_readonly_cypher
from samyama_mcp.generators.base import ToolGenerator, to_dicts


class GenericToolGenerator(ToolGenerator):
    """Registers ``cypher_query`` and ``schema_info`` — always active."""

    def register(self, mcp) -> list[str]:
        client = self.client
        graph = self.graph
        schema = self.schema

        @mcp.tool()
        def cypher_query(cypher: str) -> str:
            """Execute a read-only Cypher query against the graph.

            Only SELECT/MATCH queries are allowed — write operations
            (CREATE, DELETE, SET, MERGE, DROP) are rejected.

            Args:
                cypher: A valid Cypher query string.

            Returns:
                JSON array of result rows.
            """
            if not is_readonly_cypher(cypher):
                return json.dumps({"error": "Write operations are not allowed."})
            try:
                result = client.query_readonly(cypher, graph)
                return json.dumps(to_dicts(result), default=str)
            except Exception as exc:
                return json.dumps({"error": str(exc)})

        @mcp.tool()
        def schema_info() -> str:
            """Return the full graph schema including node types, edge types,
            indexes, and statistics.

            Returns:
                JSON object describing the graph schema.
            """
            return json.dumps(schema.to_dict(), default=str)

        return ["cypher_query", "schema_info"]
