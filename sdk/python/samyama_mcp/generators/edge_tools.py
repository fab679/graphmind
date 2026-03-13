"""Edge tools: find connections and traverse paths per edge type."""

from __future__ import annotations

from samyama_mcp.escape import escape_string, validate_identifier
from samyama_mcp.generators.base import ToolGenerator, to_dicts


class EdgeToolGenerator(ToolGenerator):
    """Generates ``find_{type}_connections`` and ``traverse_{type}``
    for every discovered edge type."""

    def register(self, mcp) -> list[str]:
        names: list[str] = []
        for et in self.schema.edge_types:
            names.extend(self._register_edge_type(mcp, et))
        return names

    def _register_edge_type(self, mcp, et) -> list[str]:
        names: list[str] = []
        etype = et.type
        etype_lower = etype.lower()

        fn = self._make_find_connections(etype, etype_lower, et)
        mcp.tool()(fn)
        names.append(fn.__name__)

        fn = self._make_traverse(etype, etype_lower, et)
        mcp.tool()(fn)
        names.append(fn.__name__)

        return names

    # ------------------------------------------------------------------

    def _make_find_connections(self, etype, etype_lower, et):
        client = self.client
        graph = self.graph
        sources = ", ".join(et.source_labels) or "any"
        targets = ", ".join(et.target_labels) or "any"

        def find_connections(
            node_label: str,
            node_property: str,
            node_value: str,
            direction: str = "both",
            limit: int = 25,
        ) -> str:
            try:
                validate_identifier(node_label)
                validate_identifier(node_property)
            except ValueError as exc:
                return self._json({"error": str(exc)})

            safe_v = escape_string(node_value)
            safe_limit = min(int(limit), 100)
            d = direction.lower()

            if d == "outgoing":
                pattern = (
                    f'MATCH (a:{node_label} {{{node_property}: "{safe_v}"}})'
                    f"-[r:{etype}]->(b)"
                )
            elif d == "incoming":
                pattern = (
                    f"MATCH (b)-[r:{etype}]->"
                    f'(a:{node_label} {{{node_property}: "{safe_v}"}})'
                )
            else:
                pattern = (
                    f'MATCH (a:{node_label} {{{node_property}: "{safe_v}"}})'
                    f"-[r:{etype}]-(b)"
                )

            cypher = (
                f"{pattern} "
                f"RETURN id(a) AS source_id, labels(a) AS source_labels, "
                f"id(b) AS target_id, labels(b) AS target_labels, "
                f"type(r) AS rel_type "
                f"LIMIT {safe_limit}"
            )
            try:
                result = client.query_readonly(cypher, graph)
                return self._json(to_dicts(result))
            except Exception as exc:
                return self._json({"error": str(exc)})

        find_connections.__name__ = f"find_{etype_lower}_connections"
        find_connections.__doc__ = (
            f"Find {etype} connections for a given node.\n"
            f"Source labels: {sources}. Target labels: {targets}.\n\n"
            f"Args:\n"
            f"    node_label: Label of the starting node (e.g. 'Person').\n"
            f"    node_property: Property to match on (e.g. 'name').\n"
            f"    node_value: Value of the property.\n"
            f"    direction: 'outgoing', 'incoming', or 'both' (default).\n"
            f"    limit: Maximum results (default 25, max 100)."
        )
        return find_connections

    def _make_traverse(self, etype, etype_lower, et):
        client = self.client
        graph = self.graph

        def traverse(
            start_label: str,
            start_property: str,
            start_value: str,
            max_hops: int = 3,
            limit: int = 25,
        ) -> str:
            try:
                validate_identifier(start_label)
                validate_identifier(start_property)
            except ValueError as exc:
                return self._json({"error": str(exc)})

            safe_v = escape_string(start_value)
            safe_hops = min(int(max_hops), 5)
            safe_limit = min(int(limit), 100)

            cypher = (
                f'MATCH (s:{start_label} {{{start_property}: "{safe_v}"}})'
                f"-[:{etype}*1..{safe_hops}]->(t) "
                f"RETURN DISTINCT id(t) AS node_id, labels(t) AS labels "
                f"LIMIT {safe_limit}"
            )
            try:
                result = client.query_readonly(cypher, graph)
                return self._json(to_dicts(result))
            except Exception as exc:
                return self._json({"error": str(exc)})

        traverse.__name__ = f"traverse_{etype_lower}"
        traverse.__doc__ = (
            f"Traverse {etype} relationships up to N hops from a start node.\n\n"
            f"Args:\n"
            f"    start_label: Label of the starting node.\n"
            f"    start_property: Property to identify the start node.\n"
            f"    start_value: Value of the start property.\n"
            f"    max_hops: Maximum traversal depth (default 3, max 5).\n"
            f"    limit: Maximum results (default 25, max 100)."
        )
        return traverse
