"""Node tools: search, get-by-index, and count per label."""

from __future__ import annotations

from graphmind_mcp.escape import escape_string, validate_identifier
from graphmind_mcp.generators.base import ToolGenerator, to_dicts


class NodeToolGenerator(ToolGenerator):
    """Generates ``search_{label}``, ``get_{label}_by_{prop}``,
    ``count_{label}`` for every discovered node label."""

    def register(self, mcp) -> list[str]:
        names: list[str] = []
        for nt in self.schema.node_types:
            names.extend(self._register_label(mcp, nt))
        return names

    def _register_label(self, mcp, nt) -> list[str]:
        names: list[str] = []
        label = nt.label
        label_lower = label.lower()
        string_props = [p.name for p in nt.properties if p.type == "String"]
        indexed_props = [p for p in nt.properties if p.indexed]
        return_clause = self._return_clause(nt)

        # --- search_{label} -----------------------------------------------
        if string_props:
            fn = self._make_search(label, label_lower, string_props, return_clause)
            mcp.tool()(fn)
            names.append(fn.__name__)

        # --- get_{label}_by_{prop} ----------------------------------------
        for prop in indexed_props:
            fn = self._make_get_by(label, label_lower, prop, return_clause)
            mcp.tool()(fn)
            names.append(fn.__name__)

        # --- count_{label} ------------------------------------------------
        fn = self._make_count(label, label_lower)
        mcp.tool()(fn)
        names.append(fn.__name__)

        return names

    # ------------------------------------------------------------------
    # Factory methods (create closures with captured variables)
    # ------------------------------------------------------------------

    def _make_search(self, label, label_lower, string_props, return_clause):
        client = self.client
        graph = self.graph

        def search(query: str, limit: int = 25) -> str:
            safe_q = escape_string(query)
            where = " OR ".join(
                f'n.{p} CONTAINS "{safe_q}"' for p in string_props
            )
            cypher = (
                f"MATCH (n:{label}) WHERE {where} "
                f"RETURN {return_clause} LIMIT {min(int(limit), 100)}"
            )
            try:
                result = client.query_readonly(cypher, graph)
                return self._json(to_dicts(result))
            except Exception as exc:
                return self._json({"error": str(exc)})

        search.__name__ = f"search_{label_lower}"
        search.__doc__ = (
            f"Search {label} nodes by text content across: "
            f"{', '.join(string_props)}.\n\n"
            f"Args:\n"
            f"    query: Text to search for (case-sensitive CONTAINS).\n"
            f"    limit: Maximum results to return (default 25, max 100)."
        )
        return search

    def _make_get_by(self, label, label_lower, prop, return_clause):
        client = self.client
        graph = self.graph
        prop_name = prop.name

        def get_by(value: str) -> str:
            safe_v = escape_string(str(value))
            cypher = (
                f'MATCH (n:{label} {{{prop_name}: "{safe_v}"}}) '
                f"RETURN {return_clause}"
            )
            try:
                result = client.query_readonly(cypher, graph)
                rows = to_dicts(result)
                if not rows:
                    return self._json(
                        {"error": f"{label} with {prop_name}={value!r} not found"}
                    )
                return self._json(rows[0] if len(rows) == 1 else rows)
            except Exception as exc:
                return self._json({"error": str(exc)})

        get_by.__name__ = f"get_{label_lower}_by_{prop_name}"
        get_by.__doc__ = (
            f"Look up a {label} node by its indexed ``{prop_name}`` property.\n\n"
            f"Args:\n"
            f"    value: The exact {prop_name} value to match."
        )
        return get_by

    def _make_count(self, label, label_lower):
        client = self.client
        graph = self.graph

        def count(filter_property: str = "", filter_value: str = "") -> str:
            if filter_property and filter_value:
                try:
                    validate_identifier(filter_property)
                except ValueError:
                    return self._json(
                        {"error": f"Invalid property name: {filter_property!r}"}
                    )
                safe_v = escape_string(filter_value)
                cypher = (
                    f"MATCH (n:{label}) "
                    f'WHERE n.{filter_property} = "{safe_v}" '
                    f"RETURN count(n) AS count"
                )
            else:
                cypher = f"MATCH (n:{label}) RETURN count(n) AS count"

            try:
                result = client.query_readonly(cypher, graph)
                rows = to_dicts(result)
                return self._json(rows[0] if rows else {"count": 0})
            except Exception as exc:
                return self._json({"error": str(exc)})

        count.__name__ = f"count_{label_lower}"
        count.__doc__ = (
            f"Count {label} nodes, optionally filtered by a property value.\n\n"
            f"Args:\n"
            f"    filter_property: Optional property name to filter on.\n"
            f"    filter_value: Value the property must equal."
        )
        return count

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _return_clause(nt) -> str:
        parts = ["id(n) AS _id"]
        for p in nt.properties:
            parts.append(f"n.{p.name} AS {p.name}")
        return ", ".join(parts)
