"""GraphmindMCPServer — orchestrates schema discovery and tool registration."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from fastmcp import FastMCP

from graphmind_mcp.config import ToolConfig
from graphmind_mcp.escape import escape_string, is_readonly_cypher
from graphmind_mcp.schema import CypherSchemaDiscovery

if TYPE_CHECKING:
    from graphmind_mcp.schema import GraphSchema


class GraphmindMCPServer:
    """Auto-generates and serves MCP tools from a live Graphmind graph.

    Usage::

        from graphmind import GraphmindClient
        from graphmind_mcp import GraphmindMCPServer

        client = GraphmindClient.embedded()
        server = GraphmindMCPServer(client)
        server.run()
    """

    def __init__(
        self,
        client,
        graph: str = "default",
        server_name: str | None = None,
        config: ToolConfig | None = None,
    ):
        self.client = client
        self.graph = graph
        self.config = config or ToolConfig.default()

        # Discover schema
        self.schema: GraphSchema = CypherSchemaDiscovery(client, graph).discover()

        # Build descriptive server name
        if server_name is None:
            labels = ", ".join(nt.label for nt in self.schema.node_types[:5])
            if len(self.schema.node_types) > 5:
                labels += ", ..."
            server_name = f"Graphmind Graph ({labels})" if labels else "Graphmind Graph"

        self.mcp = FastMCP(server_name)
        self.tool_names: list[str] = []

        # Register tools
        self._register_tools()

    # ------------------------------------------------------------------
    # Tool registration
    # ------------------------------------------------------------------

    def _register_tools(self) -> None:
        from graphmind_mcp.generators import (
            AlgorithmToolGenerator,
            EdgeToolGenerator,
            GenericToolGenerator,
            NodeToolGenerator,
            VectorToolGenerator,
        )

        cfg = self.config
        exclude = set(cfg.exclude_labels)

        # Filter schema by excluded labels
        schema = self.schema
        if exclude:
            from graphmind_mcp.schema import GraphSchema

            schema = GraphSchema(
                node_types=[
                    nt for nt in self.schema.node_types if nt.label not in exclude
                ],
                edge_types=self.schema.edge_types,
                indexes=[
                    idx for idx in self.schema.indexes if idx[0] not in exclude
                ],
                constraints=self.schema.constraints,
                vector_indexes=[
                    vi
                    for vi in self.schema.vector_indexes
                    if vi.label not in exclude
                ],
                total_nodes=self.schema.total_nodes,
                total_edges=self.schema.total_edges,
            )

        # Always register generic tools
        gen = GenericToolGenerator(self.client, self.graph, schema)
        self.tool_names.extend(gen.register(self.mcp))

        if cfg.include_node_tools:
            gen = NodeToolGenerator(self.client, self.graph, schema)
            self.tool_names.extend(gen.register(self.mcp))

        if cfg.include_edge_tools:
            gen = EdgeToolGenerator(self.client, self.graph, schema)
            self.tool_names.extend(gen.register(self.mcp))

        if cfg.include_algorithm_tools:
            gen = AlgorithmToolGenerator(self.client, self.graph, schema)
            self.tool_names.extend(gen.register(self.mcp))

        if cfg.include_vector_tools:
            gen = VectorToolGenerator(self.client, self.graph, schema)
            self.tool_names.extend(gen.register(self.mcp))

        # Custom tools from config
        for ct in cfg.custom_tools:
            self._register_custom_tool(ct)

    def _register_custom_tool(self, ct) -> None:
        client = self.client
        graph = self.graph
        template = ct.cypher_template
        params = {p["name"]: p.get("default") for p in ct.parameters}
        param_types = {p["name"]: p.get("type", "str") for p in ct.parameters}

        def custom_tool(**kwargs) -> str:
            merged = {**params, **kwargs}
            for key, val in merged.items():
                if isinstance(val, str):
                    merged[key] = escape_string(val)
            cypher = template.format(**merged)
            if not is_readonly_cypher(cypher):
                return json.dumps({"error": "Write operations are not allowed."})
            try:
                from graphmind_mcp.generators.base import to_dicts

                result = client.query_readonly(cypher, graph)
                return json.dumps(to_dicts(result), default=str)
            except Exception as exc:
                return json.dumps({"error": str(exc)})

        custom_tool.__name__ = ct.name
        custom_tool.__doc__ = ct.description

        # Build annotations for FastMCP parameter discovery
        annotations = {}
        for p in ct.parameters:
            ptype = p.get("type", "str")
            if ptype == "int":
                annotations[p["name"]] = int
            elif ptype == "float":
                annotations[p["name"]] = float
            else:
                annotations[p["name"]] = str
        annotations["return"] = str
        custom_tool.__annotations__ = annotations

        # Set defaults
        import inspect

        sig_params = []
        for p in ct.parameters:
            default = p.get("default", inspect.Parameter.empty)
            ptype = p.get("type", "str")
            if ptype == "int" and default is not inspect.Parameter.empty:
                default = int(default)
            elif ptype == "float" and default is not inspect.Parameter.empty:
                default = float(default)
            sig_params.append(
                inspect.Parameter(
                    p["name"],
                    inspect.Parameter.KEYWORD_ONLY,
                    default=default,
                )
            )
        custom_tool.__signature__ = inspect.Signature(sig_params)

        self.mcp.tool()(custom_tool)
        self.tool_names.append(ct.name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_tools(self) -> list[str]:
        """Return all registered tool names."""
        return list(self.tool_names)

    def run(self) -> None:
        """Start the MCP server (stdio transport)."""
        self.mcp.run()
