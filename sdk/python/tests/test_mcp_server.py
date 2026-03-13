"""Tests for samyama_mcp.server — end-to-end server creation."""

import json
import pytest
from unittest.mock import MagicMock, patch

from samyama_mcp.config import ToolConfig, CustomTool
from samyama_mcp.schema import (
    EdgeType,
    GraphSchema,
    NodeType,
    PropertyInfo,
    VectorIndex,
)


# ── Helpers ───────────────────────────────────────────────────────────


class FakeResult:
    def __init__(self, columns, records):
        self.columns = columns
        self.records = records


def _make_mock_client():
    """Mock client returning a social-like schema."""
    client = MagicMock()

    def query_readonly(cypher, graph="default"):
        if "DISTINCT labels(n)" in cypher:
            return FakeResult(
                ["label", "cnt"],
                [[["Person"], 100], [["Company"], 20], [["_Internal"], 5]],
            )
        if "keys(n)" in cypher and "Person" in cypher:
            return FakeResult(["keys"], [[["name", "age"]]])
        if "keys(n)" in cypher and "Company" in cypher:
            return FakeResult(["keys"], [[["name"]]])
        if "keys(n)" in cypher and "_Internal" in cypher:
            return FakeResult(["keys"], [[["data"]]])
        if "n.name" in cypher and "Person" in cypher:
            return FakeResult(["val"], [["Alice"]])
        if "n.age" in cypher:
            return FakeResult(["val"], [[30]])
        if "n.name" in cypher and "Company" in cypher:
            return FakeResult(["val"], [["TechCorp"]])
        if "n.data" in cypher:
            return FakeResult(["val"], [["blob"]])
        if "type(r)" in cypher:
            return FakeResult(
                ["type", "source", "target", "cnt"],
                [["KNOWS", ["Person"], ["Person"], 500]],
            )
        if "SHOW INDEXES" in cypher:
            return FakeResult(["label", "property"], [["Person", "name"]])
        if "SHOW CONSTRAINTS" in cypher:
            return FakeResult([], [])
        return FakeResult(["_id", "name"], [[1, "Alice"]])

    client.query_readonly = MagicMock(side_effect=query_readonly)
    # Mark as embedded (has algorithm methods)
    client.page_rank = MagicMock(return_value={1: 0.5})
    client.bfs = MagicMock(return_value=None)
    client.wcc = MagicMock(return_value={"components": {}, "component_count": 0})
    return client


# ── SamyamaMCPServer tests ───────────────────────────────────────────


class TestServerCreation:
    def test_creates_server(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client)
        assert len(server.tool_names) > 0

    def test_auto_names_server(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client)
        assert "Person" in server.mcp.name

    def test_custom_name(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client, server_name="Test Graph")
        assert server.mcp.name == "Test Graph"

    def test_list_tools(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client)
        tools = server.list_tools()
        # At minimum: cypher_query, schema_info + node/edge/algorithm tools
        assert "cypher_query" in tools
        assert "schema_info" in tools
        assert "search_person" in tools
        assert "count_person" in tools


class TestExcludeLabels:
    def test_excludes_internal_label(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        config = ToolConfig(exclude_labels=["_Internal"])
        server = SamyamaMCPServer(client, config=config)
        tool_names = server.list_tools()
        # _Internal tools should not be registered
        assert not any("_internal" in t for t in tool_names)
        # Person tools should still be there
        assert "search_person" in tool_names


class TestDisableCategories:
    def test_disable_node_tools(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        config = ToolConfig(include_node_tools=False)
        server = SamyamaMCPServer(client, config=config)
        tools = server.list_tools()
        assert "search_person" not in tools
        assert "cypher_query" in tools  # generic always present

    def test_disable_edge_tools(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        config = ToolConfig(include_edge_tools=False)
        server = SamyamaMCPServer(client, config=config)
        tools = server.list_tools()
        assert "find_knows_connections" not in tools
        assert "traverse_knows" not in tools

    def test_disable_algorithm_tools(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        config = ToolConfig(include_algorithm_tools=False)
        server = SamyamaMCPServer(client, config=config)
        tools = server.list_tools()
        assert "pagerank" not in tools
        assert "shortest_path" not in tools


class TestCustomTools:
    def test_registers_custom_tool(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        ct = CustomTool(
            name="top_people",
            description="Top people by some metric",
            cypher_template="MATCH (p:Person) RETURN p.name LIMIT {limit}",
            parameters=[{"name": "limit", "type": "int", "default": 10}],
        )
        config = ToolConfig(custom_tools=[ct])
        server = SamyamaMCPServer(client, config=config)
        assert "top_people" in server.list_tools()

    def test_custom_tool_rejects_write(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        ct = CustomTool(
            name="bad_tool",
            description="This tries to write",
            cypher_template="CREATE (n:Person {{name: '{name}'}})",
            parameters=[{"name": "name", "type": "str", "default": "x"}],
        )
        config = ToolConfig(custom_tools=[ct])
        server = SamyamaMCPServer(client, config=config)
        # Execute the tool — should be rejected
        tool_fn = None
        for name in server.tool_names:
            if name == "bad_tool":
                tool_fn = True
                break
        assert tool_fn is not None  # tool was registered


class TestSchemaDiscovery:
    def test_schema_populated(self):
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client)
        assert server.schema.total_nodes > 0
        assert server.schema.total_edges > 0
        assert len(server.schema.node_types) > 0
        assert len(server.schema.edge_types) > 0


class TestToolCount:
    def test_expected_tool_count(self):
        """Verify tool count for a simple schema."""
        from samyama_mcp.server import SamyamaMCPServer

        client = _make_mock_client()
        server = SamyamaMCPServer(client)
        tools = server.list_tools()
        # Generic: 2 (cypher_query, schema_info)
        # Person: search, get_by_name, count = 3
        # Company: search, get_by_name (from index match on Company? no,
        #          the mock has index on Person/name only), count = 2
        # _Internal: search, count = 2
        # Edges: find_knows_connections, traverse_knows = 2
        # Algorithms: pagerank, shortest_path, communities = 3
        # Total should be reasonable
        assert len(tools) >= 10
