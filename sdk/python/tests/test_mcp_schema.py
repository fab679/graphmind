"""Tests for samyama_mcp.schema — schema discovery."""

import pytest
from unittest.mock import MagicMock

from samyama_mcp.schema import (
    CypherSchemaDiscovery,
    GraphSchema,
    NodeType,
    PropertyInfo,
    _infer_type,
)


# ── Helpers ───────────────────────────────────────────────────────────


class FakeResult:
    """Lightweight stand-in for QueryResult."""

    def __init__(self, columns, records):
        self.columns = columns
        self.records = records


def _make_mock_client():
    """Build a mock client whose query_readonly returns canned results."""
    client = MagicMock()

    def query_readonly(cypher, graph="default"):
        # Node labels
        if "DISTINCT labels(n)" in cypher:
            return FakeResult(
                ["label", "cnt"],
                [[["Person"], 100], [["Company"], 20]],
            )
        # Property keys
        if "keys(n)" in cypher and "Person" in cypher:
            return FakeResult(["keys"], [[["name", "age", "email"]]])
        if "keys(n)" in cypher and "Company" in cypher:
            return FakeResult(["keys"], [[["name", "industry"]]])
        # Property samples
        if "n.name" in cypher and "Person" in cypher:
            return FakeResult(["val"], [["Alice"], ["Bob"], ["Carol"]])
        if "n.age" in cypher:
            return FakeResult(["val"], [[30], [25], [35]])
        if "n.email" in cypher:
            return FakeResult(["val"], [["a@example.com"], ["b@example.com"]])
        if "n.name" in cypher and "Company" in cypher:
            return FakeResult(["val"], [["TechCorp"], ["DataInc"]])
        if "n.industry" in cypher:
            return FakeResult(["val"], [["Technology"], ["Data"]])
        # Edge types
        if "DISTINCT type(r)" in cypher or "type(r)" in cypher:
            return FakeResult(
                ["type", "source", "target", "cnt"],
                [
                    ["KNOWS", ["Person"], ["Person"], 500],
                    ["WORKS_AT", ["Person"], ["Company"], 100],
                ],
            )
        # Indexes
        if "SHOW INDEXES" in cypher:
            return FakeResult(
                ["label", "property"],
                [["Person", "name"], ["Person", "email"]],
            )
        # Constraints
        if "SHOW CONSTRAINTS" in cypher:
            return FakeResult(["label", "property", "type"], [])
        return FakeResult([], [])

    client.query_readonly = MagicMock(side_effect=query_readonly)
    return client


@pytest.fixture
def mock_client():
    return _make_mock_client()


# ── CypherSchemaDiscovery ────────────────────────────────────────────


class TestDiscoverNodeTypes:
    def test_discovers_two_labels(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.node_types) == 2

    def test_person_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        assert person.count == 100

    def test_person_properties(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        prop_names = [p.name for p in person.properties]
        assert "name" in prop_names
        assert "age" in prop_names
        assert "email" in prop_names

    def test_company_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        company = next(nt for nt in schema.node_types if nt.label == "Company")
        assert company.count == 20


class TestDiscoverEdgeTypes:
    def test_discovers_two_edge_types(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.edge_types) == 2

    def test_knows_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        knows = next(et for et in schema.edge_types if et.type == "KNOWS")
        assert knows.count == 500

    def test_knows_endpoints(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        knows = next(et for et in schema.edge_types if et.type == "KNOWS")
        assert "Person" in knows.source_labels
        assert "Person" in knows.target_labels

    def test_works_at_endpoints(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        wa = next(et for et in schema.edge_types if et.type == "WORKS_AT")
        assert "Person" in wa.source_labels
        assert "Company" in wa.target_labels


class TestDiscoverIndexes:
    def test_index_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.indexes) == 2

    def test_index_entries(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert ("Person", "name") in schema.indexes
        assert ("Person", "email") in schema.indexes


class TestIndexedPropertyMarking:
    def test_name_is_indexed(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        name_prop = next(p for p in person.properties if p.name == "name")
        assert name_prop.indexed is True

    def test_age_not_indexed(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        age_prop = next(p for p in person.properties if p.name == "age")
        assert age_prop.indexed is False


class TestTotals:
    def test_total_nodes(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert schema.total_nodes == 120

    def test_total_edges(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert schema.total_edges == 600


# ── Type inference ───────────────────────────────────────────────────


class TestInferType:
    def test_string(self):
        assert _infer_type(["hello"]) == "String"

    def test_integer(self):
        assert _infer_type([42]) == "Integer"

    def test_float(self):
        assert _infer_type([3.14]) == "Float"

    def test_boolean(self):
        assert _infer_type([True]) == "Boolean"

    def test_list(self):
        assert _infer_type([[1, 2, 3]]) == "Array"

    def test_dict(self):
        assert _infer_type([{"a": 1}]) == "Map"

    def test_empty(self):
        assert _infer_type([]) == "Unknown"


class TestSchemaToDict:
    def test_roundtrip(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        d = schema.to_dict()
        assert d["total_nodes"] == 120
        assert d["total_edges"] == 600
        assert len(d["node_types"]) == 2
        assert len(d["edge_types"]) == 2
        assert len(d["indexes"]) == 2
