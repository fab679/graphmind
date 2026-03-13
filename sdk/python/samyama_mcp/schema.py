"""Graph schema discovery via Cypher introspection."""

from __future__ import annotations

from dataclasses import dataclass, field

from samyama_mcp.escape import validate_identifier


@dataclass
class PropertyInfo:
    """Metadata for a single node/edge property."""

    name: str
    type: str  # "String", "Integer", "Float", "Boolean", "Array", "Map", "Unknown"
    indexed: bool = False
    samples: list = field(default_factory=list)


@dataclass
class NodeType:
    """Discovered node label with count and property metadata."""

    label: str
    count: int
    properties: list[PropertyInfo] = field(default_factory=list)


@dataclass
class EdgeType:
    """Discovered edge type with endpoint labels and count."""

    type: str
    count: int
    source_labels: list[str] = field(default_factory=list)
    target_labels: list[str] = field(default_factory=list)
    properties: list[PropertyInfo] = field(default_factory=list)


@dataclass
class VectorIndex:
    """A vector index on a label/property pair."""

    label: str
    property: str


@dataclass
class GraphSchema:
    """Complete discovered schema of a graph."""

    node_types: list[NodeType] = field(default_factory=list)
    edge_types: list[EdgeType] = field(default_factory=list)
    indexes: list[tuple[str, str]] = field(default_factory=list)
    constraints: list[tuple] = field(default_factory=list)
    vector_indexes: list[VectorIndex] = field(default_factory=list)
    total_nodes: int = 0
    total_edges: int = 0

    def to_dict(self) -> dict:
        """Serialize to a JSON-friendly dictionary."""
        return {
            "total_nodes": self.total_nodes,
            "total_edges": self.total_edges,
            "node_types": [
                {
                    "label": nt.label,
                    "count": nt.count,
                    "properties": [
                        {"name": p.name, "type": p.type, "indexed": p.indexed}
                        for p in nt.properties
                    ],
                }
                for nt in self.node_types
            ],
            "edge_types": [
                {
                    "type": et.type,
                    "count": et.count,
                    "source_labels": et.source_labels,
                    "target_labels": et.target_labels,
                }
                for et in self.edge_types
            ],
            "indexes": [{"label": l, "property": p} for l, p in self.indexes],
            "vector_indexes": [
                {"label": vi.label, "property": vi.property}
                for vi in self.vector_indexes
            ],
        }


class CypherSchemaDiscovery:
    """Discover a :class:`GraphSchema` by querying a live Samyama graph."""

    def __init__(self, client, graph: str = "default"):
        self.client = client
        self.graph = graph

    def discover(self) -> GraphSchema:
        schema = GraphSchema()
        self._discover_node_types(schema)
        self._discover_edge_types(schema)
        self._discover_indexes(schema)
        self._mark_indexed_properties(schema)
        self._detect_vector_indexes(schema)
        self._compute_totals(schema)
        return schema

    # ------------------------------------------------------------------
    # Internal discovery steps
    # ------------------------------------------------------------------

    def _discover_node_types(self, schema: GraphSchema) -> None:
        result = self.client.query_readonly(
            "MATCH (n) RETURN DISTINCT labels(n) AS label, count(n) AS cnt",
            self.graph,
        )
        for row in result.records:
            raw_label = row[0]
            label = raw_label[0] if isinstance(raw_label, list) else raw_label
            label = str(label)
            count = int(row[1])
            try:
                validate_identifier(label)
            except ValueError:
                continue  # skip labels that aren't safe identifiers
            props = self._discover_node_properties(label)
            schema.node_types.append(
                NodeType(label=label, count=count, properties=props)
            )

    def _discover_node_properties(self, label: str) -> list[PropertyInfo]:
        result = self.client.query_readonly(
            f"MATCH (n:{label}) RETURN keys(n) LIMIT 1",
            self.graph,
        )
        if not result.records:
            return []

        keys = result.records[0][0]
        if not isinstance(keys, list):
            return []

        props: list[PropertyInfo] = []
        for key in keys:
            try:
                validate_identifier(key)
            except ValueError:
                continue
            sample_result = self.client.query_readonly(
                f"MATCH (n:{label}) WHERE n.{key} IS NOT NULL "
                f"RETURN n.{key} LIMIT 5",
                self.graph,
            )
            samples = [r[0] for r in sample_result.records]
            prop_type = _infer_type(samples)
            props.append(PropertyInfo(name=key, type=prop_type, samples=samples))
        return props

    def _discover_edge_types(self, schema: GraphSchema) -> None:
        result = self.client.query_readonly(
            "MATCH (s)-[r]->(t) "
            "RETURN type(r) AS type, labels(s) AS source, "
            "labels(t) AS target, count(r) AS cnt",
            self.graph,
        )
        # Aggregate rows by edge type
        edge_map: dict[str, EdgeType] = {}
        for row in result.records:
            etype = str(row[0])
            src = row[1]
            tgt = row[2]
            cnt = int(row[3])

            src_label = src[0] if isinstance(src, list) else str(src)
            tgt_label = tgt[0] if isinstance(tgt, list) else str(tgt)

            try:
                validate_identifier(etype)
            except ValueError:
                continue

            if etype not in edge_map:
                edge_map[etype] = EdgeType(type=etype, count=0)

            et = edge_map[etype]
            et.count += cnt
            if src_label not in et.source_labels:
                et.source_labels.append(src_label)
            if tgt_label not in et.target_labels:
                et.target_labels.append(tgt_label)

        schema.edge_types = list(edge_map.values())

    def _discover_indexes(self, schema: GraphSchema) -> None:
        try:
            result = self.client.query_readonly("SHOW INDEXES", self.graph)
            for row in result.records:
                if len(row) >= 2:
                    schema.indexes.append((str(row[0]), str(row[1])))
        except Exception:
            pass  # SHOW INDEXES may not be available on remote servers

        try:
            result = self.client.query_readonly("SHOW CONSTRAINTS", self.graph)
            for row in result.records:
                schema.constraints.append(tuple(str(c) for c in row))
        except Exception:
            pass

    def _mark_indexed_properties(self, schema: GraphSchema) -> None:
        index_set = set(schema.indexes)
        for nt in schema.node_types:
            for prop in nt.properties:
                if (nt.label, prop.name) in index_set:
                    prop.indexed = True

    def _detect_vector_indexes(self, schema: GraphSchema) -> None:
        for nt in schema.node_types:
            for prop in nt.properties:
                if prop.type == "Array" and prop.samples:
                    sample = prop.samples[0]
                    if (
                        isinstance(sample, list)
                        and len(sample) > 2
                        and all(isinstance(v, (int, float)) for v in sample[:3])
                    ):
                        schema.vector_indexes.append(
                            VectorIndex(label=nt.label, property=prop.name)
                        )

    def _compute_totals(self, schema: GraphSchema) -> None:
        schema.total_nodes = sum(nt.count for nt in schema.node_types)
        schema.total_edges = sum(et.count for et in schema.edge_types)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _infer_type(samples: list) -> str:
    """Infer property type from sample values."""
    if not samples:
        return "Unknown"
    sample = samples[0]
    if isinstance(sample, bool):
        return "Boolean"
    if isinstance(sample, int):
        return "Integer"
    if isinstance(sample, float):
        return "Float"
    if isinstance(sample, str):
        return "String"
    if isinstance(sample, list):
        return "Array"
    if isinstance(sample, dict):
        return "Map"
    return "Unknown"
