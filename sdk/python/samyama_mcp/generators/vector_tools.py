"""Vector tools: similarity search for labels with vector indexes."""

from __future__ import annotations

from samyama_mcp.generators.base import ToolGenerator, to_dicts


class VectorToolGenerator(ToolGenerator):
    """Generates ``find_similar_{label}`` for labels with vector properties."""

    def register(self, mcp) -> list[str]:
        if not self.schema.vector_indexes:
            return []
        if not hasattr(self.client, "vector_search"):
            return []

        names: list[str] = []
        for vi in self.schema.vector_indexes:
            fn = self._make_find_similar(vi.label, vi.property)
            mcp.tool()(fn)
            names.append(fn.__name__)
        return names

    def _make_find_similar(self, label: str, prop: str):
        client = self.client
        graph = self.graph

        def find_similar(query_vector: list, k: int = 10) -> str:
            """Find nodes with similar vector embeddings using k-NN search.

            Args:
                query_vector: The query vector (list of floats).
                k: Number of nearest neighbours to return (default 10).

            Returns:
                JSON array of similar nodes with similarity scores.
            """
            try:
                safe_k = min(int(k), 100)
                neighbours = client.vector_search(
                    label, prop, query_vector, safe_k
                )

                results = []
                for node_id, distance in neighbours:
                    rows = to_dicts(
                        client.query_readonly(
                            f"MATCH (n:{label}) WHERE id(n) = {node_id} "
                            f"RETURN id(n) AS _id, labels(n) AS labels",
                            graph,
                        )
                    )
                    entry = {
                        "node_id": node_id,
                        "similarity": round(1.0 - distance, 4),
                    }
                    if rows:
                        entry.update(rows[0])
                    results.append(entry)
                return self._json(results)
            except Exception as exc:
                return self._json({"error": str(exc)})

        find_similar.__name__ = f"find_similar_{label.lower()}"
        find_similar.__doc__ = (
            f"Find {label} nodes similar to a query vector "
            f"(property: {prop}).\n\n"
            f"Args:\n"
            f"    query_vector: List of floats representing the query.\n"
            f"    k: Number of nearest neighbours (default 10, max 100)."
        )
        return find_similar
