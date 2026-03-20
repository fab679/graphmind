"""Algorithm tools: pagerank, shortest_path, communities (embedded only)."""

from __future__ import annotations

from graphmind_mcp.generators.base import ToolGenerator, to_dicts


class AlgorithmToolGenerator(ToolGenerator):
    """Generates graph algorithm tools.

    These call the Python SDK's direct algorithm methods which are only
    available in embedded mode.
    """

    def register(self, mcp) -> list[str]:
        # Check if client supports algorithms (embedded mode)
        if not hasattr(self.client, "page_rank"):
            return []

        names: list[str] = []

        fn = self._make_pagerank()
        mcp.tool()(fn)
        names.append(fn.__name__)

        fn = self._make_shortest_path()
        mcp.tool()(fn)
        names.append(fn.__name__)

        fn = self._make_communities()
        mcp.tool()(fn)
        names.append(fn.__name__)

        return names

    # ------------------------------------------------------------------

    def _make_pagerank(self):
        client = self.client
        graph = self.graph

        def pagerank(
            label: str = "",
            edge_type: str = "",
            top_n: int = 20,
        ) -> str:
            """Rank nodes by structural importance using the PageRank algorithm.

            Args:
                label: Optional node label to restrict the analysis.
                edge_type: Optional edge type to restrict the analysis.
                top_n: Number of top-ranked nodes to return (default 20).

            Returns:
                JSON array of nodes ranked by PageRank score.
            """
            try:
                kwargs = {}
                if label:
                    kwargs["label"] = label
                if edge_type:
                    kwargs["edge_type"] = edge_type
                scores = client.page_rank(**kwargs)

                ranked = sorted(
                    scores.items(), key=lambda x: x[1], reverse=True
                )[: int(top_n)]

                results = []
                for node_id, score in ranked:
                    rows = to_dicts(
                        client.query_readonly(
                            f"MATCH (n) WHERE id(n) = {node_id} "
                            f"RETURN id(n) AS _id, labels(n) AS labels",
                            graph,
                        )
                    )
                    entry = {
                        "node_id": node_id,
                        "pagerank_score": round(score, 6),
                    }
                    if rows:
                        entry["labels"] = rows[0].get("labels")
                    results.append(entry)

                return self._json(results)
            except Exception as exc:
                return self._json({"error": str(exc)})

        return pagerank

    def _make_shortest_path(self):
        client = self.client
        graph = self.graph

        def shortest_path(
            source_id: int,
            target_id: int,
            label: str = "",
            edge_type: str = "",
        ) -> str:
            """Find the shortest path between two nodes using BFS.

            Args:
                source_id: ID of the source node.
                target_id: ID of the target node.
                label: Optional node label to constrain traversal.
                edge_type: Optional edge type to constrain traversal.

            Returns:
                JSON object with path and cost, or error if no path exists.
            """
            try:
                kwargs = {
                    "source": int(source_id),
                    "target": int(target_id),
                }
                if label:
                    kwargs["label"] = label
                if edge_type:
                    kwargs["edge_type"] = edge_type
                result = client.bfs(**kwargs)
                if result is None:
                    return self._json({"error": "No path found."})
                return self._json(result)
            except Exception as exc:
                return self._json({"error": str(exc)})

        return shortest_path

    def _make_communities(self):
        client = self.client

        def communities(
            label: str = "",
            edge_type: str = "",
        ) -> str:
            """Detect communities using Weakly Connected Components (WCC).

            Args:
                label: Optional node label to restrict the analysis.
                edge_type: Optional edge type to restrict the analysis.

            Returns:
                JSON object with component count and size distribution.
            """
            try:
                kwargs = {}
                if label:
                    kwargs["label"] = label
                if edge_type:
                    kwargs["edge_type"] = edge_type
                result = client.wcc(**kwargs)
                components = result.get("components", {})
                sizes = {}
                for comp_id, members in components.items():
                    size = len(members) if isinstance(members, list) else members
                    sizes[comp_id] = size

                sorted_sizes = sorted(sizes.values(), reverse=True)
                return self._json({
                    "component_count": result.get("component_count", len(sizes)),
                    "largest_component": sorted_sizes[0] if sorted_sizes else 0,
                    "size_distribution": sorted_sizes[:20],
                })
            except Exception as exc:
                return self._json({"error": str(exc)})

        return communities
