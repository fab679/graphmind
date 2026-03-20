"""CLI entry point: ``graphmind-mcp-serve``."""

from __future__ import annotations

import argparse
import sys


def _build_social_demo(client, graph: str = "default") -> None:
    """Create a small social-network dataset for demo / testing."""
    q = lambda cypher: client.query(cypher, graph)  # noqa: E731

    # Cities
    q('CREATE (c:City {name: "San Francisco", population: 870000})')
    q('CREATE (c:City {name: "New York", population: 8300000})')
    q('CREATE (c:City {name: "London", population: 8900000})')

    # Companies
    q('CREATE (c:Company {name: "TechCorp", industry: "Technology", employees: 500, founded: 2010})')
    q('CREATE (c:Company {name: "DataInc", industry: "Analytics", employees: 200, founded: 2015})')

    # Company locations
    q('MATCH (c:Company {name: "TechCorp"}), (city:City {name: "San Francisco"}) CREATE (c)-[:LOCATED_IN]->(city)')
    q('MATCH (c:Company {name: "DataInc"}), (city:City {name: "New York"}) CREATE (c)-[:LOCATED_IN]->(city)')

    # Tags
    q('CREATE (t:Tag {name: "python"})')
    q('CREATE (t:Tag {name: "rust"})')
    q('CREATE (t:Tag {name: "graphs"})')

    # People
    q('CREATE (p:Person {name: "Alice", age: 30, email: "alice@example.com"})')
    q('CREATE (p:Person {name: "Bob", age: 25, email: "bob@example.com"})')
    q('CREATE (p:Person {name: "Carol", age: 35, email: "carol@example.com"})')
    q('CREATE (p:Person {name: "Dave", age: 28, email: "dave@example.com"})')
    q('CREATE (p:Person {name: "Eve", age: 32, email: "eve@example.com"})')

    # LIVES_IN
    q('MATCH (p:Person {name: "Alice"}), (c:City {name: "San Francisco"}) CREATE (p)-[:LIVES_IN]->(c)')
    q('MATCH (p:Person {name: "Bob"}), (c:City {name: "New York"}) CREATE (p)-[:LIVES_IN]->(c)')
    q('MATCH (p:Person {name: "Carol"}), (c:City {name: "London"}) CREATE (p)-[:LIVES_IN]->(c)')
    q('MATCH (p:Person {name: "Dave"}), (c:City {name: "San Francisco"}) CREATE (p)-[:LIVES_IN]->(c)')
    q('MATCH (p:Person {name: "Eve"}), (c:City {name: "New York"}) CREATE (p)-[:LIVES_IN]->(c)')

    # WORKS_AT
    q('MATCH (p:Person {name: "Alice"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {since: 2018, role: "Engineer"}]->(c)')
    q('MATCH (p:Person {name: "Bob"}), (c:Company {name: "DataInc"}) CREATE (p)-[:WORKS_AT {since: 2020, role: "Analyst"}]->(c)')
    q('MATCH (p:Person {name: "Carol"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {since: 2019, role: "Manager"}]->(c)')
    q('MATCH (p:Person {name: "Dave"}), (c:Company {name: "DataInc"}) CREATE (p)-[:WORKS_AT {since: 2021, role: "Developer"}]->(c)')
    q('MATCH (p:Person {name: "Eve"}), (c:Company {name: "TechCorp"}) CREATE (p)-[:WORKS_AT {since: 2017, role: "Lead"}]->(c)')

    # KNOWS
    q('MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS]->(b)')
    q('MATCH (a:Person {name: "Alice"}), (b:Person {name: "Carol"}) CREATE (a)-[:KNOWS]->(b)')
    q('MATCH (a:Person {name: "Bob"}), (b:Person {name: "Dave"}) CREATE (a)-[:KNOWS]->(b)')
    q('MATCH (a:Person {name: "Carol"}), (b:Person {name: "Eve"}) CREATE (a)-[:KNOWS]->(b)')
    q('MATCH (a:Person {name: "Dave"}), (b:Person {name: "Eve"}) CREATE (a)-[:KNOWS]->(b)')
    q('MATCH (a:Person {name: "Eve"}), (b:Person {name: "Alice"}) CREATE (a)-[:KNOWS]->(b)')

    # Posts
    q('CREATE (p:Post {title: "Getting Started with Graphs", content: "Graphs are amazing data structures...", views: 150})')
    q('CREATE (p:Post {title: "Rust for Performance", content: "Why we chose Rust for our database...", views: 320})')
    q('CREATE (p:Post {title: "Data Analytics Tips", content: "Top techniques for data analysis...", views: 95})')

    # WROTE (Person -> Post)
    q('MATCH (p:Person {name: "Alice"}), (post:Post {title: "Getting Started with Graphs"}) CREATE (p)-[:WROTE]->(post)')
    q('MATCH (p:Person {name: "Bob"}), (post:Post {title: "Data Analytics Tips"}) CREATE (p)-[:WROTE]->(post)')
    q('MATCH (p:Person {name: "Carol"}), (post:Post {title: "Rust for Performance"}) CREATE (p)-[:WROTE]->(post)')

    # HAS_TAG (Post -> Tag)
    q('MATCH (post:Post {title: "Getting Started with Graphs"}), (t:Tag {name: "graphs"}) CREATE (post)-[:HAS_TAG]->(t)')
    q('MATCH (post:Post {title: "Rust for Performance"}), (t:Tag {name: "rust"}) CREATE (post)-[:HAS_TAG]->(t)')
    q('MATCH (post:Post {title: "Data Analytics Tips"}), (t:Tag {name: "python"}) CREATE (post)-[:HAS_TAG]->(t)')

    # Comments
    q('CREATE (c:Comment {text: "Great introduction!", created_at: "2024-01-15"})')
    q('CREATE (c:Comment {text: "Very insightful, thanks!", created_at: "2024-02-10"})')

    # WROTE (Person -> Comment)
    q('MATCH (p:Person {name: "Bob"}), (c:Comment {text: "Great introduction!"}) CREATE (p)-[:WROTE]->(c)')
    q('MATCH (p:Person {name: "Eve"}), (c:Comment {text: "Very insightful, thanks!"}) CREATE (p)-[:WROTE]->(c)')

    # COMMENTED (Comment -> Post)
    q('MATCH (c:Comment {text: "Great introduction!"}), (post:Post {title: "Getting Started with Graphs"}) CREATE (c)-[:COMMENTED]->(post)')
    q('MATCH (c:Comment {text: "Very insightful, thanks!"}), (post:Post {title: "Rust for Performance"}) CREATE (c)-[:COMMENTED]->(post)')

    # REPLIED_TO (Comment -> Comment)
    q('MATCH (a:Comment {text: "Very insightful, thanks!"}), (b:Comment {text: "Great introduction!"}) CREATE (a)-[:REPLIED_TO]->(b)')

    # LIKES (Person -> Post)
    q('MATCH (p:Person {name: "Bob"}), (post:Post {title: "Getting Started with Graphs"}) CREATE (p)-[:LIKES]->(post)')
    q('MATCH (p:Person {name: "Dave"}), (post:Post {title: "Rust for Performance"}) CREATE (p)-[:LIKES]->(post)')
    q('MATCH (p:Person {name: "Eve"}), (post:Post {title: "Data Analytics Tips"}) CREATE (p)-[:LIKES]->(post)')
    q('MATCH (p:Person {name: "Alice"}), (post:Post {title: "Rust for Performance"}) CREATE (p)-[:LIKES]->(post)')


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="graphmind-mcp-serve",
        description="Auto-generate an MCP server from a Graphmind graph.",
    )
    parser.add_argument(
        "--graph",
        default="default",
        help="Graph name to serve (default: 'default').",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Connect to a running Graphmind server at this URL.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to a YAML tool configuration file.",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print discovered tools and exit.",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Custom MCP server name.",
    )
    parser.add_argument(
        "--demo",
        choices=["social"],
        default=None,
        help="Load a built-in demo dataset before serving.",
    )

    args = parser.parse_args(argv)

    # --- Client ---
    from graphmind import GraphmindClient

    if args.url:
        client = GraphmindClient.connect(args.url)
    else:
        client = GraphmindClient.embedded()

    # --- Demo data ---
    if args.demo == "social":
        _build_social_demo(client, args.graph)

    # --- Config ---
    from graphmind_mcp.config import ToolConfig

    config = (
        ToolConfig.from_yaml(args.config) if args.config else ToolConfig.default()
    )

    # --- Server ---
    from graphmind_mcp.server import GraphmindMCPServer

    server = GraphmindMCPServer(
        client,
        graph=args.graph,
        server_name=args.name,
        config=config,
    )

    if args.list_tools:
        tools = server.list_tools()
        print(f"Discovered {len(tools)} tools:\n")
        for name in sorted(tools):
            print(f"  - {name}")
        sys.exit(0)

    server.run()
