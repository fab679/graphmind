---
sidebar_position: 1
title: Web Visualizer
description: Built-in graph exploration UI
---

# Web Visualizer

Graphmind includes a built-in web-based graph explorer at `http://localhost:8080`.

## Gallery

### Graph Explorer
Force-directed graph with colored nodes, edge labels, and floating legend.

![Graph Explorer](/img/screenshots/graph-explorer.png)

### Shortest Path Visualization
Click two nodes to find and highlight the shortest path between them.

![Shortest Path](/img/screenshots/shortest-path.png)

### Node Inspector
Click any node to see its properties, labels, and connections.

![Node Inspector](/img/screenshots/node-inspector.png)

### Graph Layouts
Switch between force-directed, circular, hierarchical, and grid layouts.

| Circular | Hierarchical |
|:-:|:-:|
| ![Circular](/img/screenshots/circular-layout.png) | ![Hierarchical](/img/screenshots/hierarchical-layout.png) |

### Query Editor
Full Cypher editor with syntax highlighting, autocomplete, and execution stats.

![Query Editor](/img/screenshots/query-editor.png)

### Schema Browser
Browse node labels, edge types, properties, and customize colors and icons.

![Schema Browser](/img/screenshots/schema-browser.png)

### Database Administration
Manage graphs (multi-tenancy), view server stats, import/export data.

![Admin Panel](/img/screenshots/admin-panel.png)

### Settings
Customize node colors, icons, caption properties, and edge colors.

![Settings](/img/screenshots/settings.png)

## Opening the Visualizer

Start the Graphmind server, then open [http://localhost:8080](http://localhost:8080) in your browser.

```bash
# Start the server
graphmind

# Or with Docker
docker run -d -p 6379:6379 -p 8080:8080 fabischk/graphmind:latest
```

The visualizer works in all modern browsers (Chrome, Firefox, Safari, Edge).

## What You Get

The interface has three main areas:

1. **Cypher Editor** (top) -- write and execute Cypher queries with syntax highlighting and autocomplete
2. **Graph Canvas** (center) -- interactive D3.js force-directed graph visualization
3. **Results Table** (bottom) -- tabular query results with sortable columns

## Quick Tour

1. **Run a query**: Type a Cypher query in the editor and press `Ctrl+Enter`
2. **Explore the graph**: Click nodes to inspect properties, drag to rearrange
3. **Expand neighbors**: Right-click a node and select "Expand neighbors"
4. **Search**: Type in the search bar to highlight matching nodes
5. **Change layout**: Use the layout buttons (Force, Circular, Hierarchical, Grid)
6. **Fullscreen mode**: Click the fullscreen button for immersive exploration

## Loading Data

If the graph is empty, load the demo dataset:

1. Click the upload button in the editor toolbar
2. Select `scripts/social_network_demo.cypher` from the repository
3. The script creates 52 nodes and 142 edges with 8 node labels and 10 relationship types

Or paste this into the editor:

```cypher
CREATE (a:Person {name: "Alice", age: 30})
CREATE (b:Person {name: "Bob", age: 25})
CREATE (c:Person {name: "Carol", age: 28})
CREATE (a)-[:KNOWS]->(b)
CREATE (b)-[:KNOWS]->(c)
CREATE (a)-[:KNOWS]->(c)
```

Then query:

```cypher
MATCH (p:Person)-[:KNOWS]->(f)
RETURN p, f
```

The nodes and relationships appear on the canvas. Click any node to see its properties in the inspector panel.

## Next Steps

See [Visualizer Features](features) for the complete feature reference including:
- Keyboard shortcuts
- Color and icon customization
- NLQ (natural language) mode
- Export options
- Fullscreen explorer
