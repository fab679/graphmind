---
sidebar_position: 2
title: Visualizer Features
description: Detailed guide to the Graphmind web visualizer features
---

# Visualizer Features

The Graphmind web visualizer is available at `http://localhost:8080` when the server is running. This page documents all of its features.

## Cypher Editor

The editor uses CodeMirror 6 with:

- **Syntax highlighting** for Cypher keywords, strings, numbers, and comments
- **Autocomplete** for keywords, functions, node labels, edge types, properties, and variables (triggered automatically or with `Ctrl+Space`)
- **Multi-line editing** -- write complex queries across multiple lines
- **Execute** with `Ctrl+Enter` or the Run button

### Query Templates

Click the Templates button to access pre-built queries organized by category:

- **Exploration** -- node counts, schema overview, sample data
- **Pathfinding** -- shortest path, friends-of-friends, variable-length paths
- **Analytics** -- aggregations, degree distribution, top-N queries
- **Social Network** -- community queries for the demo dataset
- **Algorithms** -- PageRank, connected components, triangle count

### Saved Queries

Name and save frequently used queries. Saved queries persist in the browser's localStorage and appear in the sidebar.

### Query History

Every executed query is recorded in the history panel with play/delete buttons. Scroll through past queries with the timeline scrubber.

### Script Loader

Click the upload button to load and execute a `.cypher` file containing multiple statements. Each line is executed as a separate statement. Comments (`//`, `--`) and blank lines are skipped.

## Graph Canvas

### Rendering

The graph is rendered on an HTML Canvas using D3.js force-directed simulation. The canvas supports:

- **Zoom and pan** -- scroll to zoom, click-drag on empty space to pan
- **Node dragging** -- click-drag a node to reposition it
- **Click selection** -- click a node to inspect its properties
- **Multi-select** -- Shift+click to add nodes to the selection

### Layouts

Switch between layouts using the toolbar:

| Layout | Description |
|--------|-------------|
| **Force** (default) | D3 force-directed simulation with physics |
| **Circular** | Nodes arranged in a circle |
| **Hierarchical** | Top-down tree layout |
| **Grid** | Nodes arranged in a regular grid |

### Right-Click Context Menu

Right-click any node to access:

- **Expand neighbors** -- load and display connected nodes
- **Load all relationships** -- fetch all edges for this node
- **Remove from canvas** -- hide the node without deleting it from the database

### Search

Type in the search bar to highlight matching nodes on the canvas. Non-matching nodes are dimmed. The search matches against node labels and property values.

### Highlight Mode

Click a node to enter highlight mode: the selected node and its immediate neighbors are highlighted, while everything else is dimmed. This makes it easy to explore local neighborhoods in large graphs.

Toggle highlight mode with `Ctrl+Shift+H`.

### Shortest Path

Click two nodes to find and visualize the shortest path between them using BFS. The path is highlighted on the canvas.

## Fullscreen Explorer

Click the fullscreen button to enter an immersive exploration mode with:

- **Floating legend** -- glassmorphism panel showing label colors and counts
- **Search bar** -- overlay search with live highlighting
- **Minimap** -- small overview of the full graph for navigation
- **Property inspector** -- click any node or edge to see all properties in a floating panel

## Customization

### Node Colors

Each node label is automatically assigned a color. To change it:

1. Click a node to open the inspector
2. Click the color swatch next to the label name
3. Pick a new color

Custom colors are persisted in localStorage.

### Node Icons

Graphmind includes 55+ built-in SVG icons (people, buildings, cars, pets, documents, etc.). Assign icons per label:

1. Open the Schema Browser (sidebar)
2. Click the icon button next to a label
3. Choose an icon from the picker

If a node has a property containing an image URL, Graphmind auto-detects it and displays the image on the node.

### Caption Property

Choose which property is displayed as the node's label on the canvas:

1. Open the Schema Browser
2. Click the caption selector next to a label
3. Choose a property (e.g., `name`, `title`, `id`)

### Edge Colors

Edge types can also have custom colors, configured the same way through the Schema Browser.

### Theme

Toggle between dark and light themes using the theme button in the navbar. The visualizer respects your system preference by default.

## Results Table

Query results are displayed in a table below the editor using TanStack Table:

- Column sorting (click headers)
- Scrollable for large result sets
- Copy individual cell values

## Export

| Format | What it exports |
|--------|----------------|
| **PNG** | Screenshot of the current canvas |
| **CSV** | Tabular query results |
| **JSON** | Graph data (nodes and edges with properties) |

Access export options from the toolbar.

## Graph Stats

A floating panel shows:

- Node count by label
- Edge count by type
- Degree distribution (min, max, average)

## NLQ Mode

When an LLM provider is configured on the server (`OPENAI_API_KEY`, `GEMINI_API_KEY`, or `CLAUDE_CODE_NLQ`), toggle NLQ mode in the editor to type natural language questions instead of Cypher:

```
Who are Alice's friends?
```

The server translates this to Cypher, executes it, and displays results as usual. The generated Cypher is shown below the input.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `F5` | Run query |
| `Ctrl+Enter` | Run query (in editor) |
| `Escape` | Clear selection |
| `Delete` | Remove selected node from canvas |
| `Ctrl+Shift+H` | Toggle highlight mode |
| `?` | Show shortcuts help dialog |
