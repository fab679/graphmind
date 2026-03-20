---
sidebar_position: 7
title: Web Visualizer
description: Built-in graph exploration UI
---

# Web Visualizer

Graphmind includes a built-in web-based graph explorer at `http://localhost:8080`.

## Features

### Cypher Editor
- CodeMirror 6 with Cypher syntax highlighting
- Schema-aware autocomplete (labels, types, properties, variables)
- Ctrl+Enter to execute
- NLQ mode for natural language queries

### Graph Canvas
- D3.js force-directed graph visualization on HTML Canvas
- Zoom, pan, and node dragging
- Right-click context menu: expand neighbors, load all relationships
- Layouts: Force (default), Circular, Hierarchical, Grid

### Fullscreen Explorer
- Immersive graph exploration mode
- Floating glassmorphism legend with color/icon customization
- Search bar to highlight matching nodes
- Minimap for navigation
- Property inspector on node/edge click

### Customization
- 55+ built-in SVG icons assignable per node label
- Custom colors for node labels and edge types (persisted)
- Caption property selector per label
- Image URL auto-detection from node properties
- Dark/Light theme with system preference support

### Interaction
- Highlight mode: select a node to highlight connections, dim the rest
- Shortest path: click two nodes to find and visualize the shortest path
- Multi-select: Shift+click to select multiple nodes
- Export: PNG screenshot, CSV data, JSON graph

### Query Management
- Query history with play/delete buttons
- Saved queries with names (persisted to localStorage)
- Script loader: upload .cypher files

### Keyboard Shortcuts
| Key | Action |
|-----|--------|
| `F5` | Run query |
| `Ctrl+Enter` | Run query (in editor) |
| `Escape` | Clear selection |
| `Delete` | Remove selected node from canvas |
| `Ctrl+Shift+H` | Toggle highlight mode |
| `?` | Show shortcuts help |

## Tech Stack
- React 19, Vite 6, TypeScript
- Tailwind CSS v4 with shadcn/ui
- D3.js (d3-force) for graph rendering
- CodeMirror 6 for query editor
- Zustand for state management
