---
sidebar_position: 3
title: Backup & Restore
description: Export and import graph data with snapshots
---

# Backup & Restore

Graphmind provides a portable snapshot format (`.sgsnap`) for exporting and importing graph data. Snapshots are gzip-compressed JSON-lines files containing all nodes and edges with their properties.

## Export a Snapshot

### HTTP API

```bash
curl -X POST http://localhost:8080/api/snapshot/export \
  -H 'Content-Type: application/json' \
  -d '{"graph": "default"}' \
  --output backup.sgsnap
```

The response body is the binary snapshot file. The `graph` field is optional (defaults to `"default"`).

### What is in the file

The `.sgsnap` format:
- Line 0: Header with metadata (version, node count, edge count, labels, edge types)
- Lines 1..N: Node records (ID, labels, properties)
- Lines N+1..M: Edge records (source, target, type, properties)

The file is gzip-compressed, so it is typically much smaller than the in-memory representation.

## Import a Snapshot

### HTTP API

```bash
curl -X POST http://localhost:8080/api/snapshot/import \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @backup.sgsnap
```

Response:

```json
{
  "status": "ok",
  "nodes_imported": 1042,
  "edges_imported": 3891
}
```

On import, node IDs are remapped to avoid collisions with existing data.

## Persistence vs. Snapshots

Graphmind has two data durability mechanisms:

| Feature | RocksDB Persistence | Snapshots (.sgsnap) |
|---------|--------------------|--------------------|
| Automatic | Yes (on every write) | No (manual export) |
| Portable | No (tied to data directory) | Yes (single file) |
| Includes all tenants | Yes | One tenant per export |
| Use case | Crash recovery | Backup, migration, sharing |

### RocksDB Persistence

When Graphmind starts, it automatically recovers data from its data directory (default `./graphmind_data`). This happens transparently -- no manual steps required.

To ensure persistence across Docker container restarts, mount a volume:

```bash
docker run -v graphmind_data:/data fabischk/graphmind:latest
```

### Snapshots

Snapshots are for explicit backup/restore, migrating data between servers, or sharing datasets. They capture a point-in-time copy of a single graph namespace.

## Scheduling Backups

Use cron or any scheduler to automate snapshot exports:

```bash
# Backup every night at 2 AM
0 2 * * * curl -s -X POST http://localhost:8080/api/snapshot/export \
  -H 'Content-Type: application/json' \
  -d '{"graph": "default"}' \
  --output /backups/graphmind-$(date +\%Y\%m\%d).sgsnap
```

Add cleanup to remove old backups:

```bash
# Keep last 30 days of backups
0 3 * * * find /backups -name "graphmind-*.sgsnap" -mtime +30 -delete
```

## Migration Between Servers

Export from source, import to destination:

```bash
# On source server
curl -X POST http://source:8080/api/snapshot/export \
  -d '{"graph": "production"}' --output migration.sgsnap

# On destination server
curl -X POST http://destination:8080/api/snapshot/import \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @migration.sgsnap
```
