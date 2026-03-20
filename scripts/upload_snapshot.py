#!/usr/bin/env python3
"""Upload a .sgsnap snapshot to Supabase Storage and register in kg_registry.

Usage:
    python scripts/upload_snapshot.py \
        --file clinical-trials.sgsnap \
        --tenant clinical-trials \
        --name "Clinical Trials (AACT Full)"

Environment variables:
    SUPABASE_URL          Supabase project URL
    SUPABASE_SERVICE_KEY  Service-role key (for storage + DB writes)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: supabase-py not installed.  pip install supabase", file=sys.stderr)
    sys.exit(1)

BUCKET = "kg-snapshots"
SCHEMA = "graphmind_insight"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_header(path: Path) -> dict:
    """Read and parse the sgsnap header (first line of gzip stream)."""
    import gzip

    with gzip.open(path, "rt", encoding="utf-8") as f:
        first_line = f.readline()
    return json.loads(first_line)


def main():
    parser = argparse.ArgumentParser(description="Upload .sgsnap to Supabase Storage")
    parser.add_argument("--file", required=True, help="Path to .sgsnap file")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--name", required=True, help="Human-readable name")
    parser.add_argument("--description", default="", help="Optional description")
    parser.add_argument("--version", type=int, default=1, help="Version number (default: 1)")
    args = parser.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY", file=sys.stderr)
        sys.exit(1)

    snap_path = Path(args.file)
    if not snap_path.exists():
        print(f"ERROR: File not found: {snap_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading snapshot header from {snap_path}...")
    header = read_header(snap_path)
    size_bytes = snap_path.stat().st_size

    print(f"Computing SHA-256 checksum...")
    checksum = sha256_file(snap_path)

    # Storage path: tenant-id/v{version}.sgsnap
    storage_path = f"{args.tenant}/v{args.version}.sgsnap"

    print(f"Connecting to Supabase...")
    supabase: Client = create_client(url, key)

    # Upload to storage
    print(f"Uploading {size_bytes:,} bytes to {BUCKET}/{storage_path}...")
    with open(snap_path, "rb") as f:
        supabase.storage.from_(BUCKET).upload(
            storage_path,
            f.read(),
            {"content-type": "application/octet-stream"},
        )
    print("  Upload complete.")

    # Insert registry row
    print("Inserting kg_registry entry...")
    row = {
        "tenant_id": args.tenant,
        "name": args.name,
        "description": args.description or None,
        "version": args.version,
        "snapshot_path": f"{BUCKET}/{storage_path}",
        "node_count": header.get("node_count", 0),
        "edge_count": header.get("edge_count", 0),
        "labels": header.get("labels", []),
        "edge_types": header.get("edge_types", []),
        "size_bytes": size_bytes,
        "checksum_sha256": checksum,
    }
    result = supabase.schema(SCHEMA).table("kg_registry").insert(row).execute()
    reg_id = result.data[0]["id"] if result.data else "unknown"

    print(f"\nDone! Registry ID: {reg_id}")
    print(f"  Tenant:     {args.tenant}")
    print(f"  Version:    {args.version}")
    print(f"  Nodes:      {header.get('node_count', 0):,}")
    print(f"  Edges:      {header.get('edge_count', 0):,}")
    print(f"  Size:       {size_bytes:,} bytes")
    print(f"  Checksum:   {checksum[:16]}...")
    print(f"  Path:       {BUCKET}/{storage_path}")


if __name__ == "__main__":
    main()
