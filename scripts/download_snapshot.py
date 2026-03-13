#!/usr/bin/env python3
"""Download a .sgsnap snapshot from Supabase Storage by tenant and version.

Usage:
    python scripts/download_snapshot.py \
        --tenant clinical-trials \
        --version 1 \
        --output clinical-trials.sgsnap

    # Download latest version:
    python scripts/download_snapshot.py \
        --tenant clinical-trials \
        --output clinical-trials.sgsnap

Environment variables:
    SUPABASE_URL          Supabase project URL
    SUPABASE_SERVICE_KEY  Service-role key
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

try:
    from supabase import create_client, Client
except ImportError:
    print("ERROR: supabase-py not installed.  pip install supabase", file=sys.stderr)
    sys.exit(1)

BUCKET = "kg-snapshots"
SCHEMA = "samyama_insight"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description="Download .sgsnap from Supabase Storage")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--version", type=int, default=0, help="Version (0 = latest)")
    parser.add_argument("--output", "-o", required=True, help="Output file path")
    args = parser.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to Supabase...")
    supabase: Client = create_client(url, key)

    # Look up registry entry
    query = supabase.schema(SCHEMA).table("kg_registry").select("*").eq(
        "tenant_id", args.tenant
    )
    if args.version > 0:
        query = query.eq("version", args.version)
    else:
        query = query.order("version", desc=True).limit(1)

    result = query.execute()
    if not result.data:
        print(f"ERROR: No snapshot found for tenant '{args.tenant}'" +
              (f" version {args.version}" if args.version else ""), file=sys.stderr)
        sys.exit(1)

    entry = result.data[0]
    snapshot_path = entry["snapshot_path"]
    expected_checksum = entry.get("checksum_sha256")

    # Strip bucket prefix if present
    if snapshot_path.startswith(f"{BUCKET}/"):
        storage_path = snapshot_path[len(f"{BUCKET}/"):]
    else:
        storage_path = snapshot_path

    print(f"Downloading from {BUCKET}/{storage_path}...")
    print(f"  Tenant:  {entry['tenant_id']}")
    print(f"  Version: {entry['version']}")
    print(f"  Name:    {entry['name']}")
    print(f"  Nodes:   {entry.get('node_count', '?'):,}")
    print(f"  Edges:   {entry.get('edge_count', '?'):,}")
    print(f"  Size:    {entry.get('size_bytes', '?'):,} bytes")

    data = supabase.storage.from_(BUCKET).download(storage_path)

    # Verify checksum
    if expected_checksum:
        actual_checksum = sha256_bytes(data)
        if actual_checksum != expected_checksum:
            print(f"\nWARNING: Checksum mismatch!", file=sys.stderr)
            print(f"  Expected: {expected_checksum}", file=sys.stderr)
            print(f"  Actual:   {actual_checksum}", file=sys.stderr)
        else:
            print(f"  Checksum: verified")

    output_path = Path(args.output)
    output_path.write_bytes(data)
    print(f"\nSaved to {output_path} ({len(data):,} bytes)")


if __name__ == "__main__":
    main()
