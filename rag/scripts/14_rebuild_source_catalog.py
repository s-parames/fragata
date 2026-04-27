#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _ensure_project_root_on_path() -> None:
    project_root = Path(__file__).resolve().parents[1]
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_ensure_project_root_on_path()

from ingest.source_catalog import rebuild_source_catalog


def _print_summary(payload: dict) -> None:
    print(f"Dataset: {payload['dataset_path']}")
    print(f"Catalog: {payload['catalog_path']}")
    print(f"Generated at: {payload['generated_at']}")
    print(f"Dataset mtime ns: {payload.get('dataset_mtime_ns')}")
    print(f"Dataset size bytes: {payload.get('dataset_size_bytes')}")
    print(f"Total entries: {payload['total_entries']}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Rebuild the persisted logical source catalog from the configured dataset."
    )
    ap.add_argument("--config", default="config/rag.yaml", help="RAG config path")
    ap.add_argument("--dataset-path", default=None, help="Optional dataset path override")
    ap.add_argument("--catalog-path", default=None, help="Optional catalog output path override")
    ap.add_argument("--json", action="store_true", help="Emit machine-readable JSON summary")
    args = ap.parse_args()

    snapshot = rebuild_source_catalog(
        config_path=args.config,
        dataset_path_override=args.dataset_path,
        catalog_path_override=args.catalog_path,
    )
    payload = {
        "dataset_path": snapshot.dataset_path,
        "catalog_path": snapshot.catalog_path,
        "generated_at": snapshot.generated_at,
        "dataset_mtime_ns": snapshot.dataset_mtime_ns,
        "dataset_size_bytes": snapshot.dataset_size_bytes,
        "total_entries": snapshot.total_entries,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    _print_summary(payload)


if __name__ == "__main__":
    main()
