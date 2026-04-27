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
        # Make `ingest` imports robust even when script is launched from external cwd.
        sys.path.insert(0, root_str)


_ensure_project_root_on_path()

from ingest.faiss_incremental import append_delta_with_fallback, write_summary_json


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Append delta rows into existing FAISS index using configured embeddings, then atomically promote."
    )
    ap.add_argument("--config", default="config/rag.yaml", help="RAG config path")
    ap.add_argument("--delta", required=True, help="Delta JSONL path from merge step")
    ap.add_argument("--faiss-dir", default=None, help="Optional faiss_dir override")
    ap.add_argument("--summary-out", default=None, help="Optional summary JSON output path")
    ap.add_argument(
        "--disable-fallback-rebuild",
        action="store_true",
        help="Disable fallback full rebuild when append fails",
    )
    args = ap.parse_args()

    result = append_delta_with_fallback(
        config_path=args.config,
        delta_path=args.delta,
        faiss_dir_override=args.faiss_dir,
        enable_fallback_rebuild=not args.disable_fallback_rebuild,
    )
    payload = result.to_dict()
    if args.summary_out:
        write_summary_json(args.summary_out, payload)
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
