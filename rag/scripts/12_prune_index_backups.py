#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict


def _ensure_project_root_on_path() -> None:
    project_root = Path(__file__).resolve().parents[1]
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_ensure_project_root_on_path()

from RAG_v1 import load_config
from ingest.index_backup_retention import prune_index_backups as prune_index_backups_helper


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_faiss_dir(config_path: str, faiss_dir_override: str | None) -> Path:
    if faiss_dir_override:
        path = Path(faiss_dir_override)
        return path if path.is_absolute() else (_project_root() / path)

    cfg_path = Path(config_path)
    if not cfg_path.is_absolute():
        cfg_path = _project_root() / cfg_path
    cfg = load_config(str(cfg_path))
    path = Path(cfg.faiss_dir)
    return path if path.is_absolute() else (_project_root() / path)


def prune_index_backups(
    *,
    active_dir: Path,
    keep_last: int,
    apply: bool,
) -> Dict[str, Any]:
    return prune_index_backups_helper(active_dir=active_dir, keep_last=keep_last, apply=apply).to_dict()


def _print_summary(payload: Dict[str, Any]) -> None:
    print(f"Mode: {payload['mode']}")
    print(f"Active index: {payload['active_dir']}")
    print(f"Keep last: {payload['keep_last']}")

    retained = payload.get("retained", [])
    prunable = payload.get("prunable", [])
    deleted = payload.get("deleted", [])

    print(f"Retained backups ({len(retained)}):")
    for path in retained:
        print(f"KEEP {path}")

    label = "Deleted backups" if payload["mode"] == "apply" else "Prunable backups"
    print(f"{label} ({len(prunable)}):")
    prefix = "DELETE" if payload["mode"] == "apply" else "PRUNE"
    for path in prunable:
        print(f"{prefix} {path}")

    print(
        "Summary: "
        f"retained={len(retained)} "
        f"prunable={len(prunable)} "
        f"deleted={len(deleted)}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Inspect or prune obsolete FAISS backup directories for one active index family."
    )
    ap.add_argument("--config", default="config/rag.yaml", help="RAG config path")
    ap.add_argument("--faiss-dir", default=None, help="Optional faiss_dir override")
    ap.add_argument("--keep-last", type=int, default=1, help="How many newest backups to retain")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="List prune candidates without deleting them")
    mode.add_argument("--apply", action="store_true", help="Delete prune candidates")
    args = ap.parse_args()

    active_dir = _resolve_faiss_dir(args.config, args.faiss_dir)
    payload = prune_index_backups(
        active_dir=active_dir,
        keep_last=args.keep_last,
        apply=bool(args.apply),
    )
    _print_summary(payload)


if __name__ == "__main__":
    main()
