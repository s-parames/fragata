#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _ensure_project_root_on_path() -> None:
    project_root = Path(__file__).resolve().parents[1]
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_ensure_project_root_on_path()

from RAG_v1 import load_config
from ingest.web_job_cleanup import (
    cleanup_web_job_artifacts,
    discover_web_job_cleanup_targets,
)


def _script_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_config_path(config_path: str) -> Path:
    path = Path(config_path)
    if not path.is_absolute():
        path = _script_project_root() / path
    return path.resolve()


def _resolve_project_root(config_path: str) -> Path:
    cfg_path = _resolve_config_path(config_path)
    load_config(str(cfg_path))
    if cfg_path.parent.name == "config":
        return cfg_path.parent.parent
    return _script_project_root()


def _resolve_web_jobs_root(config_path: str) -> Path:
    return _resolve_project_root(config_path) / "data" / "raw_site" / "jobs"


def _print_summary(*, mode: str, web_jobs_root: Path, targets: list[dict], deleted_jobs: int) -> None:
    print(f"Mode: {mode}")
    print(f"Web jobs root: {web_jobs_root}")
    print(f"Selected jobs: {len(targets)}")

    prefix = "DELETE" if mode == "apply" else "PRUNE"
    total_bytes = 0
    for target in targets:
        total_bytes += int(target["total_bytes"])
        print(
            f"{prefix} job_id={target['job_id']} "
            f"path={target['raw_job_root']} "
            f"bytes={target['total_bytes']} "
            f"files={target['file_count']} "
            f"dirs={target['dir_count']}"
        )

    print(
        "Summary: "
        f"jobs={len(targets)} "
        f"estimated_reclaimed_bytes={total_bytes} "
        f"deleted_jobs={deleted_jobs}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Inspect or prune accumulated raw web-ingest job trees under data/raw_site/jobs."
    )
    ap.add_argument("--config", default="config/rag.yaml", help="RAG config path")
    scope = ap.add_mutually_exclusive_group(required=True)
    scope.add_argument("--job-id", help="Delete one explicit web job raw tree by job id")
    scope.add_argument("--all-web-jobs", action="store_true", help="Delete all discovered web job raw trees")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="List raw job trees without deleting them")
    mode.add_argument("--apply", action="store_true", help="Delete selected raw job trees")
    args = ap.parse_args()

    try:
        web_jobs_root = _resolve_web_jobs_root(args.config)
        targets = discover_web_job_cleanup_targets(
            web_jobs_root,
            job_id=args.job_id,
        )

        mode_name = "apply" if args.apply else "dry-run"
        payloads: list[dict] = []
        deleted_jobs = 0
        for target in targets:
            result = cleanup_web_job_artifacts(
                Path(target.raw_job_root),
                base_root=web_jobs_root,
                apply=bool(args.apply),
            )
            payload = result.to_dict()
            payloads.append(payload)
            if payload["deleted_roots"]:
                deleted_jobs += 1

        _print_summary(mode=mode_name, web_jobs_root=web_jobs_root, targets=payloads, deleted_jobs=deleted_jobs)
    except Exception as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
