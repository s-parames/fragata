#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def utc_now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _json_dumps_compact(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            dst.write(content)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _normalize_host(raw: str) -> str:
    return (raw or "").strip().lower()


def _host_from_url(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    return _normalize_host(parsed.hostname or "")


def _is_slurm_host_value(raw: Any, slurm_host: str) -> bool:
    if not isinstance(raw, str):
        return False
    text = raw.strip().lower()
    if not text:
        return False
    host = _host_from_url(text)
    if host == slurm_host:
        return True
    # Fallback for malformed URL-like values.
    return slurm_host in text


def _row_is_slurm(row: Dict[str, Any], slurm_host: str) -> bool:
    for key in ("source", "source_exact", "link"):
        if _is_slurm_host_value(row.get(key), slurm_host):
            return True
    return False


@dataclass
class MigrationStats:
    total_rows: int = 0
    slurm_rows: int = 0
    changed_rows: int = 0
    invalid_rows: int = 0


def iter_dataset_lines(path: Path) -> Iterable[tuple[int, str]]:
    with path.open("r", encoding="utf-8") as src:
        for line_no, raw in enumerate(src, start=1):
            yield line_no, raw


def build_default_report_path(dataset_path: Path) -> Path:
    root = dataset_path.resolve().parents[1] if len(dataset_path.resolve().parents) >= 2 else dataset_path.parent
    return root / "reports" / "migrations" / f"slurm_department_migration_{utc_now_tag()}.json"


def migrate_dataset(
    *,
    dataset_path: Path,
    target_department: str,
    slurm_host: str,
    apply_changes: bool,
) -> Dict[str, Any]:
    stats = MigrationStats()
    source_departments = Counter()
    changed_from_departments = Counter()

    fd = -1
    tmp_path: Optional[Path] = None
    writer = None
    if apply_changes:
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        fd, raw_tmp = tempfile.mkstemp(
            prefix=f"{dataset_path.name}.",
            suffix=".tmp",
            dir=str(dataset_path.parent),
        )
        tmp_path = Path(raw_tmp)
        writer = os.fdopen(fd, "w", encoding="utf-8")

    try:
        for line_no, raw_line in iter_dataset_lines(dataset_path):
            line = raw_line.rstrip("\n")
            if not line.strip():
                if writer is not None:
                    writer.write(raw_line)
                continue

            stats.total_rows += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                stats.invalid_rows += 1
                raise RuntimeError(
                    f"Invalid JSON at {dataset_path}:{line_no}: {exc.msg}"
                ) from exc

            if not isinstance(row, dict):
                stats.invalid_rows += 1
                raise RuntimeError(f"Invalid row type at {dataset_path}:{line_no}: expected object")

            is_slurm = _row_is_slurm(row, slurm_host)
            changed = False
            if is_slurm:
                stats.slurm_rows += 1
                original_department = str(row.get("department") or "")
                source_departments[original_department] += 1
                if original_department != target_department:
                    row["department"] = target_department
                    stats.changed_rows += 1
                    changed_from_departments[original_department] += 1
                    changed = True

            if writer is not None:
                if changed:
                    writer.write(_json_dumps_compact(row) + "\n")
                else:
                    writer.write(raw_line)
    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise
    finally:
        if writer is not None and not writer.closed:
            writer.close()

    if apply_changes and tmp_path is not None:
        os.replace(str(tmp_path), str(dataset_path))

    return {
        "status": "applied" if apply_changes else "dry_run",
        "dataset_path": str(dataset_path),
        "target_department": target_department,
        "slurm_host": slurm_host,
        "stats": {
            "total_rows": stats.total_rows,
            "slurm_rows": stats.slurm_rows,
            "changed_rows": stats.changed_rows,
            "invalid_rows": stats.invalid_rows,
        },
        "source_departments": dict(sorted(source_departments.items(), key=lambda kv: kv[0])),
        "changed_from_departments": dict(sorted(changed_from_departments.items(), key=lambda kv: kv[0])),
        "executed_at": utc_now_iso(),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Migrate Slurm dataset rows to a dedicated department tag."
    )
    ap.add_argument(
        "--dataset",
        default="data/datasetFinal.jsonl",
        help="Target JSONL dataset path",
    )
    ap.add_argument(
        "--target-department",
        default="slurm",
        help="Department value to apply to Slurm rows",
    )
    ap.add_argument(
        "--slurm-host",
        default="slurm.schedmd.com",
        help="Host used to identify Slurm rows",
    )
    ap.add_argument(
        "--report-out",
        default=None,
        help="Optional path for migration report JSON",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to dataset. Without this flag, run in dry-run mode.",
    )
    args = ap.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise SystemExit(f"Dataset file does not exist: {dataset_path}")
    if not dataset_path.is_file():
        raise SystemExit(f"Dataset path is not a file: {dataset_path}")

    target_department = str(args.target_department or "").strip().lower()
    if not target_department:
        raise SystemExit("--target-department must not be empty")

    slurm_host = _normalize_host(str(args.slurm_host or ""))
    if not slurm_host:
        raise SystemExit("--slurm-host must not be empty")

    summary = migrate_dataset(
        dataset_path=dataset_path,
        target_department=target_department,
        slurm_host=slurm_host,
        apply_changes=bool(args.apply),
    )

    report_path = Path(args.report_out) if args.report_out else build_default_report_path(dataset_path)
    _atomic_write_text(report_path, json.dumps(summary, ensure_ascii=False, indent=2) + "\n")

    print(json.dumps({**summary, "report_out": str(report_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
