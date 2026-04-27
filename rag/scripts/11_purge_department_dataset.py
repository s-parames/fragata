#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from scripts.common_department import normalize_department, validate_ingest_department
except ModuleNotFoundError:
    from common_department import normalize_department, validate_ingest_department


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            json.dump(payload, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def purge_department_dataset(
    *,
    dataset_path: Path,
    target_department: str,
    dry_run: bool,
    backup_out: Optional[Path] = None,
) -> Dict[str, Any]:
    rows_before = 0
    rows_removed = 0
    backup_dataset_path: Optional[str] = None

    fd = -1
    tmp_path: Optional[Path] = None
    writer = None
    if not dry_run:
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        fd, raw_tmp = tempfile.mkstemp(
            prefix=f"{dataset_path.name}.",
            suffix=".tmp",
            dir=str(dataset_path.parent),
        )
        tmp_path = Path(raw_tmp)
        writer = os.fdopen(fd, "w", encoding="utf-8")

    try:
        with dataset_path.open("r", encoding="utf-8") as src:
            for line_no, raw_line in enumerate(src, start=1):
                line = raw_line.strip()
                if not line:
                    if writer is not None:
                        writer.write(raw_line)
                    continue

                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"Invalid JSON at {dataset_path}:{line_no}: {exc.msg}"
                    ) from exc
                if not isinstance(row, dict):
                    raise RuntimeError(f"Invalid row type at {dataset_path}:{line_no}: expected object")

                rows_before += 1
                row_department = normalize_department(row.get("department"), allow_unknown=True)
                remove_row = row_department == target_department
                if remove_row:
                    rows_removed += 1
                    continue
                if writer is not None:
                    writer.write(raw_line)
    except Exception:
        if writer is not None and not writer.closed:
            writer.close()
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise
    finally:
        if writer is not None and not writer.closed:
            writer.close()

    rows_after = rows_before - rows_removed
    dataset_modified = (not dry_run) and rows_removed > 0

    if dataset_modified and tmp_path is not None:
        if backup_out is not None:
            backup_out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dataset_path, backup_out)
            backup_dataset_path = str(backup_out)
        os.replace(str(tmp_path), str(dataset_path))
    elif tmp_path is not None and tmp_path.exists():
        tmp_path.unlink()

    return {
        "rows_before": rows_before,
        "rows_removed": rows_removed,
        "rows_after": rows_after,
        "target_department": target_department,
        "dry_run": bool(dry_run),
        "output_dataset_path": str(dataset_path),
        "dataset_modified": bool(dataset_modified),
        "backup_dataset_path": backup_dataset_path,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Purge all rows for one department from a JSONL dataset.")
    ap.add_argument(
        "--dataset",
        default="data/datasetFinal.jsonl",
        help="Path to global JSONL dataset",
    )
    ap.add_argument(
        "--department",
        required=True,
        help="Department to purge",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute expected impact only; do not modify dataset",
    )
    ap.add_argument(
        "--summary-out",
        default=None,
        help="Optional output path for purge summary JSON",
    )
    ap.add_argument(
        "--backup-out",
        default=None,
        help="Optional backup path for pre-purge dataset (apply mode only)",
    )
    args = ap.parse_args()

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise SystemExit(f"Dataset file does not exist: {dataset_path}")
    if not dataset_path.is_file():
        raise SystemExit(f"Dataset path is not a file: {dataset_path}")

    target_department = validate_ingest_department(args.department)
    backup_out = Path(args.backup_out) if args.backup_out else None
    summary = purge_department_dataset(
        dataset_path=dataset_path,
        target_department=target_department,
        dry_run=bool(args.dry_run),
        backup_out=backup_out,
    )

    if args.summary_out:
        _atomic_write_json(Path(args.summary_out), summary)

    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
