#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    from scripts.common_department import normalize_department
except ModuleNotFoundError:
    from common_department import normalize_department


def normalize_text(text: str) -> str:
    return " ".join((text or "").replace("\r", "\n").split())


def stable_key(row: Dict[str, Any]) -> str:
    # Keep dedupe behavior backward compatible: new ingestion metadata is intentionally
    # excluded from key composition.
    source = (row.get("source") or row.get("link") or "").strip()
    chunk_orig = (row.get("chunk_id_original") or "").strip()
    if source and chunk_orig:
        return f"{source}|{chunk_orig}"

    text_hash = hashlib.sha1(normalize_text(row.get("text", "")).encode("utf-8")).hexdigest()
    if source:
        return f"{source}|{text_hash}"

    fallback = (
        str(row.get("ticket_id") or ""),
        str(row.get("conversation_id_original") or row.get("conversation_id") or ""),
        text_hash,
    )
    return "|".join(fallback)


def load_rows(path: Optional[str]) -> Iterable[Dict[str, Any]]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as src:
        for line in src:
            if not line.strip():
                continue
            row = json.loads(line)
            row["department"] = normalize_department(row.get("department"), allow_unknown=True)
            rows.append(row)
    return rows


def row_signature(row: Dict[str, Any]) -> str:
    # Keep merge/delta stable across re-ingests where only ingestion-tracking
    # metadata changes (job id / ingest timestamp / batch label).
    sanitized = dict(row)
    sanitized.pop("ingest_label", None)
    sanitized.pop("ingest_job_id", None)
    sanitized.pop("ingested_at", None)
    return json.dumps(sanitized, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def ticket_group_key(row: Dict[str, Any]) -> Optional[str]:
    source_type = str(row.get("source_type") or "").strip().lower()
    if source_type and source_type not in {"ticket", "tickets", "conversation"}:
        return None

    ticket_id = _as_int(row.get("ticket_id"))
    if ticket_id is not None:
        return f"ticket:{ticket_id}"

    source = str(row.get("source") or row.get("link") or "").strip()
    match = re.search(r"id=(\d+)", source)
    if match:
        return f"ticket:{match.group(1)}"

    conversation_id = str(row.get("conversation_id") or "").strip()
    legacy_match = re.fullmatch(r"conv_(\d+)", conversation_id)
    if legacy_match:
        return f"ticket:{legacy_match.group(1)}"
    return None


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10**15


def sort_key(row: Dict[str, Any]):
    return (
        safe_int(row.get("ticket_id")),
        str(row.get("conversation_id") or ""),
        str(row.get("chunk_id") or ""),
    )


def atomic_write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            for row in rows:
                dst.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp_path, target)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            json.dump(payload, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(tmp_path, target)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def merge_with_delta(
    base_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    merged: Dict[str, Dict[str, Any]] = {}
    delta_rows: List[Dict[str, Any]] = []
    new_count = 0
    updated_count = 0
    unchanged_count = 0
    affected_ticket_groups = {key for row in new_rows if (key := ticket_group_key(row))}
    filtered_base_rows: List[Dict[str, Any]] = []
    ticket_rows_removed = 0

    if affected_ticket_groups:
        for row in base_rows:
            key = ticket_group_key(row)
            if key and key in affected_ticket_groups:
                ticket_rows_removed += 1
                continue
            filtered_base_rows.append(row)
    else:
        filtered_base_rows = base_rows

    for row in filtered_base_rows:
        merged[stable_key(row)] = row
    for row in new_rows:
        key = stable_key(row)
        existing = merged.get(key)
        if existing is None:
            new_count += 1
            delta_rows.append(row)
            merged[key] = row
        elif row_signature(existing) != row_signature(row):
            updated_count += 1
            delta_rows.append(row)
            merged[key] = row
        else:
            unchanged_count += 1

    rows = sorted(merged.values(), key=sort_key)
    delta_sorted = sorted(delta_rows, key=sort_key)
    summary = {
        "base_rows": len(base_rows),
        "base_rows_effective": len(filtered_base_rows),
        "new_rows": len(new_rows),
        "out_rows": len(rows),
        "delta_rows": len(delta_sorted),
        "new_count": new_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "affected_ticket_groups": len(affected_ticket_groups),
        "ticket_rows_removed": ticket_rows_removed,
    }
    return rows, delta_sorted, summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=None, help="Existing JSONL dataset (optional)")
    ap.add_argument("--new", required=True, help="New JSONL dataset to merge")
    ap.add_argument("--out", required=True, help="Output merged JSONL path")
    ap.add_argument(
        "--out-delta",
        default=None,
        help="Optional output JSONL path for new/changed rows only",
    )
    ap.add_argument(
        "--summary-out",
        default=None,
        help="Optional JSON summary output path",
    )
    args = ap.parse_args()

    base_rows = list(load_rows(args.base))
    new_rows = list(load_rows(args.new))
    rows, delta_rows, summary = merge_with_delta(base_rows, new_rows)
    atomic_write_jsonl(args.out, rows)
    if args.out_delta:
        atomic_write_jsonl(args.out_delta, delta_rows)
    summary = {
        **summary,
        "out_path": args.out,
        "delta_path": args.out_delta,
    }
    if args.summary_out:
        atomic_write_json(args.summary_out, summary)

    print(
        f"Merged dataset written to {args.out} "
        f"(base_rows={len(base_rows)} new_rows={len(new_rows)} out_rows={summary['out_rows']} "
        f"delta_rows={summary['delta_rows']} new={summary['new_count']} "
        f"updated={summary['updated_count']} unchanged={summary['unchanged_count']})."
    )


if __name__ == "__main__":
    main()
