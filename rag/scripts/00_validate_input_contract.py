#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_row(row: Dict[str, Any], line_no: int) -> List[str]:
    errors: List[str] = []
    if not _is_non_empty_string(row.get("link")):
        errors.append(f"line {line_no}: missing/empty 'link'")

    has_last_updated = _is_non_empty_string(row.get("lastUpdated")) or _is_non_empty_string(
        row.get("last_updated")
    )
    if not has_last_updated:
        errors.append(f"line {line_no}: missing 'lastUpdated' or 'last_updated'")

    messages = row.get("messages")
    has_subject = _is_non_empty_string(row.get("subject"))
    if not isinstance(messages, list):
        errors.append(f"line {line_no}: 'messages' must be a list")
        return errors

    non_empty_messages = 0
    for idx, msg in enumerate(messages):
        if not isinstance(msg, dict):
            errors.append(f"line {line_no}: messages[{idx}] must be an object")
            continue
        has_content = _is_non_empty_string(msg.get("content"))
        if has_content and not _is_non_empty_string(msg.get("role")):
            errors.append(f"line {line_no}: messages[{idx}].role missing/empty")
        if has_content:
            non_empty_messages += 1

    if not non_empty_messages and not has_subject:
        errors.append(
            f"line {line_no}: row must contain at least one non-empty messages[].content or a non-empty 'subject'"
        )
    return errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to RT JSONL input dataset")
    args = ap.parse_args()

    total = 0
    broken = 0
    all_errors: List[str] = []
    with open(args.input, "r", encoding="utf-8") as src:
        for line_no, line in enumerate(src, start=1):
            if not line.strip():
                continue
            total += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                broken += 1
                all_errors.append(f"line {line_no}: invalid JSON ({exc.msg})")
                continue
            row_errors = _validate_row(row, line_no)
            if row_errors:
                broken += 1
                all_errors.extend(row_errors)

    if broken:
        preview = "\n".join(all_errors[:40])
        raise SystemExit(
            f"Input contract validation failed: {broken} invalid rows over {total} rows.\n{preview}"
        )

    print(f"Input contract OK: {total} rows validated.")


if __name__ == "__main__":
    main()
