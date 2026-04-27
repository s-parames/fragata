#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from scripts.common_department import normalize_department
except ModuleNotFoundError:
    from common_department import normalize_department


ROLE_ALIASES = {
    "user": "user",
    "usuario": "user",
    "client": "user",
    "customer": "user",
    "requester": "user",
    "assistant": "assistant",
    "agente": "assistant",
    "agent": "assistant",
    "support": "assistant",
    "system": "system",
}

MAX_MESSAGES_PARSE_DEPTH = 20


def infer_department_from_filename(path: Path) -> Optional[str]:
    base = path.name.lower()
    if "aplicaciones" in base:
        return "aplicaciones"
    if "sistemas" in base:
        return "sistemas"
    if "bigdata" in base or "big_data" in base or "big-data" in base:
        return "bigdata"
    if "general" in base:
        return "general"
    if "comunicaciones" in base or "comunicacion" in base:
        return "comunicaciones"
    return None


def normalize_role(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "user"
    return ROLE_ALIASES.get(raw, "user")


def clean_text(value: Any) -> str:
    text = str(value or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_optional_text(value: Any) -> Optional[str]:
    text = clean_text(value)
    return text or None


def parse_datetime_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()

    raw = str(value).strip()
    if not raw:
        return None

    iso_raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_raw)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%Y-%m-%d" or fmt == "%d/%m/%Y":
                return dt.date().isoformat()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return raw


def parse_messages_value(value: Any, _depth: int = 0) -> List[Dict[str, str]]:
    if _depth >= MAX_MESSAGES_PARSE_DEPTH:
        content = clean_text(value)
        if content:
            return [{"role": "user", "content": content}]
        return []

    if value is None:
        return []

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") or raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                # Malformed JSON-like text should be preserved as plain content,
                # not recursively reparsed forever.
                return [{"role": "user", "content": clean_text(raw)}]
            if isinstance(parsed, str) and parsed == raw:
                return [{"role": "user", "content": clean_text(raw)}]
            return parse_messages_value(parsed, _depth + 1)
        return [{"role": "user", "content": clean_text(raw)}]

    if isinstance(value, dict):
        if isinstance(value.get("messages"), list):
            return parse_messages_value(value.get("messages"), _depth + 1)
        if isinstance(value.get("turns"), list):
            return parse_messages_value(value.get("turns"), _depth + 1)
        if "messages" in value:
            return parse_messages_value(value.get("messages"), _depth + 1)
        if "turns" in value:
            return parse_messages_value(value.get("turns"), _depth + 1)

        role = normalize_role(value.get("role"))
        content = clean_text(value.get("content"))
        if content:
            return [{"role": role, "content": content}]

        pairs: List[Dict[str, str]] = []
        question = clean_text(value.get("question") or value.get("query"))
        answer = clean_text(value.get("answer") or value.get("response"))
        if question:
            pairs.append({"role": "user", "content": question})
        if answer:
            pairs.append({"role": "assistant", "content": answer})
        return pairs

    if isinstance(value, list):
        out: List[Dict[str, str]] = []
        for item in value:
            out.extend(parse_messages_value(item, _depth + 1))
        deduped: List[Dict[str, str]] = []
        for msg in out:
            content = clean_text(msg.get("content"))
            if not content:
                continue
            deduped.append({"role": normalize_role(msg.get("role")), "content": content})
        return deduped

    return [{"role": "user", "content": clean_text(value)}]


def build_link(row: Dict[str, Any], link_template: str) -> str:
    for key in ("link", "source", "ticket_link", "url"):
        raw = str(row.get(key) or "").strip()
        if raw:
            return raw

    ticket_id = row.get("ticket_id") or row.get("id")
    ticket_id_raw = str(ticket_id or "").strip()
    if ticket_id_raw and link_template:
        return link_template.format(ticket_id=ticket_id_raw)
    return ""


def extract_messages(row: Dict[str, Any]) -> List[Dict[str, str]]:
    for key in ("messages", "messages_json", "conversation", "conversation_json", "turns", "thread"):
        if key in row:
            messages = parse_messages_value(row.get(key))
            if messages:
                return messages

    fallback: List[Dict[str, str]] = []
    question = clean_text(row.get("question") or row.get("query"))
    answer = clean_text(row.get("answer") or row.get("response"))
    content = clean_text(row.get("content") or row.get("body"))
    if question:
        fallback.append({"role": "user", "content": question})
    if answer:
        fallback.append({"role": "assistant", "content": answer})
    if not fallback and content:
        fallback.append({"role": "user", "content": content})
    return fallback


def build_contract_row(
    row: Dict[str, Any],
    *,
    department: str,
    link_template: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    link = build_link(row, link_template)
    if not link:
        return None, "missing link"

    last_updated_raw = (
        row.get("lastUpdated")
        or row.get("last_updated")
        or row.get("updated_at")
        or row.get("resolved_at")
        or row.get("closed_at")
    )
    last_updated = parse_datetime_like(last_updated_raw)
    if not last_updated:
        return None, "missing lastUpdated/last_updated"

    messages = extract_messages(row)
    if not messages:
        return None, "missing messages"

    contract_row = {
        "link": link,
        "lastUpdated": last_updated,
        "department": normalize_department(department, allow_unknown=False) or department,
        "subject": clean_optional_text(row.get("subject")),
        "messages": messages,
    }
    return contract_row, None


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as src:
        for line_no, line in enumerate(src, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON ({exc.msg})") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{line_no}: each row must be a JSON object")
            yield row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--summary-out", required=True)
    ap.add_argument(
        "--link-template",
        default="https://rt.cesga.es/Ticket/Display.html?id={ticket_id}",
        help="Used only when input rows have no link/source fields",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Fail the run if at least one row is skipped",
    )
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    summary_out = Path(args.summary_out)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_out.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("*.jsonl"))
    if not files:
        raise SystemExit(f"No JSONL files found in {input_dir}")

    total_in = 0
    total_out = 0
    total_skipped = 0
    file_summaries: List[Dict[str, Any]] = []

    for source_path in files:
        department = infer_department_from_filename(source_path)
        if department is None:
            raise SystemExit(
                "Cannot infer department from filename. Expected one of: "
                f"aplicaciones/sistemas/bigdata/general/comunicaciones in {source_path.name}"
            )

        out_path = out_dir / source_path.name
        in_rows = 0
        out_rows = 0
        skipped_rows = 0
        skipped_reasons: Dict[str, int] = {}

        with out_path.open("w", encoding="utf-8") as dst:
            for row in iter_jsonl(source_path):
                in_rows += 1
                total_in += 1

                contract_row, err = build_contract_row(
                    row,
                    department=department,
                    link_template=args.link_template,
                )
                if err:
                    skipped_rows += 1
                    total_skipped += 1
                    skipped_reasons[err] = skipped_reasons.get(err, 0) + 1
                    continue

                dst.write(json.dumps(contract_row, ensure_ascii=False) + "\n")
                out_rows += 1
                total_out += 1

        file_summary = {
            "source": str(source_path),
            "output": str(out_path),
            "department": department,
            "rows_in": in_rows,
            "rows_out": out_rows,
            "rows_skipped": skipped_rows,
            "skipped_reasons": skipped_reasons,
        }
        file_summaries.append(file_summary)

    summary = {
        "total_rows_in": total_in,
        "total_rows_out": total_out,
        "total_rows_skipped": total_skipped,
        "files": file_summaries,
    }

    with summary_out.open("w", encoding="utf-8") as dst:
        json.dump(summary, dst, ensure_ascii=False, indent=2)
        dst.write("\n")

    if args.strict and total_skipped > 0:
        raise SystemExit(
            f"Preparation finished with skipped rows (strict mode): skipped={total_skipped}"
        )

    print(
        "Preparation completed: "
        f"in={total_in} out={total_out} skipped={total_skipped} summary={summary_out}"
    )


if __name__ == "__main__":
    main()
