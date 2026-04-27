#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
from pathlib import Path

from common_department import normalize_department


REQUIRED_KEYS = [
    "chunk_id",
    "conversation_id",
    "source",
    "turn_start",
    "turn_end",
    "text",
    "char_len",
    "n_turns",
    "pii_tags",
    "last_updated",
    "department",
    "subject",
    "status",
]

PAGE_AWARE_KEYS = [
    "source_exact",
    "source_type",
    "original_url",
    "canonical_url",
    "acquisition_url",
    "repo_docs_provider",
    "repo_docs_kind",
    "repo_slug",
    "repo_namespace",
    "repo_name",
    "ingest_label",
    "ingest_job_id",
    "ingested_at",
    "doc_id",
    "page_number",
    "page_total",
    "page_label",
    "page_anchor",
    "page_title",
    "chunk_in_page",
    "relpath",
]


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_optional_text(text: str) -> str | None:
    normalized = normalize_text(text)
    return normalized or None


def ticket_id_from_row(row: dict) -> int | None:
    source = row.get("source", "") or ""
    m = re.search(r"id=(\d+)", source)
    if m:
        return int(m.group(1))
    conv = row.get("conversation_id", "") or ""
    # Conversation-id numeric fallback is only valid for legacy ticket ids: conv_<digits>
    m2 = re.search(r"^conv_(\d+)$", conv)
    if m2:
        return int(m2.group(1))
    return None


def stable_conversation_id(row: dict) -> str:
    conv = (row.get("conversation_id") or "").strip()
    if conv:
        return conv

    source = (row.get("source") or "").strip()
    m = re.search(r"id=(\d+)", source)
    if m:
        return f"conv_{m.group(1)}"

    seed_text = row.get("text") or row.get("text_retrieval") or row.get("text_raw") or ""
    digest = hashlib.sha1((source + "|" + normalize_text(seed_text)).encode("utf-8")).hexdigest()[:12]
    return f"conv_{digest}"


def stable_chunk_id(row: dict, conversation_id: str, original_conv_id: str, original_chunk_id: str) -> str:
    if original_chunk_id:
        # Keep existing chunk ids stable; only realign prefix if conversation id had to be fixed.
        if original_conv_id and conversation_id != original_conv_id:
            prefix = f"{original_conv_id}_chunk_"
            if original_chunk_id.startswith(prefix):
                suffix = original_chunk_id[len(prefix) :]
                return f"{conversation_id}_chunk_{suffix}"
        return original_chunk_id

    text_hash = hashlib.sha1(normalize_text(row.get("text", "")).encode("utf-8")).hexdigest()[:10]
    return f"{conversation_id}_chunk_{text_hash}"


def safe_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10**15


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    rows = []
    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            for k in REQUIRED_KEYS:
                row.setdefault(k, None)
            for k in PAGE_AWARE_KEYS:
                row.setdefault(k, None)

            row["last_updated"] = normalize_optional_text(
                row.get("last_updated") or row.get("lastUpdated") or ""
            )
            row["subject"] = normalize_optional_text(row.get("subject") or "")
            row["status"] = normalize_optional_text(row.get("status") or "")

            text_raw = normalize_text(row.get("text_raw") or row.get("text") or "")
            text_retrieval = normalize_text(row.get("text_retrieval") or row.get("text") or text_raw)
            retrieval_fallback_used = bool(row.get("retrieval_fallback_used", False))
            if not text_retrieval and text_raw:
                text_retrieval = text_raw
                retrieval_fallback_used = True
            if not text_raw and text_retrieval:
                text_raw = text_retrieval

            row["text"] = text_retrieval or text_raw
            row["retrieval_fallback_used"] = retrieval_fallback_used
            row["char_len"] = len(row["text"])
            row["department"] = normalize_department(row.get("department"), allow_unknown=True)
            if not row["text"]:
                continue

            # Keep a single canonical text field in final dataset rows.
            row.pop("text_retrieval", None)
            row.pop("text_raw", None)
            original_conv_id = (row.get("conversation_id") or "").strip()
            original_chunk_id = (row.get("chunk_id") or "").strip()
            conversation_id = stable_conversation_id(row)

            row["conversation_id_original"] = original_conv_id
            row["conversation_id"] = conversation_id
            row["ticket_id"] = ticket_id_from_row(row)
            row["chunk_id_original"] = original_chunk_id
            row["chunk_id"] = stable_chunk_id(
                row,
                conversation_id=conversation_id,
                original_conv_id=original_conv_id,
                original_chunk_id=original_chunk_id,
            )
            rows.append(row)

    rows.sort(
        key=lambda r: (
            safe_int(r.get("ticket_id")),
            str(r.get("conversation_id", "")),
            str(r.get("chunk_id", "")),
        )
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
