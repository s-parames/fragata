#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml


REPO_DOCS_METADATA_KEYS = (
    "original_url",
    "canonical_url",
    "acquisition_url",
    "repo_docs_provider",
    "repo_docs_kind",
    "repo_slug",
    "repo_namespace",
    "repo_name",
)


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_long_text(text: str, max_chars: int) -> List[str]:
    text = normalize_text(text)
    if len(text) <= max_chars:
        return [text] if text else []

    parts: List[str] = []
    buffer = ""
    for para in text.split("\n\n"):
        para = normalize_text(para)
        if not para:
            continue
        candidate = f"{buffer}\n\n{para}".strip() if buffer else para
        if len(candidate) <= max_chars:
            buffer = candidate
            continue
        if buffer:
            parts.append(buffer)
            buffer = ""
        if len(para) <= max_chars:
            buffer = para
            continue
        start = 0
        while start < len(para):
            end = min(start + max_chars, len(para))
            chunk = normalize_text(para[start:end])
            if chunk:
                parts.append(chunk)
            start = end
    if buffer:
        parts.append(buffer)
    return parts


def build_text_from_row(row: Dict[str, Any]) -> str:
    messages = row.get("messages")
    if isinstance(messages, list):
        parts = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = normalize_text(str(msg.get("content") or ""))
            if content:
                parts.append(content)
        if parts:
            return normalize_text("\n\n".join(parts))
    direct_text = row.get("text") or row.get("text_retrieval") or row.get("text_raw") or ""
    return normalize_text(str(direct_text))


def chunk_page_text(
    text: str,
    *,
    min_chars: int,
    target_chars: int,
    max_chars: int,
    min_keep_chars: int,
) -> List[str]:
    text = normalize_text(text)
    if not text:
        return []

    paragraphs = [normalize_text(p) for p in text.split("\n\n") if normalize_text(p)]
    if not paragraphs:
        return []

    chunks: List[str] = []
    current = ""

    for para in paragraphs:
        para_parts = split_long_text(para, max_chars)
        if not para_parts:
            continue

        for part in para_parts:
            candidate = f"{current}\n\n{part}".strip() if current else part
            if len(candidate) <= max_chars:
                current = candidate
                if len(current) >= target_chars and len(current) >= min_chars:
                    chunks.append(current)
                    current = ""
                continue

            if current:
                chunks.append(current)
            current = part
            if len(current) >= target_chars and len(current) >= min_chars:
                chunks.append(current)
                current = ""

    if current:
        chunks.append(current)

    rebalanced: List[str] = []
    for chunk in chunks:
        if (
            rebalanced
            and len(chunk) < min_keep_chars
            and len(rebalanced[-1]) + 2 + len(chunk) <= max_chars
        ):
            rebalanced[-1] = normalize_text(rebalanced[-1] + "\n\n" + chunk)
        else:
            rebalanced.append(chunk)

    if (
        len(rebalanced) >= 2
        and len(rebalanced[-1]) < min_keep_chars
        and len(rebalanced[-2]) + 2 + len(rebalanced[-1]) <= max_chars
    ):
        rebalanced[-2] = normalize_text(rebalanced[-2] + "\n\n" + rebalanced[-1])
        rebalanced.pop()

    return [normalize_text(c) for c in rebalanced if normalize_text(c)]


def chunk_id_for_page(conversation_id: str, page_label: str, chunk_in_page: int) -> str:
    return f"{conversation_id}_{page_label}_chunk_{chunk_in_page:03d}"


def row_to_chunks(
    row: Dict[str, Any],
    *,
    min_chars: int,
    target_chars: int,
    max_chars: int,
    min_keep_chars: int,
) -> List[Dict[str, Any]]:
    source = str(row.get("source") or "").strip()
    source_exact = str(row.get("source_exact") or row.get("link") or source).strip()
    if not source_exact:
        source_exact = source
    department = row.get("department")
    ingest_label = row.get("ingest_label")
    ingest_job_id = row.get("ingest_job_id")
    ingested_at = row.get("ingested_at")
    conversation_id = str(row.get("conversation_id") or "").strip()
    if not conversation_id:
        return []

    page_number = int(row.get("page_number") or 0)
    if page_number <= 0:
        return []
    page_label = str(row.get("page_label") or f"page_{page_number:03d}")

    text = build_text_from_row(row)
    text_chunks = chunk_page_text(
        text,
        min_chars=min_chars,
        target_chars=target_chars,
        max_chars=max_chars,
        min_keep_chars=min_keep_chars,
    )
    if not text_chunks:
        return []

    pii_tags = {}
    stats = row.get("stats")
    if isinstance(stats, dict) and isinstance(stats.get("pii_tags"), dict):
        pii_tags = stats.get("pii_tags") or {}

    out_chunks: List[Dict[str, Any]] = []
    for idx, chunk_text in enumerate(text_chunks):
        chunk_id = chunk_id_for_page(conversation_id, page_label, idx)
        chunk_row = {
            "chunk_id": chunk_id,
            "conversation_id": conversation_id,
            "source": source,
            "source_exact": source_exact,
            "source_type": row.get("source_type"),
            "doc_id": row.get("doc_id"),
            "ingest_label": ingest_label,
            "ingest_job_id": ingest_job_id,
            "ingested_at": ingested_at,
            "page_number": page_number,
            "page_total": row.get("page_total"),
            "page_label": page_label,
            "page_anchor": row.get("page_anchor"),
            "page_title": row.get("page_title"),
            "chunk_in_page": idx,
            "last_updated": row.get("last_updated") or row.get("lastUpdated"),
            "department": department,
            "text": chunk_text,
            "text_retrieval": chunk_text,
            "text_raw": chunk_text,
            "char_len": len(chunk_text),
            "char_len_raw": len(chunk_text),
            "n_turns": 1,
            "turn_start": 0,
            "turn_end": 0,
            "pii_tags": pii_tags,
            "retrieval_fallback_used": False,
            "relpath": row.get("relpath"),
        }
        for key in REPO_DOCS_METADATA_KEYS:
            chunk_row[key] = row.get(key)
        out_chunks.append(chunk_row)
    return out_chunks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", required=True, help="Page-aware raw_site JSONL")
    ap.add_argument("--out", required=True, help="Chunked raw_site JSONL")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as src:
        cfg = yaml.safe_load(src) or {}

    chunk_cfg = cfg.get("chunking", {})
    min_chars = int(chunk_cfg.get("min_chars", 400))
    target_chars = int(chunk_cfg.get("target_chars", 500))
    max_chars = int(chunk_cfg.get("max_chars", 600))
    min_keep_chars = int(chunk_cfg.get("min_keep_chars", 120))

    rows_out: List[Dict[str, Any]] = []
    with open(args.input, "r", encoding="utf-8") as src:
        for line in src:
            if not line.strip():
                continue
            row = json.loads(line)
            rows_out.extend(
                row_to_chunks(
                    row,
                    min_chars=min_chars,
                    target_chars=target_chars,
                    max_chars=max_chars,
                    min_keep_chars=min_keep_chars,
                )
            )

    rows_out.sort(
        key=lambda r: (
            str(r.get("source") or ""),
            int(r.get("page_number") or 0),
            int(r.get("chunk_in_page") or 0),
        )
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as dst:
        for row in rows_out:
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Raw-site chunking completed: input={args.input} out_rows={len(rows_out)} out={args.out}")


if __name__ == "__main__":
    main()
