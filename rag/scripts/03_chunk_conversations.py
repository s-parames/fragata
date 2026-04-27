#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

import yaml

TECHNICAL_RE = re.compile(
    r"(?i)(error|traceback|exception|failed|fatal|module|sbatch|python|\.so\b|/opt/|/home/|gcc|openmpi|cuda|conda|pip)"
)


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_long_text(text: str, max_chars: int):
    text = normalize_text(text)
    if len(text) <= max_chars:
        return [text]

    parts = []
    buffer = ""
    for para in text.split("\n\n"):
        para = para.strip()
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
        # Fallback: hard split paragraph
        start = 0
        while start < len(para):
            end = min(start + max_chars, len(para))
            parts.append(para[start:end].strip())
            start = end
    if buffer:
        parts.append(buffer)
    return [p for p in parts if p]


def has_high_value_signal(text: str) -> bool:
    return bool(TECHNICAL_RE.search(text or ""))


def align_secondary_parts(primary_parts: List[str], secondary_parts: List[str]) -> List[str]:
    if not primary_parts:
        return list(secondary_parts)

    primary_count = len(primary_parts)
    if len(secondary_parts) <= primary_count:
        return list(secondary_parts)

    if primary_count == 1:
        return [normalize_text("\n".join(secondary_parts))]

    aligned = list(secondary_parts[: primary_count - 1])
    aligned.append(normalize_text("\n".join(secondary_parts[primary_count - 1 :])))
    return aligned


def canonical_message_text(message: dict, primary_field: str, fallback_fields: List[str]) -> str:
    for field_name in [primary_field, *fallback_fields]:
        value = normalize_text(message.get(field_name) or "")
        if value:
            return value
    return ""


def is_adjacent_duplicate_turn(prev_turn: Dict[str, str], next_turn: Dict[str, str]) -> bool:
    if not prev_turn or not next_turn:
        return False
    return (
        normalize_text(prev_turn.get("retrieval", "")) == normalize_text(next_turn.get("retrieval", ""))
        and normalize_text(prev_turn.get("raw", "")) == normalize_text(next_turn.get("raw", ""))
    )


def normalized_row_timestamp(row: dict) -> str | None:
    timestamp = normalize_text((row.get("last_updated") or row.get("lastUpdated") or ""))
    return timestamp or None


def normalized_optional_row_field(row: dict, field_name: str) -> str | None:
    value = normalize_text(row.get(field_name) or "")
    return value or None


def emit_chunk(
    chunks: List[Dict],
    conv_id: str,
    source: str,
    last_updated: str,
    department: str,
    subject: str | None,
    status: str | None,
    turns: List[Dict[str, object]],
    chunk_idx: int,
) -> int:
    text_retrieval = normalize_text(
        "\n".join(str(t.get("retrieval", "")) for t in turns if str(t.get("retrieval", "")).strip())
    )
    text_raw = normalize_text("\n".join(str(t.get("raw", "")) for t in turns if str(t.get("raw", "")).strip()))
    retrieval_fallback_used = any(bool(t.get("fallback")) for t in turns)

    if not text_retrieval and text_raw:
        text_retrieval = text_raw
        retrieval_fallback_used = True
    if not text_raw and text_retrieval:
        text_raw = text_retrieval

    if not text_retrieval:
        return chunk_idx

    chunks.append(
        {
            "chunk_id": f"{conv_id}_chunk_{chunk_idx:03d}",
            "conversation_id": conv_id,
            "source": source,
            "last_updated": last_updated,
            "department": department,
            "subject": subject,
            "status": status,
            "text": text_retrieval,
            "text_retrieval": text_retrieval,
            "text_raw": text_raw,
            "char_len": len(text_retrieval),
            "char_len_raw": len(text_raw),
            "n_turns": len(turns),
            "retrieval_fallback_used": retrieval_fallback_used,
        }
    )
    return chunk_idx + 1


def build_chunks_for_conversation(row, min_chars, target_chars, max_chars, overlap_turns, min_keep_chars):
    conv_id = row["conversation_id"]
    source = row.get("source", "")
    last_updated = normalized_row_timestamp(row)
    department = row.get("department")
    subject = normalized_optional_row_field(row, "subject")
    status = normalized_optional_row_field(row, "status")
    messages = row.get("messages", [])

    turns: List[Dict[str, str]] = []
    for m in messages:
        role = (m.get("role") or "user").strip().lower()
        label = "User" if role == "user" else "Assistant"
        content_retrieval = canonical_message_text(
            m,
            primary_field="content_retrieval",
            fallback_fields=["content_clean", "content_raw"],
        )
        content_raw = canonical_message_text(
            m,
            primary_field="content_raw",
            fallback_fields=["content_retrieval", "content_clean"],
        )
        if not content_raw and not content_retrieval:
            continue
        if not content_raw:
            content_raw = content_retrieval
        if not content_retrieval:
            content_retrieval = content_raw

        next_turn = {
            "raw": f"{label}: {content_raw}",
            "retrieval": f"{label}: {content_retrieval}",
        }
        if turns and is_adjacent_duplicate_turn(turns[-1], next_turn):
            continue
        turns.append(next_turn)

    chunks: List[Dict] = []
    current_turns: List[Dict[str, object]] = []
    current_len = 0
    chunk_idx = 0

    for turn in turns:
        retrieval_parts = split_long_text(turn["retrieval"], max_chars)
        raw_parts = split_long_text(turn["raw"], max_chars)
        aligned_raw_parts = align_secondary_parts(retrieval_parts, raw_parts)
        n_parts = len(retrieval_parts) if retrieval_parts else len(raw_parts)
        for i in range(n_parts):
            retrieval_piece = retrieval_parts[i] if i < len(retrieval_parts) else ""
            raw_piece = aligned_raw_parts[i] if i < len(aligned_raw_parts) else ""
            fallback_piece = False

            if not retrieval_piece and raw_piece:
                retrieval_piece = raw_piece
                fallback_piece = True
            if not raw_piece and retrieval_piece:
                raw_piece = retrieval_piece

            if not retrieval_piece and not raw_piece:
                continue

            piece_len = len(retrieval_piece) + (1 if current_turns else 0)
            if current_turns and current_len + piece_len > max_chars:
                chunk_idx = emit_chunk(
                    chunks, conv_id, source, last_updated, department, subject, status, current_turns, chunk_idx
                )
                carry = current_turns[-overlap_turns:] if overlap_turns > 0 else []
                current_turns = list(carry)
                current_len = len(
                    "\n".join(
                        str(t.get("retrieval", "")) for t in current_turns if str(t.get("retrieval", "")).strip()
                    )
                ) if current_turns else 0
                if current_turns and current_len + len(retrieval_piece) + 1 > max_chars:
                    current_turns = []
                    current_len = 0

            current_turns.append(
                {"raw": raw_piece, "retrieval": retrieval_piece, "fallback": fallback_piece}
            )
            current_len = len(
                "\n".join(str(t.get("retrieval", "")) for t in current_turns if str(t.get("retrieval", "")).strip())
            )

            if min_chars <= current_len <= max_chars and current_len >= target_chars:
                chunk_idx = emit_chunk(
                    chunks, conv_id, source, last_updated, department, subject, status, current_turns, chunk_idx
                )
                # Flush completed chunk without carry, otherwise the same text can be emitted twice
                # when the loop reaches end-of-turn and the final flush runs.
                current_turns = []
                current_len = 0

    if current_turns:
        chunk_idx = emit_chunk(
            chunks, conv_id, source, last_updated, department, subject, status, current_turns, chunk_idx
        )

    bounded = []
    bounded_idx = 0
    for c in chunks:
        if c["char_len"] <= max_chars:
            c["chunk_id"] = f"{conv_id}_chunk_{bounded_idx:03d}"
            bounded_idx += 1
            bounded.append(c)
            continue

        retrieval_parts = split_long_text(c.get("text_retrieval") or c.get("text") or "", max_chars)
        raw_parts = split_long_text(c.get("text_raw") or c.get("text_retrieval") or c.get("text") or "", max_chars)
        aligned_raw_parts = align_secondary_parts(retrieval_parts, raw_parts)
        n_parts = len(retrieval_parts) if retrieval_parts else len(raw_parts)
        for i in range(n_parts):
            retrieval_part = normalize_text(retrieval_parts[i] if i < len(retrieval_parts) else "")
            raw_part = normalize_text(aligned_raw_parts[i] if i < len(aligned_raw_parts) else "")
            fallback_part = False
            if not retrieval_part and raw_part:
                retrieval_part = raw_part
                fallback_part = True
            if not raw_part and retrieval_part:
                raw_part = retrieval_part
            if not retrieval_part:
                continue
            bounded.append(
                {
                    "chunk_id": f"{conv_id}_chunk_{bounded_idx:03d}",
                    "conversation_id": conv_id,
                    "source": source,
                    "last_updated": last_updated,
                    "department": department,
                    "subject": subject,
                    "status": status,
                    "text": retrieval_part,
                    "text_retrieval": retrieval_part,
                    "text_raw": raw_part,
                    "char_len": len(retrieval_part),
                    "char_len_raw": len(raw_part),
                    "n_turns": c["n_turns"],
                    "retrieval_fallback_used": bool(c.get("retrieval_fallback_used")) or fallback_part,
                }
            )
            bounded_idx += 1

    rebalanced = []
    for c in bounded:
        if (
            rebalanced
            and c["char_len"] < min_chars
            and rebalanced[-1]["char_len"] + 1 + c["char_len"] <= max_chars
        ):
            # Merge short trailing chunks into the previous chunk to keep more chunks in range.
            merged_retrieval = normalize_text(
                (rebalanced[-1].get("text_retrieval") or rebalanced[-1].get("text") or "")
                + "\n"
                + (c.get("text_retrieval") or c.get("text") or "")
            )
            merged_raw = normalize_text((rebalanced[-1].get("text_raw") or "") + "\n" + (c.get("text_raw") or ""))
            if not merged_raw:
                merged_raw = merged_retrieval

            rebalanced[-1]["text"] = merged_retrieval
            rebalanced[-1]["text_retrieval"] = merged_retrieval
            rebalanced[-1]["text_raw"] = merged_raw
            rebalanced[-1]["char_len"] = len(merged_retrieval)
            rebalanced[-1]["char_len_raw"] = len(merged_raw)
            rebalanced[-1]["n_turns"] = rebalanced[-1]["n_turns"] + c["n_turns"]
            rebalanced[-1]["retrieval_fallback_used"] = bool(rebalanced[-1].get("retrieval_fallback_used")) or bool(
                c.get("retrieval_fallback_used")
            )
        else:
            rebalanced.append(c)

    if (
        len(rebalanced) >= 2
        and rebalanced[-1]["char_len"] < min_chars
        and rebalanced[-2]["char_len"] + 1 + rebalanced[-1]["char_len"] <= max_chars
    ):
        merged_retrieval = normalize_text(
            (rebalanced[-2].get("text_retrieval") or rebalanced[-2].get("text") or "")
            + "\n"
            + (rebalanced[-1].get("text_retrieval") or rebalanced[-1].get("text") or "")
        )
        merged_raw = normalize_text((rebalanced[-2].get("text_raw") or "") + "\n" + (rebalanced[-1].get("text_raw") or ""))
        if not merged_raw:
            merged_raw = merged_retrieval

        rebalanced[-2]["text"] = merged_retrieval
        rebalanced[-2]["text_retrieval"] = merged_retrieval
        rebalanced[-2]["text_raw"] = merged_raw
        rebalanced[-2]["char_len"] = len(merged_retrieval)
        rebalanced[-2]["char_len_raw"] = len(merged_raw)
        rebalanced[-2]["n_turns"] = rebalanced[-2]["n_turns"] + rebalanced[-1]["n_turns"]
        rebalanced[-2]["retrieval_fallback_used"] = bool(rebalanced[-2].get("retrieval_fallback_used")) or bool(
            rebalanced[-1].get("retrieval_fallback_used")
        )
        rebalanced.pop()

    # Remove exact duplicates inside the same conversation while preserving order.
    deduped = []
    seen_texts = set()
    for c in rebalanced:
        key = normalize_text(c.get("text_retrieval") or c.get("text") or "")
        if key in seen_texts:
            continue
        seen_texts.add(key)
        deduped.append(c)

    filtered = []
    for c in deduped:
        text_for_quality = c.get("text_retrieval") or c.get("text") or ""
        if c["char_len"] >= min_keep_chars or has_high_value_signal(text_for_quality):
            filtered.append(c)
    if not filtered and deduped:
        filtered = deduped[-1:]

    # Final safety net: retrieval text should never be empty if raw exists.
    for c in filtered:
        retrieval_text = normalize_text(c.get("text_retrieval") or c.get("text") or "")
        raw_text = normalize_text(c.get("text_raw") or "")
        if not retrieval_text and raw_text:
            retrieval_text = raw_text
            c["retrieval_fallback_used"] = True
        if not raw_text and retrieval_text:
            raw_text = retrieval_text
        c["text"] = retrieval_text
        c["text_retrieval"] = retrieval_text
        c["text_raw"] = raw_text
        c["char_len"] = len(retrieval_text)
        c["char_len_raw"] = len(raw_text)
    return filtered


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    c = cfg["chunking"]
    min_chars = int(c["min_chars"])
    target_chars = int(c["target_chars"])
    max_chars = int(c["max_chars"])
    overlap_turns = int(c["overlap_turns"])
    min_keep_chars = int(c["min_keep_chars"])

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.input, "r", encoding="utf-8") as src, open(args.out, "w", encoding="utf-8") as dst:
        for line in src:
            if not line.strip():
                continue
            row = json.loads(line)
            chunks = build_chunks_for_conversation(
                row, min_chars, target_chars, max_chars, overlap_turns, min_keep_chars
            )
            for i, chunk in enumerate(chunks):
                chunk["turn_start"] = 0
                chunk["turn_end"] = chunk["n_turns"] - 1
                chunk["pii_tags"] = row.get("stats", {}).get("pii_tags", {})
                dst.write(json.dumps(chunk, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
