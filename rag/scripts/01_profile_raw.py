#!/usr/bin/env python3
import argparse
import json
import re
from collections import Counter
from pathlib import Path

EMAIL_RE = re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{6,}\d)(?!\w)")
SIGNATURE_RE = re.compile(r"(?i)(tel\.?|fax|e-?mail|campus sur|spain|aviso de confidencialidad)")
QUOTE_RE = re.compile(r"(^|\n)\s*>\s", re.M)
URL_RE = re.compile(r"https?://")


def ticket_id_from_link(link: str) -> str:
    m = re.search(r"id=(\d+)", link or "")
    return f"ticket_{m.group(1)}" if m else "ticket_unknown"


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    idx = int((len(values) - 1) * p)
    return values[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    conv_count = 0
    msg_count = 0
    conv_chars = []
    msg_chars = []
    msg_words = []
    msgs_per_conv = []
    noise = Counter()
    pii = Counter()

    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            conv_count += 1
            messages = row.get("messages", [])
            msgs_per_conv.append(len(messages))
            conv_text_parts = []
            for m in messages:
                content = (m.get("content") or "").replace("\r", "\n")
                msg_count += 1
                conv_text_parts.append(content)
                msg_chars.append(len(content))
                msg_words.append(len(content.split()))
                pii["email_hits"] += len(EMAIL_RE.findall(content))
                pii["phone_hits"] += len(PHONE_RE.findall(content))
                if SIGNATURE_RE.search(content):
                    noise["signature_chunks"] += 1
                if QUOTE_RE.search(content):
                    noise["quote_marked"] += 1
                if URL_RE.search(content):
                    noise["url_chunks"] += 1
            conv_text = "\n".join(conv_text_parts)
            conv_chars.append(len(conv_text))

    result = {
        "conversations": conv_count,
        "messages": msg_count,
        "avg_messages_per_conversation": round(sum(msgs_per_conv) / max(1, len(msgs_per_conv)), 3),
        "messages_per_conversation": {
            "min": min(msgs_per_conv) if msgs_per_conv else 0,
            "p50": pct(msgs_per_conv, 0.50),
            "p90": pct(msgs_per_conv, 0.90),
            "p99": pct(msgs_per_conv, 0.99),
            "max": max(msgs_per_conv) if msgs_per_conv else 0,
        },
        "message_char_len": {
            "min": min(msg_chars) if msg_chars else 0,
            "p50": pct(msg_chars, 0.50),
            "p90": pct(msg_chars, 0.90),
            "p99": pct(msg_chars, 0.99),
            "max": max(msg_chars) if msg_chars else 0,
        },
        "conversation_char_len": {
            "min": min(conv_chars) if conv_chars else 0,
            "p50": pct(conv_chars, 0.50),
            "p90": pct(conv_chars, 0.90),
            "p99": pct(conv_chars, 0.99),
            "max": max(conv_chars) if conv_chars else 0,
        },
        "noise": dict(noise),
        "pii": dict(pii),
    }

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    md = []
    md.append("# Raw Dataset Profile")
    md.append("")
    md.append(f"- Conversations: {result['conversations']}")
    md.append(f"- Messages: {result['messages']}")
    md.append(f"- Avg messages/conversation: {result['avg_messages_per_conversation']}")
    md.append(f"- Message char p50/p90/p99: {result['message_char_len']['p50']}/{result['message_char_len']['p90']}/{result['message_char_len']['p99']}")
    md.append(f"- Conversation char p50/p90/p99: {result['conversation_char_len']['p50']}/{result['conversation_char_len']['p90']}/{result['conversation_char_len']['p99']}")
    md.append("")
    md.append("## Noise")
    for k, v in sorted(result["noise"].items()):
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## PII")
    for k, v in sorted(result["pii"].items()):
        md.append(f"- {k}: {v}")

    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")


if __name__ == "__main__":
    main()
