#!/usr/bin/env python3
import argparse
import json
import re
from collections import Counter
from pathlib import Path

import yaml

EMAIL_RE = re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{6,}\d)(?!\w)")
SIGNATURE_RE = re.compile(r"(?i)(tel\.?|fax|campus sur|spain|aviso de confidencialidad)")


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    i = int((len(values) - 1) * p)
    return values[i]


def parse_optional_int(value):
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw or raw in {"none", "null"}:
        return None
    return int(value)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/preprocess.yaml")
    ap.add_argument("--input", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    thresholds = cfg.get("quality_thresholds", {})
    c_cfg = cfg.get("chunking", {})
    min_chars = int(c_cfg.get("min_chars", 400))
    max_chars = int(c_cfg.get("max_chars", 600))

    lens = []
    conv_counter = Counter()
    email_leak = 0
    phone_leak = 0
    signature_hits = 0
    tiny = 0
    in_range = 0
    total = 0

    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            total += 1
            txt = row.get("text", "")
            ln = len(txt)
            lens.append(ln)
            conv_counter[row.get("conversation_id", "unknown")] += 1
            if ln < 120:
                tiny += 1
            if min_chars <= ln <= max_chars:
                in_range += 1
            email_leak += len(EMAIL_RE.findall(txt))
            phone_leak += len(PHONE_RE.findall(txt))
            if SIGNATURE_RE.search(txt):
                signature_hits += 1

    in_range_pct = (in_range / total) if total else 0
    tiny_pct = (tiny / total) if total else 0
    max_email_leak = parse_optional_int(thresholds.get("max_email_leak"))
    max_phone_leak = parse_optional_int(thresholds.get("max_phone_leak"))

    checks = {
        "chunks_in_target_range": in_range_pct >= float(thresholds.get("min_in_range_pct", 0.90)),
        "tiny_chunks_ok": tiny_pct <= float(thresholds.get("max_tiny_chunk_pct", 0.02)),
        "email_leak_ok": True if max_email_leak is None else email_leak <= max_email_leak,
        "phone_leak_ok": True if max_phone_leak is None else phone_leak <= max_phone_leak,
    }

    result = {
        "total_chunks": total,
        "unique_conversations": len(conv_counter),
        "chunks_per_conversation": {
            "min": min(conv_counter.values()) if conv_counter else 0,
            "p50": pct(list(conv_counter.values()), 0.50),
            "p90": pct(list(conv_counter.values()), 0.90),
            "max": max(conv_counter.values()) if conv_counter else 0,
        },
        "char_len": {
            "min": min(lens) if lens else 0,
            "p50": pct(lens, 0.50),
            "p90": pct(lens, 0.90),
            "p99": pct(lens, 0.99),
            "max": max(lens) if lens else 0,
        },
        "ratios": {
            "in_range_pct": round(in_range_pct, 4),
            "tiny_chunk_pct": round(tiny_pct, 4),
        },
        "leaks": {
            "email_leak": email_leak,
            "phone_leak": phone_leak,
            "signature_hits": signature_hits,
        },
        "checks": checks,
        "all_pass": all(checks.values()),
        "thresholds": {
            "max_email_leak": max_email_leak,
            "max_phone_leak": max_phone_leak,
        },
    }

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    lines = [
        "# Chunk Quality Report",
        "",
        f"- Total chunks: {result['total_chunks']}",
        f"- Unique conversations: {result['unique_conversations']}",
        f"- In-range [{min_chars},{max_chars}] ratio: {result['ratios']['in_range_pct']:.2%}",
        f"- Tiny chunks (<120) ratio: {result['ratios']['tiny_chunk_pct']:.2%}",
        f"- Email leaks: {result['leaks']['email_leak']} (max: {result['thresholds']['max_email_leak']})",
        f"- Phone leaks: {result['leaks']['phone_leak']} (max: {result['thresholds']['max_phone_leak']})",
        f"- Signature hits: {result['leaks']['signature_hits']}",
        "",
        "## Checks",
    ]
    for k, v in checks.items():
        lines.append(f"- {k}: {'PASS' if v else 'FAIL'}")
    lines.append(f"- all_pass: {'PASS' if result['all_pass'] else 'FAIL'}")

    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
