#!/usr/bin/env python3
import argparse
import hashlib
import html
import json
import re
from pathlib import Path
from typing import Dict, List, Pattern, Tuple

import yaml

from common_department import normalize_department

# Keep only broad legal/confidentiality hints as hard-noise candidates.
# PII contact details (emails/phones/names) are intentionally preserved.
SIGNATURE_HINT_RE = re.compile(r"(?i)(aviso de confidencialidad)")
TECHNICAL_SIGNAL_RE = re.compile(
    r"(?i)(error|traceback|exception|failed|fatal|module|sbatch|python|\.so\b|/opt/|/home/|gcc|openmpi|cuda|conda|pip|make\b|cmake\b|ld:|undefined reference|segmentation fault|slurm|mpi)"
)
AUTO_TICKET_HEADER_CONTEXT_RE = re.compile(
    r"(?i)(?:greetings,\s*)?this message has been automatically generated in response to the creation of a trouble ticket regarding:"
)
HTML_BREAK_RE = re.compile(
    r"(?is)<\s*(?:br\s*/?|/p|/div|/li|/tr|/ul|/ol|/table|hr)\s*>|<\s*(?:p|div|li|tr|ul|ol|table)\b[^>]*>"
)
HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")
COMMENT_INLINE_SALUTATION_BREAK_RE = re.compile(
    r"(?i)([.!?])\s+(saludos,?|un saludo,?|best regards,?|kind regards,?)\b"
)
COMMENT_INLINE_SIGNATURE_BREAK_RE = re.compile(r"(?i)\s+--\s+(?=[A-ZÁÉÍÓÚÑ_])")
COMMENT_INLINE_CONTACT_BREAK_RE = re.compile(
    r"(?i)\s+(?=(?:tel\.?|phone|fax|e-?mail|avda\.\s*de\s+vigo|campus sur|santiago de compostela|spain)\b)"
)
TICKETFORMATER_OLD_MESSAGE_KEYWORDS = (
    "escribiu:\n",
    "escribió:\n",
    "wrote:\n",
    "De: ",
    "From: ",
)
TICKETFORMATER_CREATE_MESSAGE_KEYWORDS = (
    "helpdesk_aplicaciones@cesga.es\n",
    "helpdesk_sistemas@cesga.es\n",
    "* Descripcion:",
)
TICKETFORMATER_RT_URL_LINE_RE = re.compile(
    r"(?im)^\s*<URL:\s*https?://rt\.lan\.cesga\.es/Ticket/Display\.html\?id=\d+\s*>\s*$"
)
TICKETFORMATER_SEPARATOR_RE = re.compile(r"(?m)^\s*[-_]{3,}\s*$")


def ticket_id_from_link(link: str) -> str:
    m = re.search(r"id=(\d+)", link or "")
    if m:
        return f"conv_{m.group(1)}"
    digest = hashlib.sha1((link or "").encode("utf-8")).hexdigest()[:10]
    return f"conv_{digest}"


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _compile_patterns(patterns: List[str]) -> List[Pattern[str]]:
    compiled: List[Pattern[str]] = []
    for pat in patterns:
        if not pat:
            continue
        try:
            compiled.append(re.compile(pat, flags=re.I))
        except re.error:
            continue
    return compiled


def _base_noise_stats() -> Dict[str, int]:
    return {
        "noise_candidates": 0,
        "protected_technical": 0,
        "dropped_hard_noise": 0,
        "dropped_boilerplate": 0,
        "kept_tagged": 0,
        "phrase_cleanup_candidates": 0,
        "phrase_cleanup_replacements": 0,
        "phrase_cleanup_header_replacements": 0,
        "phrase_cleanup_removed_all_content": 0,
        "comment_role_html_normalized": 0,
        "comment_role_signature_lines_removed": 0,
        "comment_role_closure_replacements": 0,
        "comment_role_removed_all_content": 0,
        "ticketformater_quote_reply_trims": 0,
        "ticketformater_create_header_trims": 0,
        "ticketformater_rt_url_line_trims": 0,
        "ticketformater_separator_line_trims": 0,
    }


def _normalize_cleaning_mode(value: object) -> str:
    normalized = normalize_text(str(value or "")).lower()
    if normalized in {"autoreply", "auto_reply", "auto-reply", "autoreply_only", "autoreply-only"}:
        return "autoreply_only"
    return "full"


def _build_clean_runtime(cfg: dict) -> Dict[str, object]:
    cleaning_cfg = cfg.get("cleaning", {})
    comment_role_cleanup_cfg = dict(cleaning_cfg.get("comment_role_cleanup", {}) or {})
    ticket_phrase_cleanup_cfg = dict(cleaning_cfg.get("ticket_phrase_cleanup", {}) or {})
    input_normalization_cfg = cfg.get("input_normalization", {})
    cleaning_mode = _normalize_cleaning_mode(cleaning_cfg.get("mode", "full"))

    line_filter_enabled = bool(cleaning_cfg.get("line_filter_enabled", True))
    legacy_trim_enabled = bool(cleaning_cfg.get("legacy_trim_enabled", True))
    if cleaning_mode == "autoreply_only":
        comment_role_cleanup_cfg["enabled"] = False
        # Keep only auto-reply/ticket-header cleanup in this mode.
        ticket_phrase_cleanup_cfg["enabled"] = True
        line_filter_enabled = False
        legacy_trim_enabled = False

    return {
        "cleaning_mode": cleaning_mode,
        "message_passthrough": bool(cleaning_cfg.get("message_passthrough", False)),
        "line_filter_enabled": line_filter_enabled,
        "legacy_trim_enabled": legacy_trim_enabled,
        "protect_technical_lines": bool(cleaning_cfg.get("protect_technical_lines", True)),
        "boilerplate_action": str(cleaning_cfg.get("boilerplate_action", "tag")).strip().lower(),
        "drop_patterns": _compile_patterns(cleaning_cfg.get("drop_line_patterns", [])),
        "boilerplate_patterns": _compile_patterns(cleaning_cfg.get("boilerplate_line_patterns", [])),
        "comment_role_cleanup": {
            "enabled": bool(comment_role_cleanup_cfg.get("enabled", False)),
            "closure_phrase_patterns": _compile_patterns(comment_role_cleanup_cfg.get("closure_phrase_patterns", [])),
            "signature_line_patterns": _compile_patterns(comment_role_cleanup_cfg.get("signature_line_patterns", [])),
            "trivial_residual_patterns": _compile_patterns(comment_role_cleanup_cfg.get("trivial_residual_patterns", [])),
        },
        "input_normalization": {
            "passthrough_row_fields": [
                str(field).strip()
                for field in input_normalization_cfg.get("passthrough_row_fields", ["subject", "status"])
                if str(field).strip()
            ],
            "passthrough_message_roles": [
                str(role).strip().lower()
                for role in input_normalization_cfg.get(
                    "passthrough_message_roles",
                    ["user", "assistant", "comment"],
                )
                if str(role).strip()
            ],
        },
        "ticket_phrase_cleanup": {
            "enabled": bool(ticket_phrase_cleanup_cfg.get("enabled", False)),
            "hard_drop_patterns": _compile_patterns(ticket_phrase_cleanup_cfg.get("hard_drop_patterns", [])),
            "ticket_header_patterns": _compile_patterns(
                ticket_phrase_cleanup_cfg.get("ticket_header_patterns", [])
            ),
            "preserve_patterns": _compile_patterns(ticket_phrase_cleanup_cfg.get("preserve_patterns", [])),
        },
    }


def _line_has_technical_signal(line: str) -> bool:
    return bool(TECHNICAL_SIGNAL_RE.search(line or ""))


def _is_effectively_empty_after_hard_drop(text: str) -> bool:
    probe = text or ""
    probe = re.sub(r"(?i)<URL:\s*https?://[^>]+>", " ", probe)
    probe = re.sub(r"(?i)https?://\S+", " ", probe)
    probe = re.sub(r"\[[A-Z]+\]", " ", probe)
    probe = re.sub(r"[<>{}()\[\],.:;!?\-_/\\]+", " ", probe)
    probe = normalize_text(probe)
    return not probe


def _normalize_phrase_cleanup_text(text: str) -> str:
    cleaned = text or ""
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"^\s*[,.;:!?-]+\s*", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return normalize_text(cleaned)


def _apply_ticket_formater_legacy_trim(text: str, role: str) -> Tuple[str, Dict[str, int], List[str]]:
    # Mirror the historical heuristics in scriptsDescargaRAG/ticketFormater.py
    # to avoid indexing quoted-reply tails multiple times.
    if role == "comment":
        return text, {
            "quote_reply_trims": 0,
            "create_header_trims": 0,
            "rt_url_line_trims": 0,
            "separator_line_trims": 0,
        }, []

    cleaned = text or ""
    stats = {
        "quote_reply_trims": 0,
        "create_header_trims": 0,
        "rt_url_line_trims": 0,
        "separator_line_trims": 0,
    }
    flags: set[str] = set()

    for keyword in TICKETFORMATER_OLD_MESSAGE_KEYWORDS:
        if keyword not in cleaned:
            continue
        prefix = cleaned.split(keyword, 1)[0]
        prefix_lines = prefix.split("\n")
        cleaned = "\n".join(prefix_lines[:-1]) if prefix_lines else ""
        stats["quote_reply_trims"] += 1
        flags.add("ticketformater_reply_trim")

    for keyword in TICKETFORMATER_CREATE_MESSAGE_KEYWORDS:
        if keyword not in cleaned:
            continue
        tail = cleaned.split(keyword)[-1]
        cleaned = "\n".join(tail.split("\n")[1:])
        stats["create_header_trims"] += 1
        flags.add("ticketformater_create_trim")

    cleaned, url_replacements = TICKETFORMATER_RT_URL_LINE_RE.subn("", cleaned)
    if url_replacements:
        stats["rt_url_line_trims"] += url_replacements
        flags.add("ticketformater_url_trim")

    cleaned, separator_replacements = TICKETFORMATER_SEPARATOR_RE.subn("", cleaned)
    if separator_replacements:
        stats["separator_line_trims"] += separator_replacements
        flags.add("ticketformater_separator_trim")

    return normalize_text(cleaned), stats, sorted(flags)


def _normalize_comment_html_text(text: str) -> Tuple[str, int]:
    cleaned = html.unescape(text or "").replace("\xa0", " ")
    html_touched = 0

    cleaned, replacements = HTML_BREAK_RE.subn("\n", cleaned)
    html_touched += replacements
    cleaned, replacements = HTML_TAG_RE.subn(" ", cleaned)
    html_touched += replacements
    cleaned, replacements = COMMENT_INLINE_SALUTATION_BREAK_RE.subn(r"\1\n\2", cleaned)
    html_touched += replacements
    cleaned, replacements = COMMENT_INLINE_SIGNATURE_BREAK_RE.subn("\n--\n", cleaned)
    html_touched += replacements
    cleaned, replacements = COMMENT_INLINE_CONTACT_BREAK_RE.subn("\n", cleaned)
    html_touched += replacements

    return _normalize_phrase_cleanup_text(cleaned), html_touched


def _strip_trivial_comment_residuals(text: str, patterns: List[Pattern[str]]) -> Tuple[str, int]:
    kept: List[str] = []
    removed = 0
    for line in text.splitlines():
        normalized_line = normalize_text(line)
        if not normalized_line:
            continue
        if any(pattern.fullmatch(normalized_line) for pattern in patterns):
            removed += 1
            continue
        kept.append(normalized_line)
    return "\n".join(kept), removed


def _apply_comment_role_cleanup(text: str, role: str, runtime: Dict[str, object]) -> Tuple[str, Dict[str, int], List[str]]:
    cleanup_cfg = runtime.get("comment_role_cleanup", {})
    if role != "comment" or not isinstance(cleanup_cfg, dict):
        return text, {
            "enabled": 0,
            "html_normalized": 0,
            "signature_lines_removed": 0,
            "closure_replacements": 0,
            "removed_all_content": 0,
        }, []

    if not bool(cleanup_cfg.get("enabled", False)):
        return text, {
            "enabled": 0,
            "html_normalized": 0,
            "signature_lines_removed": 0,
            "closure_replacements": 0,
            "removed_all_content": 0,
        }, []

    signature_line_patterns: List[Pattern[str]] = cleanup_cfg.get("signature_line_patterns", [])  # type: ignore[assignment]
    closure_phrase_patterns: List[Pattern[str]] = cleanup_cfg.get("closure_phrase_patterns", [])  # type: ignore[assignment]
    trivial_residual_patterns: List[Pattern[str]] = cleanup_cfg.get("trivial_residual_patterns", [])  # type: ignore[assignment]

    stats = {
        "enabled": 1,
        "html_normalized": 0,
        "signature_lines_removed": 0,
        "closure_replacements": 0,
        "removed_all_content": 0,
    }
    flags: set[str] = {"comment_role_cleanup"}

    cleaned, html_touched = _normalize_comment_html_text(text)
    if html_touched > 0 or cleaned != normalize_text(text):
        stats["html_normalized"] = 1
        flags.add("comment_role_html")

    kept_lines: List[str] = []
    for line in cleaned.splitlines():
        normalized_line = normalize_text(line)
        if not normalized_line:
            continue
        if any(pattern.search(normalized_line) for pattern in signature_line_patterns):
            stats["signature_lines_removed"] += 1
            flags.add("comment_role_signature")
            continue
        kept_lines.append(normalized_line)
    cleaned = "\n".join(kept_lines)

    for pattern in closure_phrase_patterns:
        cleaned, replacements = pattern.subn(" ", cleaned)
        if replacements:
            stats["closure_replacements"] += replacements
            flags.add("comment_role_closure")

    cleaned = _normalize_phrase_cleanup_text(cleaned)

    if stats["closure_replacements"] > 0:
        cleaned, removed_trivial = _strip_trivial_comment_residuals(cleaned, trivial_residual_patterns)
        if removed_trivial:
            stats["closure_replacements"] += removed_trivial
            flags.add("comment_role_closure")
        cleaned = _normalize_phrase_cleanup_text(cleaned)

    if not cleaned:
        stats["removed_all_content"] = 1
        flags.add("comment_role_removed_all")
    elif stats["closure_replacements"] > 0 and not _line_has_technical_signal(cleaned) and len(cleaned.split()) <= 12:
        cleaned = ""
        stats["removed_all_content"] = 1
        flags.add("comment_role_removed_all")

    return cleaned, stats, sorted(flags)


def _extract_preserved_ticket_fragments(text: str, patterns: List[Pattern[str]]) -> List[str]:
    preserved: List[str] = []
    seen: set[str] = set()

    for pattern in patterns:
        for match in pattern.finditer(text or ""):
            fragment = normalize_text(match.group(0))
            if not fragment:
                continue
            lower_fragment = fragment.lower()
            if ":" in fragment and (
                lower_fragment.startswith("regarding:") or lower_fragment.startswith("subject:")
            ):
                fragment = normalize_text(fragment.split(":", 1)[1]).strip("\"' ")
            if not fragment:
                continue
            key = fragment.casefold()
            if key in seen:
                continue
            seen.add(key)
            preserved.append(fragment)

    return preserved


def _remove_leading_preserved_fragment(text: str, fragment: str) -> str:
    if not fragment:
        return text
    escaped = re.escape(fragment)
    patterns = (
        rf'^\s*"{escaped}"\s*,?\s*',
        rf"^\s*'{escaped}'\s*,?\s*",
        rf"(?i)^\s*subject:\s*{escaped}\s*,?\s*",
        rf"(?i)^\s*regarding:\s*\"?{escaped}\"?\s*,?\s*",
        rf"^\s*{escaped}\s*,?\s*",
    )
    cleaned = text
    for pattern in patterns:
        cleaned, replacements = re.subn(pattern, "", cleaned, count=1)
        if replacements:
            break
    return _normalize_phrase_cleanup_text(cleaned)


def _apply_ticket_phrase_cleanup(text: str, runtime: Dict[str, object]) -> Tuple[str, Dict[str, int], List[str]]:
    cleanup_cfg = runtime.get("ticket_phrase_cleanup", {})
    if not isinstance(cleanup_cfg, dict):
        return text, {
            "enabled": 0,
            "candidates": 0,
            "replacements": 0,
            "header_replacements": 0,
            "removed_all_content": 0,
        }, []

    if not bool(cleanup_cfg.get("enabled", False)):
        return text, {
            "enabled": 0,
            "candidates": 0,
            "replacements": 0,
            "header_replacements": 0,
            "removed_all_content": 0,
        }, []

    hard_drop_patterns: List[Pattern[str]] = cleanup_cfg.get("hard_drop_patterns", [])  # type: ignore[assignment]
    ticket_header_patterns: List[Pattern[str]] = cleanup_cfg.get("ticket_header_patterns", [])  # type: ignore[assignment]
    preserve_patterns: List[Pattern[str]] = cleanup_cfg.get("preserve_patterns", [])  # type: ignore[assignment]
    stats = {
        "enabled": 1,
        "candidates": 0,
        "replacements": 0,
        "header_replacements": 0,
        "removed_all_content": 0,
    }
    flags: set[str] = set()
    source_text = text or ""
    # Use a collapsed whitespace view for robust auto-header detection/rewrite
    # when forwarded messages split key phrases across line breaks/tabs.
    cleaned = re.sub(r"\s+", " ", source_text).strip()

    for pattern in hard_drop_patterns:
        cleaned, replacements = pattern.subn(" ", cleaned)
        if replacements:
            stats["candidates"] += replacements
            stats["replacements"] += replacements
            flags.add("ticket_phrase_hard_drop")

    if AUTO_TICKET_HEADER_CONTEXT_RE.search(cleaned):
        # Extract preserved subject/regarding fragments from the original text to
        # avoid over-capturing entire forwarded bodies after whitespace collapse.
        preserved_fragments = _extract_preserved_ticket_fragments(source_text, preserve_patterns)
        flags.add("ticket_phrase_header_context")
        for pattern in ticket_header_patterns:
            cleaned, replacements = pattern.subn(" ", cleaned)
            if replacements:
                stats["candidates"] += replacements
                stats["replacements"] += replacements
                stats["header_replacements"] += replacements
                flags.add("ticket_phrase_header")

        cleaned = _normalize_phrase_cleanup_text(cleaned)
        for fragment in preserved_fragments:
            cleaned = _remove_leading_preserved_fragment(cleaned, fragment)

        if preserved_fragments:
            parts = [
                f"Subject: {fragment}"
                for fragment in preserved_fragments
                if fragment.casefold() not in cleaned.casefold()
            ]
            if cleaned:
                parts.append(cleaned)
            cleaned = "\n".join(parts)

    cleaned = _normalize_phrase_cleanup_text(cleaned)
    if stats["replacements"] > 0 and _is_effectively_empty_after_hard_drop(cleaned):
        cleaned = ""
        stats["removed_all_content"] = 1
        flags.add("ticket_phrase_removed_all")

    return cleaned, stats, sorted(flags)


def _filter_noise_lines(text: str, runtime: Dict[str, object]) -> Tuple[str, Dict[str, int], List[str]]:
    stats = {
        "noise_candidates": 0,
        "protected_technical": 0,
        "dropped_hard_noise": 0,
        "dropped_boilerplate": 0,
        "kept_tagged": 0,
    }
    flags: set[str] = set()
    kept: List[str] = []
    action = runtime.get("boilerplate_action", "tag")
    if action not in {"tag", "drop"}:
        action = "tag"

    drop_patterns: List[Pattern[str]] = runtime.get("drop_patterns", [])  # type: ignore[assignment]
    boilerplate_patterns: List[Pattern[str]] = runtime.get("boilerplate_patterns", [])  # type: ignore[assignment]
    protect_technical = bool(runtime.get("protect_technical_lines", True))

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        reasons: List[str] = []
        if SIGNATURE_HINT_RE.search(line):
            reasons.append("signature_hint")
        if re.search(r"[\\/]{5,}|[_-]{6,}", line):
            reasons.append("separator_noise")
        if any(p.search(line) for p in drop_patterns):
            reasons.append("legacy_drop_pattern")
        if any(p.search(line) for p in boilerplate_patterns):
            reasons.append("boilerplate")

        if not reasons:
            kept.append(line)
            continue

        stats["noise_candidates"] += 1
        flags.update(reasons)

        if protect_technical and _line_has_technical_signal(line):
            stats["protected_technical"] += 1
            flags.add("protected_technical")
            kept.append(line)
            continue

        hard_noise = {"signature_hint", "separator_noise", "legacy_drop_pattern"}
        if any(reason in hard_noise for reason in reasons):
            stats["dropped_hard_noise"] += 1
            continue

        if "boilerplate" in reasons and action == "drop":
            stats["dropped_boilerplate"] += 1
            continue

        stats["kept_tagged"] += 1
        kept.append(line)

    return "\n".join(kept), stats, sorted(flags)


def _apply_pii(text: str, _cfg: dict) -> Tuple[str, Dict[str, int]]:
    # PII anonymization is intentionally disabled; keep original normalized content.
    return normalize_text(text), {"EMAIL": 0, "PHONE": 0, "PERSON": 0}


def _normalized_row_timestamp(row: dict) -> str | None:
    timestamp = normalize_text((row.get("lastUpdated") or row.get("last_updated") or ""))
    return timestamp or None


def _normalized_row_field(row: dict, field_name: str) -> str | None:
    value = normalize_text(row.get(field_name) or "")
    return value or None


def _normalized_message_role(role: object, runtime: Dict[str, object]) -> str:
    normalized = normalize_text(str(role or "")).lower() or "user"
    input_runtime = runtime.get("input_normalization", {})
    if not isinstance(input_runtime, dict):
        return normalized

    passthrough_roles = input_runtime.get("passthrough_message_roles", [])
    if isinstance(passthrough_roles, list) and normalized in passthrough_roles:
        return normalized
    return normalized


def clean_message(content: str, cfg: dict, runtime: Dict[str, object], role: str = "user"):
    if bool(runtime.get("message_passthrough", False)):
        text = normalize_text(content)
        noise_stats = _base_noise_stats()
        payload = {
            "content_clean": text,
            "content_retrieval": text,
            "content_raw": text,
            "noise_flags": [],
            "noise_stats": noise_stats,
            "retrieval_fallback_used": False,
        }
        return payload, {"EMAIL": 0, "PHONE": 0, "PERSON": 0}

    role_cleaned_text, role_stats, role_flags = _apply_comment_role_cleanup(
        normalize_text(content),
        role=role,
        runtime=runtime,
    )
    if bool(runtime.get("legacy_trim_enabled", True)):
        text_original, legacy_trim_stats, legacy_trim_flags = _apply_ticket_formater_legacy_trim(
            normalize_text(role_cleaned_text),
            role,
        )
    else:
        text_original = normalize_text(role_cleaned_text)
        legacy_trim_stats = {
            "quote_reply_trims": 0,
            "create_header_trims": 0,
            "rt_url_line_trims": 0,
            "separator_line_trims": 0,
        }
        legacy_trim_flags = []

    phrase_cleaned_text, phrase_stats, phrase_flags = _apply_ticket_phrase_cleanup(text_original, runtime)
    if bool(runtime.get("line_filter_enabled", True)):
        text_filtered, noise_stats, noise_flags = _filter_noise_lines(phrase_cleaned_text, runtime)
    else:
        text_filtered = normalize_text(phrase_cleaned_text)
        noise_stats = {
            "noise_candidates": 0,
            "protected_technical": 0,
            "dropped_hard_noise": 0,
            "dropped_boilerplate": 0,
            "kept_tagged": 0,
        }
        noise_flags = []

    suppress_raw_fallback = bool(phrase_stats.get("removed_all_content", 0) or role_stats.get("removed_all_content", 0))

    # text_raw keeps full normalized content for audits/fallbacks unless
    # phrase cleanup proved the message is pure noise and should not survive into chunking.
    text_raw_source = "" if suppress_raw_fallback else text_original
    text_raw, _ = _apply_pii(text_raw_source, cfg)
    # text_retrieval is the cleaned retrieval view.
    text_retrieval, pii_tags = _apply_pii(text_filtered, cfg)

    if phrase_stats.get("enabled"):
        noise_stats["phrase_cleanup_candidates"] = int(phrase_stats.get("candidates", 0))
        noise_stats["phrase_cleanup_replacements"] = int(phrase_stats.get("replacements", 0))
        noise_stats["phrase_cleanup_header_replacements"] = int(phrase_stats.get("header_replacements", 0))
        noise_stats["phrase_cleanup_removed_all_content"] = int(phrase_stats.get("removed_all_content", 0))
    else:
        noise_stats["phrase_cleanup_candidates"] = 0
        noise_stats["phrase_cleanup_replacements"] = 0
        noise_stats["phrase_cleanup_header_replacements"] = 0
        noise_stats["phrase_cleanup_removed_all_content"] = 0

    if role_stats.get("enabled"):
        noise_stats["comment_role_html_normalized"] = int(role_stats.get("html_normalized", 0))
        noise_stats["comment_role_signature_lines_removed"] = int(role_stats.get("signature_lines_removed", 0))
        noise_stats["comment_role_closure_replacements"] = int(role_stats.get("closure_replacements", 0))
        noise_stats["comment_role_removed_all_content"] = int(role_stats.get("removed_all_content", 0))
    else:
        noise_stats["comment_role_html_normalized"] = 0
        noise_stats["comment_role_signature_lines_removed"] = 0
        noise_stats["comment_role_closure_replacements"] = 0
        noise_stats["comment_role_removed_all_content"] = 0
    noise_stats["ticketformater_quote_reply_trims"] = int(legacy_trim_stats.get("quote_reply_trims", 0))
    noise_stats["ticketformater_create_header_trims"] = int(legacy_trim_stats.get("create_header_trims", 0))
    noise_stats["ticketformater_rt_url_line_trims"] = int(legacy_trim_stats.get("rt_url_line_trims", 0))
    noise_stats["ticketformater_separator_line_trims"] = int(legacy_trim_stats.get("separator_line_trims", 0))

    merged_flags = sorted(set(noise_flags) | set(phrase_flags) | set(role_flags) | set(legacy_trim_flags))

    # Safety net: if cleaning over-prunes retrieval text, fallback to raw.
    retrieval_fallback_used = False
    if not text_retrieval and text_raw and not suppress_raw_fallback:
        text_retrieval = text_raw
        retrieval_fallback_used = True

    payload = {
        "content_clean": text_retrieval,
        "content_retrieval": text_retrieval,
        "content_raw": text_raw,
        "noise_flags": merged_flags,
        "noise_stats": noise_stats,
        "retrieval_fallback_used": retrieval_fallback_used,
    }
    return payload, pii_tags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--department", default=None)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    runtime = _build_clean_runtime(cfg)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    with open(args.input, "r", encoding="utf-8") as src, open(args.out, "w", encoding="utf-8") as dst:
        for line in src:
            if not line.strip():
                continue
            row = json.loads(line)
            source = row.get("link", "")
            conv_id = ticket_id_from_link(source)
            messages = row.get("messages", [])
            input_runtime = runtime.get("input_normalization", {})
            passthrough_row_fields = []
            if isinstance(input_runtime, dict):
                passthrough_row_fields = list(input_runtime.get("passthrough_row_fields", []))

            out_messages = []
            row_subject = _normalized_row_field(row, "subject")
            total_tags = {"EMAIL": 0, "PHONE": 0, "PERSON": 0}
            noise_totals = {
                "empty_messages_skipped": 0,
                "messages_with_noise": 0,
                "noise_candidates": 0,
                "protected_technical": 0,
                "dropped_hard_noise": 0,
                "dropped_boilerplate": 0,
                "kept_tagged": 0,
                "phrase_cleanup_candidates": 0,
                "phrase_cleanup_replacements": 0,
                "phrase_cleanup_header_replacements": 0,
                "phrase_cleanup_removed_all_content": 0,
                "comment_role_html_normalized": 0,
                "comment_role_signature_lines_removed": 0,
                "comment_role_closure_replacements": 0,
                "comment_role_removed_all_content": 0,
                "ticketformater_quote_reply_trims": 0,
                "ticketformater_create_header_trims": 0,
                "ticketformater_rt_url_line_trims": 0,
                "ticketformater_separator_line_trims": 0,
                "retrieval_fallback_used": 0,
            }
            for m in messages:
                role = _normalized_message_role(m.get("role"), runtime)
                content = normalize_text(m.get("content") or "")
                if not content:
                    noise_totals["empty_messages_skipped"] += 1
                    continue
                payload, tags = clean_message(content, cfg, runtime, role=role)
                if not payload.get("content_retrieval") and not payload.get("content_raw"):
                    continue
                noise_stats = payload.get("noise_stats") or {}
                noise_flags = payload.get("noise_flags") or []
                if noise_flags:
                    noise_totals["messages_with_noise"] += 1
                for key in (
                    "noise_candidates",
                    "protected_technical",
                    "dropped_hard_noise",
                    "dropped_boilerplate",
                    "kept_tagged",
                    "phrase_cleanup_candidates",
                    "phrase_cleanup_replacements",
                    "phrase_cleanup_header_replacements",
                    "phrase_cleanup_removed_all_content",
                    "comment_role_html_normalized",
                    "comment_role_signature_lines_removed",
                    "comment_role_closure_replacements",
                    "comment_role_removed_all_content",
                    "ticketformater_quote_reply_trims",
                    "ticketformater_create_header_trims",
                    "ticketformater_rt_url_line_trims",
                    "ticketformater_separator_line_trims",
                ):
                    noise_totals[key] += int(noise_stats.get(key, 0))
                if payload.get("retrieval_fallback_used"):
                    noise_totals["retrieval_fallback_used"] += 1

                out_messages.append({"role": role, **payload})
                for k in total_tags:
                    total_tags[k] += tags[k]

            out_row = {
                "conversation_id": conv_id,
                "source": source,
                "last_updated": _normalized_row_timestamp(row),
                "department": normalize_department(args.department or row.get("department"), allow_unknown=True),
                "messages": out_messages,
                "stats": {
                    "n_messages": len(out_messages),
                    "pii_tags": total_tags,
                    "noise": noise_totals,
                },
            }
            for field_name in passthrough_row_fields:
                out_row[field_name] = _normalized_row_field(row, field_name)
            if not out_messages and not row_subject:
                continue
            dst.write(json.dumps(out_row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
