#!/usr/bin/env python3
from __future__ import annotations

import re
import unicodedata
from typing import Optional


CANONICAL_DEPARTMENTS = (
    "aplicaciones",
    "sistemas",
    "bigdata",
    "general",
    "comunicaciones",
    "slurm",
)
DEPARTMENT_MIN_LENGTH = 3
DEPARTMENT_MAX_LENGTH = 32

_DEPARTMENT_ALIASES = {
    "aplicacion": "aplicaciones",
    "aplicacions": "aplicaciones",
    "aplicaciones": "aplicaciones",
    "aplicaiones": "aplicaciones",
    "application": "aplicaciones",
    "applications": "aplicaciones",
    "sistema": "sistemas",
    "sistemas": "sistemas",
    "system": "sistemas",
    "systems": "sistemas",
    "bigdata": "bigdata",
    "big_data": "bigdata",
    "big-data": "bigdata",
    "big data": "bigdata",
    "bd": "bigdata",
    "general": "general",
    "comunicacion": "comunicaciones",
    "comunicaciones": "comunicaciones",
    "comunicacions": "comunicaciones",
    "communication": "comunicaciones",
    "communications": "comunicaciones",
    "slurm": "slurm",
}
_SAFE_DEPARTMENT_RE = re.compile(
    rf"^[a-z0-9][a-z0-9_-]{{{DEPARTMENT_MIN_LENGTH - 1},{DEPARTMENT_MAX_LENGTH - 1}}}$"
)
_UNSAFE_RAW_DEPARTMENT_RE = re.compile(r"[^a-z0-9 _\-/]")


def _normalize_raw_department(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = unicodedata.normalize("NFKD", str(value))
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = raw.strip().lower()
    if not raw:
        return None
    normalized = _DEPARTMENT_ALIASES.get(raw)
    if normalized:
        return normalized

    sanitized = re.sub(r"[\s/]+", "_", raw)
    sanitized = re.sub(r"[^a-z0-9_-]", "", sanitized)
    sanitized = re.sub(r"[_-]{2,}", "_", sanitized)
    sanitized = sanitized.strip("_-")
    if not sanitized:
        return None
    return _DEPARTMENT_ALIASES.get(sanitized, sanitized)


def normalize_department(value: Optional[str], *, allow_unknown: bool = False) -> Optional[str]:
    normalized = _normalize_raw_department(value)
    if normalized is None:
        return None
    if normalized in CANONICAL_DEPARTMENTS:
        return normalized
    return normalized if allow_unknown else None


def is_valid_ingest_department(value: Optional[str]) -> bool:
    normalized = normalize_department(value, allow_unknown=True)
    return bool(normalized and _SAFE_DEPARTMENT_RE.fullmatch(normalized))


def validate_ingest_department(value: Optional[str]) -> str:
    if value is None:
        raise ValueError("department must not be empty")
    raw = unicodedata.normalize("NFKD", str(value))
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = raw.strip().lower()
    if not raw:
        raise ValueError("department must not be empty")
    if _UNSAFE_RAW_DEPARTMENT_RE.search(raw):
        raise ValueError(
            "department contains unsafe characters; allowed: letters, numbers, spaces, '_' and '-'"
        )

    normalized = normalize_department(raw, allow_unknown=True)
    if normalized is None:
        raise ValueError("department must not be empty")
    if not _SAFE_DEPARTMENT_RE.fullmatch(normalized):
        raise ValueError(
            "department must be 3-32 chars using lowercase ASCII letters, numbers, '_' or '-'"
        )
    return normalized


def normalize_department_filter(value: Optional[str]) -> Optional[str]:
    raw = (value or "all").strip().lower()
    if raw == "all":
        return "all"
    return normalize_department(raw, allow_unknown=False)


def normalize_search_department_filter(value: Optional[str]) -> Optional[str]:
    raw = (value or "all").strip()
    if raw.lower() == "all":
        return "all"
    try:
        return validate_ingest_department(raw)
    except ValueError:
        return None


def valid_department_values() -> tuple[str, ...]:
    return ("all",) + CANONICAL_DEPARTMENTS
