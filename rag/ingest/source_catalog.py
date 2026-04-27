from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

import yaml


DEFAULT_CATALOG_RELATIVE_PATH = Path("data") / "reports" / "catalog" / "source_catalog.json"
VALID_SOURCE_CATALOG_TYPES = ("ticket", "web", "pdf")


@dataclass(frozen=True)
class SourceCatalogEntry:
    catalog_id: str
    source_type: str
    department: str
    source: str
    host: Optional[str]
    title: Optional[str]
    chunk_count: int
    last_updated: Optional[str]
    ingest_job_id: Optional[str]
    ingested_at: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SourceCatalogSnapshot:
    dataset_path: str
    catalog_path: str
    generated_at: str
    dataset_mtime_ns: Optional[int]
    dataset_size_bytes: Optional[int]
    total_entries: int
    items: List[SourceCatalogEntry]

    def to_dict(self) -> dict:
        return {
            "dataset_path": self.dataset_path,
            "catalog_path": self.catalog_path,
            "generated_at": self.generated_at,
            "dataset_mtime_ns": self.dataset_mtime_ns,
            "dataset_size_bytes": self.dataset_size_bytes,
            "total_entries": self.total_entries,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class SourceCatalogQueryResult:
    total: int
    page: int
    page_size: int
    has_more: bool
    items: List[SourceCatalogEntry]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "page": self.page,
            "page_size": self.page_size,
            "has_more": self.has_more,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class SourceCatalogOverviewItem:
    overview_id: str
    source_type: str
    label: str
    description: Optional[str]
    source: Optional[str]
    host: Optional[str]
    total_documents: int
    total_chunks: int
    department_count: int
    last_updated: Optional[str]
    ingested_at: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SourceCatalogOverviewBucket:
    source_type: str
    total: int
    items: List[SourceCatalogOverviewItem]

    def to_dict(self) -> dict:
        return {
            "source_type": self.source_type,
            "total": self.total,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class SourceCatalogOverview:
    total: int
    limit_per_type: int
    buckets: List[SourceCatalogOverviewBucket]

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "limit_per_type": self.limit_per_type,
            "buckets": [bucket.to_dict() for bucket in self.buckets],
        }


@dataclass(frozen=True)
class SourceCatalogTreeChild:
    child_id: str
    catalog_id: str
    source_type: str
    title: str
    source: str
    host: Optional[str]
    path: Optional[str]
    department: str
    chunk_count: int
    last_updated: Optional[str]
    ingested_at: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SourceCatalogTreeGroup:
    group_id: str
    label: str
    description: Optional[str]
    host: Optional[str]
    total_children: int
    total_chunks: int
    web_count: int
    pdf_count: int
    ticket_count: int
    children: List[SourceCatalogTreeChild]

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "label": self.label,
            "description": self.description,
            "host": self.host,
            "total_children": self.total_children,
            "total_chunks": self.total_chunks,
            "web_count": self.web_count,
            "pdf_count": self.pdf_count,
            "ticket_count": self.ticket_count,
            "children": [child.to_dict() for child in self.children],
        }


@dataclass(frozen=True)
class SourceCatalogTree:
    total_groups: int
    total_items: int
    groups: List[SourceCatalogTreeGroup]

    def to_dict(self) -> dict:
        return {
            "total_groups": self.total_groups,
            "total_items": self.total_items,
            "groups": [group.to_dict() for group in self.groups],
        }


def _clean_text(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    return raw or None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _config_root(config_path: Path) -> Path:
    resolved = config_path.resolve(strict=False)
    if resolved.parent.name == "config":
        return resolved.parent.parent
    return resolved.parent


def resolve_dataset_path(
    config_path: str | Path,
    *,
    dataset_path_override: str | Path | None = None,
) -> Path:
    if dataset_path_override is not None:
        path = Path(dataset_path_override)
        if not path.is_absolute():
            path = _config_root(Path(config_path)) / path
        return path.resolve(strict=False)

    cfg_path = Path(config_path)
    with cfg_path.open("r", encoding="utf-8") as src:
        payload = yaml.safe_load(src) or {}
    retrieval = payload.get("retrieval") or {}
    raw_value = str(retrieval.get("dataset_path") or "data/datasetFinalV2.jsonl").strip()
    path = Path(raw_value)
    if not path.is_absolute():
        path = _config_root(cfg_path) / path
    return path.resolve(strict=False)


def resolve_catalog_path(
    config_path: str | Path,
    *,
    catalog_path_override: str | Path | None = None,
) -> Path:
    if catalog_path_override is not None:
        path = Path(catalog_path_override)
        if not path.is_absolute():
            path = _config_root(Path(config_path)) / path
        return path.resolve(strict=False)

    return (_config_root(Path(config_path)) / DEFAULT_CATALOG_RELATIVE_PATH).resolve(strict=False)


def _iter_dataset_rows(dataset_path: Path) -> Iterable[Dict[str, Any]]:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {dataset_path}")
    if not dataset_path.is_file():
        raise ValueError(f"Dataset path must be a file: {dataset_path}")

    with dataset_path.open("r", encoding="utf-8") as src:
        for line_no, line in enumerate(src, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{dataset_path}:{line_no}: invalid JSON ({exc.msg})") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{dataset_path}:{line_no}: expected JSON object rows")
            yield row


def _normalize_source_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"", "ticket", "tickets", "conversation"}:
        return "ticket"
    if raw in {"html", "web", "website"}:
        return "web"
    if raw == "pdf":
        return "pdf"
    return raw or "ticket"


def normalize_source_catalog_source_type(value: Any) -> Optional[str]:
    raw = _clean_text(value)
    if raw is None:
        return None
    logical = _normalize_source_type(raw)
    if logical not in VALID_SOURCE_CATALOG_TYPES:
        return None
    return logical


def _normalized_timestamp(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    iso_raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_raw)
    except ValueError:
        return raw
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _timestamp_sort_key(value: Any) -> tuple[int, str]:
    normalized = _normalized_timestamp(value)
    if not normalized:
        return (0, "")
    return (1, normalized)


def _best_timestamp(*values: Any) -> Optional[str]:
    best = None
    best_key = (0, "")
    for value in values:
        key = _timestamp_sort_key(value)
        if key > best_key:
            best_key = key
            best = _normalized_timestamp(value)
    return best


def _host_from_source(source: str) -> Optional[str]:
    parsed = urlparse(source)
    host = (parsed.hostname or "").strip().lower()
    return host or None


def _path_from_source(source: str) -> Optional[str]:
    parsed = urlparse(source)
    path = (parsed.path or "").strip()
    return path or None


def _display_label_from_host(host: Optional[str]) -> Optional[str]:
    raw_host = str(host or "").strip().lower()
    if not raw_host:
        return None
    if raw_host.startswith("www."):
        raw_host = raw_host[4:]
    token = raw_host.split(".", 1)[0].strip()
    if not token:
        return raw_host
    parts = [part for part in token.replace("-", " ").replace("_", " ").split() if part]
    if not parts:
        return raw_host
    return " ".join(part.upper() if len(part) <= 3 else part.capitalize() for part in parts)


def _title_candidate(logical_source_type: str, row: Dict[str, Any]) -> tuple[int, Optional[str]]:
    subject = _clean_text(row.get("subject"))
    page_title = _clean_text(row.get("page_title"))
    source = _clean_text(row.get("source"))

    if logical_source_type == "ticket":
        ordered = [subject, page_title, source]
    else:
        ordered = [page_title, subject, source]

    for idx, candidate in enumerate(ordered):
        if candidate:
            return idx, candidate
    return len(ordered), None


def _group_identity(logical_source_type: str, row: Dict[str, Any]) -> str:
    ticket_id = _clean_text(row.get("ticket_id"))
    conversation_id = _clean_text(row.get("conversation_id"))
    doc_id = _clean_text(row.get("doc_id"))
    source = _clean_text(row.get("source"))
    chunk_id = _clean_text(row.get("chunk_id"))

    if logical_source_type == "ticket":
        if ticket_id:
            return f"ticket:{ticket_id}"
        if conversation_id:
            return f"conversation:{conversation_id}"
    else:
        if doc_id:
            return f"doc:{doc_id}"
        if conversation_id:
            return f"conversation:{conversation_id}"

    if source:
        return f"source:{source}"
    if chunk_id:
        return f"chunk:{chunk_id}"
    digest = hashlib.sha1(json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"row:{digest}"


def _catalog_id(*, source_type: str, department: str, identity: str) -> str:
    seed = f"{source_type}|{department}|{identity}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def build_source_catalog_entries(dataset_path: Path) -> List[SourceCatalogEntry]:
    grouped: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    for row in _iter_dataset_rows(dataset_path):
        logical_source_type = _normalize_source_type(row.get("source_type"))
        department = _clean_text(row.get("department")) or "unknown"
        identity = _group_identity(logical_source_type, row)
        key = (logical_source_type, department, identity)

        source = _clean_text(row.get("source")) or ""
        last_updated = _normalized_timestamp(row.get("last_updated") or row.get("lastUpdated"))
        ingested_at = _normalized_timestamp(row.get("ingested_at"))
        ingest_job_id = _clean_text(row.get("ingest_job_id"))
        title_rank, title_candidate = _title_candidate(logical_source_type, row)

        state = grouped.get(key)
        if state is None:
            grouped[key] = {
                "catalog_id": _catalog_id(source_type=logical_source_type, department=department, identity=identity),
                "source_type": logical_source_type,
                "department": department,
                "source": source,
                "host": _host_from_source(source),
                "title": title_candidate,
                "title_rank": title_rank,
                "chunk_count": 1,
                "last_updated": last_updated,
                "ingest_job_id": ingest_job_id,
                "ingested_at": ingested_at,
                "_ingested_at_key": _timestamp_sort_key(ingested_at),
                "_last_updated_key": _timestamp_sort_key(last_updated),
            }
            continue

        state["chunk_count"] += 1
        if not state["source"] and source:
            state["source"] = source
            state["host"] = _host_from_source(source)
        if title_candidate is not None and title_rank < state["title_rank"]:
            state["title"] = title_candidate
            state["title_rank"] = title_rank

        last_updated_key = _timestamp_sort_key(last_updated)
        if last_updated_key > state["_last_updated_key"]:
            state["last_updated"] = last_updated
            state["_last_updated_key"] = last_updated_key

        ingested_at_key = _timestamp_sort_key(ingested_at)
        if ingested_at_key > state["_ingested_at_key"]:
            state["ingested_at"] = ingested_at
            state["ingest_job_id"] = ingest_job_id
            state["_ingested_at_key"] = ingested_at_key
        elif state["ingest_job_id"] is None and ingest_job_id is not None:
            state["ingest_job_id"] = ingest_job_id

    items = [
        SourceCatalogEntry(
            catalog_id=state["catalog_id"],
            source_type=state["source_type"],
            department=state["department"],
            source=state["source"],
            host=state["host"],
            title=state["title"],
            chunk_count=int(state["chunk_count"]),
            last_updated=state["last_updated"],
            ingest_job_id=state["ingest_job_id"],
            ingested_at=state["ingested_at"],
        )
        for state in grouped.values()
    ]
    items.sort(
        key=lambda item: (
            item.source_type,
            item.department,
            (item.title or item.source or "").lower(),
            item.catalog_id,
        )
    )
    return items


def write_source_catalog(snapshot: SourceCatalogSnapshot, catalog_path: Path) -> None:
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    with catalog_path.open("w", encoding="utf-8") as dst:
        json.dump(snapshot.to_dict(), dst, ensure_ascii=False, indent=2)
        dst.write("\n")


def source_catalog_is_stale(
    *,
    dataset_path: str | Path,
    catalog_path: str | Path,
) -> bool:
    dataset = Path(dataset_path)
    catalog = Path(catalog_path)
    if not catalog.exists():
        return True
    if not dataset.exists():
        return False
    dataset_stat = dataset.stat()
    try:
        snapshot = load_source_catalog(catalog)
    except Exception:
        return True

    if snapshot.dataset_mtime_ns is not None and snapshot.dataset_size_bytes is not None:
        return (
            int(snapshot.dataset_mtime_ns) != int(dataset_stat.st_mtime_ns)
            or int(snapshot.dataset_size_bytes) != int(dataset_stat.st_size)
        )

    return catalog.stat().st_mtime_ns < dataset_stat.st_mtime_ns


def load_source_catalog(catalog_path: str | Path) -> SourceCatalogSnapshot:
    path = Path(catalog_path)
    with path.open("r", encoding="utf-8") as src:
        payload = json.load(src)

    items = [
        SourceCatalogEntry(
            catalog_id=str(row.get("catalog_id") or ""),
            source_type=str(row.get("source_type") or "ticket"),
            department=str(row.get("department") or "unknown"),
            source=str(row.get("source") or ""),
            host=_clean_text(row.get("host")),
            title=_clean_text(row.get("title")),
            chunk_count=int(row.get("chunk_count") or 0),
            last_updated=_clean_text(row.get("last_updated")),
            ingest_job_id=_clean_text(row.get("ingest_job_id")),
            ingested_at=_clean_text(row.get("ingested_at")),
        )
        for row in payload.get("items", [])
        if isinstance(row, dict)
    ]
    return SourceCatalogSnapshot(
        dataset_path=str(payload.get("dataset_path") or ""),
        catalog_path=str(path.resolve(strict=False)),
        generated_at=str(payload.get("generated_at") or ""),
        dataset_mtime_ns=(
            int(payload["dataset_mtime_ns"])
            if payload.get("dataset_mtime_ns") is not None
            else None
        ),
        dataset_size_bytes=(
            int(payload["dataset_size_bytes"])
            if payload.get("dataset_size_bytes") is not None
            else None
        ),
        total_entries=int(payload.get("total_entries") or len(items)),
        items=items,
    )


def rebuild_source_catalog(
    *,
    config_path: str | Path,
    dataset_path_override: str | Path | None = None,
    catalog_path_override: str | Path | None = None,
) -> SourceCatalogSnapshot:
    dataset_path = resolve_dataset_path(config_path, dataset_path_override=dataset_path_override)
    catalog_path = resolve_catalog_path(config_path, catalog_path_override=catalog_path_override)
    items = build_source_catalog_entries(dataset_path)
    dataset_stat = dataset_path.stat()
    snapshot = SourceCatalogSnapshot(
        dataset_path=str(dataset_path),
        catalog_path=str(catalog_path),
        generated_at=_utc_now_iso(),
        dataset_mtime_ns=int(dataset_stat.st_mtime_ns),
        dataset_size_bytes=int(dataset_stat.st_size),
        total_entries=len(items),
        items=items,
    )
    write_source_catalog(snapshot, catalog_path)
    return snapshot


def load_or_rebuild_source_catalog(
    *,
    config_path: str | Path,
    dataset_path_override: str | Path | None = None,
    catalog_path_override: str | Path | None = None,
) -> SourceCatalogSnapshot:
    dataset_path = resolve_dataset_path(config_path, dataset_path_override=dataset_path_override)
    catalog_path = resolve_catalog_path(config_path, catalog_path_override=catalog_path_override)
    if source_catalog_is_stale(dataset_path=dataset_path, catalog_path=catalog_path):
        return rebuild_source_catalog(
            config_path=config_path,
            dataset_path_override=dataset_path,
            catalog_path_override=catalog_path,
        )
    return load_source_catalog(catalog_path)


def filter_source_catalog_entries(
    entries: Sequence[SourceCatalogEntry],
    *,
    source_type: Optional[str] = None,
    department: Optional[str] = None,
    q: Optional[str] = None,
) -> List[SourceCatalogEntry]:
    logical_source_type = _normalize_source_type(source_type) if source_type is not None else None
    normalized_department = (_clean_text(department) or "").lower() if department is not None else None
    normalized_query = (_clean_text(q) or "").lower() if q is not None else None

    filtered: List[SourceCatalogEntry] = []
    for entry in entries:
        if logical_source_type is not None and entry.source_type != logical_source_type:
            continue
        if normalized_department is not None and entry.department.lower() != normalized_department:
            continue
        if normalized_query is not None:
            haystack = " ".join(
                part
                for part in (
                    entry.source_type,
                    entry.department,
                    entry.host or "",
                    entry.title or "",
                    entry.source,
                    entry.ingest_job_id or "",
                )
                if part
            ).lower()
            if normalized_query not in haystack:
                continue
        filtered.append(entry)
    return filtered


def query_source_catalog_entries(
    entries: Sequence[SourceCatalogEntry],
    *,
    source_type: Optional[str] = None,
    department: Optional[str] = None,
    q: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> SourceCatalogQueryResult:
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1:
        raise ValueError("page_size must be >= 1")

    filtered = filter_source_catalog_entries(
        entries,
        source_type=source_type,
        department=department,
        q=q,
    )

    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    items = filtered[start:end]
    return SourceCatalogQueryResult(
        total=total,
        page=page,
        page_size=page_size,
        has_more=end < total,
        items=items,
    )


def query_source_catalog(
    catalog_path: str | Path,
    *,
    source_type: Optional[str] = None,
    department: Optional[str] = None,
    q: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> SourceCatalogQueryResult:
    snapshot = load_source_catalog(catalog_path)
    return query_source_catalog_entries(
        snapshot.items,
        source_type=source_type,
        department=department,
        q=q,
        page=page,
        page_size=page_size,
    )


def build_source_catalog_overview(
    entries: Sequence[SourceCatalogEntry],
    *,
    source_type: Optional[str] = None,
    department: Optional[str] = None,
    q: Optional[str] = None,
    limit_per_type: int = 6,
) -> SourceCatalogOverview:
    if limit_per_type < 1:
        raise ValueError("limit_per_type must be >= 1")

    filtered = filter_source_catalog_entries(
        entries,
        source_type=source_type,
        department=department,
        q=q,
    )

    bucket_items: Dict[str, List[SourceCatalogOverviewItem]] = defaultdict(list)
    bucket_totals: Dict[str, int] = {}

    if source_type is None or _normalize_source_type(source_type) == "web":
        grouped_web: Dict[str, Dict[str, Any]] = {}
        for entry in (item for item in filtered if item.source_type == "web"):
            identity = entry.host or entry.source or entry.catalog_id
            state = grouped_web.get(identity)
            if state is None:
                grouped_web[identity] = {
                    "label": entry.host or entry.title or entry.source or "website",
                    "description": entry.title or entry.source or entry.host,
                    "source": entry.source or None,
                    "host": entry.host,
                    "total_documents": 1,
                    "total_chunks": int(entry.chunk_count),
                    "departments": {entry.department},
                    "last_updated": entry.last_updated,
                    "ingested_at": entry.ingested_at,
                }
                continue
            state["total_documents"] += 1
            state["total_chunks"] += int(entry.chunk_count)
            state["departments"].add(entry.department)
            if not state["source"] and entry.source:
                state["source"] = entry.source
            if not state["host"] and entry.host:
                state["host"] = entry.host
            if not state["description"] and entry.title:
                state["description"] = entry.title
            state["last_updated"] = _best_timestamp(state["last_updated"], entry.last_updated)
            state["ingested_at"] = _best_timestamp(state["ingested_at"], entry.ingested_at)

        web_items = [
            SourceCatalogOverviewItem(
                overview_id=hashlib.sha1(f"web|{identity}".encode("utf-8")).hexdigest()[:16],
                source_type="web",
                label=str(state["label"] or "website"),
                description=_clean_text(state["description"]),
                source=_clean_text(state["source"]),
                host=_clean_text(state["host"]),
                total_documents=int(state["total_documents"]),
                total_chunks=int(state["total_chunks"]),
                department_count=len(state["departments"]),
                last_updated=_clean_text(state["last_updated"]),
                ingested_at=_clean_text(state["ingested_at"]),
            )
            for identity, state in grouped_web.items()
        ]
        web_items.sort(
            key=lambda item: (
                -item.total_documents,
                -item.total_chunks,
                (item.label or "").lower(),
                item.overview_id,
            )
        )
        bucket_totals["web"] = len(web_items)
        bucket_items["web"] = web_items[:limit_per_type]

    for logical_source_type in ("pdf", "ticket"):
        if source_type is not None and _normalize_source_type(source_type) != logical_source_type:
            continue
        logical_entries = [item for item in filtered if item.source_type == logical_source_type]
        logical_entries.sort(
            key=lambda item: (
                _timestamp_sort_key(item.ingested_at),
                _timestamp_sort_key(item.last_updated),
                (item.title or item.source or "").lower(),
                item.catalog_id,
            ),
            reverse=True,
        )
        bucket_totals[logical_source_type] = len(logical_entries)
        bucket_items[logical_source_type] = [
            SourceCatalogOverviewItem(
                overview_id=hashlib.sha1(f"{logical_source_type}|{entry.catalog_id}".encode("utf-8")).hexdigest()[:16],
                source_type=logical_source_type,
                label=entry.title or entry.source or logical_source_type,
                description=entry.host or entry.department,
                source=entry.source or None,
                host=entry.host,
                total_documents=1,
                total_chunks=int(entry.chunk_count),
                department_count=1,
                last_updated=entry.last_updated,
                ingested_at=entry.ingested_at,
            )
            for entry in logical_entries[:limit_per_type]
        ]

    ordered_buckets = [
        SourceCatalogOverviewBucket(
            source_type=logical_source_type,
            total=int(bucket_totals.get(logical_source_type, 0)),
            items=list(bucket_items.get(logical_source_type, [])),
        )
        for logical_source_type in VALID_SOURCE_CATALOG_TYPES
    ]
    return SourceCatalogOverview(
        total=sum(bucket.total for bucket in ordered_buckets),
        limit_per_type=limit_per_type,
        buckets=ordered_buckets,
    )


def build_source_catalog_tree(
    entries: Sequence[SourceCatalogEntry],
    *,
    source_type: Optional[str] = None,
    department: Optional[str] = None,
    q: Optional[str] = None,
) -> SourceCatalogTree:
    filtered = filter_source_catalog_entries(
        entries,
        source_type=source_type,
        department=department,
        q=q,
    )

    grouped: Dict[str, Dict[str, Any]] = {}
    child_type_order = {"web": 0, "pdf": 1, "ticket": 2}

    for entry in filtered:
        normalized_host = (entry.host or "").strip().lower()
        if normalized_host.startswith("www."):
            normalized_host = normalized_host[4:]

        if entry.source_type == "ticket":
            group_key = "ticket:all"
            group_label = "Tickets"
            group_description = "Ticket knowledge base"
            group_host = None
        else:
            group_key = f"host:{normalized_host or entry.source_type}"
            group_label = _display_label_from_host(normalized_host) or entry.title or entry.source or "Sources"
            group_description = normalized_host or entry.department
            group_host = normalized_host or None

        state = grouped.get(group_key)
        if state is None:
            state = {
                "group_id": hashlib.sha1(group_key.encode("utf-8")).hexdigest()[:16],
                "label": group_label,
                "description": group_description,
                "host": group_host,
                "total_children": 0,
                "total_chunks": 0,
                "web_count": 0,
                "pdf_count": 0,
                "ticket_count": 0,
                "children": [],
            }
            grouped[group_key] = state

        state["total_children"] += 1
        state["total_chunks"] += int(entry.chunk_count)
        state[f"{entry.source_type}_count"] = int(state.get(f"{entry.source_type}_count", 0)) + 1
        state["children"].append(
            SourceCatalogTreeChild(
                child_id=hashlib.sha1(f"{group_key}|{entry.catalog_id}".encode("utf-8")).hexdigest()[:16],
                catalog_id=entry.catalog_id,
                source_type=entry.source_type,
                title=entry.title or entry.source or entry.source_type,
                source=entry.source,
                host=entry.host,
                path=_path_from_source(entry.source),
                department=entry.department,
                chunk_count=int(entry.chunk_count),
                last_updated=entry.last_updated,
                ingested_at=entry.ingested_at,
            )
        )

    groups: List[SourceCatalogTreeGroup] = []
    for state in grouped.values():
        children = sorted(
            state["children"],
            key=lambda child: (
                child_type_order.get(child.source_type, 9),
                (child.title or "").lower(),
                (child.path or child.source or "").lower(),
                child.child_id,
            ),
        )
        groups.append(
            SourceCatalogTreeGroup(
                group_id=state["group_id"],
                label=str(state["label"] or "Sources"),
                description=_clean_text(state["description"]),
                host=_clean_text(state["host"]),
                total_children=int(state["total_children"]),
                total_chunks=int(state["total_chunks"]),
                web_count=int(state["web_count"]),
                pdf_count=int(state["pdf_count"]),
                ticket_count=int(state["ticket_count"]),
                children=children,
            )
        )

    groups.sort(
        key=lambda group: (
            1 if group.ticket_count and not group.host else 0,
            -group.total_children,
            (group.label or "").lower(),
            group.group_id,
        )
    )
    return SourceCatalogTree(
        total_groups=len(groups),
        total_items=sum(group.total_children for group in groups),
        groups=groups,
    )
