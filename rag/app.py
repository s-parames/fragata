#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import json
from datetime import date, datetime
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel, Field, ValidationError
import yaml

from RAG_v1 import build_engine, normalize_text
from ingest.source_catalog import (
    SourceCatalogEntry,
    SourceCatalogOverview,
    SourceCatalogOverviewBucket,
    SourceCatalogOverviewItem,
    SourceCatalogSnapshot,
    SourceCatalogTree,
    SourceCatalogTreeChild,
    SourceCatalogTreeGroup,
    build_source_catalog_overview,
    build_source_catalog_tree,
    load_or_rebuild_source_catalog,
    normalize_source_catalog_source_type,
    query_source_catalog_entries,
)
from ingest import (
    EngineManager,
    IngestJobStatusResponse,
    IngestSourceType,
    IngestionJobRunner,
    IngestionJobStore,
    IngestionOrchestrator,
    PdfIngestRequest,
    PurgeDepartmentRequest,
    RepoDocsIngestRequest,
    RtWeeklyIngestRequest,
    WebIngestRequest,
)
try:
    from scripts.common_department import (
        normalize_search_department_filter,
        valid_department_values,
    )
except ModuleNotFoundError:
    from common_department import normalize_search_department_filter, valid_department_values


APP_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = APP_ROOT / "config" / "rag.yaml"


def _env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _configure_cors(fastapi_app: FastAPI) -> None:
    allow_origins = _env_csv("RAG_CORS_ALLOW_ORIGINS")
    allow_origin_regex = os.getenv("RAG_CORS_ALLOW_ORIGIN_REGEX", "").strip() or None
    if not allow_origins and allow_origin_regex is None:
        return

    allow_methods = _env_csv("RAG_CORS_ALLOW_METHODS") or ["GET", "POST", "OPTIONS"]
    allow_headers = _env_csv("RAG_CORS_ALLOW_HEADERS") or ["*"]
    expose_headers = _env_csv("RAG_CORS_EXPOSE_HEADERS")
    allow_credentials = _env_bool("RAG_CORS_ALLOW_CREDENTIALS", default=False)

    try:
        max_age = int(os.getenv("RAG_CORS_MAX_AGE", "600").strip())
    except ValueError:
        max_age = 600

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_origin_regex=allow_origin_regex,
        allow_credentials=allow_credentials,
        allow_methods=allow_methods,
        allow_headers=allow_headers,
        expose_headers=expose_headers,
        max_age=max_age,
    )


app = FastAPI(title="RAG Ticket Search", version="1.0.0")
_configure_cors(app)
app.mount("/static", StaticFiles(directory=str(APP_ROOT / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_ROOT / "templates"))

_engine_manager = EngineManager(loader=lambda: build_engine(str(CONFIG_PATH)))
_engine_artifact_sync_lock = Lock()
_engine_artifact_signature: Optional[str] = None

_ingest_store = IngestionJobStore(root_dir=str(APP_ROOT / "data" / "reports" / "ingest_jobs"))
_ingest_orchestrator = IngestionOrchestrator(job_store=_ingest_store)


def _reload_engine_on_ingest_success(
    job_id: str,
    source_type: IngestSourceType,
    _result: Any,
) -> Dict[str, Any]:
    state = _engine_manager.reload_engine()
    _mark_engine_artifact_signature_current()
    return {
        "status": "ok",
        "trigger": "ingest_completion",
        "job_id": job_id,
        "source_type": source_type.value,
        **state,
    }


_ingest_runner = IngestionJobRunner(
    orchestrator=_ingest_orchestrator,
    on_job_success=_reload_engine_on_ingest_success,
)

PDF_UPLOAD_CHUNK_SIZE = 1024 * 1024
DEFAULT_MAX_PDF_UPLOAD_BYTES = 20 * 1024 * 1024
ALLOWED_PDF_CONTENT_TYPES = {"application/pdf", "application/x-pdf"}
SEARCH_TICKET_DEDUP_OVERFETCH_FACTOR = 4
SEARCH_TICKET_DEDUP_OVERFETCH_CAP = 300
SEARCH_TICKET_DEDUP_ADAPTIVE_STEPS = 3


class SearchRequest(BaseModel):
    query: str = Field(default="", max_length=500)
    k: Optional[int] = Field(default=None, ge=0, le=20)
    date_from: Optional[str] = Field(default=None)
    date_to: Optional[str] = Field(default=None)
    department: Optional[str] = Field(default="all")


class SearchResult(BaseModel):
    rank: int
    rerank_score: float
    fused_score: float
    conversation_id: Optional[str]
    ticket_id: Optional[int]
    chunk_id: Optional[str]
    source_type: Optional[str]
    last_updated: Optional[str]
    department: Optional[str]
    source: Optional[str]
    snippet: str


class SearchResponse(BaseModel):
    query: str
    total: int
    results: List[SearchResult]


class SearchDepartmentsResponse(BaseModel):
    departments: List[str]


class CatalogSourceItemResponse(BaseModel):
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


class CatalogSourceListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    has_more: bool
    items: List[CatalogSourceItemResponse]


class CatalogOverviewItemResponse(BaseModel):
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


class CatalogOverviewBucketResponse(BaseModel):
    source_type: str
    total: int
    items: List[CatalogOverviewItemResponse]


class CatalogOverviewResponse(BaseModel):
    total: int
    limit_per_type: int
    buckets: List[CatalogOverviewBucketResponse]


class CatalogTreeChildResponse(BaseModel):
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


class CatalogTreeGroupResponse(BaseModel):
    group_id: str
    label: str
    description: Optional[str]
    host: Optional[str]
    total_children: int
    total_chunks: int
    web_count: int
    pdf_count: int
    ticket_count: int
    children: List[CatalogTreeChildResponse]


class CatalogTreeResponse(BaseModel):
    total_groups: int
    total_items: int
    groups: List[CatalogTreeGroupResponse]


class UploadValidationError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _error_detail(code: str, message: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"code": code, "message": message}
    if extra:
        payload["extra"] = extra
    return payload


def _strip_leading_subject_line(text: str) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return normalized
    lines = normalized.split("\n")
    if len(lines) <= 1:
        return normalized
    first = lines[0].strip().lower()
    if not first.startswith("subject:"):
        return normalized
    rest = normalize_text("\n".join(lines[1:]))
    return rest or normalized


def _compute_search_overfetch_k(requested_k: Optional[int], *, has_query: bool) -> Optional[int]:
    if not has_query:
        return requested_k
    if requested_k is None or requested_k <= 0:
        return requested_k
    expanded = requested_k * SEARCH_TICKET_DEDUP_OVERFETCH_FACTOR
    return max(requested_k, min(SEARCH_TICKET_DEDUP_OVERFETCH_CAP, expanded))


def _next_search_overfetch_k(current_k: int, requested_k: int) -> int:
    base = max(requested_k, requested_k * SEARCH_TICKET_DEDUP_OVERFETCH_FACTOR)
    grown = max(base, current_k * 2)
    return min(SEARCH_TICKET_DEDUP_OVERFETCH_CAP, grown)


def _dedupe_ranked_results_by_ticket(
    raw: List[Dict[str, Any]],
    *,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen_ticket_ids: set[str] = set()
    for item in raw:
        doc = item.get("doc")
        md = (doc.metadata or {}) if doc is not None else {}
        raw_ticket_id = md.get("ticket_id")
        if raw_ticket_id is not None:
            ticket_id = str(raw_ticket_id).strip()
            if ticket_id:
                if ticket_id in seen_ticket_ids:
                    continue
                seen_ticket_ids.add(ticket_id)

        deduped.append(item)
        if limit is not None and limit > 0 and len(deduped) >= limit:
            break
    return deduped


def _resolve_active_retrieval_paths() -> Optional[tuple[Path, Path, Path]]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            raw_cfg = yaml.safe_load(handle) or {}
        retrieval = raw_cfg.get("retrieval") or {}
        dataset_raw = str(retrieval.get("dataset_path", "")).strip()
        faiss_dir_raw = str(retrieval.get("faiss_dir", "")).strip()
        if not dataset_raw or not faiss_dir_raw:
            return None

        dataset_path = Path(dataset_raw)
        if not dataset_path.is_absolute():
            dataset_path = APP_ROOT / dataset_path

        faiss_dir = Path(faiss_dir_raw)
        if not faiss_dir.is_absolute():
            faiss_dir = APP_ROOT / faiss_dir

        index_faiss = faiss_dir / "index.faiss"
        index_pkl = faiss_dir / "index.pkl"
        return (
            dataset_path.resolve(strict=False),
            index_faiss.resolve(strict=False),
            index_pkl.resolve(strict=False),
        )
    except Exception:
        return None


def _file_signature(path: Path) -> Optional[str]:
    try:
        if not path.exists() or not path.is_file():
            return None
        stat = path.stat()
    except OSError:
        return None
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _compute_active_artifact_signature() -> Optional[str]:
    resolved = _resolve_active_retrieval_paths()
    if resolved is None:
        return None
    dataset_path, index_faiss, index_pkl = resolved
    dataset_sig = _file_signature(dataset_path)
    faiss_sig = _file_signature(index_faiss)
    pkl_sig = _file_signature(index_pkl)
    if dataset_sig is None or faiss_sig is None or pkl_sig is None:
        return None
    return "|".join(
        [
            str(dataset_path),
            dataset_sig,
            str(index_faiss),
            faiss_sig,
            str(index_pkl),
            pkl_sig,
        ]
    )


def _mark_engine_artifact_signature_current() -> None:
    global _engine_artifact_signature
    current = _compute_active_artifact_signature()
    if current is None:
        return
    with _engine_artifact_sync_lock:
        _engine_artifact_signature = current


def _reload_engine_if_artifacts_changed() -> None:
    global _engine_artifact_signature
    current = _compute_active_artifact_signature()
    if current is None:
        return

    with _engine_artifact_sync_lock:
        if _engine_artifact_signature is None:
            _engine_artifact_signature = current
            return
        if _engine_artifact_signature == current:
            return

        reload_fn = getattr(_engine_manager, "reload_engine", None)
        if not callable(reload_fn):
            _engine_artifact_signature = current
            return

        try:
            reload_fn()
        except Exception:
            # Keep serving previous engine generation if hot-swap fails.
            return

        _engine_artifact_signature = current


def _safe_filename(value: str, fallback: str = "upload.pdf") -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip())
    clean = clean.strip("._")
    if not clean:
        return fallback
    return clean[:255]


def _max_pdf_upload_bytes() -> int:
    raw = os.getenv("RAG_MAX_PDF_UPLOAD_BYTES", str(DEFAULT_MAX_PDF_UPLOAD_BYTES)).strip()
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError("must be > 0")
        return value
    except ValueError:
        return DEFAULT_MAX_PDF_UPLOAD_BYTES


def _pdf_job_input_dir(job_id: str) -> Path:
    return APP_ROOT / "data" / "raw_site" / "jobs" / job_id / "pdf_input"


def _is_allowed_pdf_upload(upload: UploadFile) -> bool:
    content_type = (upload.content_type or "").split(";", 1)[0].strip().lower()
    if content_type in ALLOWED_PDF_CONTENT_TYPES:
        return True
    if content_type in {"application/octet-stream", "binary/octet-stream"}:
        return (upload.filename or "").lower().endswith(".pdf")
    return False


async def _stage_uploaded_pdf(job_id: str, upload: UploadFile) -> str:
    if not _is_allowed_pdf_upload(upload):
        await upload.close()
        raise UploadValidationError(
            code="invalid_file_type",
            message="Invalid file type. Expected a PDF upload.",
            status_code=415,
        )

    filename = _safe_filename(upload.filename or "upload.pdf")
    if not filename.lower().endswith(".pdf"):
        filename = f"{filename}.pdf"

    input_dir = _pdf_job_input_dir(job_id)
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / filename
    max_bytes = _max_pdf_upload_bytes()
    total = 0
    try:
        with dest.open("wb") as dst:
            while True:
                chunk = await upload.read(PDF_UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise UploadValidationError(
                        code="file_too_large",
                        message=f"PDF file exceeds maximum allowed size ({max_bytes} bytes).",
                        status_code=413,
                    )
                dst.write(chunk)
    except Exception:
        if dest.exists():
            dest.unlink()
        raise
    finally:
        await upload.close()

    return str(dest)


def get_engine() -> Any:
    engine = _engine_manager.get_engine()
    _mark_engine_artifact_signature_current()
    return engine


def _ordered_search_departments(values: List[Any]) -> List[str]:
    canonical = [value for value in valid_department_values() if value != "all"]
    present_canonical: set[str] = set()
    extra: set[str] = set()

    for value in values:
        normalized = normalize_search_department_filter(value)
        if normalized is None or normalized == "all":
            continue
        if normalized in canonical:
            present_canonical.add(normalized)
        else:
            extra.add(normalized)

    ordered = [value for value in canonical if value in present_canonical]
    ordered.extend(sorted(extra))
    return ["all", *ordered]


def _search_departments_from_engine(engine: Any) -> List[str]:
    values: List[Any] = []

    docs = getattr(engine, "documents", None)
    if not docs:
        return ["all"]

    for doc in docs:
        md = getattr(doc, "metadata", None) or {}
        values.append(md.get("department"))

    return _ordered_search_departments(values)


def _catalog_item_response(entry: SourceCatalogEntry) -> CatalogSourceItemResponse:
    return CatalogSourceItemResponse(**entry.to_dict())


def _catalog_overview_item_response(item: SourceCatalogOverviewItem) -> CatalogOverviewItemResponse:
    return CatalogOverviewItemResponse(**item.to_dict())


def _catalog_overview_bucket_response(bucket: SourceCatalogOverviewBucket) -> CatalogOverviewBucketResponse:
    return CatalogOverviewBucketResponse(
        source_type=bucket.source_type,
        total=bucket.total,
        items=[_catalog_overview_item_response(item) for item in bucket.items],
    )


def _catalog_tree_child_response(child: SourceCatalogTreeChild) -> CatalogTreeChildResponse:
    return CatalogTreeChildResponse(**child.to_dict())


def _catalog_tree_group_response(group: SourceCatalogTreeGroup) -> CatalogTreeGroupResponse:
    return CatalogTreeGroupResponse(
        group_id=group.group_id,
        label=group.label,
        description=group.description,
        host=group.host,
        total_children=group.total_children,
        total_chunks=group.total_chunks,
        web_count=group.web_count,
        pdf_count=group.pdf_count,
        ticket_count=group.ticket_count,
        children=[_catalog_tree_child_response(child) for child in group.children],
    )


def _load_or_build_source_catalog_snapshot() -> SourceCatalogSnapshot:
    return load_or_rebuild_source_catalog(config_path=CONFIG_PATH)


def _normalize_catalog_source_type_filter(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = normalize_source_catalog_source_type(value)
    if normalized is None:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                code="invalid_catalog_source_type",
                message="Invalid catalog source_type. Allowed values: ticket, web, pdf.",
            ),
        )
    return normalized


def _normalize_catalog_department_filter(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = normalize_search_department_filter(value)
    if normalized is None:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                code="invalid_catalog_department",
                message="Invalid department filter.",
            ),
        )
    if normalized == "all":
        return None
    return normalized


def _search_departments_from_active_dataset() -> List[str]:
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            raw_cfg = yaml.safe_load(handle) or {}
        dataset_path = str(raw_cfg.get("retrieval", {}).get("dataset_path", "")).strip()
        if not dataset_path:
            return ["all"]

        file_path = Path(dataset_path)
        if not file_path.is_absolute():
            file_path = APP_ROOT / file_path
        if not file_path.exists():
            return ["all"]

        values: List[Any] = []
        with file_path.open("r", encoding="utf-8") as src:
            for line in src:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                values.append((row.get("department")))
        return _ordered_search_departments(values)
    except Exception:
        return ["all"]


def _warmup_engine_background() -> None:
    try:
        _engine_manager.get_engine()
    except Exception:
        # Exposed via /health and surfaced in /search responses.
        pass


@app.on_event("startup")
def startup_warmup() -> None:
    # Warm up models/index in background so first user query is not a cold start.
    if os.getenv("RAG_SKIP_WARMUP", "").strip() in {"1", "true", "TRUE", "yes", "YES"}:
        return
    Thread(target=_warmup_engine_background, daemon=True).start()


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        **_engine_manager.health(),
    }


@app.post("/admin/engine/reload")
def admin_engine_reload() -> Dict[str, Any]:
    try:
        state = _engine_manager.reload_engine()
        _mark_engine_artifact_signature_current()
        return {
            "status": "ok",
            **state,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="engine_reload_failed",
                message="Engine reload failed.",
                extra={
                    "error": f"{exc.__class__.__name__}: {exc}",
                    "health": _engine_manager.health(),
                },
            ),
        ) from exc


@app.post("/admin/purge-department", response_model=IngestJobStatusResponse)
def admin_purge_department(payload: Dict[str, Any]) -> IngestJobStatusResponse:
    try:
        request = PurgeDepartmentRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=_error_detail(
                code="invalid_request",
                message="Invalid purge department payload.",
                extra={
                    "errors": exc.errors(include_context=False),
                    "hint": "Provide department, confirm=true, and optional dry_run.",
                },
            ),
        ) from exc

    try:
        status = _ingest_orchestrator.enqueue_purge_department(request)
        _ingest_runner.start_job(job_id=status.job_id, source_type=IngestSourceType.PURGE_DEPARTMENT)
        return status
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="job_enqueue_failed",
                message="Failed to enqueue purge department job.",
                extra={"error": f"{exc.__class__.__name__}: {exc}"},
            ),
        ) from exc


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={},
    )


@app.get("/favicon.ico", include_in_schema=False)
def favicon_redirect():
    return RedirectResponse(url="/static/favicon.ico?v=20260407-2")


@app.get("/search/departments", response_model=SearchDepartmentsResponse)
def search_departments() -> SearchDepartmentsResponse:
    discovered = _search_departments_from_active_dataset()
    health_state = _engine_manager.health()
    if not health_state["engine_loaded"] and health_state["engine_loading"]:
        return SearchDepartmentsResponse(departments=discovered)
    if health_state["engine_error"] is not None and not health_state["engine_loaded"]:
        return SearchDepartmentsResponse(departments=discovered)

    try:
        engine = get_engine()
    except Exception:
        return SearchDepartmentsResponse(departments=discovered)

    return SearchDepartmentsResponse(departments=_search_departments_from_engine(engine))


@app.get("/catalog/sources", response_model=CatalogSourceListResponse)
def list_catalog_sources(
    source_type: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
) -> CatalogSourceListResponse:
    normalized_source_type = _normalize_catalog_source_type_filter(source_type)
    normalized_department = _normalize_catalog_department_filter(department)
    snapshot = _load_or_build_source_catalog_snapshot()
    result = query_source_catalog_entries(
        snapshot.items,
        source_type=normalized_source_type,
        department=normalized_department,
        q=q,
        page=page,
        page_size=page_size,
    )
    return CatalogSourceListResponse(
        total=result.total,
        page=result.page,
        page_size=result.page_size,
        has_more=result.has_more,
        items=[_catalog_item_response(item) for item in result.items],
    )


@app.get("/catalog/overview", response_model=CatalogOverviewResponse)
def catalog_overview(
    source_type: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=200),
    limit_per_type: int = Query(default=6, ge=1, le=20),
) -> CatalogOverviewResponse:
    normalized_source_type = _normalize_catalog_source_type_filter(source_type)
    normalized_department = _normalize_catalog_department_filter(department)
    snapshot = _load_or_build_source_catalog_snapshot()
    overview: SourceCatalogOverview = build_source_catalog_overview(
        snapshot.items,
        source_type=normalized_source_type,
        department=normalized_department,
        q=q,
        limit_per_type=limit_per_type,
    )
    return CatalogOverviewResponse(
        total=overview.total,
        limit_per_type=overview.limit_per_type,
        buckets=[_catalog_overview_bucket_response(bucket) for bucket in overview.buckets],
    )


@app.get("/catalog/tree", response_model=CatalogTreeResponse)
def catalog_tree(
    source_type: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=200),
) -> CatalogTreeResponse:
    normalized_source_type = _normalize_catalog_source_type_filter(source_type)
    normalized_department = _normalize_catalog_department_filter(department)
    snapshot = _load_or_build_source_catalog_snapshot()
    tree: SourceCatalogTree = build_source_catalog_tree(
        snapshot.items,
        source_type=normalized_source_type,
        department=normalized_department,
        q=q,
    )
    return CatalogTreeResponse(
        total_groups=tree.total_groups,
        total_items=tree.total_items,
        groups=[_catalog_tree_group_response(group) for group in tree.groups],
    )


@app.get("/catalog/sources/{catalog_id}", response_model=CatalogSourceItemResponse)
def get_catalog_source(catalog_id: str) -> CatalogSourceItemResponse:
    snapshot = _load_or_build_source_catalog_snapshot()
    for entry in snapshot.items:
        if entry.catalog_id == catalog_id:
            return _catalog_item_response(entry)
    raise HTTPException(
        status_code=404,
        detail=_error_detail(
            code="catalog_source_not_found",
            message=f"No catalog source found for catalog_id={catalog_id}",
        ),
    )


@app.post("/ingest/web", response_model=IngestJobStatusResponse)
def ingest_web(payload: WebIngestRequest):
    try:
        status = _ingest_orchestrator.enqueue_web(payload)
        _ingest_runner.start_job(job_id=status.job_id, source_type=IngestSourceType.WEB)
        return status
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="job_enqueue_failed",
                message="Failed to enqueue web ingestion job.",
                extra={"error": f"{exc.__class__.__name__}: {exc}"},
            ),
        ) from exc


@app.post("/ingest/repo-docs", response_model=IngestJobStatusResponse)
def ingest_repo_docs(payload: RepoDocsIngestRequest):
    try:
        status = _ingest_orchestrator.enqueue_repo_docs(payload)
        _ingest_runner.start_job(job_id=status.job_id, source_type=IngestSourceType.REPO_DOCS)
        return status
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="job_enqueue_failed",
                message="Failed to enqueue repository docs ingestion job.",
                extra={"error": f"{exc.__class__.__name__}: {exc}"},
            ),
        ) from exc


@app.post("/ingest/rt-weekly", response_model=IngestJobStatusResponse)
def ingest_rt_weekly(payload: RtWeeklyIngestRequest):
    try:
        status = _ingest_orchestrator.enqueue_rt_weekly(payload)
        _ingest_runner.start_job(job_id=status.job_id, source_type=IngestSourceType.RT_WEEKLY)
        return status
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="job_enqueue_failed",
                message="Failed to enqueue RT weekly ingestion job.",
                extra={"error": f"{exc.__class__.__name__}: {exc}"},
            ),
        ) from exc


@app.post("/ingest/pdf", response_model=IngestJobStatusResponse)
async def ingest_pdf(
    department: str = Form(...),
    ingest_label: Optional[str] = None,
    source_url: Optional[str] = Form(default=None),
    file: UploadFile = File(...),
):
    try:
        payload = PdfIngestRequest(
            department=department,
            ingest_label=ingest_label,
            source_url=source_url,
            original_filename=file.filename,
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=_error_detail(
                code="invalid_request",
                message="Invalid PDF ingest payload.",
                extra={"errors": exc.errors()},
            ),
        ) from exc

    status = _ingest_orchestrator.enqueue_pdf(payload)
    try:
        staged_path = await _stage_uploaded_pdf(status.job_id, file)
        _ingest_store.append_log(
            status.job_id,
            level="info",
            message="PDF upload staged",
            extra={"path": staged_path},
        )
    except UploadValidationError as exc:
        _ingest_orchestrator.mark_failed(
            status.job_id,
            stage="upload",
            error=f"{exc.code}: {exc.message}",
        )
        raise HTTPException(
            status_code=exc.status_code,
            detail=_error_detail(
                code=exc.code,
                message=exc.message,
            ),
        ) from exc
    except Exception as exc:
        _ingest_orchestrator.mark_failed(
            status.job_id,
            stage="upload",
            error=f"{exc.__class__.__name__}: {exc}",
        )
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                code="upload_stage_failed",
                message="Failed to stage uploaded PDF.",
                extra={"error": f"{exc.__class__.__name__}: {exc}"},
            ),
        ) from exc

    _ingest_runner.start_job(
        job_id=status.job_id,
        source_type=IngestSourceType.PDF,
        staged_input_path=staged_path,
    )
    return status


@app.get("/ingest/jobs/{job_id}", response_model=IngestJobStatusResponse)
def ingest_job_status(job_id: str):
    status = _ingest_orchestrator.get_job(job_id)
    if status is None:
        raise HTTPException(
            status_code=404,
            detail=_error_detail(
                code="job_not_found",
                message=f"No ingestion job found for job_id={job_id}",
            ),
        )
    return status


@app.post("/search", response_model=SearchResponse)
def search(payload: SearchRequest):
    def parse_filter_date(value: Optional[str], field_name: str) -> Optional[date]:
        if value is None:
            return None
        raw = value.strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        try:
            iso_raw = raw.replace("Z", "+00:00")
            return datetime.fromisoformat(iso_raw).date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid {field_name}. Supported formats: "
                    "YYYY-MM-DD, DD/MM/YYYY, or ISO datetime."
                ),
            )

    query = (payload.query or "").strip()

    date_from = parse_filter_date(payload.date_from, "date_from")
    date_to = parse_filter_date(payload.date_to, "date_to")
    norm_date_from = date_from.isoformat() if date_from else None
    norm_date_to = date_to.isoformat() if date_to else None
    has_date_filter = bool(norm_date_from or norm_date_to)

    if not query and not has_date_filter:
        raise HTTPException(
            status_code=400,
            detail="Provide a query or at least one date filter (date_from/date_to).",
        )

    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    if query and payload.k == 0:
        raise HTTPException(
            status_code=400,
            detail="k=0 is only allowed for date-only searches (empty query with date filters).",
        )

    department = normalize_search_department_filter(payload.department)
    if department is None:
        allowed = ", ".join(valid_department_values())
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid department. Use one of: "
                f"{allowed}, or a custom value with 3-32 chars using lowercase ASCII letters, "
                "numbers, '_' or '-'."
            ),
        )

    health_state = _engine_manager.health()
    if not health_state["engine_loaded"] and health_state["engine_loading"]:
        raise HTTPException(
            status_code=503,
            detail="Engine is loading models/index. Retry in a few seconds.",
        )
    if health_state["engine_error"] is not None and not health_state["engine_loaded"]:
        raise HTTPException(status_code=500, detail=f"Engine init failed: {health_state['engine_error']}")

    _reload_engine_if_artifacts_changed()

    try:
        engine = get_engine()
        requested_k = payload.k
        engine_k = _compute_search_overfetch_k(requested_k, has_query=bool(query))
        if not query and has_date_filter:
            raw = engine.retrieve_by_date(
                date_from=norm_date_from,
                date_to=norm_date_to,
                department=department,
                k=requested_k,
            )
        else:
            raw = engine.retrieve(
                query,
                date_from=norm_date_from,
                date_to=norm_date_to,
                department=department,
                k=engine_k,
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Search engine error: {exc}") from exc

    result_limit = requested_k if requested_k is not None and requested_k > 0 else None
    raw_before_dedupe = list(raw)
    raw = _dedupe_ranked_results_by_ticket(raw_before_dedupe, limit=result_limit)

    # Keep one-best-chunk-per-ticket in responses, but recover recall when
    # the initial candidate pool is dominated by repeated chunks from a few tickets.
    if (
        query
        and result_limit is not None
        and len(raw) < result_limit
        and len(raw_before_dedupe) >= result_limit
    ):
        current_engine_k = engine_k if engine_k is not None and engine_k > 0 else result_limit
        for _ in range(SEARCH_TICKET_DEDUP_ADAPTIVE_STEPS):
            if current_engine_k >= SEARCH_TICKET_DEDUP_OVERFETCH_CAP:
                break
            next_engine_k = _next_search_overfetch_k(current_engine_k, result_limit)
            if next_engine_k <= current_engine_k:
                break
            expanded = engine.retrieve(
                query,
                date_from=norm_date_from,
                date_to=norm_date_to,
                department=department,
                k=next_engine_k,
            )
            raw = _dedupe_ranked_results_by_ticket(expanded, limit=result_limit)
            current_engine_k = next_engine_k
            if len(raw) >= result_limit:
                break

    results: List[SearchResult] = []
    for idx, item in enumerate(raw, start=1):
        doc = item.get("doc")
        md = (doc.metadata or {}) if doc is not None else {}
        snippet_text = _strip_leading_subject_line(normalize_text(getattr(doc, "page_content", "")))
        page_title = normalize_text(str(md.get("page_title") or ""))
        if page_title and page_title.lower() not in snippet_text.lower():
            if not snippet_text or len(snippet_text) < 80:
                snippet_text = normalize_text(f"{page_title}\n\n{snippet_text}" if snippet_text else page_title)
        snippet = snippet_text[:320]
        results.append(
            SearchResult(
                rank=idx,
                rerank_score=float(item.get("rerank_score", 0.0)),
                fused_score=float(item.get("fused_score", 0.0)),
                conversation_id=md.get("conversation_id"),
                ticket_id=md.get("ticket_id"),
                chunk_id=md.get("chunk_id"),
                source_type=md.get("source_type"),
                last_updated=md.get("last_updated") or md.get("lastUpdated"),
                department=md.get("department"),
                source=md.get("source"),
                snippet=snippet,
            )
        )

    return SearchResponse(query=query, total=len(results), results=results)
