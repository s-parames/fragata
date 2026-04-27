from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, HttpUrl, field_validator
from .security import validate_public_http_url

try:
    from scripts.common_department import validate_ingest_department
except ImportError:
    from common_department import validate_ingest_department


class IngestSourceType(str, Enum):
    WEB = "web"
    PDF = "pdf"
    REPO_DOCS = "repo_docs"
    PURGE_DEPARTMENT = "purge_department"
    RT_WEEKLY = "rt_weekly"


class IngestState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class HpcExecutionMetadata(BaseModel):
    mode: str = Field(default="local", pattern=r"^(local|hpc)$")
    request_command: Optional[str] = Field(default=None, max_length=2048)
    remote_command: Optional[str] = Field(default=None, max_length=4096)
    allocation_id: Optional[str] = Field(default=None, max_length=128)
    remote_host: Optional[str] = Field(default=None, max_length=255)
    remote_workdir: Optional[str] = Field(default=None, max_length=1024)
    state: Optional[str] = Field(default=None, max_length=64)
    requested_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    released: Optional[bool] = None
    release_status: Optional[str] = Field(default=None, max_length=128)
    release_policy: Optional[str] = Field(default=None, max_length=64)
    release_evidence: Optional[str] = Field(default=None, max_length=1024)
    exit_code: Optional[int] = None

    @field_validator("requested_at", "started_at", "finished_at")
    @classmethod
    def validate_iso_like_datetime(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw = value.strip()
        if not raw:
            return None
        datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return raw


class IngestJobResultSummary(BaseModel):
    merged_rows: int = Field(default=0, ge=0)
    delta_rows: int = Field(default=0, ge=0)
    chunk_rows: int = Field(default=0, ge=0)
    index_updated: bool = Field(default=False)
    artifacts_dir: Optional[str] = Field(default=None, max_length=1024)
    output_chunked_path: Optional[str] = Field(default=None, max_length=1024)
    output_dataset_path: Optional[str] = Field(default=None, max_length=1024)
    output_delta_path: Optional[str] = Field(default=None, max_length=1024)
    output_index_path: Optional[str] = Field(default=None, max_length=1024)
    merge_summary_path: Optional[str] = Field(default=None, max_length=1024)
    index_append_summary_path: Optional[str] = Field(default=None, max_length=1024)
    purge_summary_path: Optional[str] = Field(default=None, max_length=1024)
    full_rebuild_summary_path: Optional[str] = Field(default=None, max_length=1024)
    backup_dataset_path: Optional[str] = Field(default=None, max_length=1024)
    reload_metadata: Optional[Dict[str, Any]] = None
    backup_prune_metadata: Optional[Dict[str, Any]] = None
    web_job_cleanup_metadata: Optional[Dict[str, Any]] = None
    source_catalog_refresh_metadata: Optional[Dict[str, Any]] = None
    hpc_execution: Optional[HpcExecutionMetadata] = None
    stage_metrics: Dict[str, Any] = Field(default_factory=dict)


class RepoDocsProvider(str, Enum):
    GITHUB = "github"
    GITLAB = "gitlab"


class RepoDocsDocKind(str, Enum):
    README = "readme"
    WIKI = "wiki"


class RepoDocsUrlClassification(BaseModel):
    provider: RepoDocsProvider
    doc_kind: RepoDocsDocKind
    host: str
    repo_slug: str
    repo_namespace: str
    repo_name: str
    original_url: str
    canonical_url: str
    acquisition_url: str
    ref: Optional[str] = None
    doc_path: Optional[str] = None
    wiki_page_slug: Optional[str] = None
    is_wiki_root: bool = False


REPO_DOCS_SUPPORT_MATRIX: tuple[dict[str, Any], ...] = (
    {
        "provider": RepoDocsProvider.GITHUB.value,
        "doc_kind": RepoDocsDocKind.README.value,
        "path_shape": "https://github.com/<owner>/<repo>/blob/<ref>/README.md",
    },
    {
        "provider": RepoDocsProvider.GITHUB.value,
        "doc_kind": RepoDocsDocKind.WIKI.value,
        "path_shape": "https://github.com/<owner>/<repo>/wiki",
    },
    {
        "provider": RepoDocsProvider.GITHUB.value,
        "doc_kind": RepoDocsDocKind.WIKI.value,
        "path_shape": "https://github.com/<owner>/<repo>/wiki/<page>",
    },
    {
        "provider": RepoDocsProvider.GITLAB.value,
        "doc_kind": RepoDocsDocKind.README.value,
        "path_shape": "https://gitlab.com/<namespace>/<repo>/-/blob/<ref>/README.md",
    },
    {
        "provider": RepoDocsProvider.GITLAB.value,
        "doc_kind": RepoDocsDocKind.WIKI.value,
        "path_shape": "https://gitlab.com/<namespace>/<repo>/-/wikis",
    },
    {
        "provider": RepoDocsProvider.GITLAB.value,
        "doc_kind": RepoDocsDocKind.WIKI.value,
        "path_shape": "https://gitlab.com/<namespace>/<repo>/-/wikis/<page>",
    },
)


def repo_docs_support_matrix() -> tuple[dict[str, Any], ...]:
    return REPO_DOCS_SUPPORT_MATRIX


def _clean_path_segments(path: str) -> list[str]:
    return [segment for segment in (path or "").split("/") if segment]


def _canonical_url(parsed_hostname: str, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"https://{parsed_hostname}{normalized_path}"


def _classify_github_repo_docs_url(raw_url: str) -> RepoDocsUrlClassification:
    parsed = urlparse(raw_url)
    segments = _clean_path_segments(parsed.path)
    if len(segments) < 3:
        raise ValueError("GitHub repository documentation URLs must include owner, repository, and a supported docs path")

    owner = segments[0].strip()
    repo = segments[1].strip()
    action = segments[2].strip().lower()
    if not owner or not repo:
        raise ValueError("GitHub repository documentation URLs must include owner and repository")

    repo_slug = f"{owner}/{repo}"
    if action == "blob":
        if len(segments) < 5:
            raise ValueError("GitHub README URLs must use the /blob/<ref>/README.md shape")
        ref = segments[3].strip()
        doc_path = "/".join(segments[4:])
        if not ref:
            raise ValueError("GitHub README URLs must include a branch or ref after /blob/")
        if doc_path.lower() != "readme.md":
            raise ValueError("GitHub repository documentation currently supports only README.md blob URLs")
        canonical_url = _canonical_url(parsed.hostname or "github.com", f"/{owner}/{repo}/blob/{ref}/README.md")
        acquisition_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/README.md"
        return RepoDocsUrlClassification(
            provider=RepoDocsProvider.GITHUB,
            doc_kind=RepoDocsDocKind.README,
            host=parsed.hostname or "github.com",
            repo_slug=repo_slug,
            repo_namespace=owner,
            repo_name=repo,
            original_url=raw_url,
            canonical_url=canonical_url,
            acquisition_url=acquisition_url,
            ref=ref,
            doc_path=doc_path,
        )

    if action == "wiki":
        wiki_page_slug = "/".join(segments[3:]) or None
        wiki_path = f"/{owner}/{repo}/wiki"
        if wiki_page_slug:
            wiki_path = f"{wiki_path}/{wiki_page_slug}"
        canonical_url = _canonical_url(parsed.hostname or "github.com", wiki_path)
        return RepoDocsUrlClassification(
            provider=RepoDocsProvider.GITHUB,
            doc_kind=RepoDocsDocKind.WIKI,
            host=parsed.hostname or "github.com",
            repo_slug=repo_slug,
            repo_namespace=owner,
            repo_name=repo,
            original_url=raw_url,
            canonical_url=canonical_url,
            acquisition_url=canonical_url,
            wiki_page_slug=wiki_page_slug,
            is_wiki_root=wiki_page_slug is None,
        )

    raise ValueError("Unsupported GitHub repository documentation URL. Supported shapes are /blob/<ref>/README.md and /wiki")


def _classify_gitlab_repo_docs_url(raw_url: str) -> RepoDocsUrlClassification:
    parsed = urlparse(raw_url)
    segments = _clean_path_segments(parsed.path)
    if "-" not in segments:
        raise ValueError("GitLab repository documentation URLs must include /-/ in the path")

    dash_index = segments.index("-")
    if dash_index < 2:
        raise ValueError("GitLab repository documentation URLs must include namespace, repository, and a supported docs path")
    if dash_index + 1 >= len(segments):
        raise ValueError("GitLab repository documentation URL is missing the docs action after /-/")

    namespace_parts = segments[:dash_index - 1]
    repo = segments[dash_index - 1].strip()
    action = segments[dash_index + 1].strip().lower()
    host = parsed.hostname or "gitlab.com"
    if not repo:
        raise ValueError("GitLab repository documentation URLs must include a repository name")
    if not namespace_parts:
        raise ValueError("GitLab repository documentation URLs must include at least one namespace segment")

    repo_namespace = "/".join(namespace_parts)
    repo_slug = f"{repo_namespace}/{repo}"
    if action == "blob":
        if dash_index + 3 >= len(segments):
            raise ValueError("GitLab README URLs must use the /-/blob/<ref>/README.md shape")
        ref = segments[dash_index + 2].strip()
        doc_path = "/".join(segments[dash_index + 3:])
        if not ref:
            raise ValueError("GitLab README URLs must include a branch or ref after /-/blob/")
        if doc_path.lower() != "readme.md":
            raise ValueError("GitLab repository documentation currently supports only README.md blob URLs")
        canonical_url = _canonical_url(host, f"/{repo_namespace}/{repo}/-/blob/{ref}/README.md")
        acquisition_url = _canonical_url(host, f"/{repo_namespace}/{repo}/-/raw/{ref}/README.md")
        return RepoDocsUrlClassification(
            provider=RepoDocsProvider.GITLAB,
            doc_kind=RepoDocsDocKind.README,
            host=host,
            repo_slug=repo_slug,
            repo_namespace=repo_namespace,
            repo_name=repo,
            original_url=raw_url,
            canonical_url=canonical_url,
            acquisition_url=acquisition_url,
            ref=ref,
            doc_path=doc_path,
        )

    if action == "wikis":
        wiki_page_slug = "/".join(segments[dash_index + 2:]) or None
        wiki_path = f"/{repo_namespace}/{repo}/-/wikis"
        if wiki_page_slug:
            wiki_path = f"{wiki_path}/{wiki_page_slug}"
        canonical_url = _canonical_url(host, wiki_path)
        return RepoDocsUrlClassification(
            provider=RepoDocsProvider.GITLAB,
            doc_kind=RepoDocsDocKind.WIKI,
            host=host,
            repo_slug=repo_slug,
            repo_namespace=repo_namespace,
            repo_name=repo,
            original_url=raw_url,
            canonical_url=canonical_url,
            acquisition_url=canonical_url,
            wiki_page_slug=wiki_page_slug,
            is_wiki_root=wiki_page_slug is None,
        )

    raise ValueError("Unsupported GitLab repository documentation URL. Supported shapes are /-/blob/<ref>/README.md and /-/wikis")


def classify_repo_docs_url(url: str) -> RepoDocsUrlClassification:
    raw_url = validate_public_http_url(url)
    parsed = urlparse(raw_url)
    host = (parsed.hostname or "").strip().lower()
    if host in {"github.com", "www.github.com"}:
        return _classify_github_repo_docs_url(raw_url)
    if host in {"gitlab.com", "www.gitlab.com"}:
        return _classify_gitlab_repo_docs_url(raw_url)
    raise ValueError("Only github.com and gitlab.com repository documentation URLs are currently supported")


class WebIngestRequest(BaseModel):
    url: HttpUrl
    department: str = Field(min_length=1, max_length=128)
    ingest_label: Optional[str] = Field(default=None, min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.\- ]+$")
    depth_limit: Optional[int] = Field(default=None, ge=1, le=20)

    @field_validator("department")
    @classmethod
    def validate_department(cls, value: str) -> str:
        return validate_ingest_department(value)

    @field_validator("ingest_label")
    @classmethod
    def normalize_ingest_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        clean = (value or "").strip()
        return clean or None

    @field_validator("url")
    @classmethod
    def validate_public_url(cls, value: HttpUrl) -> HttpUrl:
        validate_public_http_url(str(value))
        return value


class PdfIngestRequest(BaseModel):
    department: str = Field(min_length=1, max_length=128)
    ingest_label: Optional[str] = Field(default=None, min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.\- ]+$")
    source_url: Optional[HttpUrl] = None
    original_filename: Optional[str] = Field(default=None, max_length=255)

    @field_validator("department")
    @classmethod
    def validate_department(cls, value: str) -> str:
        return validate_ingest_department(value)

    @field_validator("ingest_label")
    @classmethod
    def normalize_ingest_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        clean = (value or "").strip()
        return clean or None


class RepoDocsIngestRequest(BaseModel):
    url: HttpUrl
    department: str = Field(min_length=1, max_length=128)
    ingest_label: Optional[str] = Field(default=None, min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.\- ]+$")

    @field_validator("department")
    @classmethod
    def validate_department(cls, value: str) -> str:
        return validate_ingest_department(value)

    @field_validator("ingest_label")
    @classmethod
    def normalize_ingest_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        clean = (value or "").strip()
        return clean or None

    @field_validator("url")
    @classmethod
    def validate_repo_docs_url(cls, value: HttpUrl) -> HttpUrl:
        classify_repo_docs_url(str(value))
        return value


class PurgeDepartmentRequest(BaseModel):
    department: str = Field(min_length=1, max_length=128)
    confirm: bool
    dry_run: bool = Field(default=False)

    @field_validator("department")
    @classmethod
    def validate_department(cls, value: str) -> str:
        return validate_ingest_department(value)

    @field_validator("confirm")
    @classmethod
    def validate_confirm(cls, value: bool) -> bool:
        if value is not True:
            raise ValueError("confirm must be true to authorize purge request")
        return value


class RtWeeklyIngestRequest(BaseModel):
    overlap_hours: Optional[int] = Field(default=None, ge=0, le=168)
    ingest_label: Optional[str] = Field(default=None, min_length=1, max_length=120, pattern=r"^[A-Za-z0-9_.\- ]+$")

    @field_validator("ingest_label")
    @classmethod
    def normalize_ingest_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        clean = (value or "").strip()
        return clean or None


class IngestJobStatusResponse(BaseModel):
    job_id: str = Field(min_length=1, max_length=64)
    source_type: IngestSourceType
    state: IngestState
    stage: str = Field(default="queued", max_length=64)
    progress: float = Field(default=0.0, ge=0.0, le=1.0)
    message: Optional[str] = Field(default=None, max_length=2048)
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    request: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[IngestJobResultSummary] = None
    error: Optional[str] = Field(default=None, max_length=4096)

    @field_validator("created_at", "started_at", "finished_at")
    @classmethod
    def validate_iso_like_datetime(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw = value.strip()
        if not raw:
            return None
        datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return raw
