from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from .contracts import RepoDocsDocKind, RepoDocsUrlClassification, classify_repo_docs_url
from .web_pipeline import atomic_write_json, count_jsonl_rows, run_command


def _atomic_write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            for row in rows:
                json.dump(row, dst, ensure_ascii=False)
                dst.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@dataclass(frozen=True)
class RepoDocsPipelinePaths:
    job_root: Path
    acquire_root: Path
    prepared_root: Path
    chunk_root: Path
    fetched_docs_jsonl: Path
    acquire_summary_json: Path
    prepared_pages_jsonl: Path
    prepare_summary_json: Path
    chunked_jsonl: Path


def build_repo_docs_pipeline_paths(job_id: str, base_root: str = "data/raw_site/jobs") -> RepoDocsPipelinePaths:
    job_root = Path(base_root) / job_id
    acquire_root = job_root / "repo_docs"
    prepared_root = job_root / "prepared"
    chunk_root = job_root / "chunked"
    return RepoDocsPipelinePaths(
        job_root=job_root,
        acquire_root=acquire_root,
        prepared_root=prepared_root,
        chunk_root=chunk_root,
        fetched_docs_jsonl=acquire_root / "repo_docs_acquired.jsonl",
        acquire_summary_json=acquire_root / "repo_docs_acquire_summary.json",
        prepared_pages_jsonl=prepared_root / "raw_site_pages.jsonl",
        prepare_summary_json=prepared_root / "raw_site_pages_prepare_summary.json",
        chunked_jsonl=chunk_root / "raw_site_pages_chunked.jsonl",
    )


def ensure_repo_docs_pipeline_layout(paths: RepoDocsPipelinePaths) -> None:
    for path in (paths.job_root, paths.acquire_root, paths.prepared_root, paths.chunk_root):
        path.mkdir(parents=True, exist_ok=True)


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: List[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)
                return


def _canonicalize_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return urlunparse(
        (
            (parsed.scheme or "https").lower(),
            (parsed.netloc or "").lower(),
            path,
            "",
            "",
            "",
        )
    )


def _fetch_url_text(url: str) -> Dict[str, str]:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 (repo-docs-ingest)"})
    with urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset, errors="replace")
        return {
            "url": response.geturl(),
            "content_type": response.headers.get("Content-Type", "text/plain"),
            "text": body,
        }


def _extract_markdown_title(text: str) -> str:
    for line in (text or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or "README.md"
        return stripped
    return "README.md"


def _extract_html_title(html_text: str, fallback: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html_text or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return fallback
    title = unescape(re.sub(r"\s+", " ", match.group(1))).strip()
    return title or fallback


def _wiki_scope_prefix(classification: RepoDocsUrlClassification) -> str:
    if classification.doc_kind != RepoDocsDocKind.WIKI:
        raise ValueError("Wiki scope is only available for wiki classifications")
    if classification.provider.value == "github":
        return f"/{classification.repo_slug}/wiki"
    return f"/{classification.repo_slug}/-/wikis"


def _wiki_page_slug_from_url(url: str, scope_prefix: str) -> Optional[str]:
    path = urlparse(url).path or "/"
    if not path.startswith(scope_prefix):
        return None
    remainder = path[len(scope_prefix):].strip("/")
    return remainder or None


def _extract_scoped_wiki_links(
    *,
    html_text: str,
    current_url: str,
    classification: RepoDocsUrlClassification,
) -> List[str]:
    parser = _AnchorExtractor()
    parser.feed(html_text or "")

    scope_prefix = _wiki_scope_prefix(classification)
    current_host = (urlparse(classification.canonical_url).netloc or "").lower()
    discovered: List[str] = []
    seen: set[str] = set()

    for href in parser.hrefs:
        absolute = _canonicalize_url(urljoin(current_url, href))
        parsed = urlparse(absolute)
        if (parsed.netloc or "").lower() != current_host:
            continue
        if not (parsed.path or "/").startswith(scope_prefix):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        discovered.append(absolute)
    return discovered


def _readme_doc_record(classification: RepoDocsUrlClassification, payload: Dict[str, str]) -> Dict[str, Any]:
    body = payload["text"]
    resolved_url = _canonicalize_url(payload.get("url") or classification.acquisition_url)
    return {
        "provider": classification.provider.value,
        "doc_kind": classification.doc_kind.value,
        "repo_slug": classification.repo_slug,
        "repo_namespace": classification.repo_namespace,
        "repo_name": classification.repo_name,
        "host": classification.host,
        "original_url": classification.original_url,
        "source": classification.canonical_url,
        "canonical_url": classification.canonical_url,
        "acquisition_url": resolved_url,
        "ref": classification.ref,
        "doc_path": classification.doc_path,
        "page_title": _extract_markdown_title(body),
        "content": body,
        "content_format": "markdown",
        "is_seed": True,
    }


def _wiki_doc_record(
    *,
    classification: RepoDocsUrlClassification,
    page_url: str,
    html_text: str,
    is_seed: bool,
) -> Dict[str, Any]:
    canonical_url = _canonicalize_url(page_url)
    scope_prefix = _wiki_scope_prefix(classification)
    wiki_page_slug = _wiki_page_slug_from_url(canonical_url, scope_prefix)
    fallback_title = wiki_page_slug or "Wiki Home"
    return {
        "provider": classification.provider.value,
        "doc_kind": classification.doc_kind.value,
        "repo_slug": classification.repo_slug,
        "repo_namespace": classification.repo_namespace,
        "repo_name": classification.repo_name,
        "host": classification.host,
        "original_url": classification.original_url,
        "source": canonical_url,
        "canonical_url": canonical_url,
        "acquisition_url": canonical_url,
        "wiki_page_slug": wiki_page_slug,
        "page_title": _extract_html_title(html_text, fallback_title),
        "content": html_text,
        "content_format": "html",
        "is_seed": bool(is_seed),
    }


def acquire_repo_docs(
    *,
    url: str,
    output_root: Path,
    wiki_page_limit: int = 20,
) -> Dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    classification = classify_repo_docs_url(url)
    docs_path = output_root / "repo_docs_acquired.jsonl"
    summary_path = output_root / "repo_docs_acquire_summary.json"

    docs: List[Dict[str, Any]] = []
    fetched_urls: List[str] = []

    if classification.doc_kind == RepoDocsDocKind.README:
        payload = _fetch_url_text(classification.acquisition_url)
        docs.append(_readme_doc_record(classification, payload))
        fetched_urls.append(_canonicalize_url(payload.get("url") or classification.acquisition_url))
    else:
        seed_url = classification.canonical_url
        queue = [seed_url]
        seen: set[str] = set()

        while queue and len(docs) < max(1, int(wiki_page_limit)):
            current_url = _canonicalize_url(queue.pop(0))
            if current_url in seen:
                continue
            seen.add(current_url)

            payload = _fetch_url_text(current_url)
            resolved_url = _canonicalize_url(payload.get("url") or current_url)
            html_text = payload["text"]
            docs.append(
                _wiki_doc_record(
                    classification=classification,
                    page_url=resolved_url,
                    html_text=html_text,
                    is_seed=(len(docs) == 0),
                )
            )
            fetched_urls.append(resolved_url)

            if classification.is_wiki_root:
                for link in _extract_scoped_wiki_links(
                    html_text=html_text,
                    current_url=resolved_url,
                    classification=classification,
                ):
                    normalized = _canonicalize_url(link)
                    if normalized not in seen and normalized not in queue:
                        queue.append(normalized)

    _atomic_write_jsonl(docs_path, docs)
    summary = {
        "provider": classification.provider.value,
        "doc_kind": classification.doc_kind.value,
        "original_url": classification.original_url,
        "canonical_url": classification.canonical_url,
        "acquisition_url": classification.acquisition_url,
        "repo_slug": classification.repo_slug,
        "repo_namespace": classification.repo_namespace,
        "repo_name": classification.repo_name,
        "fetched_page_count": len(docs),
        "fetched_urls": fetched_urls,
        "wiki_page_limit": int(wiki_page_limit),
        "output_docs_path": str(docs_path),
    }
    atomic_write_json(summary_path, summary)
    summary["acquire_summary_path"] = str(summary_path)
    return summary


def run_repo_docs_prepare_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    paths: RepoDocsPipelinePaths,
    department: str,
    ingest_label: Optional[str] = None,
    ingest_job_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
) -> Dict[str, int]:
    cmd = [
        python_bin,
        str(scripts_root / "08_prepare_raw_site_onboard_input.py"),
        "--repo-docs",
        str(paths.fetched_docs_jsonl),
        "--out",
        str(paths.prepared_pages_jsonl),
        "--summary-out",
        str(paths.prepare_summary_json),
        "--department",
        department,
    ]
    if ingest_label:
        cmd.extend(["--ingest-label", ingest_label])
    if ingest_job_id:
        cmd.extend(["--ingest-job-id", ingest_job_id])
    if ingested_at:
        cmd.extend(["--ingested-at", ingested_at])
    run_command(cmd)
    return {
        "prepared_page_rows": count_jsonl_rows(paths.prepared_pages_jsonl),
    }


def run_repo_docs_chunk_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    preprocess_config: Path,
    paths: RepoDocsPipelinePaths,
) -> Dict[str, int | str]:
    run_command(
        [
            python_bin,
            str(scripts_root / "03_chunk_raw_site_documents.py"),
            "--config",
            str(preprocess_config),
            "--input",
            str(paths.prepared_pages_jsonl),
            "--out",
            str(paths.chunked_jsonl),
        ]
    )
    return {
        "chunk_rows": count_jsonl_rows(paths.chunked_jsonl),
        "chunked_output_path": str(paths.chunked_jsonl),
    }
