from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Collection, Dict, List, Optional
from urllib.parse import urlparse


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            json.dump(payload, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def run_command(
    args: List[str],
    *,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    ok_returncodes: Optional[Collection[int]] = None,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(args, capture_output=True, text=True, cwd=cwd, env=env)
    accepted_returncodes = {0} if ok_returncodes is None else set(ok_returncodes)
    if proc.returncode not in accepted_returncodes:
        raise RuntimeError(
            "Command failed "
            f"(code={proc.returncode}): {' '.join(args)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc


def count_jsonl_rows(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            if line.strip():
                count += 1
    return count


@dataclass(frozen=True)
class WebPipelinePaths:
    job_root: Path
    mirror_root: Path
    extract_root: Path
    prepared_root: Path
    chunk_root: Path
    html_docs_jsonl: Path
    pdf_pages_jsonl: Path
    pdf_docs_jsonl: Path
    prepared_pages_jsonl: Path
    prepare_summary_json: Path
    chunked_jsonl: Path
    mirror_marker_json: Path
    crawl_summary_json: Path


def build_web_pipeline_paths(job_id: str, base_root: str = "data/raw_site/jobs") -> WebPipelinePaths:
    job_root = Path(base_root) / job_id
    mirror_root = job_root / "mirror"
    extract_root = job_root / "extract"
    prepared_root = job_root / "prepared"
    chunk_root = job_root / "chunked"
    return WebPipelinePaths(
        job_root=job_root,
        mirror_root=mirror_root,
        extract_root=extract_root,
        prepared_root=prepared_root,
        chunk_root=chunk_root,
        html_docs_jsonl=extract_root / "html_docs.jsonl",
        pdf_pages_jsonl=extract_root / "pdf_pages.jsonl",
        pdf_docs_jsonl=extract_root / "pdf_docs.jsonl",
        prepared_pages_jsonl=prepared_root / "raw_site_pages.jsonl",
        prepare_summary_json=prepared_root / "raw_site_pages_prepare_summary.json",
        chunked_jsonl=chunk_root / "raw_site_pages_chunked.jsonl",
        mirror_marker_json=mirror_root / "_mirror_marker.json",
        crawl_summary_json=mirror_root / "_crawl_summary.json",
    )


def ensure_web_pipeline_layout(paths: WebPipelinePaths) -> None:
    for p in (
        paths.job_root,
        paths.mirror_root,
        paths.extract_root,
        paths.prepared_root,
        paths.chunk_root,
    ):
        p.mkdir(parents=True, exist_ok=True)


def _base_url_prefix(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https URLs are supported")
    if not parsed.hostname:
        raise ValueError("URL must include a valid host")
    host = parsed.hostname.strip().lower()
    return f"{parsed.scheme}://{host}"


def _domain_for_wget(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Only http/https URLs are supported")
    if not parsed.hostname:
        raise ValueError("URL must include a valid host")
    return parsed.hostname.strip().lower()


def _scope_prefix_for_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    path = (parsed.path or "/").strip()
    if not path:
        return "/"
    if not path.startswith("/"):
        path = f"/{path}"
    if path.endswith("/"):
        return path
    parent, _sep, _name = path.rpartition("/")
    if not parent:
        return "/"
    return f"{parent}/"


def _file_url_path(output_root: Path, file_path: Path, domain: str) -> str:
    rel_parts = file_path.relative_to(output_root).parts
    if len(rel_parts) < 2:
        return ""
    domain_root = rel_parts[0].strip().lower()
    if domain_root != domain:
        return ""
    url_path = "/" + "/".join(rel_parts[1:])
    return url_path.replace("//", "/")


def _is_url_path_in_scope(url_path: str, scope_prefix: str) -> bool:
    normalized = (url_path or "").strip()
    if not normalized:
        return False
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    if scope_prefix == "/":
        return True
    return normalized.startswith(scope_prefix)


def _enforce_scope_on_mirror(
    *,
    output_root: Path,
    domain: str,
    scope_prefix: str,
) -> Dict[str, int]:
    html_before = 0
    pdf_before = 0
    html_after = 0
    pdf_after = 0
    removed_out_of_scope = 0

    for file_path in sorted(output_root.rglob("*")):
        if not file_path.is_file():
            continue
        suffix = file_path.suffix.lower()
        if suffix not in {".html", ".pdf"}:
            continue

        if suffix == ".html":
            html_before += 1
        else:
            pdf_before += 1

        url_path = _file_url_path(output_root, file_path, domain)
        if not _is_url_path_in_scope(url_path, scope_prefix):
            file_path.unlink(missing_ok=True)
            removed_out_of_scope += 1
            continue

        if suffix == ".html":
            html_after += 1
        else:
            pdf_after += 1

    return {
        "mirrored_html_files_before_scope": html_before,
        "mirrored_pdf_files_before_scope": pdf_before,
        "mirrored_html_files": html_after,
        "mirrored_pdf_files": pdf_after,
        "removed_out_of_scope_files": removed_out_of_scope,
    }


def _count_wget_http_errors(stderr: str) -> int:
    if not stderr:
        return 0
    return len(re.findall(r"ERROR \d{3}:", stderr))


def mirror_website(
    *,
    url: str,
    output_root: Path,
    depth_limit: Optional[int] = None,
) -> Dict[str, int | str]:
    output_root.mkdir(parents=True, exist_ok=True)
    marker = output_root / "_mirror_marker.json"
    crawl_summary_path = output_root / "_crawl_summary.json"
    domain = _domain_for_wget(url)
    scope_prefix = _scope_prefix_for_url(url)
    if marker.exists():
        payload = json.loads(marker.read_text(encoding="utf-8"))
        if payload.get("scope_prefix"):
            scope_prefix = str(payload["scope_prefix"])
        scope_stats = _enforce_scope_on_mirror(
            output_root=output_root,
            domain=domain,
            scope_prefix=scope_prefix,
        )
        summary_payload = {
            "seed_url": url,
            "domain": domain,
            "scope_prefix": scope_prefix,
            "mirror_reused": 1,
            **scope_stats,
        }
        atomic_write_json(crawl_summary_path, summary_payload)
        marker_payload = {
            "domain": domain,
            "scope_prefix": scope_prefix,
            "mirrored_html_files": scope_stats["mirrored_html_files"],
            "mirrored_pdf_files": scope_stats["mirrored_pdf_files"],
        }
        atomic_write_json(marker, marker_payload)
        return {
            "domain": domain,
            "scope_prefix": scope_prefix,
            "mirrored_html_files": scope_stats["mirrored_html_files"],
            "mirrored_pdf_files": scope_stats["mirrored_pdf_files"],
            "removed_out_of_scope_files": scope_stats["removed_out_of_scope_files"],
            "crawl_summary_path": str(crawl_summary_path),
            "mirror_reused": 1,
        }

    cmd = [
        "wget",
        "--mirror",
        "--no-parent",
        "--domains",
        domain,
        "--adjust-extension",
        "--convert-links",
        "--page-requisites",
        "--wait=1",
        "--random-wait",
        "--user-agent=Mozilla/5.0",
        "-P",
        str(output_root),
    ]
    if depth_limit is not None:
        cmd.extend(["--level", str(int(depth_limit))])
    cmd.append(url)
    proc = run_command(cmd, ok_returncodes={0, 8})

    scope_stats = _enforce_scope_on_mirror(
        output_root=output_root,
        domain=domain,
        scope_prefix=scope_prefix,
    )
    mirrored_total = int(scope_stats["mirrored_html_files"]) + int(scope_stats["mirrored_pdf_files"])
    wget_http_error_count = _count_wget_http_errors(proc.stderr)
    partial_warning: Optional[str] = None
    if proc.returncode == 8:
        if mirrored_total == 0:
            raise RuntimeError(
                "Website mirror reported HTTP retrieval errors and produced no HTML or PDF files within scope"
            )
        partial_warning = (
            "wget reported HTTP retrieval errors for some linked resources, but mirrored content was captured "
            "and ingestion will continue."
        )
    marker_payload = {
        "domain": domain,
        "scope_prefix": scope_prefix,
        "mirrored_html_files": scope_stats["mirrored_html_files"],
        "mirrored_pdf_files": scope_stats["mirrored_pdf_files"],
    }
    summary_payload = {
        "seed_url": url,
        "domain": domain,
        "scope_prefix": scope_prefix,
        "mirror_reused": 0,
        **scope_stats,
    }
    if proc.returncode != 0:
        summary_payload["wget_returncode"] = proc.returncode
    if wget_http_error_count > 0:
        summary_payload["wget_http_error_count"] = wget_http_error_count
    if partial_warning is not None:
        summary_payload["warning"] = partial_warning
    atomic_write_json(marker, marker_payload)
    atomic_write_json(crawl_summary_path, summary_payload)
    result = {
        **marker_payload,
        "removed_out_of_scope_files": scope_stats["removed_out_of_scope_files"],
        "crawl_summary_path": str(crawl_summary_path),
        "mirror_reused": 0,
    }
    if proc.returncode != 0:
        result["wget_returncode"] = proc.returncode
    if wget_http_error_count > 0:
        result["wget_http_error_count"] = wget_http_error_count
    if partial_warning is not None:
        result["warning"] = partial_warning
    return result


def run_extract_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    paths: WebPipelinePaths,
    base_url: str,
) -> Dict[str, int]:
    html_files = len([p for p in paths.mirror_root.rglob("*.html") if p.is_file()])
    pdf_files = len([p for p in paths.mirror_root.rglob("*.pdf") if p.is_file()])
    if html_files == 0 and pdf_files == 0:
        raise RuntimeError("Mirror completed but produced no HTML or PDF files")

    if html_files > 0:
        run_command(
            [
                python_bin,
                str(scripts_root / "07_extract_html_to_jsonl.py"),
                "--input-root",
                str(paths.mirror_root),
                "--out-docs",
                str(paths.html_docs_jsonl),
                "--base-url",
                base_url,
                "--fail-fast",
            ]
        )

    if pdf_files > 0:
        run_command(
            [
                python_bin,
                str(scripts_root / "07_extract_pdfs_to_jsonl.py"),
                "--input-root",
                str(paths.mirror_root),
                "--out-pages",
                str(paths.pdf_pages_jsonl),
                "--out-docs",
                str(paths.pdf_docs_jsonl),
                "--base-url",
                base_url,
                "--fail-fast",
            ]
        )

    return {
        "mirrored_html_files": html_files,
        "mirrored_pdf_files": pdf_files,
        "extracted_html_docs": count_jsonl_rows(paths.html_docs_jsonl),
        "extracted_pdf_pages": count_jsonl_rows(paths.pdf_pages_jsonl),
    }


def run_prepare_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    paths: WebPipelinePaths,
    department: str,
    ingest_label: Optional[str] = None,
    ingest_job_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
) -> Dict[str, int]:
    cmd = [
        python_bin,
        str(scripts_root / "08_prepare_raw_site_onboard_input.py"),
        "--html-docs",
        str(paths.html_docs_jsonl),
        "--pdf-pages",
        str(paths.pdf_pages_jsonl),
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


def run_chunk_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    preprocess_config: Path,
    paths: WebPipelinePaths,
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


def base_url_for_scripts(url: str) -> str:
    return _base_url_prefix(url)
