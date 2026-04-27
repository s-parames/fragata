from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from .web_pipeline import count_jsonl_rows, run_command


@dataclass(frozen=True)
class PdfPipelinePaths:
    job_root: Path
    pdf_input_root: Path
    extract_root: Path
    prepared_root: Path
    chunk_root: Path
    pdf_pages_jsonl: Path
    pdf_docs_jsonl: Path
    prepared_pages_jsonl: Path
    prepare_summary_json: Path
    chunked_jsonl: Path


def build_pdf_pipeline_paths(job_id: str, base_root: str = "data/raw_site/jobs") -> PdfPipelinePaths:
    job_root = Path(base_root) / job_id
    pdf_input_root = job_root / "pdf_input"
    extract_root = job_root / "extract"
    prepared_root = job_root / "prepared"
    chunk_root = job_root / "chunked"
    return PdfPipelinePaths(
        job_root=job_root,
        pdf_input_root=pdf_input_root,
        extract_root=extract_root,
        prepared_root=prepared_root,
        chunk_root=chunk_root,
        pdf_pages_jsonl=extract_root / "pdf_pages.jsonl",
        pdf_docs_jsonl=extract_root / "pdf_docs.jsonl",
        prepared_pages_jsonl=prepared_root / "raw_site_pages.jsonl",
        prepare_summary_json=prepared_root / "raw_site_pages_prepare_summary.json",
        chunked_jsonl=chunk_root / "raw_site_pages_chunked.jsonl",
    )


def ensure_pdf_pipeline_layout(paths: PdfPipelinePaths) -> None:
    for p in (
        paths.job_root,
        paths.pdf_input_root,
        paths.extract_root,
        paths.prepared_root,
        paths.chunk_root,
    ):
        p.mkdir(parents=True, exist_ok=True)


def run_pdf_extract_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    paths: PdfPipelinePaths,
    base_url: str,
) -> Dict[str, int]:
    pdf_files = len([p for p in paths.pdf_input_root.rglob("*.pdf") if p.is_file()])
    if pdf_files == 0:
        raise RuntimeError("No PDF files found under pdf_input")

    run_command(
        [
            python_bin,
            str(scripts_root / "07_extract_pdfs_to_jsonl.py"),
            "--input-root",
            str(paths.pdf_input_root),
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
        "input_pdf_files": pdf_files,
        "extracted_pdf_pages": count_jsonl_rows(paths.pdf_pages_jsonl),
    }


def run_pdf_prepare_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    paths: PdfPipelinePaths,
    department: str,
    ingest_label: Optional[str] = None,
    ingest_job_id: Optional[str] = None,
    ingested_at: Optional[str] = None,
) -> Dict[str, int]:
    # --html-docs points to a non-existing file by design for direct PDF ingestion.
    cmd = [
        python_bin,
        str(scripts_root / "08_prepare_raw_site_onboard_input.py"),
        "--html-docs",
        str(paths.extract_root / "html_docs.jsonl"),
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


def run_pdf_chunk_stage(
    *,
    python_bin: str,
    scripts_root: Path,
    preprocess_config: Path,
    paths: PdfPipelinePaths,
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
