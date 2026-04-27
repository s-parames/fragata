#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    lines: List[str] = []
    for line in text.split("\n"):
        clean = re.sub(r"[ \t]+", " ", line).strip()
        if clean:
            lines.append(clean)
    merged = "\n".join(lines)
    merged = re.sub(r"\n{3,}", "\n\n", merged)
    return merged.strip()


def run_cmd(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def parse_pdf_pages(pdf_path: Path) -> int:
    proc = run_cmd(["pdfinfo", str(pdf_path)])
    if proc.returncode != 0:
        return 0
    for line in proc.stdout.splitlines():
        if line.lower().startswith("pages:"):
            value = line.split(":", 1)[1].strip()
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


def extract_page_text(pdf_path: Path, page: int) -> Tuple[str, str | None, int]:
    proc = run_cmd(
        ["pdftotext", "-enc", "UTF-8", "-f", str(page), "-l", str(page), "-nopgbrk", str(pdf_path), "-"]
    )
    if proc.returncode != 0:
        error = proc.stderr.strip() or f"pdftotext failed (code={proc.returncode})"
        return "", error, proc.returncode
    return normalize_text(proc.stdout), None, 0


def extract_full_text(pdf_path: Path) -> Tuple[str, str | None, int]:
    proc = run_cmd(["pdftotext", "-enc", "UTF-8", "-nopgbrk", str(pdf_path), "-"])
    if proc.returncode != 0:
        error = proc.stderr.strip() or f"pdftotext failed (code={proc.returncode})"
        return "", error, proc.returncode
    return normalize_text(proc.stdout), None, 0


def atomic_write_jsonl(path: Path, rows: List[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            for row in rows:
                dst.write(json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def build_source_url(base_url: str, rel_pdf_path: str) -> str:
    base = (base_url or "").strip().rstrip("/")
    rel = (rel_pdf_path or "").strip().lstrip("/")
    if not base:
        return rel

    parsed = urlparse(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return f"{base}/{rel}" if rel else base

    # Direct PDF uploads with source_url should keep the original PDF URL.
    if parsed.path.lower().endswith(".pdf"):
        return base

    host = parsed.netloc.lower()
    rel_lower = rel.lower()
    if host and rel_lower == host:
        rel = ""
    elif host and rel_lower.startswith(f"{host}/"):
        rel = rel[len(host) + 1 :]

    if not rel:
        return base
    return urljoin(f"{base}/", rel)


def collect_pdfs(input_root: Path, limit: int) -> List[Path]:
    pdfs = sorted([p for p in input_root.rglob("*.pdf") if p.is_file()])
    if limit > 0:
        return pdfs[:limit]
    return pdfs


def process_pdf(
    pdf_path: Path,
    input_root: Path,
    base_url: str,
    extracted_at: str,
    fail_fast: bool,
) -> Tuple[List[Dict], Dict]:
    rel_pdf_path = pdf_path.relative_to(input_root).as_posix()
    source = build_source_url(base_url, rel_pdf_path)
    total_pages = parse_pdf_pages(pdf_path)

    page_rows: List[Dict] = []
    doc_errors: List[str] = []
    page_texts: List[str] = []
    pages_with_text = 0
    pages_with_error = 0

    if total_pages <= 0:
        text, error, _ = extract_full_text(pdf_path)
        status = "error" if error else ("empty" if not text else "ok")
        if error:
            pages_with_error = 1
            doc_errors.append(f"page=1: {error}")
            if fail_fast:
                raise RuntimeError(f"{pdf_path}: {error}")
        if text:
            pages_with_text = 1
            page_texts.append(text)

        page_rows.append(
            {
                "id": stable_sha1(f"{source}#page=1"),
                "source": source,
                "pdf_relpath": rel_pdf_path,
                "pdf_path": str(pdf_path.resolve()),
                "page": 1,
                "total_pages": 1,
                "text": text,
                "char_len": len(text),
                "extraction_status": status,
                "error": error,
                "extracted_at": extracted_at,
            }
        )
        total_pages = 1
    else:
        for page in range(1, total_pages + 1):
            text, error, _ = extract_page_text(pdf_path, page)
            status = "error" if error else ("empty" if not text else "ok")
            if error:
                pages_with_error += 1
                doc_errors.append(f"page={page}: {error}")
                if fail_fast:
                    raise RuntimeError(f"{pdf_path} page={page}: {error}")
            if text:
                pages_with_text += 1
                page_texts.append(f"--- PAGE {page} ---\n{text}")

            page_rows.append(
                {
                    "id": stable_sha1(f"{source}#page={page}"),
                    "source": source,
                    "pdf_relpath": rel_pdf_path,
                    "pdf_path": str(pdf_path.resolve()),
                    "page": page,
                    "total_pages": total_pages,
                    "text": text,
                    "char_len": len(text),
                    "extraction_status": status,
                    "error": error,
                    "extracted_at": extracted_at,
                }
            )

    if pages_with_error == total_pages:
        doc_status = "error"
    elif pages_with_error > 0 or pages_with_text == 0:
        doc_status = "partial"
    else:
        doc_status = "ok"

    doc_text = "\n\n".join(page_texts).strip()
    doc_row: Dict = {
        "id": stable_sha1(source),
        "source": source,
        "pdf_relpath": rel_pdf_path,
        "pdf_path": str(pdf_path.resolve()),
        "total_pages": total_pages,
        "pages_with_text": pages_with_text,
        "text": doc_text,
        "char_len": len(doc_text),
        "extraction_status": doc_status,
        "errors": doc_errors,
        "extracted_at": extracted_at,
    }
    return page_rows, doc_row


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract all text from PDFs under a mirror folder and write page/doc JSONL outputs."
    )
    ap.add_argument(
        "--input-root",
        default="data/raw_site/slurm.schedmd.com/slurm.schedmd.com",
        help="Root folder to scan recursively for PDFs",
    )
    ap.add_argument(
        "--out-pages",
        default="data/raw_site/slurm.schedmd.com/pdf_pages.jsonl",
        help="Output JSONL with one row per PDF page",
    )
    ap.add_argument(
        "--out-docs",
        default="data/raw_site/slurm.schedmd.com/pdf_docs.jsonl",
        help="Output JSONL with one row per PDF document",
    )
    ap.add_argument(
        "--base-url",
        default="https://slurm.schedmd.com",
        help="Base URL used to build source URLs from relative PDF paths",
    )
    ap.add_argument("--limit", type=int, default=0, help="Process only first N PDFs (0 means all)")
    ap.add_argument("--fail-fast", action="store_true", help="Stop on first extraction error")
    args = ap.parse_args()

    input_root = Path(args.input_root)
    if not input_root.exists():
        raise SystemExit(f"Input root does not exist: {input_root}")

    pdf_files = collect_pdfs(input_root, args.limit)
    if not pdf_files:
        raise SystemExit(f"No PDF files found under: {input_root}")

    extracted_at = iso_utc_now()
    all_page_rows: List[Dict] = []
    all_doc_rows: List[Dict] = []

    for pdf_path in pdf_files:
        page_rows, doc_row = process_pdf(
            pdf_path=pdf_path,
            input_root=input_root,
            base_url=args.base_url,
            extracted_at=extracted_at,
            fail_fast=args.fail_fast,
        )
        all_page_rows.extend(page_rows)
        all_doc_rows.append(doc_row)

    out_pages = Path(args.out_pages)
    out_docs = Path(args.out_docs)
    atomic_write_jsonl(out_pages, all_page_rows)
    atomic_write_jsonl(out_docs, all_doc_rows)

    errors = sum(1 for row in all_page_rows if row["extraction_status"] == "error")
    print(
        f"PDF extraction completed. pdfs={len(pdf_files)} pages={len(all_page_rows)} "
        f"page_errors={errors} out_pages={out_pages} out_docs={out_docs}"
    )


if __name__ == "__main__":
    main()
