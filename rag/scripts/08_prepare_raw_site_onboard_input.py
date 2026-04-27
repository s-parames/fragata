#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from scripts.common_department import validate_ingest_department
except ModuleNotFoundError:
    from common_department import validate_ingest_department


def clean_text(value: Any) -> str:
    text = str(value or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_optional_text(value: Any) -> Optional[str]:
    cleaned = clean_text(value)
    return cleaned or None


def parse_datetime_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()

    raw = str(value).strip()
    if not raw:
        return None

    iso_raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_raw)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                return dt.date().isoformat()
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return raw


def stable_sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def conversation_id_from_source(source: str) -> str:
    digest = stable_sha1(source)[:12]
    # Suffix with letter to avoid accidental numeric ticket-id inference.
    return f"doc_{digest}_d"


def source_exact_for_pdf(source: str, page_number: int) -> str:
    return f"{source}#page={page_number}"


def source_exact_for_html(source: str, section_idx: int) -> str:
    return f"{source}#section-{section_idx:03d}"


def split_html_into_synthetic_pages(text: str, target_chars: int, hard_max_chars: int) -> List[str]:
    text = clean_text(text)
    if not text:
        return []

    paragraphs = [clean_text(part) for part in text.split("\n\n") if clean_text(part)]
    if not paragraphs:
        return [text]

    pages: List[str] = []
    current = ""
    for para in paragraphs:
        if len(para) > hard_max_chars:
            if current:
                pages.append(current)
                current = ""
            start = 0
            while start < len(para):
                end = min(start + hard_max_chars, len(para))
                pages.append(clean_text(para[start:end]))
                start = end
            continue

        candidate = f"{current}\n\n{para}".strip() if current else para
        if len(candidate) <= hard_max_chars:
            current = candidate
            if len(current) >= target_chars:
                pages.append(current)
                current = ""
            continue

        if current:
            pages.append(current)
            current = para
        else:
            pages.append(para)
            current = ""

    if current:
        pages.append(current)
    return [clean_text(page) for page in pages if clean_text(page)]


def first_nonempty_line(text: str) -> str:
    for line in clean_text(text).split("\n"):
        line = clean_text(line)
        if line:
            return line
    return ""


def extract_text_from_html_fragment(raw_html: str) -> str:
    working = raw_html or ""
    working = re.sub(r"(?is)<!--.*?-->", " ", working)

    body_match = re.search(r"(?is)<body\b[^>]*>(.*)</body>", working)
    if body_match:
        working = body_match.group(1)

    working = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", working)
    working = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", working)
    working = re.sub(r"(?is)<noscript\b[^>]*>.*?</noscript>", " ", working)
    working = re.sub(r"(?is)<template\b[^>]*>.*?</template>", " ", working)
    working = re.sub(r"(?is)<svg\b[^>]*>.*?</svg>", " ", working)
    working = re.sub(r"(?is)<iframe\b[^>]*>.*?</iframe>", " ", working)
    working = re.sub(r"(?is)<br\s*/?>", "\n", working)
    working = re.sub(r"(?is)</p\s*>", "\n\n", working)
    working = re.sub(r"(?is)</(h1|h2|h3|h4|h5|h6)\s*>", "\n\n", working)
    working = re.sub(r"(?is)</(li|tr|dt|dd)\s*>", "\n", working)
    working = re.sub(r"(?is)<[^>]+>", " ", working)
    working = html.unescape(working)
    return clean_text(working)


def repo_docs_text(content: Any, content_format: Any) -> str:
    fmt = clean_text(content_format).lower()
    raw = str(content or "")
    if fmt == "html":
        return extract_text_from_html_fragment(raw)
    return clean_text(raw)


def repo_docs_relpath(row: Dict[str, Any]) -> str:
    provider = clean_text(row.get("provider")).lower() or "repo_docs"
    repo_slug = clean_text(row.get("repo_slug")).strip("/") or "repo"
    doc_kind = clean_text(row.get("doc_kind")).lower()
    wiki_page_slug = clean_text(row.get("wiki_page_slug")).strip("/")
    doc_path = clean_text(row.get("doc_path")).strip("/") or "README.md"

    if doc_kind == "wiki":
        tail = f"wiki/{wiki_page_slug}.html" if wiki_page_slug else "wiki/index.html"
    else:
        tail = doc_path or "README.md"
    return f"repo_docs/{provider}/{repo_slug}/{tail}"


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as src:
        for line_no, line in enumerate(src, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON ({exc.msg})") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{line_no}: each row must be a JSON object")
            yield row


def base_common_row(
    *,
    source: str,
    source_exact: str,
    source_type: str,
    department: str,
    ingest_label: Optional[str],
    ingest_job_id: Optional[str],
    ingested_at: Optional[str],
    extracted_at: str,
    conversation_id: str,
    doc_id: str,
    page_number: int,
    page_total: int,
    page_label: str,
    page_anchor: str,
    page_title: str,
    text: str,
    relpath: str,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cleaned = clean_text(text)
    row = {
        "link": source_exact,
        "source": source,
        "source_exact": source_exact,
        "source_type": source_type,
        "department": department,
        "ingest_label": clean_text(ingest_label),
        "ingest_job_id": clean_text(ingest_job_id),
        "ingested_at": parse_datetime_like(ingested_at) or extracted_at,
        "lastUpdated": extracted_at,
        "last_updated": extracted_at,
        "conversation_id": conversation_id,
        "doc_id": doc_id,
        "page_number": page_number,
        "page_total": page_total,
        "page_label": page_label,
        "page_anchor": page_anchor,
        "page_title": clean_text(page_title),
        "messages": [{"role": "assistant", "content": cleaned}],
        "stats": {"pii_tags": {}},
        "char_len": len(cleaned),
        "relpath": relpath,
    }
    if extra_metadata:
        row.update(extra_metadata)
    return row


def build_rows_from_pdf_pages(
    rows: Iterable[Dict[str, Any]],
    *,
    department: str,
    ingest_label: Optional[str],
    ingest_job_id: Optional[str],
    ingested_at: Optional[str],
    min_char_len: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    out: List[Dict[str, Any]] = []
    stats = {"rows_in": 0, "rows_out": 0, "rows_skipped": 0}
    reasons: Dict[str, int] = {}

    for row in rows:
        stats["rows_in"] += 1

        source = clean_text(row.get("source"))
        extracted_at = parse_datetime_like(row.get("extracted_at"))
        status = clean_text(row.get("extraction_status")).lower()
        text = clean_text(row.get("text"))
        page_number = int(row.get("page") or 0)
        page_total = int(row.get("total_pages") or 0)
        relpath = clean_text(row.get("pdf_relpath"))

        err: Optional[str] = None
        if not source:
            err = "missing source"
        elif not extracted_at:
            err = "missing extracted_at"
        elif status and status not in {"ok", "partial"}:
            err = f"status={status}"
        elif not text:
            err = "empty text"
        elif len(text) < min_char_len:
            err = f"text too short (<{min_char_len})"
        elif page_number <= 0:
            err = "invalid page number"
        elif page_total <= 0:
            err = "invalid total pages"

        if err:
            stats["rows_skipped"] += 1
            reasons[err] = reasons.get(err, 0) + 1
            continue

        doc_id = f"pdf_{stable_sha1(source)[:12]}"
        conversation_id = conversation_id_from_source(source)
        page_label = f"page_{page_number:03d}"
        page_anchor = f"page={page_number}"
        source_exact = source_exact_for_pdf(source, page_number)

        page_row = base_common_row(
            source=source,
            source_exact=source_exact,
            source_type="pdf",
            department=department,
            ingest_label=ingest_label,
            ingest_job_id=ingest_job_id,
            ingested_at=ingested_at,
            extracted_at=extracted_at,
            conversation_id=conversation_id,
            doc_id=doc_id,
            page_number=page_number,
            page_total=page_total,
            page_label=page_label,
            page_anchor=page_anchor,
            page_title=first_nonempty_line(text),
            text=text,
            relpath=relpath,
        )
        out.append(page_row)
        stats["rows_out"] += 1

    stats["skipped_reasons"] = reasons  # type: ignore[assignment]
    return out, stats


def build_rows_from_html_docs(
    rows: Iterable[Dict[str, Any]],
    *,
    department: str,
    ingest_label: Optional[str],
    ingest_job_id: Optional[str],
    ingested_at: Optional[str],
    min_char_len: int,
    html_page_chars: int,
    html_page_hard_max: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    out: List[Dict[str, Any]] = []
    stats = {"rows_in": 0, "rows_out": 0, "rows_skipped": 0}
    reasons: Dict[str, int] = {}

    for row in rows:
        stats["rows_in"] += 1

        source = clean_text(row.get("source"))
        extracted_at = parse_datetime_like(row.get("extracted_at"))
        status = clean_text(row.get("extraction_status")).lower()
        text = clean_text(row.get("text"))
        relpath = clean_text(row.get("html_relpath"))
        title = clean_text(row.get("title"))

        err: Optional[str] = None
        if not source:
            err = "missing source"
        elif not extracted_at:
            err = "missing extracted_at"
        elif status and status not in {"ok", "partial"}:
            err = f"status={status}"
        elif not text:
            err = "empty text"

        if err:
            stats["rows_skipped"] += 1
            reasons[err] = reasons.get(err, 0) + 1
            continue

        pages = split_html_into_synthetic_pages(
            text=text, target_chars=html_page_chars, hard_max_chars=html_page_hard_max
        )
        if not pages:
            stats["rows_skipped"] += 1
            reasons["empty synthetic pages"] = reasons.get("empty synthetic pages", 0) + 1
            continue

        conversation_id = conversation_id_from_source(source)
        doc_id = f"html_{stable_sha1(source)[:12]}"
        page_total = len(pages)

        for page_number, page_text in enumerate(pages, start=1):
            if len(page_text) < min_char_len:
                stats["rows_skipped"] += 1
                reasons[f"page text too short (<{min_char_len})"] = reasons.get(
                    f"page text too short (<{min_char_len})", 0
                ) + 1
                continue
            page_label = f"page_{page_number:03d}"
            page_anchor = f"section-{page_number:03d}"
            source_exact = source_exact_for_html(source, page_number)
            page_title = title or first_nonempty_line(page_text)

            page_row = base_common_row(
                source=source,
                source_exact=source_exact,
                source_type="html",
                department=department,
                ingest_label=ingest_label,
                ingest_job_id=ingest_job_id,
                ingested_at=ingested_at,
                extracted_at=extracted_at,
                conversation_id=conversation_id,
                doc_id=doc_id,
                page_number=page_number,
                page_total=page_total,
                page_label=page_label,
                page_anchor=page_anchor,
                page_title=page_title,
                text=page_text,
                relpath=relpath,
            )
            out.append(page_row)
            stats["rows_out"] += 1

    stats["skipped_reasons"] = reasons  # type: ignore[assignment]
    return out, stats


def build_rows_from_repo_docs(
    rows: Iterable[Dict[str, Any]],
    *,
    department: str,
    ingest_label: Optional[str],
    ingest_job_id: Optional[str],
    ingested_at: Optional[str],
    min_char_len: int,
    html_page_chars: int,
    html_page_hard_max: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    out: List[Dict[str, Any]] = []
    stats = {"rows_in": 0, "rows_out": 0, "rows_skipped": 0}
    reasons: Dict[str, int] = {}

    for row in rows:
        stats["rows_in"] += 1

        source = clean_text(row.get("source") or row.get("canonical_url"))
        original_url = clean_optional_text(row.get("original_url")) or source
        canonical_url = clean_optional_text(row.get("canonical_url")) or source
        acquisition_url = clean_optional_text(row.get("acquisition_url"))
        extracted_at = (
            parse_datetime_like(row.get("extracted_at"))
            or parse_datetime_like(row.get("acquired_at"))
            or parse_datetime_like(ingested_at)
        )
        text = repo_docs_text(row.get("content"), row.get("content_format"))
        title = clean_text(row.get("page_title"))
        relpath = repo_docs_relpath(row)
        provider = clean_optional_text(row.get("provider"))
        repo_slug = clean_optional_text(row.get("repo_slug"))
        repo_namespace = clean_optional_text(row.get("repo_namespace"))
        repo_name = clean_optional_text(row.get("repo_name"))
        doc_kind = clean_optional_text(row.get("doc_kind"))

        err: Optional[str] = None
        if not source:
            err = "missing source"
        elif not extracted_at:
            err = "missing extracted_at"
        elif not text:
            err = "empty text"
        elif not provider:
            err = "missing provider"
        elif not repo_slug:
            err = "missing repo_slug"
        elif not doc_kind:
            err = "missing doc_kind"

        if err:
            stats["rows_skipped"] += 1
            reasons[err] = reasons.get(err, 0) + 1
            continue

        pages = split_html_into_synthetic_pages(
            text=text,
            target_chars=html_page_chars,
            hard_max_chars=html_page_hard_max,
        )
        if not pages:
            stats["rows_skipped"] += 1
            reasons["empty synthetic pages"] = reasons.get("empty synthetic pages", 0) + 1
            continue

        conversation_id = conversation_id_from_source(source)
        doc_id = f"html_{stable_sha1(source)[:12]}"
        page_total = len(pages)
        extra_metadata = {
            "original_url": original_url,
            "canonical_url": canonical_url,
            "acquisition_url": acquisition_url or canonical_url or source,
            "repo_docs_provider": provider,
            "repo_docs_kind": doc_kind,
            "repo_slug": repo_slug,
            "repo_namespace": repo_namespace,
            "repo_name": repo_name,
        }

        for page_number, page_text in enumerate(pages, start=1):
            if len(page_text) < min_char_len:
                stats["rows_skipped"] += 1
                reasons[f"page text too short (<{min_char_len})"] = reasons.get(
                    f"page text too short (<{min_char_len})", 0
                ) + 1
                continue

            page_label = f"page_{page_number:03d}"
            page_anchor = f"section-{page_number:03d}"
            source_exact = source_exact_for_html(source, page_number)
            page_title = title or first_nonempty_line(page_text)
            page_row = base_common_row(
                source=source,
                source_exact=source_exact,
                source_type="html",
                department=department,
                ingest_label=ingest_label,
                ingest_job_id=ingest_job_id,
                ingested_at=ingested_at,
                extracted_at=extracted_at,
                conversation_id=conversation_id,
                doc_id=doc_id,
                page_number=page_number,
                page_total=page_total,
                page_label=page_label,
                page_anchor=page_anchor,
                page_title=page_title,
                text=page_text,
                relpath=relpath,
                extra_metadata=extra_metadata,
            )
            out.append(page_row)
            stats["rows_out"] += 1

    stats["skipped_reasons"] = reasons  # type: ignore[assignment]
    return out, stats


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row.get('source_exact')}|{row.get('page_label')}"
        current = by_key.get(key)
        if current is None:
            by_key[key] = row
            continue
        current_len = int(current.get("char_len") or 0)
        candidate_len = int(row.get("char_len") or 0)
        if candidate_len > current_len:
            by_key[key] = row
    ordered = [by_key[key] for key in sorted(by_key.keys())]
    return ordered


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--html-docs",
        default="data/raw_site/slurm.schedmd.com/html_docs.jsonl",
        help="HTML docs JSONL from 07_extract_html_to_jsonl.py",
    )
    ap.add_argument(
        "--pdf-pages",
        default="data/raw_site/slurm.schedmd.com/pdf_pages.jsonl",
        help="PDF pages JSONL from 07_extract_pdfs_to_jsonl.py",
    )
    ap.add_argument(
        "--repo-docs",
        default=None,
        help="Repo docs JSONL from ingest/repo_docs_pipeline.py",
    )
    ap.add_argument(
        "--out",
        default="data/raw_site/slurm.schedmd.com/raw_site_pages_sistemas.jsonl",
        help="Output page-aware JSONL for raw_site chunking",
    )
    ap.add_argument(
        "--summary-out",
        default="data/raw_site/slurm.schedmd.com/raw_site_pages_prepare_summary.json",
        help="Summary JSON output path",
    )
    ap.add_argument(
        "--department",
        default="sistemas",
        help="Target department for generated rows (sanitized lowercase ASCII)",
    )
    ap.add_argument(
        "--min-char-len",
        type=int,
        default=40,
        help="Skip page rows shorter than this length",
    )
    ap.add_argument(
        "--html-page-chars",
        type=int,
        default=2200,
        help="Target chars per synthetic HTML page before chunking",
    )
    ap.add_argument(
        "--html-page-hard-max",
        type=int,
        default=2800,
        help="Hard max chars per synthetic HTML page before split",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Fail if at least one row is skipped",
    )
    ap.add_argument(
        "--ingest-label",
        default=None,
        help="Optional ingestion label to propagate to downstream chunks/ready rows",
    )
    ap.add_argument(
        "--ingest-job-id",
        default=None,
        help="Optional ingestion job id to propagate to downstream chunks/ready rows",
    )
    ap.add_argument(
        "--ingested-at",
        default=None,
        help="Optional ingestion timestamp (ISO-like). Defaults to current UTC time if omitted",
    )
    args = ap.parse_args()

    try:
        dept = validate_ingest_department(args.department)
    except ValueError as exc:
        raise SystemExit(f"Invalid --department: {exc}") from exc

    html_path = Path(args.html_docs)
    pdf_pages_path = Path(args.pdf_pages)
    repo_docs_path = Path(args.repo_docs) if args.repo_docs else None
    out_path = Path(args.out)
    summary_path = Path(args.summary_out)

    if not html_path.exists() and not pdf_pages_path.exists() and not (repo_docs_path and repo_docs_path.exists()):
        raise SystemExit(
            "No input docs found. At least one of --html-docs, --pdf-pages, or --repo-docs must exist."
        )

    normalized_ingested_at = parse_datetime_like(args.ingested_at)
    if normalized_ingested_at is None:
        normalized_ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    all_rows: List[Dict[str, Any]] = []
    files_summary: List[Dict[str, Any]] = []
    total_skipped = 0

    if pdf_pages_path.exists():
        pdf_rows, pdf_stats = build_rows_from_pdf_pages(
            iter_jsonl(pdf_pages_path),
            department=dept,
            ingest_label=args.ingest_label,
            ingest_job_id=args.ingest_job_id,
            ingested_at=normalized_ingested_at,
            min_char_len=args.min_char_len,
        )
        all_rows.extend(pdf_rows)
        total_skipped += int(pdf_stats["rows_skipped"])
        files_summary.append(
            {
                "source": str(pdf_pages_path),
                **pdf_stats,
            }
        )

    if html_path.exists():
        html_rows, html_stats = build_rows_from_html_docs(
            iter_jsonl(html_path),
            department=dept,
            ingest_label=args.ingest_label,
            ingest_job_id=args.ingest_job_id,
            ingested_at=normalized_ingested_at,
            min_char_len=args.min_char_len,
            html_page_chars=args.html_page_chars,
            html_page_hard_max=args.html_page_hard_max,
        )
        all_rows.extend(html_rows)
        total_skipped += int(html_stats["rows_skipped"])
        files_summary.append(
            {
                "source": str(html_path),
                **html_stats,
            }
        )

    if repo_docs_path and repo_docs_path.exists():
        repo_rows, repo_stats = build_rows_from_repo_docs(
            iter_jsonl(repo_docs_path),
            department=dept,
            ingest_label=args.ingest_label,
            ingest_job_id=args.ingest_job_id,
            ingested_at=normalized_ingested_at,
            min_char_len=args.min_char_len,
            html_page_chars=args.html_page_chars,
            html_page_hard_max=args.html_page_hard_max,
        )
        all_rows.extend(repo_rows)
        total_skipped += int(repo_stats["rows_skipped"])
        files_summary.append(
            {
                "source": str(repo_docs_path),
                **repo_stats,
            }
        )

    deduped = dedupe_rows(all_rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as dst:
        for row in deduped:
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = {
        "department": dept,
        "ingest_label": clean_text(args.ingest_label),
        "ingest_job_id": clean_text(args.ingest_job_id),
        "ingested_at": normalized_ingested_at,
        "total_rows_out": len(deduped),
        "total_rows_skipped": total_skipped,
        "files": files_summary,
        "output": str(out_path),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as dst:
        json.dump(summary, dst, ensure_ascii=False, indent=2)
        dst.write("\n")

    if args.strict and total_skipped > 0:
        raise SystemExit(
            f"Raw-site page preparation finished with skipped rows (strict mode): {total_skipped}"
        )

    print(
        "Raw-site page preparation completed: "
        f"out={len(deduped)} skipped={total_skipped} summary={summary_path}"
    )


if __name__ == "__main__":
    main()
