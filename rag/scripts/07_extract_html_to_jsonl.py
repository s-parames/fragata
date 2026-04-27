#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse


STRIP_BLOCK_PATTERNS = [
    r"(?is)<script\b[^>]*>.*?</script>",
    r"(?is)<style\b[^>]*>.*?</style>",
    r"(?is)<noscript\b[^>]*>.*?</noscript>",
    r"(?is)<template\b[^>]*>.*?</template>",
    r"(?is)<svg\b[^>]*>.*?</svg>",
    r"(?is)<canvas\b[^>]*>.*?</canvas>",
    r"(?is)<iframe\b[^>]*>.*?</iframe>",
    r"(?is)<header\b[^>]*>.*?</header>",
    r"(?is)<nav\b[^>]*>.*?</nav>",
    r"(?is)<footer\b[^>]*>.*?</footer>",
    r"(?is)<section\b[^>]*class=[\"'][^\"']*slurm-search[^\"']*[\"'][^>]*>.*?</section>",
    r"(?is)<div\b[^>]*class=[\"'][^\"']*site-nav[^\"']*[\"'][^>]*>.*?</div>",
    r"(?is)<div\b[^>]*class=[\"'][^\"']*site-masthead[^\"']*[\"'][^>]*>.*?</div>",
]

DROP_LINES_EXACT = {
    "Navigation",
    "About",
    "Using",
    "Installing",
    "Getting Help",
    "Slurm Workload Manager",
    "SchedMD",
}

DROP_LINE_PATTERNS = [
    re.compile(r"(?i)^\s*version\s+\d+(\.\d+)*\s*$"),
    re.compile(r"(?i)^\s*(overview|release notes|documentation|faq)\s*$"),
    re.compile(r"(?i)^\s*(download|related software|installation guide)\s*$"),
    re.compile(r"(?i)^\s*(mailing lists|support and training|troubleshooting)\s*$"),
]


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"\u00a0", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def canonicalize_path(path: str) -> str:
    clean = (path or "").strip()
    if not clean:
        return clean
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    if clean.startswith("/"):
        return clean
    clean = clean.lstrip("./")
    return clean


def build_source_url(base_url: str, rel_path: str) -> str:
    base = (base_url or "").strip().rstrip("/")
    rel = canonicalize_path(rel_path)
    if rel.startswith("http://") or rel.startswith("https://"):
        return rel
    parsed = urlparse(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return f"{base}/{rel}" if rel else base

    base_root = f"{parsed.scheme}://{parsed.netloc}"
    host = parsed.netloc.lower()
    rel_lower = rel.lower()
    if host and rel_lower == host:
        rel = "/"
    elif host and rel_lower.startswith(f"{host}/"):
        rel = rel[len(host) :]
    if not rel:
        return base_root
    return urljoin(f"{base_root}/", rel)


def _extract_title(raw_html: str) -> str:
    match = re.search(r"(?is)<title[^>]*>(.*?)</title>", raw_html or "")
    if not match:
        return ""
    title = html.unescape(re.sub(r"(?is)<[^>]+>", " ", match.group(1)))
    return normalize_text(title)


def _extract_canonical_href(raw_html: str) -> Optional[str]:
    match = re.search(
        r"(?is)<link\b[^>]*rel=[\"']canonical[\"'][^>]*href=[\"']([^\"']+)[\"'][^>]*>",
        raw_html or "",
    )
    if not match:
        return None
    href = match.group(1).strip()
    return href or None


def _line_is_noise(line: str) -> bool:
    if not line:
        return True
    if line in DROP_LINES_EXACT:
        return True
    for pattern in DROP_LINE_PATTERNS:
        if pattern.search(line):
            return True
    return False


def extract_text_from_html(raw_html: str) -> str:
    working = raw_html or ""
    working = re.sub(r"(?is)<!--.*?-->", " ", working)

    body_match = re.search(r"(?is)<body\b[^>]*>(.*)</body>", working)
    if body_match:
        working = body_match.group(1)

    for pattern in STRIP_BLOCK_PATTERNS:
        working = re.sub(pattern, " ", working)

    working = re.sub(r"(?is)<br\s*/?>", "\n", working)
    working = re.sub(r"(?is)</p\s*>", "\n\n", working)
    working = re.sub(r"(?is)</(h1|h2|h3|h4|h5|h6)\s*>", "\n\n", working)
    working = re.sub(r"(?is)</(li|tr|dt|dd)\s*>", "\n", working)
    working = re.sub(r"(?is)<[^>]+>", " ", working)
    working = html.unescape(working)
    working = normalize_text(working)

    lines: List[str] = []
    previous = ""
    for raw_line in working.split("\n"):
        line = normalize_text(raw_line)
        if _line_is_noise(line):
            continue
        if line == previous:
            continue
        if len(line) < 2:
            continue
        lines.append(line)
        previous = line
    return normalize_text("\n".join(lines))


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


def collect_html_files(input_root: Path, limit: int) -> List[Path]:
    html_files = sorted([path for path in input_root.rglob("*.html") if path.is_file()])
    if limit > 0:
        return html_files[:limit]
    return html_files


def process_html_file(
    html_path: Path,
    input_root: Path,
    base_url: str,
    extracted_at: str,
    fail_fast: bool,
) -> Dict:
    rel_html_path = html_path.relative_to(input_root).as_posix()
    try:
        raw_html = html_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover - defensive guard
        if fail_fast:
            raise
        return {
            "id": stable_sha1(rel_html_path),
            "source": build_source_url(base_url, rel_html_path),
            "html_relpath": rel_html_path,
            "html_path": str(html_path.resolve()),
            "text": "",
            "char_len": 0,
            "title": "",
            "extraction_status": "error",
            "error": f"{exc.__class__.__name__}: {exc}",
            "extracted_at": extracted_at,
        }

    canonical_href = _extract_canonical_href(raw_html)
    source = build_source_url(base_url, canonical_href or rel_html_path)
    title = _extract_title(raw_html)
    text = extract_text_from_html(raw_html)
    if title and title.lower() not in text.lower():
        text = normalize_text(f"{title}\n\n{text}" if text else title)

    status = "ok" if text else "empty"
    return {
        "id": stable_sha1(source),
        "source": source,
        "html_relpath": rel_html_path,
        "html_path": str(html_path.resolve()),
        "title": title,
        "text": text,
        "char_len": len(text),
        "extraction_status": status,
        "error": None,
        "extracted_at": extracted_at,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract text from mirrored HTML docs and write one JSONL row per HTML page."
    )
    ap.add_argument(
        "--input-root",
        default="data/raw_site/slurm.schedmd.com/slurm.schedmd.com",
        help="Root folder to scan recursively for HTML files",
    )
    ap.add_argument(
        "--out-docs",
        default="data/raw_site/slurm.schedmd.com/html_docs.jsonl",
        help="Output JSONL with one row per HTML document",
    )
    ap.add_argument(
        "--base-url",
        default="https://slurm.schedmd.com",
        help="Base URL used to build source URLs from relative HTML paths",
    )
    ap.add_argument("--limit", type=int, default=0, help="Process only first N HTML files (0 means all)")
    ap.add_argument("--fail-fast", action="store_true", help="Stop on first extraction error")
    args = ap.parse_args()

    input_root = Path(args.input_root)
    if not input_root.exists():
        raise SystemExit(f"Input root does not exist: {input_root}")

    html_files = collect_html_files(input_root, args.limit)
    if not html_files:
        raise SystemExit(f"No HTML files found under: {input_root}")

    extracted_at = iso_utc_now()
    rows: List[Dict] = []
    for html_path in html_files:
        row = process_html_file(
            html_path=html_path,
            input_root=input_root,
            base_url=args.base_url,
            extracted_at=extracted_at,
            fail_fast=args.fail_fast,
        )
        rows.append(row)

    out_docs = Path(args.out_docs)
    atomic_write_jsonl(out_docs, rows)

    errors = sum(1 for row in rows if row.get("extraction_status") == "error")
    empty = sum(1 for row in rows if row.get("extraction_status") == "empty")
    print(
        f"HTML extraction completed. html_files={len(html_files)} "
        f"errors={errors} empty={empty} out_docs={out_docs}"
    )


if __name__ == "__main__":
    main()
