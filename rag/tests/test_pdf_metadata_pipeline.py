from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


prepare_mod = load_module("prepare_raw_site", "scripts/08_prepare_raw_site_onboard_input.py")
chunk_mod = load_module("chunk_raw_site_docs", "scripts/03_chunk_raw_site_documents.py")


class PdfMetadataPropagationTests(unittest.TestCase):
    def test_pdf_rows_and_chunks_keep_ingest_metadata(self) -> None:
        pdf_pages = [
            {
                "source": "https://example.com/doc.pdf",
                "pdf_relpath": "doc.pdf",
                "page": 1,
                "total_pages": 1,
                "text": "Alpha section\n\nBeta section",
                "extraction_status": "ok",
                "extracted_at": "2026-02-26T10:00:00Z",
            }
        ]
        rows, stats = prepare_mod.build_rows_from_pdf_pages(
            pdf_pages,
            department="data_science_team",
            ingest_label="batch_pdf",
            ingest_job_id="ing_pdf_001",
            ingested_at="2026-02-26 10:05:00",
            min_char_len=10,
        )
        self.assertEqual(stats["rows_out"], 1)
        self.assertEqual(rows[0]["source_type"], "pdf")
        self.assertEqual(rows[0]["department"], "data_science_team")
        self.assertEqual(rows[0]["ingest_label"], "batch_pdf")
        self.assertEqual(rows[0]["ingest_job_id"], "ing_pdf_001")

        chunks = chunk_mod.row_to_chunks(
            rows[0],
            min_chars=10,
            target_chars=20,
            max_chars=120,
            min_keep_chars=5,
        )
        self.assertGreaterEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["source_type"], "pdf")
        self.assertEqual(chunks[0]["department"], "data_science_team")
        self.assertEqual(chunks[0]["ingest_label"], "batch_pdf")
        self.assertEqual(chunks[0]["ingest_job_id"], "ing_pdf_001")

    def test_html_rows_and_chunks_keep_custom_department(self) -> None:
        html_docs = [
            {
                "source": "https://example.com/guide",
                "html_relpath": "guide.html",
                "title": "Guide",
                "text": "Intro section\n\nDetails section\n\nMore details",
                "extraction_status": "ok",
                "extracted_at": "2026-02-26T10:00:00Z",
            }
        ]
        rows, stats = prepare_mod.build_rows_from_html_docs(
            html_docs,
            department="platform_ops",
            ingest_label="batch_web",
            ingest_job_id="ing_web_001",
            ingested_at="2026-02-26 10:05:00",
            min_char_len=10,
            html_page_chars=80,
            html_page_hard_max=120,
        )
        self.assertGreaterEqual(stats["rows_out"], 1)
        self.assertEqual(rows[0]["source_type"], "html")
        self.assertEqual(rows[0]["department"], "platform_ops")

        chunks = chunk_mod.row_to_chunks(
            rows[0],
            min_chars=10,
            target_chars=20,
            max_chars=120,
            min_keep_chars=5,
        )
        self.assertGreaterEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["source_type"], "html")
        self.assertEqual(chunks[0]["department"], "platform_ops")


if __name__ == "__main__":
    unittest.main()
