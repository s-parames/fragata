from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingest.web_pipeline import mirror_website


class WebPipelineMirrorTests(unittest.TestCase):
    def test_mirror_website_accepts_partial_wget_failures_when_content_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            def fake_run(args, capture_output, text, cwd=None, env=None):
                docs_root = output_root / "example.com" / "docs"
                docs_root.mkdir(parents=True, exist_ok=True)
                (docs_root / "index.html").write_text("<html>ok</html>\n", encoding="utf-8")
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=8,
                    stdout="",
                    stderr="2026-03-02 11:30:35 ERROR 404: Not Found.\n",
                )

            with patch("ingest.web_pipeline.subprocess.run", side_effect=fake_run):
                stats = mirror_website(
                    url="https://example.com/docs/index.html",
                    output_root=output_root,
                )

            self.assertEqual(stats["mirrored_html_files"], 1)
            self.assertEqual(stats["mirrored_pdf_files"], 0)
            self.assertEqual(stats["wget_returncode"], 8)
            self.assertEqual(stats["wget_http_error_count"], 1)
            self.assertIn("warning", stats)

            summary = json.loads((output_root / "_crawl_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["wget_returncode"], 8)
            self.assertEqual(summary["wget_http_error_count"], 1)

    def test_mirror_website_rejects_partial_wget_failures_without_html_or_pdf(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            def fake_run(args, capture_output, text, cwd=None, env=None):
                site_root = output_root / "example.com"
                site_root.mkdir(parents=True, exist_ok=True)
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=8,
                    stdout="",
                    stderr="2026-03-02 11:30:35 ERROR 404: Not Found.\n",
                )

            with patch("ingest.web_pipeline.subprocess.run", side_effect=fake_run):
                with self.assertRaises(RuntimeError):
                    mirror_website(
                        url="https://example.com/docs/index.html",
                        output_root=output_root,
                    )


if __name__ == "__main__":
    unittest.main()
