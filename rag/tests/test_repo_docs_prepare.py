from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingest.repo_docs_pipeline import acquire_repo_docs


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


prepare_mod = load_module("prepare_raw_site_repo_docs", "scripts/08_prepare_raw_site_onboard_input.py")
chunk_mod = load_module("chunk_raw_site_repo_docs", "scripts/03_chunk_raw_site_documents.py")


class RepoDocsPrepareTests(unittest.TestCase):
    def _acquired_docs(self, output_root: Path) -> list[dict]:
        return [
            json.loads(line)
            for line in (output_root / "repo_docs_acquired.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def _run_end_to_end_case(
        self,
        *,
        url: str,
        responses: dict[str, dict[str, str]],
        department: str,
        expected_provider: str,
        expected_kind: str,
        expected_slug: str,
        expected_pages: int,
        expected_title: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            def fake_fetch(fetch_url: str) -> dict[str, str]:
                return responses[fetch_url]

            with patch("ingest.repo_docs_pipeline._fetch_url_text", side_effect=fake_fetch):
                summary = acquire_repo_docs(
                    url=url,
                    output_root=output_root,
                    wiki_page_limit=10,
                )

            docs = self._acquired_docs(output_root)
            rows, stats = prepare_mod.build_rows_from_repo_docs(
                docs,
                department=department,
                ingest_label="repo_batch",
                ingest_job_id="ing_repo_e2e",
                ingested_at="2026-03-04 12:00:00",
                min_char_len=10,
                html_page_chars=120,
                html_page_hard_max=180,
            )

            self.assertEqual(summary["provider"], expected_provider)
            self.assertEqual(summary["doc_kind"], expected_kind)
            self.assertEqual(summary["repo_slug"], expected_slug)
            self.assertEqual(summary["fetched_page_count"], expected_pages)
            self.assertGreaterEqual(stats["rows_out"], expected_pages)
            self.assertTrue(rows)
            self.assertEqual(rows[0]["repo_docs_provider"], expected_provider)
            self.assertEqual(rows[0]["repo_docs_kind"], expected_kind)
            self.assertEqual(rows[0]["repo_slug"], expected_slug)
            self.assertEqual(rows[0]["department"], department)
            self.assertEqual(rows[0]["ingest_label"], "repo_batch")
            self.assertEqual(rows[0]["page_title"], expected_title)

            chunks = chunk_mod.row_to_chunks(
                rows[0],
                min_chars=10,
                target_chars=80,
                max_chars=220,
                min_keep_chars=5,
            )
            self.assertTrue(chunks)
            self.assertEqual(chunks[0]["repo_docs_provider"], expected_provider)
            self.assertEqual(chunks[0]["repo_docs_kind"], expected_kind)
            self.assertEqual(chunks[0]["repo_slug"], expected_slug)

    def test_repo_docs_github_readme_end_to_end_to_chunk(self) -> None:
        self._run_end_to_end_case(
            url="https://github.com/ACEsuit/mace/blob/main/README.md",
            responses={
                "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md": {
                    "url": "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md",
                    "content_type": "text/plain; charset=utf-8",
                    "text": "# MACE\nFast atomic simulations.\nInstallation steps.\nUsage notes.\n",
                }
            },
            department="sistemas",
            expected_provider="github",
            expected_kind="readme",
            expected_slug="ACEsuit/mace",
            expected_pages=1,
            expected_title="MACE",
        )

    def test_repo_docs_github_wiki_end_to_end_to_chunk(self) -> None:
        self._run_end_to_end_case(
            url="https://github.com/trinityrnaseq/trinityrnaseq/wiki",
            responses={
                "https://github.com/trinityrnaseq/trinityrnaseq/wiki": {
                    "url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                    "content_type": "text/html; charset=utf-8",
                    "text": """
                        <html><head><title>Wiki Home</title></head><body>
                        <h1>Wiki Home</h1>
                        <p>Install guide</p>
                        <a href="/trinityrnaseq/trinityrnaseq/wiki/Usage">Usage</a>
                        <a href="https://example.com/offsite">Offsite</a>
                        </body></html>
                    """,
                },
                "https://github.com/trinityrnaseq/trinityrnaseq/wiki/Usage": {
                    "url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki/Usage",
                    "content_type": "text/html; charset=utf-8",
                    "text": "<html><head><title>Usage</title></head><body><p>Usage notes</p></body></html>",
                },
            },
            department="platform_ops",
            expected_provider="github",
            expected_kind="wiki",
            expected_slug="trinityrnaseq/trinityrnaseq",
            expected_pages=2,
            expected_title="Wiki Home",
        )

    def test_repo_docs_gitlab_readme_end_to_end_to_chunk(self) -> None:
        self._run_end_to_end_case(
            url="https://gitlab.com/example-group/demo-project/-/blob/main/README.md",
            responses={
                "https://gitlab.com/example-group/demo-project/-/raw/main/README.md": {
                    "url": "https://gitlab.com/example-group/demo-project/-/raw/main/README.md",
                    "content_type": "text/plain; charset=utf-8",
                    "text": "# Demo Project\nDeployment notes.\nRunbook.\n",
                }
            },
            department="sistemas",
            expected_provider="gitlab",
            expected_kind="readme",
            expected_slug="example-group/demo-project",
            expected_pages=1,
            expected_title="Demo Project",
        )

    def test_repo_docs_gitlab_wiki_end_to_end_to_chunk(self) -> None:
        self._run_end_to_end_case(
            url="https://gitlab.com/example-group/demo-project/-/wikis",
            responses={
                "https://gitlab.com/example-group/demo-project/-/wikis": {
                    "url": "https://gitlab.com/example-group/demo-project/-/wikis",
                    "content_type": "text/html; charset=utf-8",
                    "text": """
                        <html><head><title>Wiki Home</title></head><body>
                        <h1>Wiki Home</h1>
                        <p>Getting started</p>
                        <a href="/example-group/demo-project/-/wikis/install">Install</a>
                        <a href="/example-group/demo-project/-/issues/2">Issue</a>
                        </body></html>
                    """,
                },
                "https://gitlab.com/example-group/demo-project/-/wikis/install": {
                    "url": "https://gitlab.com/example-group/demo-project/-/wikis/install",
                    "content_type": "text/html; charset=utf-8",
                    "text": "<html><head><title>Install</title></head><body><p>Install steps</p></body></html>",
                },
            },
            department="bigdata",
            expected_provider="gitlab",
            expected_kind="wiki",
            expected_slug="example-group/demo-project",
            expected_pages=2,
            expected_title="Wiki Home",
        )

    def test_repo_docs_readme_rows_and_chunks_keep_repo_metadata(self) -> None:
        repo_docs = [
            {
                "provider": "github",
                "doc_kind": "readme",
                "repo_slug": "ACEsuit/mace",
                "repo_namespace": "ACEsuit",
                "repo_name": "mace",
                "original_url": "https://github.com/ACEsuit/mace/blob/main/README.md?plain=1",
                "source": "https://github.com/ACEsuit/mace/blob/main/README.md",
                "canonical_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                "acquisition_url": "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md",
                "ref": "main",
                "doc_path": "README.md",
                "page_title": "MACE",
                "content": "# MACE\n\nFast atomic simulations.\n\nInstallation steps.\n\nUsage notes.",
                "content_format": "markdown",
            }
        ]

        rows, stats = prepare_mod.build_rows_from_repo_docs(
            repo_docs,
            department="sistemas",
            ingest_label="repo_batch",
            ingest_job_id="ing_repo_001",
            ingested_at="2026-03-04 12:00:00",
            min_char_len=10,
            html_page_chars=60,
            html_page_hard_max=80,
        )

        self.assertGreaterEqual(stats["rows_out"], 1)
        self.assertEqual(rows[0]["source_type"], "html")
        self.assertEqual(rows[0]["department"], "sistemas")
        self.assertEqual(rows[0]["page_title"], "MACE")
        self.assertEqual(rows[0]["original_url"], "https://github.com/ACEsuit/mace/blob/main/README.md?plain=1")
        self.assertEqual(rows[0]["canonical_url"], "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(rows[0]["repo_docs_provider"], "github")
        self.assertEqual(rows[0]["repo_docs_kind"], "readme")
        self.assertEqual(rows[0]["repo_slug"], "ACEsuit/mace")
        self.assertEqual(rows[0]["repo_name"], "mace")

        chunks = chunk_mod.row_to_chunks(
            rows[0],
            min_chars=10,
            target_chars=20,
            max_chars=120,
            min_keep_chars=5,
        )
        self.assertGreaterEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["source_type"], "html")
        self.assertEqual(chunks[0]["repo_docs_provider"], "github")
        self.assertEqual(chunks[0]["repo_slug"], "ACEsuit/mace")
        self.assertEqual(chunks[0]["canonical_url"], "https://github.com/ACEsuit/mace/blob/main/README.md")

    def test_repo_docs_wiki_html_is_prepared_as_clean_text(self) -> None:
        repo_docs = [
            {
                "provider": "github",
                "doc_kind": "wiki",
                "repo_slug": "trinityrnaseq/trinityrnaseq",
                "repo_namespace": "trinityrnaseq",
                "repo_name": "trinityrnaseq",
                "original_url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "source": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "canonical_url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "acquisition_url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "wiki_page_slug": None,
                "page_title": "Wiki Home",
                "content": "<html><body><h1>Wiki Home</h1><p>Install guide</p><script>ignore()</script><p>Usage tips</p></body></html>",
                "content_format": "html",
            }
        ]

        rows, stats = prepare_mod.build_rows_from_repo_docs(
            repo_docs,
            department="platform_ops",
            ingest_label="wiki_batch",
            ingest_job_id="ing_repo_002",
            ingested_at="2026-03-04 12:10:00",
            min_char_len=10,
            html_page_chars=120,
            html_page_hard_max=160,
        )

        self.assertEqual(stats["rows_out"], 1)
        content = rows[0]["messages"][0]["content"]
        self.assertIn("Wiki Home", content)
        self.assertIn("Install guide", content)
        self.assertIn("Usage tips", content)
        self.assertNotIn("<script>", content)
        self.assertEqual(rows[0]["source_type"], "html")
        self.assertEqual(rows[0]["repo_docs_kind"], "wiki")
        self.assertEqual(rows[0]["relpath"], "repo_docs/github/trinityrnaseq/trinityrnaseq/wiki/index.html")


if __name__ == "__main__":
    unittest.main()
