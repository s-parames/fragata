from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingest.repo_docs_pipeline import (
    acquire_repo_docs,
    build_repo_docs_pipeline_paths,
    ensure_repo_docs_pipeline_layout,
)


class RepoDocsPipelineTests(unittest.TestCase):
    def test_build_repo_docs_pipeline_paths(self) -> None:
        paths = build_repo_docs_pipeline_paths("ing_repo", base_root="/tmp/rag_jobs")
        self.assertEqual(str(paths.job_root), "/tmp/rag_jobs/ing_repo")
        self.assertEqual(str(paths.acquire_root), "/tmp/rag_jobs/ing_repo/repo_docs")
        self.assertTrue(str(paths.fetched_docs_jsonl).endswith("/ing_repo/repo_docs/repo_docs_acquired.jsonl"))

    def test_ensure_repo_docs_pipeline_layout_creates_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            paths = build_repo_docs_pipeline_paths("ing_repo", base_root=tmp)
            ensure_repo_docs_pipeline_layout(paths)
            self.assertTrue(paths.job_root.exists())
            self.assertTrue(paths.acquire_root.exists())

    def test_acquire_repo_docs_fetches_github_readme_as_single_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            def fake_fetch(url: str) -> dict[str, str]:
                self.assertEqual(url, "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md")
                return {
                    "url": url,
                    "content_type": "text/plain; charset=utf-8",
                    "text": "# MACE\nA foundation model.\n",
                }

            with patch("ingest.repo_docs_pipeline._fetch_url_text", side_effect=fake_fetch):
                summary = acquire_repo_docs(
                    url="https://github.com/ACEsuit/mace/blob/main/README.md",
                    output_root=output_root,
                )

            self.assertEqual(summary["provider"], "github")
            self.assertEqual(summary["doc_kind"], "readme")
            self.assertEqual(summary["fetched_page_count"], 1)
            docs = [
                json.loads(line)
                for line in (output_root / "repo_docs_acquired.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(docs), 1)
            self.assertEqual(docs[0]["content_format"], "markdown")
            self.assertEqual(docs[0]["source"], "https://github.com/ACEsuit/mace/blob/main/README.md")
            self.assertEqual(docs[0]["page_title"], "MACE")

    def test_acquire_repo_docs_fetches_gitlab_readme_as_single_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)

            def fake_fetch(url: str) -> dict[str, str]:
                self.assertEqual(url, "https://gitlab.com/example-group/demo-project/-/raw/main/README.md")
                return {
                    "url": url,
                    "content_type": "text/plain; charset=utf-8",
                    "text": "# Demo Project\nHello.\n",
                }

            with patch("ingest.repo_docs_pipeline._fetch_url_text", side_effect=fake_fetch):
                summary = acquire_repo_docs(
                    url="https://gitlab.com/example-group/demo-project/-/blob/main/README.md",
                    output_root=output_root,
                )

            self.assertEqual(summary["provider"], "gitlab")
            self.assertEqual(summary["doc_kind"], "readme")
            self.assertEqual(summary["fetched_page_count"], 1)

    def test_acquire_repo_docs_fetches_github_wiki_within_scope_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            fetched: list[str] = []
            responses = {
                "https://github.com/org/repo/wiki": {
                    "url": "https://github.com/org/repo/wiki",
                    "content_type": "text/html; charset=utf-8",
                    "text": """
                        <html><head><title>Repo Wiki Home</title></head><body>
                        <a href="/org/repo/wiki/Install">Install</a>
                        <a href="https://github.com/org/repo/wiki/Usage">Usage</a>
                        <a href="/org/repo/issues/1">Issue</a>
                        <a href="https://example.com/offsite">Offsite</a>
                        </body></html>
                    """,
                },
                "https://github.com/org/repo/wiki/Install": {
                    "url": "https://github.com/org/repo/wiki/Install",
                    "content_type": "text/html; charset=utf-8",
                    "text": "<html><head><title>Install</title></head><body>install steps</body></html>",
                },
                "https://github.com/org/repo/wiki/Usage": {
                    "url": "https://github.com/org/repo/wiki/Usage",
                    "content_type": "text/html; charset=utf-8",
                    "text": "<html><head><title>Usage</title></head><body>usage notes</body></html>",
                },
            }

            def fake_fetch(url: str) -> dict[str, str]:
                fetched.append(url)
                return responses[url]

            with patch("ingest.repo_docs_pipeline._fetch_url_text", side_effect=fake_fetch):
                summary = acquire_repo_docs(
                    url="https://github.com/org/repo/wiki",
                    output_root=output_root,
                    wiki_page_limit=10,
                )

            self.assertEqual(summary["provider"], "github")
            self.assertEqual(summary["doc_kind"], "wiki")
            self.assertEqual(summary["fetched_page_count"], 3)
            self.assertEqual(
                fetched,
                [
                    "https://github.com/org/repo/wiki",
                    "https://github.com/org/repo/wiki/Install",
                    "https://github.com/org/repo/wiki/Usage",
                ],
            )
            docs = [
                json.loads(line)
                for line in (output_root / "repo_docs_acquired.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(docs), 3)
            self.assertTrue(all(doc["content_format"] == "html" for doc in docs))
            self.assertTrue(all("/wiki" in doc["canonical_url"] for doc in docs))

    def test_acquire_repo_docs_fetches_gitlab_wiki_page_without_broad_crawl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = Path(tmp)
            fetched: list[str] = []

            def fake_fetch(url: str) -> dict[str, str]:
                fetched.append(url)
                return {
                    "url": url,
                    "content_type": "text/html; charset=utf-8",
                    "text": "<html><head><title>Getting Started</title></head><body>hello</body></html>",
                }

            with patch("ingest.repo_docs_pipeline._fetch_url_text", side_effect=fake_fetch):
                summary = acquire_repo_docs(
                    url="https://gitlab.com/example-group/demo-project/-/wikis/getting-started",
                    output_root=output_root,
                    wiki_page_limit=10,
                )

            self.assertEqual(summary["provider"], "gitlab")
            self.assertEqual(summary["doc_kind"], "wiki")
            self.assertEqual(summary["fetched_page_count"], 1)
            self.assertEqual(
                fetched,
                ["https://gitlab.com/example-group/demo-project/-/wikis/getting-started"],
            )


if __name__ == "__main__":
    unittest.main()
