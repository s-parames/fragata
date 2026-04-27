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


pdf_mod = load_module("extract_pdfs", "scripts/07_extract_pdfs_to_jsonl.py")


class PdfSourceUrlTests(unittest.TestCase):
    def test_web_mirror_pdf_url_strips_duplicated_domain_prefix(self) -> None:
        source = pdf_mod.build_source_url(
            "https://slurm.schedmd.com",
            "slurm.schedmd.com/pdfs/summary.pdf",
        )
        self.assertEqual(source, "https://slurm.schedmd.com/pdfs/summary.pdf")

    def test_direct_pdf_source_url_is_preserved(self) -> None:
        source = pdf_mod.build_source_url(
            "https://example.com/manual.pdf",
            "manual.pdf",
        )
        self.assertEqual(source, "https://example.com/manual.pdf")

    def test_uploaded_pdf_without_source_url_keeps_job_scoped_virtual_url(self) -> None:
        source = pdf_mod.build_source_url(
            "https://uploaded.local/ing_123",
            "tiny.pdf",
        )
        self.assertEqual(source, "https://uploaded.local/ing_123/tiny.pdf")


if __name__ == "__main__":
    unittest.main()
