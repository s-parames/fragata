from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ingest.web_job_cleanup import (
    cleanup_web_job_artifacts,
    discover_web_job_cleanup_target,
    discover_web_job_cleanup_targets,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "13_prune_web_job_artifacts.py"


class WebJobCleanupTests(unittest.TestCase):
    def _build_raw_job_tree(self, base_root: Path, job_id: str) -> tuple[Path, int]:
        raw_root = base_root / job_id
        mirror_dir = raw_root / "mirror" / "example.com" / "docs"
        chunk_dir = raw_root / "chunked"
        mirror_dir.mkdir(parents=True, exist_ok=True)
        chunk_dir.mkdir(parents=True, exist_ok=True)
        html = mirror_dir / "index.html"
        chunked = chunk_dir / "raw_site_pages_chunked.jsonl"
        html.write_text("<html>ok</html>", encoding="utf-8")
        chunked.write_text('{"text":"ok"}\n', encoding="utf-8")
        total_bytes = html.stat().st_size + chunked.stat().st_size
        return raw_root, total_bytes

    def _write_config(self, root: Path) -> Path:
        config_path = root / "config" / "rag.yaml"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            "\n".join(
                [
                    "retrieval:",
                    "  dataset_path: data/datasetFinalV2.jsonl",
                    "  faiss_dir: data/index/faiss_v2",
                    "fusion:",
                    "  semantic_weight: 0.8",
                    "  lexical_weight: 0.2",
                    "  rrf_k: 60",
                    "web_job_cleanup:",
                    "  enabled: true",
                    "  trigger: after_merge",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return config_path

    def test_discover_web_job_cleanup_target_accepts_valid_job_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            raw_root, total_bytes = self._build_raw_job_tree(base_root, "ing_abc")

            target = discover_web_job_cleanup_target(raw_root, base_root=base_root)

            self.assertEqual(target.job_id, "ing_abc")
            self.assertEqual(target.raw_job_root, str(raw_root.resolve()))
            self.assertEqual(target.delete_roots, [str(raw_root.resolve())])
            self.assertEqual(target.file_count, 2)
            self.assertGreaterEqual(target.dir_count, 4)
            self.assertEqual(target.total_bytes, total_bytes)

    def test_discover_web_job_cleanup_target_rejects_unsafe_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            raw_root, _total_bytes = self._build_raw_job_tree(base_root, "ing_unsafe")
            nested_root = raw_root / "mirror"
            outside_root = tmp_path / "outside_job"
            outside_root.mkdir(parents=True, exist_ok=True)
            symlink_root = base_root / "ing_symlink"
            symlink_root.symlink_to(raw_root, target_is_directory=True)

            with self.assertRaises(ValueError):
                discover_web_job_cleanup_target(base_root, base_root=base_root)
            with self.assertRaises(ValueError):
                discover_web_job_cleanup_target(nested_root, base_root=base_root)
            with self.assertRaises(ValueError):
                discover_web_job_cleanup_target(outside_root, base_root=base_root)
            with self.assertRaises(ValueError):
                discover_web_job_cleanup_target(symlink_root, base_root=base_root)

    def test_cleanup_web_job_artifacts_dry_run_reports_structure_without_deleting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            raw_root, total_bytes = self._build_raw_job_tree(base_root, "ing_dry")

            result = cleanup_web_job_artifacts(raw_root, base_root=base_root, apply=False)
            payload = result.to_dict()

            self.assertEqual(payload["mode"], "dry-run")
            self.assertEqual(payload["job_id"], "ing_dry")
            self.assertEqual(payload["raw_job_root"], str(raw_root.resolve()))
            self.assertEqual(payload["delete_roots"], [str(raw_root.resolve())])
            self.assertEqual(payload["deleted_roots"], [])
            self.assertEqual(payload["file_count"], 2)
            self.assertEqual(payload["total_bytes"], total_bytes)
            self.assertTrue(raw_root.exists())

    def test_cleanup_web_job_artifacts_apply_deletes_only_selected_raw_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            raw_root, _total_bytes = self._build_raw_job_tree(base_root, "ing_apply")
            sibling_root, _sibling_bytes = self._build_raw_job_tree(base_root, "ing_keep")

            reports_root = tmp_path / "data" / "reports" / "ingest_jobs" / "ing_apply" / "artifacts"
            dataset_path = tmp_path / "data" / "datasetFinalV2.jsonl"
            faiss_dir = tmp_path / "data" / "index" / "faiss_v2"
            reports_root.mkdir(parents=True, exist_ok=True)
            faiss_dir.mkdir(parents=True, exist_ok=True)
            (reports_root / "merge_summary.json").write_text('{"ok": true}\n', encoding="utf-8")
            dataset_path.parent.mkdir(parents=True, exist_ok=True)
            dataset_path.write_text('{"text":"persisted"}\n', encoding="utf-8")
            (faiss_dir / "index.faiss").write_text("index", encoding="utf-8")

            result = cleanup_web_job_artifacts(raw_root, base_root=base_root, apply=True)

            self.assertEqual(result.mode, "apply")
            self.assertEqual(result.deleted_roots, [str(raw_root.resolve())])
            self.assertFalse(raw_root.exists())
            self.assertTrue(sibling_root.exists())
            self.assertTrue(reports_root.exists())
            self.assertTrue(dataset_path.exists())
            self.assertTrue(faiss_dir.exists())

    def test_discover_web_job_cleanup_targets_lists_direct_children_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            self._build_raw_job_tree(base_root, "ing_a")
            self._build_raw_job_tree(base_root, "ing_b")
            (base_root / "note.txt").parent.mkdir(parents=True, exist_ok=True)
            (base_root / "note.txt").write_text("ignore", encoding="utf-8")

            targets = discover_web_job_cleanup_targets(base_root)

            self.assertEqual([target.job_id for target in targets], ["ing_a", "ing_b"])

    def test_cli_dry_run_lists_all_web_jobs_and_estimated_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            first_root, first_bytes = self._build_raw_job_tree(base_root, "ing_a")
            second_root, second_bytes = self._build_raw_job_tree(base_root, "ing_b")
            config_path = self._write_config(tmp_path)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--config",
                    str(config_path),
                    "--all-web-jobs",
                    "--dry-run",
                ],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")
            self.assertIn("Mode: dry-run", proc.stdout)
            self.assertIn(f"job_id=ing_a path={first_root.resolve()}", proc.stdout)
            self.assertIn(f"job_id=ing_b path={second_root.resolve()}", proc.stdout)
            self.assertIn(f"estimated_reclaimed_bytes={first_bytes + second_bytes}", proc.stdout)
            self.assertTrue(first_root.exists())
            self.assertTrue(second_root.exists())

    def test_cli_apply_with_job_id_deletes_only_selected_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            selected_root, selected_bytes = self._build_raw_job_tree(base_root, "ing_apply")
            sibling_root, _sibling_bytes = self._build_raw_job_tree(base_root, "ing_keep")
            config_path = self._write_config(tmp_path)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--config",
                    str(config_path),
                    "--job-id",
                    "ing_apply",
                    "--apply",
                ],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")
            self.assertIn("Mode: apply", proc.stdout)
            self.assertIn(f"job_id=ing_apply path={selected_root.resolve()}", proc.stdout)
            self.assertIn(f"bytes={selected_bytes}", proc.stdout)
            self.assertFalse(selected_root.exists())
            self.assertTrue(sibling_root.exists())

    def test_cli_rejects_unsafe_job_id_selector(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            base_root = tmp_path / "data" / "raw_site" / "jobs"
            self._build_raw_job_tree(base_root, "ing_safe")
            outside_root = tmp_path / "outside_job"
            outside_root.mkdir(parents=True, exist_ok=True)
            config_path = self._write_config(tmp_path)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--config",
                    str(config_path),
                    "--job-id",
                    "../outside_job",
                    "--apply",
                ],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("job_id must be a single direct child name", proc.stderr)
            self.assertTrue(outside_root.exists())
            self.assertTrue((base_root / "ing_safe").exists())


if __name__ == "__main__":
    unittest.main()
