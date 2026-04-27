from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from RAG_v1 import load_config
from ingest.index_backup_retention import compute_backup_retention, discover_index_backups

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "12_prune_index_backups.py"


class IndexBackupRetentionTests(unittest.TestCase):
    def test_load_config_reads_index_backup_retention_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "rag.yaml"
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
                        "index_backups:",
                        "  enabled: false",
                        "  keep_last: 3",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            cfg = load_config(str(config_path))

            self.assertFalse(cfg.index_backup_retention_enabled)
            self.assertEqual(cfg.index_backup_keep_last, 3)

    def test_discover_index_backups_ignores_malformed_and_unrelated_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            active_dir = root / "faiss_v2"
            active_dir.mkdir()

            (root / "faiss_v2.backup.20260302_114041").mkdir()
            (root / "faiss_v2.backup.20260302_120531").mkdir()
            (root / "faiss_v2.backup.not_a_timestamp").mkdir()
            (root / "faiss_v2.staging.20260302_120531").mkdir()
            (root / "faiss.backup.20260302_120531").mkdir()
            (root / "faiss_v2.backup.20260302_120531.txt").mkdir()

            backups = discover_index_backups(active_dir)

            self.assertEqual(
                [item.path.name for item in backups],
                [
                    "faiss_v2.backup.20260302_120531",
                    "faiss_v2.backup.20260302_114041",
                ],
            )

    def test_compute_backup_retention_keeps_latest_n(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            active_dir = root / "faiss_v2"
            active_dir.mkdir()

            for name in (
                "faiss_v2.backup.20260302_114041",
                "faiss_v2.backup.20260302_115751",
                "faiss_v2.backup.20260302_120531",
                "faiss_v2.backup.20260303_071457",
            ):
                (root / name).mkdir()

            retained, prunable = compute_backup_retention(active_dir, keep_last=2)

            self.assertEqual(
                [item.path.name for item in retained],
                [
                    "faiss_v2.backup.20260303_071457",
                    "faiss_v2.backup.20260302_120531",
                ],
            )
            self.assertEqual(
                [item.path.name for item in prunable],
                [
                    "faiss_v2.backup.20260302_115751",
                    "faiss_v2.backup.20260302_114041",
                ],
            )

    def test_prune_script_dry_run_output_is_coherent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            active_dir = root / "faiss_v2"
            active_dir.mkdir()
            keep_dir = root / "faiss_v2.backup.20260303_071457"
            prune_dir = root / "faiss_v2.backup.20260302_120531"
            keep_dir.mkdir()
            prune_dir.mkdir()

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--faiss-dir",
                    str(active_dir),
                    "--keep-last",
                    "1",
                    "--dry-run",
                ],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")
            self.assertIn("Mode: dry-run", proc.stdout)
            self.assertIn(f"KEEP {keep_dir}", proc.stdout)
            self.assertIn(f"PRUNE {prune_dir}", proc.stdout)
            self.assertTrue(prune_dir.exists())

    def test_prune_script_apply_removes_prunable_backups_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            active_dir = root / "faiss_v2"
            active_dir.mkdir()
            keep_dir = root / "faiss_v2.backup.20260303_071457"
            prune_dir = root / "faiss_v2.backup.20260302_120531"
            keep_dir.mkdir()
            prune_dir.mkdir()

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--faiss-dir",
                    str(active_dir),
                    "--keep-last",
                    "1",
                    "--apply",
                ],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")
            self.assertIn("Mode: apply", proc.stdout)
            self.assertIn(f"DELETE {prune_dir}", proc.stdout)
            self.assertTrue(keep_dir.exists())
            self.assertFalse(prune_dir.exists())


if __name__ == "__main__":
    unittest.main()
