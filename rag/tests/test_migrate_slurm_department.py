from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


migrate_mod = load_module("migrate_slurm_department_mod", "scripts/migrate_slurm_department.py")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as dst:
        for row in rows:
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    out = []
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            if line.strip():
                out.append(json.loads(line))
    return out


class MigrateSlurmDepartmentTests(unittest.TestCase):
    def test_row_is_slurm_matches_source_fields(self) -> None:
        host = "slurm.schedmd.com"
        self.assertTrue(
            migrate_mod._row_is_slurm({"source": "https://slurm.schedmd.com/jwt.html"}, host)
        )
        self.assertTrue(
            migrate_mod._row_is_slurm(
                {"source_exact": "https://slurm.schedmd.com/jwt.html#section-001"}, host
            )
        )
        self.assertTrue(
            migrate_mod._row_is_slurm({"link": "https://slurm.schedmd.com/quickstart_admin.html"}, host)
        )
        self.assertFalse(
            migrate_mod._row_is_slurm({"source": "https://example.com/docs/index.html"}, host)
        )

    def test_migrate_dataset_dry_run_keeps_file_unchanged(self) -> None:
        rows = [
            {"source": "https://slurm.schedmd.com/jwt.html", "department": "sistemas", "text": "a"},
            {"source": "https://slurm.schedmd.com/srun.html", "department": "aplicaiones", "text": "b"},
            {"source": "https://example.com/docs", "department": "sistemas", "text": "c"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "datasetFinal.jsonl"
            _write_jsonl(dataset, rows)
            before = dataset.read_text(encoding="utf-8")

            summary = migrate_mod.migrate_dataset(
                dataset_path=dataset,
                target_department="slurm",
                slurm_host="slurm.schedmd.com",
                apply_changes=False,
            )

            after = dataset.read_text(encoding="utf-8")
            self.assertEqual(before, after)
            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["stats"]["slurm_rows"], 2)
            self.assertEqual(summary["stats"]["changed_rows"], 2)
            self.assertEqual(summary["source_departments"]["sistemas"], 1)
            self.assertEqual(summary["source_departments"]["aplicaiones"], 1)

    def test_migrate_dataset_apply_updates_only_slurm_rows(self) -> None:
        rows = [
            {"source": "https://slurm.schedmd.com/jwt.html", "department": "sistemas", "text": "a"},
            {"source": "https://slurm.schedmd.com/srun.html", "department": "slurm", "text": "b"},
            {"source": "https://example.com/docs", "department": "sistemas", "text": "c"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "datasetFinal.jsonl"
            _write_jsonl(dataset, rows)

            summary = migrate_mod.migrate_dataset(
                dataset_path=dataset,
                target_department="slurm",
                slurm_host="slurm.schedmd.com",
                apply_changes=True,
            )
            out_rows = _read_jsonl(dataset)

            self.assertEqual(summary["status"], "applied")
            self.assertEqual(summary["stats"]["slurm_rows"], 2)
            self.assertEqual(summary["stats"]["changed_rows"], 1)
            self.assertEqual(out_rows[0]["department"], "slurm")
            self.assertEqual(out_rows[1]["department"], "slurm")
            self.assertEqual(out_rows[2]["department"], "sistemas")


if __name__ == "__main__":
    unittest.main()
