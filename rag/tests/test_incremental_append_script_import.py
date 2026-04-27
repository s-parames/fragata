from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "10_incremental_faiss_append.py"


class IncrementalAppendScriptImportTests(unittest.TestCase):
    def test_help_runs_from_external_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "--help"],
                cwd=tmp,
                capture_output=True,
                text=True,
            )

        self.assertEqual(proc.returncode, 0, msg=f"stdout={proc.stdout}\nstderr={proc.stderr}")
        self.assertNotIn("ModuleNotFoundError", proc.stderr)


if __name__ == "__main__":
    unittest.main()
