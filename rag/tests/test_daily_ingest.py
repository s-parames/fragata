from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
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


extract_mod = load_module("extract_resolved_tickets", "scripts/07_extract_resolved_tickets.py")
prepare_mod = load_module("prepare_onboard_input", "scripts/08_prepare_onboard_input.py")
dept_mod = load_module("common_department", "scripts/common_department.py")


class DepartmentNormalizationTests(unittest.TestCase):
    def test_bigdata_aliases(self) -> None:
        self.assertEqual(dept_mod.normalize_department("bigdata"), "bigdata")
        self.assertEqual(dept_mod.normalize_department("BIG DATA"), "bigdata")
        self.assertEqual(dept_mod.normalize_department("bd"), "bigdata")

    def test_filter_values_include_bigdata(self) -> None:
        self.assertIn("bigdata", dept_mod.valid_department_values())

    def test_filter_values_include_slurm(self) -> None:
        self.assertIn("slurm", dept_mod.valid_department_values())

    def test_general_aliases(self) -> None:
        self.assertEqual(dept_mod.normalize_department("general"), "general")
        self.assertEqual(dept_mod.normalize_department("General"), "general")

    def test_comunicaciones_aliases(self) -> None:
        self.assertEqual(dept_mod.normalize_department("comunicaciones"), "comunicaciones")
        self.assertEqual(dept_mod.normalize_department("COMUNICACION"), "comunicaciones")
        self.assertEqual(dept_mod.normalize_department("communications"), "comunicaciones")

    def test_filter_values_include_new_departments(self) -> None:
        self.assertIn("general", dept_mod.valid_department_values())
        self.assertIn("comunicaciones", dept_mod.valid_department_values())


class ExtractWindowTests(unittest.TestCase):
    def test_compute_window_with_overlap(self) -> None:
        watermark = datetime(2026, 2, 25, 10, 0, 0, tzinfo=timezone.utc)
        now_utc = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)
        bootstrap = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        lower, upper = extract_mod.compute_window(
            watermark,
            overlap_hours=48,
            bootstrap_watermark=bootstrap,
            now_utc=now_utc,
        )
        self.assertEqual(lower, datetime(2026, 2, 23, 10, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(upper, now_utc)

    def test_compute_window_clamped_to_bootstrap(self) -> None:
        watermark = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        now_utc = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)
        bootstrap = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        lower, _ = extract_mod.compute_window(
            watermark,
            overlap_hours=72,
            bootstrap_watermark=bootstrap,
            now_utc=now_utc,
        )
        self.assertEqual(lower, bootstrap)

    def test_compute_window_without_watermark_uses_bootstrap(self) -> None:
        now_utc = datetime(2026, 2, 25, 12, 0, 0, tzinfo=timezone.utc)
        bootstrap = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        lower, upper = extract_mod.compute_window(
            None,
            overlap_hours=48,
            bootstrap_watermark=bootstrap,
            now_utc=now_utc,
        )
        self.assertEqual(lower, bootstrap)
        self.assertEqual(upper, now_utc)

    def test_parse_watermark_missing_file_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "does_not_exist.txt"
            self.assertIsNone(extract_mod.parse_watermark(str(missing)))

    def test_parse_watermark_empty_file_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            watermark = Path(td) / "last_success_ts.txt"
            watermark.write_text("   \n", encoding="utf-8")
            self.assertIsNone(extract_mod.parse_watermark(str(watermark)))


class PrepareOnboardTests(unittest.TestCase):
    def test_infer_department_from_filename(self) -> None:
        self.assertEqual(
            prepare_mod.infer_department_from_filename(Path("resolved_aplicaciones.jsonl")),
            "aplicaciones",
        )
        self.assertEqual(
            prepare_mod.infer_department_from_filename(Path("resolved_bigdata.jsonl")),
            "bigdata",
        )
        self.assertEqual(
            prepare_mod.infer_department_from_filename(Path("resolved_big_data.jsonl")),
            "bigdata",
        )
        self.assertEqual(
            prepare_mod.infer_department_from_filename(Path("resolved_general.jsonl")),
            "general",
        )
        self.assertEqual(
            prepare_mod.infer_department_from_filename(Path("resolved_comunicaciones.jsonl")),
            "comunicaciones",
        )
        self.assertIsNone(prepare_mod.infer_department_from_filename(Path("resolved_unknown.jsonl")))

    def test_build_contract_row_from_messages_json(self) -> None:
        row = {
            "ticket_id": 123,
            "last_updated": "2026-02-25 09:00:00",
            "subject": "Incidencia Slurm",
            "messages_json": '[{"role":"user","content":"Hola"},{"role":"assistant","content":"Respuesta"}]',
        }
        built, err = prepare_mod.build_contract_row(
            row,
            department="sistemas",
            link_template="https://rt.cesga.es/Ticket/Display.html?id={ticket_id}",
        )
        self.assertIsNone(err)
        self.assertIsNotNone(built)
        assert built is not None
        self.assertEqual(built["link"], "https://rt.cesga.es/Ticket/Display.html?id=123")
        self.assertEqual(built["department"], "sistemas")
        self.assertEqual(built["subject"], "Incidencia Slurm")
        self.assertEqual(len(built["messages"]), 2)

    def test_build_contract_row_missing_data(self) -> None:
        row = {"messages": [{"role": "user", "content": "hola"}]}
        built, err = prepare_mod.build_contract_row(
            row,
            department="aplicaciones",
            link_template="https://rt.cesga.es/Ticket/Display.html?id={ticket_id}",
        )
        self.assertIsNone(built)
        self.assertEqual(err, "missing link")

    def test_parse_messages_value_malformed_json_like_string(self) -> None:
        malformed = '{"role":"user","content":"hola"'
        parsed = prepare_mod.parse_messages_value(malformed)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["role"], "user")
        self.assertIn("hola", parsed[0]["content"])

    def test_parse_messages_value_nested_messages_string(self) -> None:
        raw = {"messages": '[{"role":"assistant","content":"ok"}]'}
        parsed = prepare_mod.parse_messages_value(raw)
        self.assertEqual(parsed, [{"role": "assistant", "content": "ok"}])


class MainDailyIngestCompletionOrderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        (self.repo / "scripts").mkdir(parents=True, exist_ok=True)
        (self.repo / "config").mkdir(parents=True, exist_ok=True)
        (self.repo / "state").mkdir(parents=True, exist_ok=True)
        (self.repo / "data").mkdir(parents=True, exist_ok=True)
        (self.repo / "bin").mkdir(parents=True, exist_ok=True)

        main_script = (ROOT / "scripts" / "main_daily_ingest.sh").read_text(encoding="utf-8")
        target_main = self.repo / "scripts" / "main_daily_ingest.sh"
        target_main.write_text(main_script, encoding="utf-8")
        target_main.chmod(0o755)

        (self.repo / "scripts" / "07_extract_resolved_tickets.py").write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import argparse",
                    "import json",
                    "import os",
                    "from pathlib import Path",
                    "",
                    "parser = argparse.ArgumentParser()",
                    "parser.add_argument('--summary-out', required=True)",
                    "args, _ = parser.parse_known_args()",
                    "payload = {",
                    "    'total_rows': int(os.environ.get('TEST_EXTRACTED_ROWS', '0')),",
                    "    'next_watermark_utc': os.environ.get(",
                    "        'TEST_NEXT_WATERMARK_UTC',",
                    "        '2026-03-30T00:00:00Z',",
                    "    ),",
                    "}",
                    "summary_path = Path(args.summary_out)",
                    "summary_path.parent.mkdir(parents=True, exist_ok=True)",
                    "summary_path.write_text(json.dumps(payload, ensure_ascii=False) + '\\n', encoding='utf-8')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        (self.repo / "scripts" / "08_prepare_onboard_input.py").write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import argparse",
                    "import json",
                    "import os",
                    "from pathlib import Path",
                    "",
                    "parser = argparse.ArgumentParser()",
                    "parser.add_argument('--out-dir', required=True)",
                    "parser.add_argument('--summary-out', required=True)",
                    "args, _ = parser.parse_known_args()",
                    "out_rows = int(os.environ.get('TEST_PREPARED_ROWS', os.environ.get('TEST_EXTRACTED_ROWS', '0')))",
                    "out_dir = Path(args.out_dir)",
                    "out_dir.mkdir(parents=True, exist_ok=True)",
                    "if out_rows > 0:",
                    "    sample = {",
                    "        'link': 'https://rt.lan.cesga.es/Ticket/Display.html?id=1',",
                    "        'lastUpdated': '2026-03-31 01:00:00',",
                    "        'department': 'sistemas',",
                    "        'messages': [{'role': 'user', 'content': 'hola'}],",
                    "    }",
                    "    (out_dir / 'updated_tickets_20260331T010000Z_sistemas.jsonl').write_text(",
                    "        json.dumps(sample, ensure_ascii=False) + '\\n',",
                    "        encoding='utf-8',",
                    "    )",
                    "summary = {",
                    "    'total_rows_in': int(os.environ.get('TEST_EXTRACTED_ROWS', '0')),",
                    "    'total_rows_out': out_rows,",
                    "    'total_rows_skipped': 0,",
                    "    'files': [],",
                    "}",
                    "summary_path = Path(args.summary_out)",
                    "summary_path.parent.mkdir(parents=True, exist_ok=True)",
                    "summary_path.write_text(json.dumps(summary, ensure_ascii=False) + '\\n', encoding='utf-8')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        (self.repo / "scripts" / "09_route_and_onboard.sh").write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    "route_summary_out=\"\"",
                    "while [[ $# -gt 0 ]]; do",
                    "  case \"$1\" in",
                    "    --route-summary-out)",
                    "      route_summary_out=\"${2:-}\"",
                    "      shift 2",
                    "      ;;",
                    "    *)",
                    "      shift",
                    "      ;;",
                    "  esac",
                    "done",
                    "if [[ -z \"$route_summary_out\" ]]; then",
                    "  echo \"missing --route-summary-out\" >&2",
                    "  exit 1",
                    "fi",
                    "mkdir -p \"$(dirname \"$route_summary_out\")\"",
                    "delta_rows=\"${TEST_GLOBAL_DELTA_ROWS:-1}\"",
                    "cat > \"$route_summary_out\" <<JSON",
                    "{",
                    "  \"aggregate\": {\"global_delta_rows_total\": ${delta_rows}},",
                    "  \"entries\": []",
                    "}",
                    "JSON",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        (self.repo / "scripts" / "09_route_and_onboard.sh").chmod(0o755)

        (self.repo / "scripts" / "10_incremental_faiss_append.py").write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import argparse",
                    "import json",
                    "from pathlib import Path",
                    "",
                    "parser = argparse.ArgumentParser()",
                    "parser.add_argument('--summary-out', default='')",
                    "args, _ = parser.parse_known_args()",
                    "Path('append_called.marker').write_text('1\\n', encoding='utf-8')",
                    "if args.summary_out:",
                    "    Path(args.summary_out).write_text(",
                    "        json.dumps({'applied': True, 'reason': 'ok'}, ensure_ascii=False) + '\\n',",
                    "        encoding='utf-8',",
                    "    )",
                    "print(json.dumps({'applied': True, 'reason': 'ok'}, ensure_ascii=False))",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        (self.repo / "RAG_v1.py").write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import argparse",
                    "from pathlib import Path",
                    "",
                    "parser = argparse.ArgumentParser()",
                    "parser.add_argument('--config', default='')",
                    "parser.add_argument('--rebuild-index', action='store_true')",
                    "parser.add_argument('--query', default='')",
                    "parser.parse_args()",
                    "Path('rebuild_called.marker').write_text('1\\n', encoding='utf-8')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        curl_stub = self.repo / "bin" / "curl"
        curl_stub.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    "output_file=\"\"",
                    "while [[ $# -gt 0 ]]; do",
                    "  case \"$1\" in",
                    "    -o)",
                    "      output_file=\"${2:-}\"",
                    "      shift 2",
                    "      ;;",
                    "    *)",
                    "      shift",
                    "      ;;",
                    "  esac",
                    "done",
                    "if [[ -n \"$output_file\" ]]; then",
                    "  printf '%s' \"${TEST_CURL_BODY:-stub} \" > \"$output_file\"",
                    "fi",
                    "printf '%s' \"${TEST_CURL_HTTP_CODE:-200}\"",
                    "exit \"${TEST_CURL_RC:-0}\"",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        curl_stub.chmod(0o755)

        (self.repo / "config" / "daily_ingest.yaml").write_text("daily_ingest: {}\n", encoding="utf-8")
        (self.repo / "config" / "preprocess.yaml").write_text("preprocess: {}\n", encoding="utf-8")
        (self.repo / "config" / "rag.yaml").write_text(
            "\n".join(
                [
                    "retrieval:",
                    "  faiss_dir: data/index/faiss_v2",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _run_main_daily_ingest(
        self,
        *,
        curl_http_code: str,
        curl_rc: str,
        env_overrides: dict[str, str] | None = None,
        extra_args: list[str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PYTHON_BIN": sys.executable,
                "PATH": f"{self.repo / 'bin'}:{env.get('PATH', '')}",
                "TEST_EXTRACTED_ROWS": "0",
                "TEST_NEXT_WATERMARK_UTC": "2026-03-31T02:00:00Z",
                "TEST_CURL_HTTP_CODE": curl_http_code,
                "TEST_CURL_RC": curl_rc,
            }
        )
        if env_overrides:
            env.update(env_overrides)
        cmd = [
            "bash",
            "scripts/main_daily_ingest.sh",
            "--state-dir",
            "state",
            "--incoming-root",
            "data/incoming",
            "--config-daily",
            "config/daily_ingest.yaml",
            "--config-preprocess",
            "config/preprocess.yaml",
            "--config-rag",
            "config/rag.yaml",
            "--reload-endpoint",
            "http://reload.local/api/reload",
            "--reload-required",
            "--reload-retries",
            "1",
            "--reload-timeout-sec",
            "1",
            "--retry-max",
            "0",
        ]
        if extra_args:
            cmd.extend(extra_args)
        return subprocess.run(
            cmd,
            cwd=self.repo,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    @staticmethod
    def _completion_checkpoints(output: str) -> list[dict]:
        events: list[dict] = []
        for line in output.splitlines():
            raw = line.strip()
            if not raw.startswith("{"):
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if payload.get("event") == "daily_ingest_completion_checkpoint":
                events.append(payload)
        return events

    def test_required_reload_success_commits_watermark_after_reload(self) -> None:
        watermark_file = self.repo / "state" / "last_success_ts.txt"
        watermark_file.write_text("2026-03-24T02:00:00Z\n", encoding="utf-8")

        completed = self._run_main_daily_ingest(curl_http_code="200", curl_rc="0")
        self.assertEqual(0, completed.returncode, msg=completed.stdout + "\n" + completed.stderr)
        self.assertEqual("2026-03-31T02:00:00Z", watermark_file.read_text(encoding="utf-8").strip())

        checkpoints = self._completion_checkpoints(completed.stdout + "\n" + completed.stderr)
        self.assertEqual(
            [
                ("data_index_update", "ok"),
                ("engine_reload", "ok"),
                ("watermark_commit", "start"),
                ("watermark_commit", "ok"),
            ],
            [(event.get("checkpoint"), event.get("status")) for event in checkpoints],
        )

    def test_required_reload_failure_keeps_existing_watermark(self) -> None:
        watermark_file = self.repo / "state" / "last_success_ts.txt"
        watermark_file.write_text("2026-03-24T02:00:00Z\n", encoding="utf-8")

        completed = self._run_main_daily_ingest(curl_http_code="500", curl_rc="0")
        self.assertNotEqual(0, completed.returncode, msg=completed.stdout + "\n" + completed.stderr)
        self.assertEqual("2026-03-24T02:00:00Z", watermark_file.read_text(encoding="utf-8").strip())

        checkpoints = self._completion_checkpoints(completed.stdout + "\n" + completed.stderr)
        self.assertEqual(
            [
                ("data_index_update", "ok"),
                ("engine_reload", "error"),
                ("watermark_commit", "skipped"),
            ],
            [(event.get("checkpoint"), event.get("status")) for event in checkpoints],
        )

    def test_with_rows_defaults_to_full_rebuild_instead_of_append(self) -> None:
        completed = self._run_main_daily_ingest(
            curl_http_code="200",
            curl_rc="0",
            env_overrides={
                "TEST_EXTRACTED_ROWS": "1",
                "TEST_PREPARED_ROWS": "1",
                "TEST_GLOBAL_DELTA_ROWS": "1",
            },
        )
        self.assertEqual(0, completed.returncode, msg=completed.stdout + "\n" + completed.stderr)
        self.assertTrue((self.repo / "rebuild_called.marker").exists())
        self.assertFalse((self.repo / "append_called.marker").exists())


if __name__ == "__main__":
    unittest.main()
