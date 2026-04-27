from __future__ import annotations

import os
import threading
import unittest
from unittest.mock import patch

import fastapi.dependencies.utils as fastapi_dep_utils
from fastapi.testclient import TestClient

from ingest.engine_manager import EngineManager

os.environ["RAG_SKIP_WARMUP"] = "1"
fastapi_dep_utils.ensure_multipart_is_installed = lambda: None  # type: ignore[assignment]
import app as app_module  # noqa: E402


class EngineHotSwapTests(unittest.TestCase):
    def test_concurrent_get_engine_during_reload_uses_old_engine(self) -> None:
        start_reload = threading.Event()
        release_reload = threading.Event()
        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"id": "v1"}
            start_reload.set()
            release_reload.wait(timeout=2)
            return {"id": "v2"}

        manager = EngineManager(loader=loader)
        first = manager.get_engine()
        self.assertEqual(first["id"], "v1")

        err = {"value": None}

        def run_reload():
            try:
                manager.reload_engine()
            except Exception as exc:  # pragma: no cover - defensive
                err["value"] = exc

        t = threading.Thread(target=run_reload, daemon=True)
        t.start()
        self.assertTrue(start_reload.wait(timeout=2))

        during = manager.get_engine()
        self.assertEqual(during["id"], "v1")

        release_reload.set()
        t.join(timeout=2)
        self.assertIsNone(err["value"])
        after = manager.get_engine()
        self.assertEqual(after["id"], "v2")
        self.assertEqual(manager.health()["engine_generation"], 2)

    def test_failed_reload_keeps_old_engine(self) -> None:
        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"id": "stable"}
            raise RuntimeError("reload boom")

        manager = EngineManager(loader=loader)
        first = manager.get_engine()
        self.assertEqual(first["id"], "stable")
        with self.assertRaises(RuntimeError):
            manager.reload_engine()
        current = manager.get_engine()
        self.assertEqual(current["id"], "stable")
        health = manager.health()
        self.assertEqual(health["engine_generation"], 1)
        self.assertIn("RuntimeError", health["engine_error"])

    def test_health_endpoint_exposes_generation_state(self) -> None:
        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return {"id": f"v{calls['n']}"}

        manager = EngineManager(loader=loader)
        with patch.object(app_module, "_engine_manager", manager):
            with TestClient(app_module.app) as client:
                h0 = client.get("/health")
                self.assertEqual(h0.status_code, 200)
                self.assertIn("engine_generation", h0.json())
                self.assertEqual(h0.json()["engine_generation"], 0)

                reload_resp = client.post("/admin/engine/reload")
                self.assertEqual(reload_resp.status_code, 200)
                self.assertEqual(reload_resp.json()["engine_generation"], 1)

                h1 = client.get("/health")
                self.assertEqual(h1.status_code, 200)
                self.assertEqual(h1.json()["engine_generation"], 1)
                self.assertIsNotNone(h1.json()["engine_loaded_at"])

    def test_admin_reload_failure_response_keeps_active_engine(self) -> None:
        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            if calls["n"] == 1:
                return {"id": "stable"}
            raise RuntimeError("reload boom")

        manager = EngineManager(loader=loader)
        baseline = manager.get_engine()
        self.assertEqual(baseline["id"], "stable")
        self.assertEqual(manager.health()["engine_generation"], 1)

        with patch.object(app_module, "_engine_manager", manager):
            with TestClient(app_module.app) as client:
                resp = client.post("/admin/engine/reload")
                self.assertEqual(resp.status_code, 500)
                detail = resp.json()["detail"]
                self.assertEqual(detail["code"], "engine_reload_failed")
                self.assertIn("error", detail["extra"])
                self.assertEqual(detail["extra"]["health"]["engine_generation"], 1)

        current = manager.get_engine()
        self.assertEqual(current["id"], "stable")
        self.assertEqual(manager.health()["engine_generation"], 1)


if __name__ == "__main__":
    unittest.main()
