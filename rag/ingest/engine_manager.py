from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable, Dict, Optional


@dataclass
class EngineManager:
    """
    Session-01 placeholder interface for runtime engine lifecycle.
    Hot-swap/reload operations are intentionally out of scope at this stage.
    """

    loader: Callable[[], Any]

    def __post_init__(self) -> None:
        self._engine: Optional[Any] = None
        self._swap_lock = Lock()
        self._build_lock = Lock()
        self._loading = False
        self._error: Optional[str] = None
        self._generation = 0
        self._loaded_at: Optional[str] = None

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _set_loading(self, value: bool) -> None:
        with self._swap_lock:
            self._loading = value

    def _health_unlocked(self) -> Dict[str, Any]:
        return {
            "engine_loaded": self._engine is not None,
            "engine_loading": self._loading,
            "engine_error": self._error,
            "engine_generation": self._generation,
            "engine_loaded_at": self._loaded_at,
        }

    def _apply_new_engine(self, engine: Any) -> Dict[str, Any]:
        with self._swap_lock:
            self._engine = engine
            self._generation += 1
            self._loaded_at = self._utc_now_iso()
            self._error = None
            self._loading = False
            return self._health_unlocked()

    def get_engine(self) -> Any:
        with self._swap_lock:
            current = self._engine
        if current is not None:
            return current

        # Cold-start path; keep one builder and publish only when fully ready.
        with self._build_lock:
            with self._swap_lock:
                current = self._engine
            if current is not None:
                return current
            self._set_loading(True)
            try:
                candidate = self.loader()
            except Exception as exc:
                with self._swap_lock:
                    self._error = f"{exc.__class__.__name__}: {exc}"
                    self._loading = False
                raise
            self._apply_new_engine(candidate)
            return candidate

    def reload_engine(self) -> Dict[str, Any]:
        # Build outside swap lock so current traffic keeps using old engine.
        with self._build_lock:
            self._set_loading(True)
            try:
                candidate = self.loader()
            except Exception as exc:
                with self._swap_lock:
                    self._error = f"{exc.__class__.__name__}: {exc}"
                    self._loading = False
                raise
            return self._apply_new_engine(candidate)

    def health(self) -> Dict[str, Any]:
        with self._swap_lock:
            return self._health_unlocked()
