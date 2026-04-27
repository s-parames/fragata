from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional
from uuid import uuid4

from .contracts import IngestJobStatusResponse, IngestSourceType, IngestState


_STATUS_MESSAGE_MAX_LENGTH = 2048
_STATUS_ERROR_MAX_LENGTH = 4096
_TRUNCATION_HINT = " [truncated; see job.log]"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            json.dump(payload, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _truncate_field(value: Any, max_length: int) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= max_length:
        return value
    budget = max_length - len(_TRUNCATION_HINT)
    if budget <= 0:
        return value[:max_length]
    return value[:budget] + _TRUNCATION_HINT


def _sanitize_status_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(payload)
    cleaned["message"] = _truncate_field(cleaned.get("message"), _STATUS_MESSAGE_MAX_LENGTH)
    cleaned["error"] = _truncate_field(cleaned.get("error"), _STATUS_ERROR_MAX_LENGTH)
    return cleaned


class IngestionJobStore:
    def __init__(self, root_dir: str = "data/reports/ingest_jobs"):
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self._io_lock = Lock()

    def job_dir(self, job_id: str) -> Path:
        return self.root_dir / job_id

    def manifest_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "manifest.json"

    def log_path(self, job_id: str) -> Path:
        return self.job_dir(job_id) / "logs" / "job.log"

    def ensure_job_layout(self, job_id: str) -> None:
        base = self.job_dir(job_id)
        for rel in ("logs", "artifacts", "raw_input"):
            (base / rel).mkdir(parents=True, exist_ok=True)

    def create_job(self, source_type: IngestSourceType, request_payload: dict) -> IngestJobStatusResponse:
        job_id = f"ing_{uuid4().hex[:20]}"
        self.ensure_job_layout(job_id)
        status = IngestJobStatusResponse(
            job_id=job_id,
            source_type=source_type,
            state=IngestState.QUEUED,
            stage="queued",
            progress=0.0,
            message="Job accepted",
            created_at=utc_now_iso(),
            request=request_payload,
        )
        self.write_manifest(status)
        self.append_log(job_id, level="info", message="Job created", extra={"state": status.state.value})
        return status

    def write_manifest(self, status: IngestJobStatusResponse) -> None:
        with self._io_lock:
            atomic_write_json(
                self.manifest_path(status.job_id),
                _sanitize_status_payload(status.model_dump()),
            )

    def read_manifest(self, job_id: str) -> Optional[IngestJobStatusResponse]:
        path = self.manifest_path(job_id)
        if not path.exists():
            return None
        with self._io_lock:
            payload = json.loads(path.read_text(encoding="utf-8"))
        return IngestJobStatusResponse.model_validate(_sanitize_status_payload(payload))

    def append_log(
        self,
        job_id: str,
        *,
        level: str,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.ensure_job_layout(job_id)
        entry: Dict[str, Any] = {
            "ts": utc_now_iso(),
            "level": level,
            "message": message,
        }
        if extra:
            entry["extra"] = extra
        log_line = json.dumps(entry, ensure_ascii=False)
        path = self.log_path(job_id)
        with self._io_lock:
            with path.open("a", encoding="utf-8") as dst:
                dst.write(log_line + "\n")
