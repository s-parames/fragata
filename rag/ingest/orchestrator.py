from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .contracts import (
    IngestJobResultSummary,
    IngestJobStatusResponse,
    IngestSourceType,
    IngestState,
    PurgeDepartmentRequest,
    PdfIngestRequest,
    RtWeeklyIngestRequest,
    RepoDocsIngestRequest,
    WebIngestRequest,
)
from .storage import IngestionJobStore, utc_now_iso


@dataclass
class IngestionOrchestrator:
    """
    Session-01 skeleton orchestrator.
    Real stage execution and async workers are added in later sessions.
    """

    job_store: IngestionJobStore

    def enqueue_web(self, request: WebIngestRequest) -> IngestJobStatusResponse:
        return self.job_store.create_job(
            source_type=IngestSourceType.WEB,
            request_payload=request.model_dump(mode="json"),
        )

    def enqueue_pdf(self, request: PdfIngestRequest) -> IngestJobStatusResponse:
        return self.job_store.create_job(
            source_type=IngestSourceType.PDF,
            request_payload=request.model_dump(mode="json"),
        )

    def enqueue_repo_docs(self, request: RepoDocsIngestRequest) -> IngestJobStatusResponse:
        return self.job_store.create_job(
            source_type=IngestSourceType.REPO_DOCS,
            request_payload=request.model_dump(mode="json"),
        )

    def enqueue_purge_department(self, request: PurgeDepartmentRequest) -> IngestJobStatusResponse:
        return self.job_store.create_job(
            source_type=IngestSourceType.PURGE_DEPARTMENT,
            request_payload=request.model_dump(mode="json"),
        )

    def enqueue_rt_weekly(self, request: RtWeeklyIngestRequest) -> IngestJobStatusResponse:
        return self.job_store.create_job(
            source_type=IngestSourceType.RT_WEEKLY,
            request_payload=request.model_dump(mode="json"),
        )

    def get_job(self, job_id: str) -> Optional[IngestJobStatusResponse]:
        return self.job_store.read_manifest(job_id)

    def mark_running(self, job_id: str, stage: str, progress: float, message: str) -> IngestJobStatusResponse:
        status = self._require_job(job_id)
        status.state = IngestState.RUNNING
        status.stage = stage
        status.progress = max(0.0, min(1.0, progress))
        status.message = message
        if status.started_at is None:
            status.started_at = utc_now_iso()
        self.job_store.write_manifest(status)
        return status

    def mark_failed(
        self,
        job_id: str,
        stage: str,
        error: str,
        result: Optional[IngestJobResultSummary] = None,
    ) -> IngestJobStatusResponse:
        status = self._require_job(job_id)
        status.state = IngestState.FAILED
        status.stage = stage
        status.error = error
        if result is not None:
            status.result = result
        status.finished_at = utc_now_iso()
        status.progress = max(status.progress, 0.0)
        self.job_store.write_manifest(status)
        return status

    def mark_succeeded(
        self,
        job_id: str,
        result: Optional[IngestJobResultSummary] = None,
    ) -> IngestJobStatusResponse:
        status = self._require_job(job_id)
        status.state = IngestState.SUCCEEDED
        status.stage = "completed"
        status.finished_at = utc_now_iso()
        status.progress = 1.0
        status.message = "Job completed successfully"
        status.result = result or IngestJobResultSummary()
        self.job_store.write_manifest(status)
        return status

    def _require_job(self, job_id: str) -> IngestJobStatusResponse:
        status = self.job_store.read_manifest(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")
        return status
