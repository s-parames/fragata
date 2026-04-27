from __future__ import annotations

import json
import os
import shlex
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any, Callable, Dict, Optional
import sys

import yaml

from RAG_v1 import load_config
from .contracts import (
    HpcExecutionMetadata,
    IngestJobResultSummary,
    IngestSourceType,
    PdfIngestRequest,
    PurgeDepartmentRequest,
    RtWeeklyIngestRequest,
    RepoDocsIngestRequest,
    WebIngestRequest,
)
from .faiss_incremental import rebuild_full_faiss, write_summary_json
from .hpc_executor import HpcExecutor, HpcExecutorConfig, HpcResourceSpec
from .index_backup_retention import prune_index_backups
from .orchestrator import IngestionOrchestrator
from .source_catalog import rebuild_source_catalog
from .storage import utc_now_iso
from .pdf_pipeline import (
    build_pdf_pipeline_paths,
    ensure_pdf_pipeline_layout,
    run_pdf_chunk_stage,
    run_pdf_extract_stage,
    run_pdf_prepare_stage,
)
from .repo_docs_pipeline import (
    acquire_repo_docs,
    build_repo_docs_pipeline_paths,
    ensure_repo_docs_pipeline_layout,
    run_repo_docs_chunk_stage,
    run_repo_docs_prepare_stage,
)
from .web_job_cleanup import cleanup_web_job_artifacts
from .web_pipeline import (
    base_url_for_scripts,
    build_web_pipeline_paths,
    ensure_web_pipeline_layout,
    mirror_website,
    run_chunk_stage,
    run_command,
    run_extract_stage,
    run_prepare_stage,
)


class IngestionJobRunner:
    """
    Session-02 async runner abstraction.
    Executes lightweight background transitions only; heavy extraction is added later.
    """

    def __init__(
        self,
        orchestrator: IngestionOrchestrator,
        max_workers: int = 2,
        on_job_success: Optional[
            Callable[[str, IngestSourceType, IngestJobResultSummary], Optional[Dict[str, Any]]]
        ] = None,
    ):
        self.orchestrator = orchestrator
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ingest-job")
        self._futures: Dict[str, Future] = {}
        self._lock = Lock()
        self._index_mutation_lock = Lock()
        self.project_root = Path(__file__).resolve().parents[1]
        self.scripts_root = self.project_root / "scripts"
        self.preprocess_config = self.project_root / "config" / "preprocess.yaml"
        self.python_bin = sys.executable
        self.on_job_success = on_job_success
        self._hpc_enabled = self._env_bool("RAG_HPC_ENABLED", default=False)
        self._hpc_source_types = self._parse_hpc_source_types(
            os.getenv("RAG_HPC_SOURCE_TYPES", "web,pdf,repo_docs,rt_weekly")
        )
        self._hpc_timeout_sec = self._env_int("RAG_HPC_TIMEOUT_SEC", default=0)
        self._hpc_submit_template = (os.getenv("RAG_HPC_SUBMIT_TEMPLATE") or "").strip() or None
        self._hpc_python_bin = (os.getenv("RAG_HPC_PYTHON_BIN") or self.python_bin).strip() or self.python_bin
        self._hpc_release_policy = self._parse_hpc_release_policy(os.getenv("RAG_HPC_RELEASE_POLICY"))
        self._hpc_spec = self._build_hpc_resource_spec()
        self._hpc_executor: Optional[HpcExecutor] = None
        hpc_config = HpcExecutorConfig.from_env()
        if hpc_config is not None:
            self._hpc_executor = HpcExecutor(config=hpc_config)

    @staticmethod
    def _env_bool(name: str, *, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_int(name: str, *, default: int = 0) -> int:
        raw = (os.getenv(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    @staticmethod
    def _parse_hpc_release_policy(raw: Optional[str]) -> str:
        normalized = (raw or "").strip().lower()
        if normalized in {"explicit_cancel", "scancel", "cancel"}:
            return "explicit_cancel"
        return "auto"

    def _resolve_hpc_release_outcome(
        self,
        *,
        allocation_id: Optional[str],
        workload_succeeded: bool,
    ) -> Dict[str, Any]:
        assert self._hpc_executor is not None
        release_reason = "completion" if workload_succeeded else "failure"
        requires_explicit_cancel = (not workload_succeeded) or self._hpc_release_policy == "explicit_cancel"

        if not requires_explicit_cancel:
            return {
                "released": True,
                "release_status": "auto_release_after_completion",
                "release_policy": self._hpc_release_policy,
                "release_action": "auto_release",
                "release_attempted": False,
                "release_evidence": "remote_command_exit_code=0",
            }

        clean_id = (allocation_id or "").strip()
        if not clean_id:
            return {
                "released": False,
                "release_status": f"cancel_skipped_missing_allocation_id_after_{release_reason}",
                "release_policy": self._hpc_release_policy,
                "release_action": "scancel",
                "release_attempted": False,
                "release_evidence": "allocation_id_missing",
            }

        try:
            cancel_result = self._hpc_executor.cancel(clean_id)
        except Exception as exc:  # pragma: no cover - defensive fallback
            return {
                "released": False,
                "release_status": f"cancel_error_after_{release_reason}:{exc.__class__.__name__}",
                "release_policy": self._hpc_release_policy,
                "release_action": "scancel",
                "release_attempted": True,
                "release_evidence": f"cancel_exception={exc.__class__.__name__}",
            }

        released = int(cancel_result.return_code) == 0
        prefix = "cancel_ok" if released else "cancel_failed"
        evidence = f"cancel_return_code={cancel_result.return_code}"
        return {
            "released": released,
            "release_status": f"{prefix}_after_{release_reason}",
            "release_policy": self._hpc_release_policy,
            "release_action": "scancel",
            "release_attempted": True,
            "release_evidence": evidence,
        }

    def _build_hpc_resource_spec(self) -> HpcResourceSpec:
        cores = max(1, self._env_int("RAG_HPC_CORES", default=32))
        mem = (os.getenv("RAG_HPC_MEM") or "32G").strip() or "32G"
        gpu = self._env_bool("RAG_HPC_GPU", default=True)
        return HpcResourceSpec(cores=cores, memory=mem, gpu=gpu)

    def _parse_hpc_source_types(self, raw: str) -> set[IngestSourceType]:
        out: set[IngestSourceType] = set()
        for token in str(raw or "").split(","):
            normalized = token.strip().lower()
            if not normalized:
                continue
            try:
                out.add(IngestSourceType(normalized))
            except ValueError:
                continue
        return out

    def _hpc_enabled_for_source_type(self, source_type: IngestSourceType) -> bool:
        if not self._hpc_enabled:
            return False
        if self._hpc_executor is None:
            return False
        return source_type in self._hpc_source_types

    def start_job(
        self,
        *,
        job_id: str,
        source_type: IngestSourceType,
        staged_input_path: Optional[str] = None,
    ) -> None:
        with self._lock:
            existing = self._futures.get(job_id)
            if existing and not existing.done():
                return
            fut = self.executor.submit(self._run_job, job_id, source_type, staged_input_path)
            self._futures[job_id] = fut

    def _rag_config_path(self) -> Path:
        return self.project_root / "config" / "rag.yaml"

    def _resolve_active_index_backup_settings(self, result: IngestJobResultSummary) -> tuple[Optional[Path], bool, int]:
        active_dir: Optional[Path] = None
        enabled = True
        keep_last = 1
        config_path = self._rag_config_path()

        if config_path.exists():
            cfg = load_config(str(config_path))
            active_dir = Path(cfg.faiss_dir)
            if not active_dir.is_absolute():
                active_dir = self.project_root / active_dir
            enabled = bool(cfg.index_backup_retention_enabled)
            keep_last = max(0, int(cfg.index_backup_keep_last))

        if active_dir is None and result.output_index_path:
            active_dir = Path(result.output_index_path)
            if not active_dir.is_absolute():
                active_dir = self.project_root / active_dir

        return active_dir, enabled, keep_last

    def _run_backup_prune_stage(self, job_id: str, result: IngestJobResultSummary) -> None:
        active_dir, enabled, keep_last = self._resolve_active_index_backup_settings(result)
        if active_dir is None:
            return

        if not enabled:
            payload = {
                "status": "skipped",
                "reason": "retention_disabled",
                "active_dir": str(active_dir),
                "keep_last": keep_last,
            }
            result.stage_metrics["backup_prune"] = payload
            result.backup_prune_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Backup prune stage skipped",
                extra={"stage": "backup_prune", **payload},
            )
            return

        prune_result = prune_index_backups(active_dir, keep_last=keep_last, apply=True)
        payload = {
            "status": "ok",
            **prune_result.to_dict(),
        }
        result.stage_metrics["backup_prune"] = payload
        result.backup_prune_metadata = payload
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Backup prune stage completed",
            extra={"stage": "backup_prune", **payload},
        )

    def _resolve_web_job_cleanup_settings(self) -> tuple[Path, bool, str, Path]:
        raw_jobs_root = self.project_root / "data" / "raw_site" / "jobs"
        enabled = True
        trigger = "after_merge"
        dataset_path = self._resolve_global_dataset_path()
        config_path = self._rag_config_path()

        if config_path.exists():
            cfg = load_config(str(config_path))
            enabled = bool(cfg.web_job_cleanup_enabled)
            trigger = str(cfg.web_job_cleanup_trigger or "after_merge")

        return raw_jobs_root, enabled, trigger, dataset_path

    def _run_web_job_cleanup_stage(self, job_id: str, result: IngestJobResultSummary) -> None:
        raw_jobs_root, enabled, trigger, configured_dataset_path = self._resolve_web_job_cleanup_settings()
        raw_job_root = build_web_pipeline_paths(job_id=job_id, base_root=str(raw_jobs_root)).job_root
        output_dataset_value = (result.output_dataset_path or "").strip()

        if not enabled:
            payload = {
                "status": "skipped",
                "trigger": trigger,
                "reason": "cleanup_disabled",
                "raw_job_root": str(raw_job_root),
            }
            result.stage_metrics["web_job_cleanup"] = payload
            result.web_job_cleanup_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Web job cleanup stage skipped",
                extra={"stage": "web_job_cleanup", **payload},
            )
            return

        if trigger != "after_merge":
            payload = {
                "status": "skipped",
                "trigger": trigger,
                "reason": "unsupported_trigger",
                "raw_job_root": str(raw_job_root),
            }
            result.stage_metrics["web_job_cleanup"] = payload
            result.web_job_cleanup_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Web job cleanup stage skipped",
                extra={"stage": "web_job_cleanup", **payload},
            )
            return

        if not output_dataset_value:
            payload = {
                "status": "skipped",
                "trigger": trigger,
                "reason": "missing_output_dataset_path",
                "raw_job_root": str(raw_job_root),
            }
            result.stage_metrics["web_job_cleanup"] = payload
            result.web_job_cleanup_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Web job cleanup stage skipped",
                extra={"stage": "web_job_cleanup", **payload},
            )
            return

        output_dataset_path = Path(output_dataset_value)
        if not output_dataset_path.is_absolute():
            output_dataset_path = self.project_root / output_dataset_path
        if output_dataset_path.resolve(strict=False) != configured_dataset_path.resolve(strict=False):
            payload = {
                "status": "skipped",
                "trigger": trigger,
                "reason": "output_dataset_path_mismatch",
                "raw_job_root": str(raw_job_root),
                "output_dataset_path": str(output_dataset_path),
                "configured_dataset_path": str(configured_dataset_path),
            }
            result.stage_metrics["web_job_cleanup"] = payload
            result.web_job_cleanup_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Web job cleanup stage skipped",
                extra={"stage": "web_job_cleanup", **payload},
            )
            return

        try:
            cleanup_result = cleanup_web_job_artifacts(
                raw_job_root,
                base_root=raw_jobs_root,
                apply=True,
            )
        except Exception as exc:
            payload = {
                "status": "error",
                "trigger": trigger,
                "reason": "cleanup_failed",
                "raw_job_root": str(raw_job_root),
                "error": f"{exc.__class__.__name__}: {exc}",
            }
            result.stage_metrics["web_job_cleanup"] = payload
            result.web_job_cleanup_metadata = payload
            self.orchestrator.job_store.append_log(
                job_id,
                level="error",
                message="Web job cleanup stage failed",
                extra={"stage": "web_job_cleanup", **payload},
            )
            return

        payload = {
            "status": "ok",
            "trigger": trigger,
            "deleted_job_root": cleanup_result.raw_job_root,
            "deleted_roots": cleanup_result.deleted_roots,
            "reclaimed_bytes": cleanup_result.total_bytes,
            "file_count": cleanup_result.file_count,
            "dir_count": cleanup_result.dir_count,
            "mode": cleanup_result.mode,
        }
        result.stage_metrics["web_job_cleanup"] = payload
        result.web_job_cleanup_metadata = payload
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Web job cleanup stage completed",
            extra={"stage": "web_job_cleanup", **payload},
        )

    def _set_source_catalog_refresh_stage(
        self,
        job_id: str,
        result: IngestJobResultSummary,
        *,
        payload: Dict[str, Any],
        log_level: str,
        log_message: str,
    ) -> None:
        result.stage_metrics["source_catalog_refresh"] = payload
        result.source_catalog_refresh_metadata = payload
        self.orchestrator.job_store.append_log(
            job_id,
            level=log_level,
            message=log_message,
            extra={"stage": "source_catalog_refresh", **payload},
        )

    def _skip_source_catalog_refresh_stage(
        self,
        job_id: str,
        result: IngestJobResultSummary,
        *,
        trigger: str,
        reason: str,
        message: str,
    ) -> None:
        payload = {
            "status": "skipped",
            "trigger": trigger,
            "reason": reason,
            "dataset_path": result.output_dataset_path,
        }
        self._set_source_catalog_refresh_stage(
            job_id,
            result,
            payload=payload,
            log_level="info",
            log_message=message,
        )

    def _run_source_catalog_refresh_stage(self, job_id: str, result: IngestJobResultSummary, *, trigger: str) -> None:
        output_dataset_value = (result.output_dataset_path or "").strip()
        if not output_dataset_value:
            self._skip_source_catalog_refresh_stage(
                job_id,
                result,
                trigger=trigger,
                reason="missing_output_dataset_path",
                message="Source catalog refresh stage skipped",
            )
            return

        output_dataset_path = Path(output_dataset_value)
        if not output_dataset_path.is_absolute():
            output_dataset_path = self.project_root / output_dataset_path
        configured_dataset_path = self._resolve_global_dataset_path()
        if output_dataset_path.resolve(strict=False) != configured_dataset_path.resolve(strict=False):
            self._skip_source_catalog_refresh_stage(
                job_id,
                result,
                trigger=trigger,
                reason="output_dataset_path_mismatch",
                message="Source catalog refresh stage skipped",
            )
            return

        self.orchestrator.mark_running(
            job_id,
            stage="source_catalog_refresh",
            progress=0.945 if trigger == "after_merge" else 0.82,
            message="Refreshing source catalog from current dataset",
        )
        try:
            snapshot = rebuild_source_catalog(
                config_path=self._rag_config_path(),
                dataset_path_override=str(output_dataset_path),
            )
        except Exception as exc:
            payload = {
                "status": "error",
                "trigger": trigger,
                "reason": "refresh_failed",
                "dataset_path": str(output_dataset_path),
                "error": f"{exc.__class__.__name__}: {exc}",
            }
            self._set_source_catalog_refresh_stage(
                job_id,
                result,
                payload=payload,
                log_level="error",
                log_message="Source catalog refresh stage failed",
            )
            raise RuntimeError(f"Source catalog refresh failed: {exc.__class__.__name__}: {exc}") from exc

        payload = {
            "status": "ok",
            "trigger": trigger,
            "dataset_path": snapshot.dataset_path,
            "catalog_path": snapshot.catalog_path,
            "generated_at": snapshot.generated_at,
            "total_entries": snapshot.total_entries,
        }
        self._set_source_catalog_refresh_stage(
            job_id,
            result,
            payload=payload,
            log_level="info",
            log_message="Source catalog refresh stage completed",
        )

    def _run_hpc_command_stage(
        self,
        *,
        job_id: str,
        source_type: IngestSourceType,
        payload_command: str,
        result: IngestJobResultSummary,
        context_stage: str,
    ) -> HpcExecutionMetadata:
        if self._hpc_executor is None:
            raise RuntimeError("HPC mode is enabled but executor is not configured (missing RAG_HPC_REMOTE_HOST).")

        requested_at = utc_now_iso()
        self.orchestrator.mark_running(
            job_id,
            stage="resource_requested",
            progress=0.94,
            message="Requesting remote compute resources",
        )
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="HPC resource request submitted",
            extra={
                "stage": "resource_requested",
                "source_type": source_type.value,
                "context_stage": context_stage,
                "payload_command": payload_command,
            },
        )
        self.orchestrator.mark_running(
            job_id,
            stage="waiting_resources",
            progress=0.945,
            message="Waiting for remote compute allocation",
        )
        self.orchestrator.mark_running(
            job_id,
            stage="running_remote",
            progress=0.95,
            message="Running workload on remote compute node",
        )

        run_result = self._hpc_executor.run(
            payload_command=payload_command,
            spec=self._hpc_spec,
            timeout_sec=self._hpc_timeout_sec if self._hpc_timeout_sec > 0 else None,
            submit_template=self._hpc_submit_template,
        )
        finished_at = utc_now_iso()

        hpc_metadata = HpcExecutionMetadata(
            mode="hpc",
            request_command=run_result.request_command,
            remote_command=run_result.remote_command,
            allocation_id=run_result.allocation_id,
            remote_host=self._hpc_executor.config.remote_host,
            remote_workdir=self._hpc_executor.config.remote_workdir,
            state="running_remote",
            requested_at=requested_at,
            started_at=requested_at,
            finished_at=finished_at,
            released=False,
            release_policy=self._hpc_release_policy,
            exit_code=run_result.return_code,
        )
        result.hpc_execution = hpc_metadata
        result.stage_metrics["hpc_execution"] = {
            "status": "running",
            "context_stage": context_stage,
            "allocation_id": run_result.allocation_id,
            "request_command": run_result.request_command,
            "remote_command": run_result.remote_command,
        }

        if run_result.return_code != 0:
            release = self._resolve_hpc_release_outcome(
                allocation_id=run_result.allocation_id,
                workload_succeeded=False,
            )
            hpc_metadata = hpc_metadata.model_copy(
                update={
                    "state": "failed",
                    "released": bool(release["released"]),
                    "release_status": str(release["release_status"]),
                    "release_policy": str(release["release_policy"]),
                    "release_evidence": str(release["release_evidence"]),
                }
            )
            result.hpc_execution = hpc_metadata
            result.stage_metrics["hpc_execution"] = {
                "status": "error",
                "context_stage": context_stage,
                "allocation_id": run_result.allocation_id,
                "request_command": run_result.request_command,
                "remote_command": run_result.remote_command,
                "return_code": run_result.return_code,
                "released": bool(release["released"]),
                "release_status": str(release["release_status"]),
                "release_policy": str(release["release_policy"]),
                "release_action": str(release["release_action"]),
                "release_attempted": bool(release["release_attempted"]),
                "release_evidence": str(release["release_evidence"]),
            }
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="HPC release attempt completed",
                extra={
                    "stage": "resource_release",
                    "source_type": source_type.value,
                    "context_stage": context_stage,
                    "allocation_id": run_result.allocation_id,
                    "workload_succeeded": False,
                    "release_policy": release["release_policy"],
                    "release_action": release["release_action"],
                    "release_attempted": release["release_attempted"],
                    "released": release["released"],
                    "release_status": release["release_status"],
                    "release_evidence": release["release_evidence"],
                },
            )
            stderr_tail = (run_result.stderr or "").strip()[-2000:]
            stdout_tail = (run_result.stdout or "").strip()[-2000:]
            raise RuntimeError(
                "HPC remote command failed "
                f"(stage={context_stage} return_code={run_result.return_code} "
                f"allocation_id={run_result.allocation_id or 'n/a'}): "
                f"stderr={stderr_tail or '<empty>'} stdout={stdout_tail or '<empty>'}"
            )

        self.orchestrator.mark_running(
            job_id,
            stage="sync_back",
            progress=0.955,
            message="Synchronizing remote artifacts",
        )
        release = self._resolve_hpc_release_outcome(
            allocation_id=run_result.allocation_id,
            workload_succeeded=True,
        )
        release_ok = bool(release["released"])
        hpc_state = "completed" if release_ok else "release_failed"
        hpc_status = "ok" if release_ok else "error"
        hpc_metadata = hpc_metadata.model_copy(
            update={
                "state": hpc_state,
                "released": release_ok,
                "release_status": str(release["release_status"]),
                "release_policy": str(release["release_policy"]),
                "release_evidence": str(release["release_evidence"]),
            }
        )
        result.hpc_execution = hpc_metadata
        result.stage_metrics["hpc_execution"] = {
            "status": hpc_status,
            "context_stage": context_stage,
            "allocation_id": run_result.allocation_id,
            "request_command": run_result.request_command,
            "remote_command": run_result.remote_command,
            "return_code": run_result.return_code,
            "released": release_ok,
            "release_status": str(release["release_status"]),
            "release_policy": str(release["release_policy"]),
            "release_action": str(release["release_action"]),
            "release_attempted": bool(release["release_attempted"]),
            "release_evidence": str(release["release_evidence"]),
        }
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="HPC release attempt completed",
            extra={
                "stage": "resource_release",
                "source_type": source_type.value,
                "context_stage": context_stage,
                "allocation_id": run_result.allocation_id,
                "workload_succeeded": True,
                "release_policy": release["release_policy"],
                "release_action": release["release_action"],
                "release_attempted": release["release_attempted"],
                "released": release["released"],
                "release_status": release["release_status"],
                "release_evidence": release["release_evidence"],
            },
        )
        if not release_ok:
            raise RuntimeError(
                "HPC workload completed but resource release failed "
                f"(policy={self._hpc_release_policy} allocation_id={run_result.allocation_id or 'n/a'} "
                f"release_status={release['release_status']})"
            )
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="HPC remote command completed",
            extra={
                "stage": "sync_back",
                "source_type": source_type.value,
                "context_stage": context_stage,
                "allocation_id": run_result.allocation_id,
                "request_command": run_result.request_command,
                "return_code": run_result.return_code,
                "release_policy": release["release_policy"],
                "release_status": release["release_status"],
            },
        )
        return hpc_metadata

    def _build_rt_weekly_payload_command(self, request: RtWeeklyIngestRequest) -> str:
        cmd: list[str] = [
            "bash",
            "scripts/main_daily_ingest.sh",
            "--config-daily",
            "config/daily_ingest.yaml",
            "--state-dir",
            "state",
            "--env-file",
            "state/daily_ingest.env",
        ]
        if request.overlap_hours is not None:
            cmd.extend(["--overlap-hours", str(request.overlap_hours)])
        return shlex.join(cmd)

    def _run_rt_weekly_job(self, job_id: str) -> IngestJobResultSummary:
        status = self.orchestrator.get_job(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")
        request = RtWeeklyIngestRequest.model_validate(status.request)
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        summary_path = artifacts_dir / "rt_weekly_summary.json"
        payload_command = self._build_rt_weekly_payload_command(request)

        result = IngestJobResultSummary(
            merged_rows=0,
            delta_rows=0,
            chunk_rows=0,
            index_updated=True,
            artifacts_dir=str(artifacts_dir),
            stage_metrics={},
        )

        if self._hpc_enabled_for_source_type(IngestSourceType.RT_WEEKLY):
            hpc_meta = self._run_hpc_command_stage(
                job_id=job_id,
                source_type=IngestSourceType.RT_WEEKLY,
                payload_command=payload_command,
                result=result,
                context_stage="rt_weekly",
            )
            summary = {
                "status": "ok",
                "mode": "hpc",
                "payload_command": payload_command,
                "hpc_execution": hpc_meta.model_dump(mode="json"),
            }
        else:
            self.orchestrator.mark_running(
                job_id,
                stage="running_remote",
                progress=0.95,
                message="Running RT weekly ingestion locally",
            )
            proc = run_command(
                shlex.split(payload_command),
                cwd=str(self.project_root),
                env=os.environ.copy(),
            )
            summary = {
                "status": "ok",
                "mode": "local",
                "payload_command": payload_command,
                "return_code": int(proc.returncode),
            }
            result.stage_metrics["hpc_execution"] = {
                "status": "skipped",
                "reason": "hpc_disabled_or_unconfigured",
                "context_stage": "rt_weekly",
            }
            result.hpc_execution = HpcExecutionMetadata(
                mode="local",
                state="completed",
                requested_at=utc_now_iso(),
                started_at=utc_now_iso(),
                finished_at=utc_now_iso(),
                released=True,
                release_status="not_required_local_mode",
                release_policy="not_applicable_local_mode",
                release_evidence="local_execution",
                exit_code=int(proc.returncode),
            )

        write_summary_json(str(summary_path), summary)
        result.stage_metrics["rt_weekly"] = {
            "status": "ok",
            "summary_path": str(summary_path),
            "mode": summary.get("mode", "unknown"),
        }
        return result

    def _run_job(
        self,
        job_id: str,
        source_type: IngestSourceType,
        staged_input_path: Optional[str],
    ) -> None:
        store = self.orchestrator.job_store
        result: Optional[IngestJobResultSummary] = None
        try:
            self.orchestrator.mark_running(job_id, stage="dispatch", progress=0.05, message="Job picked by worker")
            store.append_log(
                job_id,
                level="info",
                message="Background worker started",
                extra={
                    "source_type": source_type.value,
                    "staged_input_path": staged_input_path,
                },
            )
            if source_type == IngestSourceType.WEB:
                result = self._run_web_job(job_id)
            elif source_type == IngestSourceType.PDF:
                result = self._run_pdf_job(job_id, staged_input_path)
            elif source_type == IngestSourceType.REPO_DOCS:
                result = self._run_repo_docs_job(job_id)
            elif source_type == IngestSourceType.RT_WEEKLY:
                result = self._run_rt_weekly_job(job_id)
                if self.on_job_success is not None:
                    try:
                        self.orchestrator.mark_running(
                            job_id,
                            stage="reload",
                            progress=0.97,
                            message="Reloading search engine",
                        )
                        reload_meta = self.on_job_success(job_id, source_type, result)
                    except Exception as exc:
                        store.append_log(
                            job_id,
                            level="error",
                            message="Post-ingest reload failed",
                            extra={
                                "stage": "reload",
                                "error": f"{exc.__class__.__name__}: {exc}",
                                "hint": (
                                    "Verify FAISS artifacts and retry /admin/engine/reload "
                                    "after fixing index consistency."
                                ),
                            },
                        )
                        self.orchestrator.mark_failed(
                            job_id,
                            stage="reload",
                            error=f"{exc.__class__.__name__}: {exc}",
                            result=result,
                        )
                        return
                    if isinstance(reload_meta, dict):
                        result.stage_metrics["reload"] = reload_meta
                        result.reload_metadata = reload_meta
                    else:
                        result.stage_metrics["reload"] = {
                            "status": "ok",
                            "message": "Reload callback completed with non-dict metadata.",
                        }
                        result.reload_metadata = result.stage_metrics["reload"]
                else:
                    reload_meta = {
                        "status": "skipped",
                        "reason": "no_reload_callback",
                    }
                    result.stage_metrics["reload"] = reload_meta
                    result.reload_metadata = reload_meta
                self.orchestrator.mark_succeeded(job_id, result=result)
                store.append_log(job_id, level="info", message="RT weekly job completed")
                return
            elif source_type == IngestSourceType.PURGE_DEPARTMENT:
                queued_status = self.orchestrator.get_job(job_id)
                request_payload = queued_status.request if queued_status is not None else {}
                store.append_log(
                    job_id,
                    level="info",
                    message="Purge request loaded",
                    extra={
                        "source_type": source_type.value,
                        "requested_department": request_payload.get("department"),
                        "dry_run": bool(request_payload.get("dry_run", False)),
                    },
                )
                store.append_log(
                    job_id,
                    level="info",
                    message="Waiting for index mutation lock",
                    extra={
                        "stages": ["purge_dataset", "full_rebuild", "reload"],
                        "requested_department": request_payload.get("department"),
                        "dry_run": bool(request_payload.get("dry_run", False)),
                    },
                )
                with self._index_mutation_lock:
                    store.append_log(
                        job_id,
                        level="info",
                        message="Index mutation lock acquired",
                        extra={"stages": ["purge_dataset", "full_rebuild", "reload"]},
                    )
                    try:
                        result = self._run_purge_department_job(job_id)
                        if bool(request_payload.get("dry_run", False)):
                            store.append_log(
                                job_id,
                                level="info",
                                message="Purge dry-run skips runtime reload",
                                extra={
                                    "stage": "reload",
                                    **result.stage_metrics.get("reload", {}),
                                },
                            )
                        elif self.on_job_success is not None:
                            try:
                                self.orchestrator.mark_running(
                                    job_id,
                                    stage="reload",
                                    progress=0.97,
                                    message="Reloading search engine",
                                )
                                reload_meta = self.on_job_success(job_id, source_type, result)
                            except Exception as exc:
                                failure_extra = self._purge_observability_extra(result)
                                failure_extra.update(
                                    {
                                        "stage": "reload",
                                        "reload_status": "failed",
                                        "error": f"{exc.__class__.__name__}: {exc}",
                                        "hint": (
                                            "Verify FAISS artifacts and retry /admin/engine/reload "
                                            "after fixing index consistency."
                                        ),
                                    }
                                )
                                store.append_log(
                                    job_id,
                                    level="error",
                                    message="Post-ingest reload failed",
                                    extra=failure_extra,
                                )
                                self.orchestrator.mark_failed(
                                    job_id,
                                    stage="reload",
                                    error=f"{exc.__class__.__name__}: {exc}",
                                    result=result,
                                )
                                return
                            if isinstance(reload_meta, dict):
                                result.stage_metrics["reload"] = reload_meta
                                result.reload_metadata = reload_meta
                                store.append_log(
                                    job_id,
                                    level="info",
                                    message="Reload stage completed",
                                    extra={
                                        "stage": "reload",
                                        "engine_generation": reload_meta.get("engine_generation"),
                                        "engine_loaded_at": reload_meta.get("engine_loaded_at"),
                                    },
                                )
                                if reload_meta.get("status") == "ok" and result.index_updated:
                                    self._run_backup_prune_stage(job_id, result)
                            else:
                                reload_payload = {
                                    "status": "ok",
                                    "message": "Reload callback completed with non-dict metadata.",
                                }
                                result.stage_metrics["reload"] = reload_payload
                                result.reload_metadata = reload_payload
                                store.append_log(
                                    job_id,
                                    level="info",
                                    message="Reload stage completed",
                                    extra={"stage": "reload", **reload_payload},
                                )
                                if result.index_updated:
                                    self._run_backup_prune_stage(job_id, result)
                        else:
                            reload_meta = {
                                "status": "skipped",
                                "reason": "no_reload_callback",
                            }
                            self.orchestrator.mark_running(
                                job_id,
                                stage="reload",
                                progress=0.97,
                                message="Reload stage placeholder (no callback configured)",
                            )
                            result.stage_metrics["reload"] = reload_meta
                            result.reload_metadata = reload_meta
                            store.append_log(
                                job_id,
                                level="info",
                                message="Reload stage skipped",
                                extra={"stage": "reload", **reload_meta},
                            )
                        store.append_log(
                            job_id,
                            level="info",
                            message="Purge flow completed",
                            extra=self._purge_observability_extra(result),
                        )
                    finally:
                        store.append_log(
                            job_id,
                            level="info",
                            message="Index mutation lock released",
                            extra={"stages": ["purge_dataset", "full_rebuild", "reload"]},
                        )
                self.orchestrator.mark_succeeded(job_id, result=result)
                store.append_log(
                    job_id,
                    level="info",
                    message="Purge department job completed",
                    extra=self._purge_observability_extra(result),
                )
                return
            else:
                raise RuntimeError(f"Unsupported source_type: {source_type}")

            store.append_log(
                job_id,
                level="info",
                message="Waiting for index mutation lock",
                extra={"stages": ["merge", "index_append", "reload"]},
            )
            with self._index_mutation_lock:
                store.append_log(
                    job_id,
                    level="info",
                    message="Index mutation lock acquired",
                    extra={"stages": ["merge", "index_append", "reload"]},
                )
                try:
                    try:
                        self._run_merge_stage(job_id, result)
                    except Exception as exc:
                        store.append_log(
                            job_id,
                            level="error",
                            message="Merge stage failed",
                            extra={
                                "stage": "merge",
                                "error": f"{exc.__class__.__name__}: {exc}",
                                "hint": "Inspect merge_summary.json and delta.jsonl under job artifacts.",
                            },
                        )
                        self.orchestrator.mark_failed(
                            job_id,
                            stage="merge",
                            error=f"{exc.__class__.__name__}: {exc}",
                            result=result,
                        )
                        return

                    if result.delta_rows > 0:
                        self._run_source_catalog_refresh_stage(
                            job_id,
                            result,
                            trigger="after_merge",
                        )
                    else:
                        self._skip_source_catalog_refresh_stage(
                            job_id,
                            result,
                            trigger="after_merge",
                            reason="delta_empty_no_mutation",
                            message="Source catalog refresh stage skipped",
                        )

                    if source_type == IngestSourceType.WEB:
                        self._run_web_job_cleanup_stage(job_id, result)

                    try:
                        self._run_index_append_stage(job_id, source_type, result)
                    except Exception as exc:
                        store.append_log(
                            job_id,
                            level="error",
                            message="Index append stage failed",
                            extra={
                                "stage": "index_append",
                                "error": f"{exc.__class__.__name__}: {exc}",
                                "hint": (
                                    "Check faiss_append_summary.json and index artifacts under "
                                    "retrieval.faiss_dir."
                                ),
                            },
                        )
                        self.orchestrator.mark_failed(
                            job_id,
                            stage="index_append",
                            error=f"{exc.__class__.__name__}: {exc}",
                            result=result,
                        )
                        return

                    if self.on_job_success is not None:
                        try:
                            self.orchestrator.mark_running(
                                job_id,
                                stage="reload",
                                progress=0.97,
                                message="Reloading search engine",
                            )
                            reload_meta = self.on_job_success(job_id, source_type, result)
                        except Exception as exc:
                            store.append_log(
                                job_id,
                                level="error",
                                message="Post-ingest reload failed",
                                extra={
                                    "stage": "reload",
                                    "error": f"{exc.__class__.__name__}: {exc}",
                                    "hint": (
                                        "Verify FAISS artifacts and retry /admin/engine/reload "
                                        "after fixing index consistency."
                                    ),
                                },
                            )
                            self.orchestrator.mark_failed(
                                job_id,
                                stage="reload",
                                error=f"{exc.__class__.__name__}: {exc}",
                                result=result,
                            )
                            return
                        if isinstance(reload_meta, dict):
                            result.stage_metrics["reload"] = reload_meta
                            result.reload_metadata = reload_meta
                            store.append_log(
                                job_id,
                                level="info",
                                message="Reload stage completed",
                                extra={
                                    "stage": "reload",
                                    "engine_generation": reload_meta.get("engine_generation"),
                                    "engine_loaded_at": reload_meta.get("engine_loaded_at"),
                                },
                            )
                            if reload_meta.get("status") == "ok" and result.index_updated:
                                self._run_backup_prune_stage(job_id, result)
                        else:
                            result.reload_metadata = {
                                "status": "ok",
                                "message": "Reload callback completed with non-dict metadata.",
                            }
                            result.stage_metrics["reload"] = result.reload_metadata
                            store.append_log(
                                job_id,
                                level="info",
                                message="Reload stage completed",
                                extra={"stage": "reload", **result.reload_metadata},
                            )
                            if result.index_updated:
                                self._run_backup_prune_stage(job_id, result)
                    else:
                        reload_meta = {
                            "status": "skipped",
                            "reason": "no_reload_callback",
                        }
                        self.orchestrator.mark_running(
                            job_id,
                            stage="reload",
                            progress=0.97,
                            message="Reload stage placeholder (no callback configured)",
                        )
                        result.stage_metrics["reload"] = reload_meta
                        result.reload_metadata = reload_meta
                        store.append_log(
                            job_id,
                            level="info",
                            message="Reload stage skipped",
                            extra={"stage": "reload", **reload_meta},
                        )
                finally:
                    store.append_log(
                        job_id,
                        level="info",
                        message="Index mutation lock released",
                        extra={"stages": ["merge", "index_append", "reload"]},
                    )

            self.orchestrator.mark_succeeded(job_id, result=result)
            store.append_log(job_id, level="info", message="Job completed")
        except Exception as exc:
            status = self.orchestrator.get_job(job_id)
            current_stage = status.stage if status is not None else "failed"
            if source_type == IngestSourceType.PURGE_DEPARTMENT and result is None:
                result = self._recover_purge_partial_result(job_id)
            failure_extra: Dict[str, Any] = {
                "stage": current_stage,
                "error": f"{exc.__class__.__name__}: {exc}",
            }
            if source_type == IngestSourceType.PURGE_DEPARTMENT and result is not None:
                failure_extra.update(self._purge_observability_extra(result))
            store.append_log(
                job_id,
                level="error",
                message="Job failed",
                extra=failure_extra,
            )
            self.orchestrator.mark_failed(
                job_id,
                stage=current_stage,
                error=f"{exc.__class__.__name__}: {exc}",
                result=result,
            )

    def _run_purge_department_job(self, job_id: str) -> IngestJobResultSummary:
        status = self.orchestrator.get_job(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")

        request = PurgeDepartmentRequest.model_validate(status.request)
        dataset_path = self._resolve_global_dataset_path()
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        purge_summary_path = artifacts_dir / "purge_summary.json"
        backup_dataset_path = artifacts_dir / "dataset_before_purge.jsonl"

        self.orchestrator.mark_running(
            job_id,
            stage="purge_dataset",
            progress=0.6,
            message="Purging department rows from global dataset",
        )
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Purge dataset stage started",
            extra={
                "stage": "purge_dataset",
                "requested_department": request.department,
                "dry_run": bool(request.dry_run),
                "output_dataset_path": str(dataset_path),
            },
        )

        cmd = [
            self.python_bin,
            str(self.scripts_root / "11_purge_department_dataset.py"),
            "--dataset",
            str(dataset_path),
            "--department",
            request.department,
            "--summary-out",
            str(purge_summary_path),
            "--backup-out",
            str(backup_dataset_path),
        ]
        if request.dry_run:
            cmd.append("--dry-run")
        proc = run_command(cmd)

        summary_payload: Dict[str, Any] = {}
        if purge_summary_path.exists():
            with purge_summary_path.open("r", encoding="utf-8") as src:
                summary_payload = json.load(src)
        else:
            stdout = (proc.stdout or "").strip()
            if stdout:
                summary_payload = json.loads(stdout)
        if not summary_payload:
            raise RuntimeError("Purge stage did not produce summary payload.")

        rows_before = int(summary_payload.get("rows_before", 0))
        rows_removed = int(summary_payload.get("rows_removed", 0))
        rows_after = int(summary_payload.get("rows_after", max(0, rows_before - rows_removed)))
        backup_path_value = summary_payload.get("backup_dataset_path")
        if backup_path_value:
            backup_path = str(backup_path_value)
        elif backup_dataset_path.exists():
            backup_path = str(backup_dataset_path)
        else:
            backup_path = None

        stage_metric = {
            "status": "ok",
            "rows_before": rows_before,
            "rows_removed": rows_removed,
            "rows_after": rows_after,
            "target_department": request.department,
            "dry_run": bool(request.dry_run),
            "output_dataset_path": str(dataset_path),
            "summary_path": str(purge_summary_path),
            "backup_dataset_path": backup_path,
        }
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Purge dataset stage completed",
            extra={"stage": "purge_dataset", **stage_metric},
        )
        if rows_removed == 0:
            self.orchestrator.job_store.append_log(
                job_id,
                level="warning",
                message="Purge removed zero rows",
                extra={
                    "stage": "purge_dataset",
                    "requested_department": request.department,
                    "dry_run": bool(request.dry_run),
                    "rows_before": rows_before,
                    "rows_removed": rows_removed,
                    "rows_after": rows_after,
                },
            )
        result = IngestJobResultSummary(
            merged_rows=rows_after,
            delta_rows=rows_removed,
            chunk_rows=0,
            index_updated=False,
            artifacts_dir=str(artifacts_dir),
            output_dataset_path=str(dataset_path),
            purge_summary_path=str(purge_summary_path),
            backup_dataset_path=backup_path,
            stage_metrics={
                "purge_dataset": stage_metric,
            }
        )
        if request.dry_run:
            skip_reason = "dry_run_no_mutation"
            self._skip_source_catalog_refresh_stage(
                job_id,
                result,
                trigger="after_purge_dataset",
                reason=skip_reason,
                message="Source catalog refresh stage skipped",
            )
            result.stage_metrics["full_rebuild"] = {
                "status": "skipped",
                "reason": skip_reason,
                "message": "Dry-run purge does not rebuild FAISS.",
            }
            reload_meta = {
                "status": "skipped",
                "reason": skip_reason,
                "message": "Dry-run purge does not reload the runtime engine.",
            }
            result.stage_metrics["reload"] = reload_meta
            result.reload_metadata = reload_meta
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Full rebuild stage skipped",
                extra={"stage": "full_rebuild", **result.stage_metrics["full_rebuild"]},
            )
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Reload stage skipped",
                extra={"stage": "reload", **reload_meta},
            )
            return result
        if rows_removed > 0:
            self._run_source_catalog_refresh_stage(
                job_id,
                result,
                trigger="after_purge_dataset",
            )
        else:
            self._skip_source_catalog_refresh_stage(
                job_id,
                result,
                trigger="after_purge_dataset",
                reason="rows_removed_zero_no_mutation",
                message="Source catalog refresh stage skipped",
            )
        self._run_full_rebuild_stage(
            job_id=job_id,
            result=result,
            dataset_path=dataset_path,
        )
        return result

    def _run_full_rebuild_stage(
        self,
        *,
        job_id: str,
        result: IngestJobResultSummary,
        dataset_path: Path,
    ) -> None:
        artifacts_dir = Path(result.artifacts_dir or str(self._artifacts_dir_for_job(job_id)))
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        rebuild_summary_path = artifacts_dir / "full_rebuild_summary.json"
        config_path = self.project_root / "config" / "rag.yaml"
        # Full rebuild helper requires delta_path for metadata only; purge flow has no delta artifact.
        delta_hint_path = artifacts_dir / "purge_delta.jsonl"

        self.orchestrator.mark_running(
            job_id,
            stage="full_rebuild",
            progress=0.85,
            message="Rebuilding FAISS index from filtered dataset",
        )
        started = perf_counter()
        try:
            rebuild_result = rebuild_full_faiss(
                config_path=str(config_path),
                delta_path=str(delta_hint_path),
                dataset_path_override=str(dataset_path),
            )
            raw_payload = rebuild_result.to_dict()
            duration_seconds = round(perf_counter() - started, 3)
            index_path = str(raw_payload.get("faiss_dir") or "")
            vector_count = int(raw_payload.get("index_count_after", 0) or 0)
            doc_count = int(raw_payload.get("docstore_count_after", 0) or 0)
            rebuild_summary = {
                "status": "ok",
                "error": None,
                "duration_seconds": duration_seconds,
                "index_path": index_path,
                "vector_count": vector_count,
                "doc_count": doc_count,
                "dataset_path": str(dataset_path),
                "backup_dir": raw_payload.get("backup_dir"),
                "staging_dir": raw_payload.get("staging_dir"),
                "raw_result": raw_payload,
            }
            write_summary_json(str(rebuild_summary_path), rebuild_summary)

            stage_metric = {
                "status": "ok",
                "error": None,
                "duration_seconds": duration_seconds,
                "index_path": index_path,
                "vector_count": vector_count,
                "doc_count": doc_count,
                "summary_path": str(rebuild_summary_path),
                "backup_dir": raw_payload.get("backup_dir"),
            }
            result.index_updated = True
            result.output_index_path = index_path or result.output_index_path
            result.full_rebuild_summary_path = str(rebuild_summary_path)
            result.stage_metrics["full_rebuild"] = stage_metric
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Full rebuild stage completed",
                extra={"stage": "full_rebuild", **stage_metric},
            )
        except Exception as exc:
            duration_seconds = round(perf_counter() - started, 3)
            error_text = f"{exc.__class__.__name__}: {exc}"
            failure_summary = {
                "status": "error",
                "error": error_text,
                "duration_seconds": duration_seconds,
                "index_path": None,
                "vector_count": 0,
                "doc_count": 0,
                "dataset_path": str(dataset_path),
            }
            summary_write_error = None
            try:
                write_summary_json(str(rebuild_summary_path), failure_summary)
            except Exception as write_exc:  # pragma: no cover - defensive
                summary_write_error = f"{write_exc.__class__.__name__}: {write_exc}"
            result.full_rebuild_summary_path = str(rebuild_summary_path)
            result.stage_metrics["full_rebuild"] = {
                **failure_summary,
                "summary_path": str(rebuild_summary_path),
            }
            self.orchestrator.job_store.append_log(
                job_id,
                level="error",
                message="Full rebuild stage failed",
                extra={
                    "stage": "full_rebuild",
                    "error": error_text,
                    "duration_seconds": duration_seconds,
                    "summary_path": str(rebuild_summary_path),
                    "summary_write_error": summary_write_error,
                    "hint": "Inspect full_rebuild_summary.json and verify dataset/index consistency.",
                },
            )
            if summary_write_error:
                raise RuntimeError(
                    f"Full rebuild failed: {error_text}; also failed to persist summary: {summary_write_error}"
                ) from exc
            raise RuntimeError(f"Full rebuild failed: {error_text}") from exc

    def _purge_observability_extra(self, result: IngestJobResultSummary) -> Dict[str, Any]:
        purge_metric = result.stage_metrics.get("purge_dataset", {}) if isinstance(result.stage_metrics, dict) else {}
        rebuild_metric = result.stage_metrics.get("full_rebuild", {}) if isinstance(result.stage_metrics, dict) else {}
        reload_metric = result.stage_metrics.get("reload", {}) if isinstance(result.stage_metrics, dict) else {}
        prune_metric = result.stage_metrics.get("backup_prune", {}) if isinstance(result.stage_metrics, dict) else {}
        catalog_metric = result.stage_metrics.get("source_catalog_refresh", {}) if isinstance(result.stage_metrics, dict) else {}
        return {
            "requested_department": purge_metric.get("target_department"),
            "dry_run": purge_metric.get("dry_run"),
            "rows_removed": purge_metric.get("rows_removed", result.delta_rows),
            "source_catalog_refresh_status": catalog_metric.get("status"),
            "rebuild_status": rebuild_metric.get("status"),
            "reload_status": reload_metric.get("status"),
            "backup_prune_status": prune_metric.get("status"),
            "purge_summary_path": result.purge_summary_path,
            "full_rebuild_summary_path": result.full_rebuild_summary_path,
            "output_index_path": result.output_index_path,
        }

    def _recover_purge_partial_result(self, job_id: str) -> Optional[IngestJobResultSummary]:
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        purge_summary_path = artifacts_dir / "purge_summary.json"
        rebuild_summary_path = artifacts_dir / "full_rebuild_summary.json"
        if not purge_summary_path.exists() and not rebuild_summary_path.exists():
            return None

        result = IngestJobResultSummary(artifacts_dir=str(artifacts_dir))

        if purge_summary_path.exists():
            try:
                with purge_summary_path.open("r", encoding="utf-8") as src:
                    payload = json.load(src)
            except Exception:  # pragma: no cover - defensive read path
                payload = {}

            rows_before = int(payload.get("rows_before", 0) or 0)
            rows_removed = int(payload.get("rows_removed", 0) or 0)
            rows_after = int(payload.get("rows_after", max(0, rows_before - rows_removed)) or 0)
            output_dataset_path = str(payload.get("output_dataset_path") or "")
            if not output_dataset_path:
                try:
                    output_dataset_path = str(self._resolve_global_dataset_path())
                except Exception:  # pragma: no cover - defensive fallback
                    output_dataset_path = ""
            backup_dataset_path = payload.get("backup_dataset_path")

            result.delta_rows = rows_removed
            result.merged_rows = rows_after
            if output_dataset_path:
                result.output_dataset_path = output_dataset_path
            result.purge_summary_path = str(purge_summary_path)
            if backup_dataset_path:
                result.backup_dataset_path = str(backup_dataset_path)

            result.stage_metrics["purge_dataset"] = {
                "status": "ok",
                "rows_before": rows_before,
                "rows_removed": rows_removed,
                "rows_after": rows_after,
                "target_department": payload.get("target_department"),
                "dry_run": bool(payload.get("dry_run", False)),
                "output_dataset_path": output_dataset_path or None,
                "summary_path": str(purge_summary_path),
                "backup_dataset_path": result.backup_dataset_path,
            }

        if rebuild_summary_path.exists():
            try:
                with rebuild_summary_path.open("r", encoding="utf-8") as src:
                    payload = json.load(src)
            except Exception:  # pragma: no cover - defensive read path
                payload = {}

            index_path_value = payload.get("index_path")
            if index_path_value:
                result.output_index_path = str(index_path_value)
            result.full_rebuild_summary_path = str(rebuild_summary_path)

            status_value = str(payload.get("status") or "unknown")
            result.index_updated = status_value == "ok"
            result.stage_metrics["full_rebuild"] = {
                "status": status_value,
                "error": payload.get("error"),
                "duration_seconds": payload.get("duration_seconds"),
                "index_path": payload.get("index_path"),
                "vector_count": payload.get("vector_count"),
                "doc_count": payload.get("doc_count"),
                "summary_path": str(rebuild_summary_path),
            }

        return result

    def _run_index_append_stage(
        self,
        job_id: str,
        source_type: IngestSourceType,
        result: IngestJobResultSummary,
    ) -> None:
        delta_rows = max(0, int(result.delta_rows or 0))
        delta_path_value = result.output_delta_path
        if not delta_path_value:
            raise RuntimeError("Missing delta path for index append stage.")
        delta_path = Path(delta_path_value)
        append_summary_value = result.index_append_summary_path
        if append_summary_value:
            append_summary_path = Path(append_summary_value)
        else:
            artifacts_dir = Path(result.artifacts_dir or str(self._artifacts_dir_for_job(job_id)))
            append_summary_path = artifacts_dir / "faiss_append_summary.json"

        is_no_op_candidate = delta_rows == 0
        self.orchestrator.mark_running(
            job_id,
            stage="index_append",
            progress=0.95,
            message=(
                "Index append no-op candidate (delta empty)"
                if is_no_op_candidate
                else "Preparing index append stage (placeholder)"
            ),
        )

        if is_no_op_candidate:
            result.index_updated = False
            result.index_append_summary_path = str(append_summary_path)
            result.stage_metrics["index_append"] = {
                "status": "no_op_candidate",
                "delta_rows": delta_rows,
                "message": "Delta is empty, append can be skipped safely.",
            }
            self.orchestrator.job_store.append_log(
                job_id,
                level="info",
                message="Index append stage skipped",
                extra={
                    "stage": "index_append",
                    "status": "no_op_candidate",
                    "delta_rows": delta_rows,
                },
            )
            return

        config_path = self.project_root / "config" / "rag.yaml"
        append_summary_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        root = str(self.project_root)
        current_pythonpath = (env.get("PYTHONPATH") or "").strip()
        if current_pythonpath:
            if root not in current_pythonpath.split(os.pathsep):
                env["PYTHONPATH"] = f"{root}{os.pathsep}{current_pythonpath}"
        else:
            env["PYTHONPATH"] = root
        append_cmd = [
            str(self.python_bin),
            str(self.scripts_root / "10_incremental_faiss_append.py"),
            "--config",
            str(config_path),
            "--delta",
            str(delta_path),
            "--summary-out",
            str(append_summary_path),
            "--disable-fallback-rebuild",
        ]
        proc = None
        if self._hpc_enabled_for_source_type(source_type):
            remote_cmd = [
                "env",
                f"PYTHONPATH={root}",
                str(self._hpc_python_bin),
                str(self.scripts_root / "10_incremental_faiss_append.py"),
                "--config",
                str(config_path),
                "--delta",
                str(delta_path),
                "--summary-out",
                str(append_summary_path),
                "--disable-fallback-rebuild",
            ]
            self._run_hpc_command_stage(
                job_id=job_id,
                source_type=source_type,
                payload_command=shlex.join(remote_cmd),
                result=result,
                context_stage="index_append",
            )
        else:
            proc = run_command(
                append_cmd,
                cwd=root,
                env=env,
            )

        payload: Dict[str, Any] = {}
        if append_summary_path.exists():
            with append_summary_path.open("r", encoding="utf-8") as src:
                payload = json.load(src)
        else:
            stdout = ((proc.stdout if proc is not None else "") or "").strip()
            if stdout:
                payload = json.loads(stdout)

        applied = bool(payload.get("applied", False))
        faiss_dir = str(payload.get("faiss_dir") or "")

        result.index_updated = applied
        result.output_index_path = faiss_dir or result.output_index_path
        result.index_append_summary_path = str(append_summary_path)
        result.stage_metrics["index_append"] = {
            "status": "ok",
            "delta_rows": delta_rows,
            "applied": applied,
            "reason": payload.get("reason"),
            "faiss_dir": faiss_dir,
            "summary_path": str(append_summary_path),
            "fallback_used": bool(payload.get("fallback_used", False)),
        }
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Index append stage completed",
            extra={
                "stage": "index_append",
                "delta_rows": delta_rows,
                "applied": applied,
                "faiss_dir": faiss_dir,
                "fallback_used": bool(payload.get("fallback_used", False)),
            },
        )

        result.stage_metrics.setdefault(
            "reload",
            {
                "status": "placeholder",
                "message": "Reload stage will run via callback when configured.",
            },
        )

    def _artifacts_dir_for_job(self, job_id: str) -> Path:
        return self.orchestrator.job_store.job_dir(job_id) / "artifacts"

    def _resolve_global_dataset_path(self) -> Path:
        config_path = self.project_root / "config" / "rag.yaml"
        default_value = "data/datasetFinal.jsonl"
        raw_dataset_path = default_value
        if config_path.exists():
            with config_path.open("r", encoding="utf-8") as src:
                payload = yaml.safe_load(src) or {}
            retrieval = payload.get("retrieval") or {}
            value = retrieval.get("dataset_path")
            if isinstance(value, str) and value.strip():
                raw_dataset_path = value.strip()
        dataset_path = Path(raw_dataset_path)
        if not dataset_path.is_absolute():
            dataset_path = self.project_root / dataset_path
        return dataset_path

    def _run_merge_stage(self, job_id: str, result: IngestJobResultSummary) -> None:
        chunked_path_value = result.output_chunked_path or result.output_dataset_path
        if not chunked_path_value:
            raise RuntimeError("Missing chunked output path for merge stage.")
        chunked_path = Path(chunked_path_value)
        if not chunked_path.exists():
            raise RuntimeError(f"Chunked output path not found for merge stage: {chunked_path}")

        artifacts_dir = Path(result.artifacts_dir or str(self._artifacts_dir_for_job(job_id)))
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        delta_path = Path(result.output_delta_path or str(artifacts_dir / "delta.jsonl"))
        merge_summary_path = Path(result.merge_summary_path or str(artifacts_dir / "merge_summary.json"))
        dataset_path = self._resolve_global_dataset_path()

        self.orchestrator.mark_running(
            job_id,
            stage="merge",
            progress=0.93,
            message="Merging chunked rows into global dataset",
        )

        run_command(
            [
                self.python_bin,
                str(self.scripts_root / "06_merge_datasets.py"),
                "--base",
                str(dataset_path),
                "--new",
                str(chunked_path),
                "--out",
                str(dataset_path),
                "--out-delta",
                str(delta_path),
                "--summary-out",
                str(merge_summary_path),
            ]
        )

        if not merge_summary_path.exists():
            raise RuntimeError(f"Merge summary not generated: {merge_summary_path}")
        with merge_summary_path.open("r", encoding="utf-8") as src:
            summary = json.load(src)

        out_rows = int(summary.get("out_rows", 0))
        delta_rows = int(summary.get("delta_rows", 0))

        result.artifacts_dir = str(artifacts_dir)
        result.merged_rows = out_rows
        result.delta_rows = delta_rows
        result.output_dataset_path = str(dataset_path)
        result.output_delta_path = str(delta_path)
        result.merge_summary_path = str(merge_summary_path)
        result.stage_metrics["merge"] = {
            "status": "ok",
            "out_rows": out_rows,
            "delta_rows": delta_rows,
            "delta_empty": delta_rows == 0,
            "summary_path": str(merge_summary_path),
            "delta_path": str(delta_path),
        }
        self.orchestrator.job_store.append_log(
            job_id,
            level="info",
            message="Merge stage completed",
            extra={
                "stage": "merge",
                "out_rows": out_rows,
                "delta_rows": delta_rows,
                "delta_empty": delta_rows == 0,
                "delta_path": str(delta_path),
                "summary_path": str(merge_summary_path),
            },
        )

    def _run_web_job(self, job_id: str) -> IngestJobResultSummary:
        store = self.orchestrator.job_store
        status = self.orchestrator.get_job(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")

        request = WebIngestRequest.model_validate(status.request)
        paths = build_web_pipeline_paths(
            job_id=job_id,
            base_root=str(self.project_root / "data" / "raw_site" / "jobs"),
        )
        ensure_web_pipeline_layout(paths)

        self.orchestrator.mark_running(
            job_id,
            stage="acquire_source",
            progress=0.2,
            message="Mirroring website",
        )
        mirror_stats = mirror_website(
            url=str(request.url),
            output_root=paths.mirror_root,
            depth_limit=request.depth_limit,
        )
        store.append_log(job_id, level="info", message="Mirror stage completed", extra=mirror_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="extract",
            progress=0.45,
            message="Extracting HTML/PDF documents",
        )
        extract_stats = run_extract_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            paths=paths,
            base_url=base_url_for_scripts(str(request.url)),
        )
        store.append_log(job_id, level="info", message="Extract stage completed", extra=extract_stats)

        mirrored_html = int(mirror_stats.get("mirrored_html_files", 0))
        mirrored_pdf = int(mirror_stats.get("mirrored_pdf_files", 0))
        extracted_html = int(extract_stats.get("extracted_html_docs", 0))
        extracted_pdf = int(extract_stats.get("extracted_pdf_pages", 0))
        mirrored_total = mirrored_html + mirrored_pdf
        extracted_total = extracted_html + extracted_pdf
        low_coverage_warning: Optional[Dict[str, Any]] = None
        if mirrored_total <= 2 or extracted_total <= 2:
            low_coverage_warning = {
                "message": (
                    "Low crawl coverage detected. The source may be a preview page or have limited crawlable links "
                    "within the requested path scope."
                ),
                "mirrored_total": mirrored_total,
                "mirrored_html_files": mirrored_html,
                "mirrored_pdf_files": mirrored_pdf,
                "extracted_total": extracted_total,
                "extracted_html_docs": extracted_html,
                "extracted_pdf_pages": extracted_pdf,
            }
            store.append_log(
                job_id,
                level="warning",
                message="Low crawl coverage detected",
                extra=low_coverage_warning,
            )

        self.orchestrator.mark_running(
            job_id,
            stage="prepare",
            progress=0.7,
            message="Preparing page rows",
        )
        prepare_stats = run_prepare_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            paths=paths,
            department=request.department,
            ingest_label=request.ingest_label,
            ingest_job_id=job_id,
            ingested_at=status.created_at,
        )
        store.append_log(job_id, level="info", message="Prepare stage completed", extra=prepare_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="chunk",
            progress=0.9,
            message="Chunking prepared rows",
        )
        chunk_stats = run_chunk_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            preprocess_config=self.preprocess_config,
            paths=paths,
        )
        store.append_log(job_id, level="info", message="Chunk stage completed", extra=chunk_stats)
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        delta_path = artifacts_dir / "delta.jsonl"
        merge_summary_path = artifacts_dir / "merge_summary.json"
        append_summary_path = artifacts_dir / "faiss_append_summary.json"

        return IngestJobResultSummary(
            merged_rows=0,
            delta_rows=0,
            chunk_rows=int(chunk_stats.get("chunk_rows", 0)),
            index_updated=False,
            artifacts_dir=str(artifacts_dir),
            output_chunked_path=str(paths.chunked_jsonl),
            output_dataset_path=str(paths.chunked_jsonl),
            output_delta_path=str(delta_path),
            output_index_path=None,
            merge_summary_path=str(merge_summary_path),
            index_append_summary_path=str(append_summary_path),
            stage_metrics={
                "mirror": mirror_stats,
                "extract": extract_stats,
                "prepare": prepare_stats,
                "chunk": chunk_stats,
                **({"coverage_warning": low_coverage_warning} if low_coverage_warning else {}),
            },
        )

    def _run_pdf_job(self, job_id: str, staged_input_path: Optional[str]) -> IngestJobResultSummary:
        store = self.orchestrator.job_store
        status = self.orchestrator.get_job(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")

        request = PdfIngestRequest.model_validate(status.request)
        paths = build_pdf_pipeline_paths(
            job_id=job_id,
            base_root=str(self.project_root / "data" / "raw_site" / "jobs"),
        )
        ensure_pdf_pipeline_layout(paths)

        if staged_input_path:
            staged = Path(staged_input_path)
            if not staged.exists():
                raise RuntimeError(f"Staged PDF path does not exist: {staged_input_path}")
            if paths.pdf_input_root.resolve() not in staged.resolve().parents:
                raise RuntimeError("Staged PDF must be inside job pdf_input directory")

        base_url = str(request.source_url) if request.source_url is not None else f"https://uploaded.local/{job_id}"

        self.orchestrator.mark_running(
            job_id,
            stage="extract",
            progress=0.45,
            message="Extracting PDF pages",
        )
        extract_stats = run_pdf_extract_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            paths=paths,
            base_url=base_url,
        )
        store.append_log(job_id, level="info", message="Extract stage completed", extra=extract_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="prepare",
            progress=0.7,
            message="Preparing page rows",
        )
        prepare_stats = run_pdf_prepare_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            paths=paths,
            department=request.department,
            ingest_label=request.ingest_label,
            ingest_job_id=job_id,
            ingested_at=status.created_at,
        )
        store.append_log(job_id, level="info", message="Prepare stage completed", extra=prepare_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="chunk",
            progress=0.9,
            message="Chunking prepared rows",
        )
        chunk_stats = run_pdf_chunk_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            preprocess_config=self.preprocess_config,
            paths=paths,
        )
        store.append_log(job_id, level="info", message="Chunk stage completed", extra=chunk_stats)
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        delta_path = artifacts_dir / "delta.jsonl"
        merge_summary_path = artifacts_dir / "merge_summary.json"
        append_summary_path = artifacts_dir / "faiss_append_summary.json"

        return IngestJobResultSummary(
            merged_rows=0,
            delta_rows=0,
            chunk_rows=int(chunk_stats.get("chunk_rows", 0)),
            index_updated=False,
            artifacts_dir=str(artifacts_dir),
            output_chunked_path=str(paths.chunked_jsonl),
            output_dataset_path=str(paths.chunked_jsonl),
            output_delta_path=str(delta_path),
            output_index_path=None,
            merge_summary_path=str(merge_summary_path),
            index_append_summary_path=str(append_summary_path),
            stage_metrics={
                "extract": extract_stats,
                "prepare": prepare_stats,
                "chunk": chunk_stats,
            },
        )

    def _run_repo_docs_job(self, job_id: str) -> IngestJobResultSummary:
        store = self.orchestrator.job_store
        status = self.orchestrator.get_job(job_id)
        if status is None:
            raise KeyError(f"job_id not found: {job_id}")

        request = RepoDocsIngestRequest.model_validate(status.request)
        paths = build_repo_docs_pipeline_paths(
            job_id=job_id,
            base_root=str(self.project_root / "data" / "raw_site" / "jobs"),
        )
        ensure_repo_docs_pipeline_layout(paths)

        self.orchestrator.mark_running(
            job_id,
            stage="acquire_source",
            progress=0.2,
            message="Fetching repository documentation",
        )
        acquire_stats = acquire_repo_docs(
            url=str(request.url),
            output_root=paths.acquire_root,
        )
        store.append_log(job_id, level="info", message="Acquire source stage completed", extra=acquire_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="prepare",
            progress=0.7,
            message="Preparing page rows",
        )
        prepare_stats = run_repo_docs_prepare_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            paths=paths,
            department=request.department,
            ingest_label=request.ingest_label,
            ingest_job_id=job_id,
            ingested_at=status.created_at,
        )
        store.append_log(job_id, level="info", message="Prepare stage completed", extra=prepare_stats)

        self.orchestrator.mark_running(
            job_id,
            stage="chunk",
            progress=0.9,
            message="Chunking prepared rows",
        )
        chunk_stats = run_repo_docs_chunk_stage(
            python_bin=self.python_bin,
            scripts_root=self.scripts_root,
            preprocess_config=self.preprocess_config,
            paths=paths,
        )
        store.append_log(job_id, level="info", message="Chunk stage completed", extra=chunk_stats)
        artifacts_dir = self._artifacts_dir_for_job(job_id)
        delta_path = artifacts_dir / "delta.jsonl"
        merge_summary_path = artifacts_dir / "merge_summary.json"
        append_summary_path = artifacts_dir / "faiss_append_summary.json"

        return IngestJobResultSummary(
            merged_rows=0,
            delta_rows=0,
            chunk_rows=int(chunk_stats.get("chunk_rows", 0)),
            index_updated=False,
            artifacts_dir=str(artifacts_dir),
            output_chunked_path=str(paths.chunked_jsonl),
            output_dataset_path=str(paths.chunked_jsonl),
            output_delta_path=str(delta_path),
            output_index_path=None,
            merge_summary_path=str(merge_summary_path),
            index_append_summary_path=str(append_summary_path),
            stage_metrics={
                "acquire_source": acquire_stats,
                "prepare": prepare_stats,
                "chunk": chunk_stats,
            },
        )
