#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

try:
    from scripts.common_department import normalize_department
except ModuleNotFoundError:
    from common_department import normalize_department


UTC = timezone.utc
DEFAULT_BOOTSTRAP_WATERMARK = "2000-01-01T00:00:00Z"
DEFAULT_FETCH_SIZE = 500


def parse_utc(value: str) -> datetime:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("empty datetime value")

    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Invalid datetime format: {value}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0)


def datetime_to_iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def datetime_to_db_param(value: datetime) -> str:
    return value.astimezone(UTC).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def parse_watermark(path: Optional[str]) -> Optional[datetime]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    return parse_utc(raw)


def to_json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    if isinstance(value, bytearray):
        return to_json_value(bytes(value))
    if isinstance(value, memoryview):
        return to_json_value(value.tobytes())
    if isinstance(value, datetime):
        return value.astimezone(UTC).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, dict):
        return {str(k): to_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_value(v) for v in value]
    return value


def resolve_env(name: str, *, required: bool = True, default: Optional[str] = None) -> str:
    raw = os.getenv(name, default if default is not None else "")
    value = str(raw).strip()
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def compute_window(
    watermark: Optional[datetime],
    *,
    overlap_hours: float,
    bootstrap_watermark: datetime,
    now_utc: datetime,
) -> tuple[datetime, datetime]:
    if watermark is None:
        lower_bound = bootstrap_watermark
    else:
        lower_bound = watermark - timedelta(hours=overlap_hours)
        if lower_bound < bootstrap_watermark:
            lower_bound = bootstrap_watermark
    upper_bound = now_utc
    return lower_bound, upper_bound


def normalize_row_department(value: Any) -> str:
    normalized = normalize_department(None if value is None else str(value), allow_unknown=True)
    if normalized:
        return normalized
    return "unknown"


def coerce_last_updated(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).replace(microsecond=0)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return parse_utc(raw)
        except ValueError:
            return None
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/daily_ingest.yaml")
    ap.add_argument("--out-dir", required=True, help="Output directory for raw extracted JSONL files")
    ap.add_argument(
        "--watermark-file",
        default="state/last_success_ts.txt",
        help="Path storing last successful watermark in UTC ISO format",
    )
    ap.add_argument("--summary-out", required=True, help="Where to write extraction summary JSON")
    ap.add_argument("--overlap-hours", type=float, default=None, help="Override overlap window in hours")
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as cfg_file:
        cfg = yaml.safe_load(cfg_file) or {}

    db_cfg = cfg.get("database", {})
    extract_cfg = cfg.get("extract", {})
    extract_backend = str(extract_cfg.get("backend", "ticket_downloader")).strip().lower()
    if extract_backend not in {"ticket_downloader", "sql"}:
        raise SystemExit("extract.backend must be either 'ticket_downloader' or 'sql'")

    sql = ""
    if extract_backend == "sql":
        sql = (extract_cfg.get("sql") or "").strip()
        if not sql:
            raise SystemExit("Missing extract.sql in config/daily_ingest.yaml")

    department_field = str(extract_cfg.get("department_field", "department"))
    last_updated_field = str(extract_cfg.get("last_updated_field", "last_updated"))
    fetch_size = int(extract_cfg.get("fetch_size", DEFAULT_FETCH_SIZE))
    output_prefix = str(extract_cfg.get("output_prefix", "resolved_tickets")).strip() or "resolved_tickets"

    configured_overlap = float(extract_cfg.get("overlap_hours", 48))
    overlap_hours = configured_overlap if args.overlap_hours is None else float(args.overlap_hours)
    bootstrap_watermark = parse_utc(
        str(extract_cfg.get("bootstrap_watermark_utc", DEFAULT_BOOTSTRAP_WATERMARK))
    )

    host = resolve_env(str(db_cfg.get("host_env", "DAILY_DB_HOST")))
    port = int(resolve_env(str(db_cfg.get("port_env", "DAILY_DB_PORT")), required=False, default="3306"))
    user = resolve_env(str(db_cfg.get("user_env", "DAILY_DB_USER")))
    password = resolve_env(str(db_cfg.get("password_env", "DAILY_DB_PASSWORD")))
    database = resolve_env(str(db_cfg.get("name_env", "DAILY_DB_NAME")))
    charset = str(db_cfg.get("charset", "utf8mb4"))
    connect_timeout = int(db_cfg.get("connect_timeout_sec", 15))
    read_timeout = int(db_cfg.get("read_timeout_sec", 300))
    write_timeout = int(db_cfg.get("write_timeout_sec", 300))
    # Some legacy RT rows contain malformed byte sequences for utf-8.
    # When use_unicode=False, PyMySQL returns bytes and we normalize safely in to_json_value().
    use_unicode = parse_bool(db_cfg.get("use_unicode"), default=False)

    watermark = parse_watermark(args.watermark_file)
    now_utc = datetime.now(UTC).replace(microsecond=0)
    lower_bound, upper_bound = compute_window(
        watermark,
        overlap_hours=overlap_hours,
        bootstrap_watermark=bootstrap_watermark,
        now_utc=now_utc,
    )

    query_params: Dict[str, Any] = {
        "lower_bound": datetime_to_db_param(lower_bound),
        "upper_bound": datetime_to_db_param(upper_bound),
        "lower_bound_utc": datetime_to_iso_utc(lower_bound),
        "upper_bound_utc": datetime_to_iso_utc(upper_bound),
    }
    configured_params = extract_cfg.get("query_params", {}) or {}
    if not isinstance(configured_params, dict):
        raise SystemExit("extract.query_params must be an object")
    for key, value in configured_params.items():
        query_params[str(key)] = value

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_out = Path(args.summary_out)
    summary_out.parent.mkdir(parents=True, exist_ok=True)

    run_stamp = upper_bound.strftime("%Y%m%dT%H%M%SZ")
    counts_by_department: Dict[str, int] = {}
    files_by_department: Dict[str, Path] = {}
    total_rows = 0
    max_last_updated: Optional[datetime] = None

    if extract_backend == "ticket_downloader":
        downloader_script_raw = str(
            extract_cfg.get("ticket_downloader_script", "scriptsDescargaRAG/ticketDownloader.py")
        ).strip()
        if not downloader_script_raw:
            raise SystemExit("extract.ticket_downloader_script cannot be empty")
        downloader_script = Path(downloader_script_raw)
        if not downloader_script.is_absolute():
            downloader_script = Path.cwd() / downloader_script
        if not downloader_script.exists():
            raise SystemExit(f"ticketDownloader script not found: {downloader_script}")

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
        ) as tmp_summary:
            downloader_summary_path = Path(tmp_summary.name)
        try:
            downloader_cmd = [
                sys.executable,
                str(downloader_script),
                "--since",
                datetime_to_db_param(lower_bound),
                "--until",
                datetime_to_db_param(upper_bound),
                "--out-dir",
                str(out_dir),
                "--summary-out",
                str(downloader_summary_path),
                "--output-mode",
                "pipeline",
                "--output-prefix",
                output_prefix,
                "--run-stamp",
                run_stamp,
            ]
            result = subprocess.run(
                downloader_cmd,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.stdout:
                print(result.stdout.rstrip())
            if result.returncode != 0:
                if result.stderr:
                    print(result.stderr.rstrip(), file=sys.stderr)
                raise SystemExit(
                    f"ticketDownloader extraction failed (exit={result.returncode})"
                )

            downloader_summary = json.loads(
                downloader_summary_path.read_text(encoding="utf-8")
            )
            counts_raw = downloader_summary.get("counts_by_department", {}) or {}
            if isinstance(counts_raw, dict):
                for key, value in counts_raw.items():
                    department = normalize_row_department(key)
                    counts_by_department[department] = int(value or 0)

            files_raw = downloader_summary.get("files", []) or []
            if isinstance(files_raw, list):
                for file_item in files_raw:
                    if not isinstance(file_item, dict):
                        continue
                    department = normalize_row_department(file_item.get("department"))
                    file_path_raw = str(file_item.get("path") or "").strip()
                    if not file_path_raw:
                        continue
                    path = Path(file_path_raw)
                    if not path.is_absolute():
                        path = Path.cwd() / path
                    files_by_department[department] = path
                    if department not in counts_by_department:
                        counts_by_department[department] = int(file_item.get("rows") or 0)

            total_rows = int(downloader_summary.get("total_rows") or 0)
            if total_rows == 0 and counts_by_department:
                total_rows = int(sum(counts_by_department.values()))

            max_last_updated_raw = downloader_summary.get("max_row_last_updated_utc")
            max_last_updated = coerce_last_updated(max_last_updated_raw)
        finally:
            if downloader_summary_path.exists():
                downloader_summary_path.unlink()
    else:
        try:
            import pymysql  # Local import to keep helper functions testable without DB deps.
        except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency check
            raise SystemExit(
                "Missing dependency: PyMySQL. Install requirements before running daily ingestion."
            ) from exc

        writers: Dict[str, Any] = {}
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=use_unicode,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
        )

        try:
            with conn.cursor() as cur:
                cur.execute(sql, query_params)
                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    for row in rows:
                        row_dict = to_json_value(dict(row))
                        department = normalize_row_department(row_dict.get(department_field))
                        row_dict["department"] = department

                        path = files_by_department.get(department)
                        if path is None:
                            path = out_dir / f"{output_prefix}_{run_stamp}_{department}.jsonl"
                            files_by_department[department] = path
                            writers[department] = path.open("w", encoding="utf-8")
                        writer = writers[department]
                        writer.write(json.dumps(row_dict, ensure_ascii=False) + "\n")

                        counts_by_department[department] = counts_by_department.get(department, 0) + 1
                        total_rows += 1

                        parsed_last_updated = coerce_last_updated(row_dict.get(last_updated_field))
                        if parsed_last_updated is not None and (
                            max_last_updated is None or parsed_last_updated > max_last_updated
                        ):
                            max_last_updated = parsed_last_updated
        finally:
            for writer in writers.values():
                writer.close()
            conn.close()

    summary = {
        "generated_at_utc": datetime_to_iso_utc(datetime.now(UTC).replace(microsecond=0)),
        "extract_backend": extract_backend,
        "lower_bound_utc": datetime_to_iso_utc(lower_bound),
        "upper_bound_utc": datetime_to_iso_utc(upper_bound),
        "next_watermark_utc": datetime_to_iso_utc(upper_bound),
        "max_row_last_updated_utc": datetime_to_iso_utc(max_last_updated) if max_last_updated else None,
        "total_rows": total_rows,
        "counts_by_department": counts_by_department,
        "files": [
            {
                "department": dept,
                "rows": counts_by_department.get(dept, 0),
                "path": str(path),
            }
            for dept, path in sorted(files_by_department.items())
        ],
    }

    with summary_out.open("w", encoding="utf-8") as dst:
        json.dump(summary, dst, ensure_ascii=False, indent=2)
        dst.write("\n")

    print(
        "Extraction completed: "
        f"rows={total_rows} departments={len(counts_by_department)} summary={summary_out}"
    )


if __name__ == "__main__":
    main()
