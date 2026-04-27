#!/usr/bin/env python3
"""Print the next planned execution time for daily ingest scheduler."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml


WEEKDAY_ALIASES = {
    "0": 0,
    "sun": 0,
    "sunday": 0,
    "1": 1,
    "mon": 1,
    "monday": 1,
    "2": 2,
    "tue": 2,
    "tuesday": 2,
    "3": 3,
    "wed": 3,
    "wednesday": 3,
    "4": 4,
    "thu": 4,
    "thursday": 4,
    "5": 5,
    "fri": 5,
    "friday": 5,
    "6": 6,
    "sat": 6,
    "saturday": 6,
}


@dataclass(frozen=True)
class Schedule:
    mode: str
    interval_minutes: int
    daily_time_local: time
    weekly_day_of_week: int  # 0=Sunday, 6=Saturday


def parse_daily_time(raw: Any) -> time:
    txt = str(raw).strip()
    parts = txt.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("schedule.daily_time_local must be HH:MM or HH:MM:SS")
    hh = int(parts[0])
    mm = int(parts[1])
    ss = int(parts[2]) if len(parts) == 3 else 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        raise ValueError("schedule.daily_time_local out of range")
    return time(hour=hh, minute=mm, second=ss)


def normalize_weekday(raw: Any) -> int:
    if isinstance(raw, int):
        if 0 <= raw <= 6:
            return raw
        raise ValueError("schedule.weekly_day_of_week must be in range 0..6")
    key = str(raw).strip().lower()
    if key not in WEEKDAY_ALIASES:
        raise ValueError("schedule.weekly_day_of_week must be 0-6 or sun..sat")
    return WEEKDAY_ALIASES[key]


def load_schedule(config_path: Path) -> Schedule:
    with config_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    raw = cfg.get("schedule") or {}
    mode = str(raw.get("mode", "interval")).strip().lower()
    if mode not in {"interval", "daily_at", "weekly_at"}:
        raise ValueError("schedule.mode must be 'interval', 'daily_at', or 'weekly_at'")
    interval_minutes = int(raw.get("interval_minutes", 1440))
    if interval_minutes < 1:
        raise ValueError("schedule.interval_minutes must be >= 1")
    daily_time_local = parse_daily_time(raw.get("daily_time_local", "02:00:00"))
    weekly_day_of_week = normalize_weekday(raw.get("weekly_day_of_week", "sunday"))
    return Schedule(
        mode=mode,
        interval_minutes=interval_minutes,
        daily_time_local=daily_time_local,
        weekly_day_of_week=weekly_day_of_week,
    )


def next_run(schedule: Schedule, now_local: datetime) -> datetime:
    if schedule.mode == "interval":
        return now_local + timedelta(minutes=schedule.interval_minutes)

    daily_candidate = now_local.replace(
        hour=schedule.daily_time_local.hour,
        minute=schedule.daily_time_local.minute,
        second=schedule.daily_time_local.second,
        microsecond=0,
    )

    if schedule.mode == "daily_at":
        if daily_candidate <= now_local:
            daily_candidate += timedelta(days=1)
        return daily_candidate

    # weekly_at: convert Python weekday (Mon=0..Sun=6) to Sun=0..Sat=6
    current_sun0 = (now_local.weekday() + 1) % 7
    delta_days = (schedule.weekly_day_of_week - current_sun0 + 7) % 7
    weekly_candidate = daily_candidate + timedelta(days=delta_days)
    if weekly_candidate <= now_local:
        weekly_candidate += timedelta(days=7)
    return weekly_candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show next planned execution time from config/daily_ingest.yaml schedule."
    )
    parser.add_argument(
        "--config",
        default="config/daily_ingest.yaml",
        help="Path to daily ingest config (default: config/daily_ingest.yaml)",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")

    schedule = load_schedule(config_path)
    now_local = datetime.now().astimezone()
    nxt_local = next_run(schedule, now_local)
    nxt_utc = nxt_local.astimezone(timezone.utc)
    wait_seconds = int((nxt_local - now_local).total_seconds())

    print(f"config={config_path}")
    print(f"mode={schedule.mode}")
    print(f"now_local={now_local.isoformat(timespec='seconds')}")
    print(f"next_run_local={nxt_local.isoformat(timespec='seconds')}")
    print(f"next_run_utc={nxt_utc.isoformat(timespec='seconds')}")
    print(f"seconds_until_next_run={wait_seconds}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
