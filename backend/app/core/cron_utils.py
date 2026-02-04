"""
Lightweight cron utilities (no external dependencies)
Supports 5-field cron: minute hour day month weekday
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Set


_RANGE_LIMITS = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day": (1, 31),
    "month": (1, 12),
    "weekday": (0, 6),  # Monday=0..Sunday=6 (we normalize from cron 0=Sunday)
}


@dataclass
class CronSchedule:
    minutes: Set[int]
    hours: Set[int]
    days: Set[int]
    months: Set[int]
    weekdays: Set[int]


def _parse_field(field: str, min_val: int, max_val: int) -> Set[int]:
    values: Set[int] = set()
    parts = [p.strip() for p in field.split(",") if p.strip()]
    if not parts:
        raise ValueError("Empty cron field")

    for part in parts:
        if part == "*":
            values.update(range(min_val, max_val + 1))
            continue

        step = 1
        if "/" in part:
            base, step_str = part.split("/", 1)
            step = int(step_str)
            part = base

        if part == "*" or part == "":
            values.update(range(min_val, max_val + 1, step))
            continue

        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if start > end:
                raise ValueError(f"Invalid range: {part}")
            values.update(range(start, end + 1, step))
            continue

        # single value
        values.add(int(part))

    # clamp to range
    return {v for v in values if min_val <= v <= max_val}


def parse_cron(expr: str) -> CronSchedule:
    parts = [p for p in expr.strip().split() if p]
    if len(parts) != 5:
        raise ValueError("Cron expression must have 5 fields: min hour day month weekday")

    minute_s, hour_s, day_s, month_s, weekday_s = parts
    minutes = _parse_field(minute_s, *_RANGE_LIMITS["minute"])
    hours = _parse_field(hour_s, *_RANGE_LIMITS["hour"])
    days = _parse_field(day_s, *_RANGE_LIMITS["day"])
    months = _parse_field(month_s, *_RANGE_LIMITS["month"])

    # Cron weekday: 0 or 7 = Sunday, 1=Monday
    raw_weekdays = _parse_field(weekday_s, 0, 7)
    normalized = set()
    for w in raw_weekdays:
        if w == 7:
            w = 0
        # convert to Monday=0..Sunday=6
        normalized.add((w - 1) % 7)
    weekdays = normalized

    return CronSchedule(minutes=minutes, hours=hours, days=days, months=months, weekdays=weekdays)


def cron_matches(schedule: CronSchedule, dt: datetime) -> bool:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    weekday = dt.weekday()
    return (
        dt.minute in schedule.minutes
        and dt.hour in schedule.hours
        and dt.day in schedule.days
        and dt.month in schedule.months
        and weekday in schedule.weekdays
    )


def next_run(expr: str, from_dt: datetime | None = None, max_minutes: int = 525600) -> datetime:
    if from_dt is None:
        from_dt = datetime.now(timezone.utc)
    if from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=timezone.utc)

    schedule = parse_cron(expr)
    probe = from_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)

    for _ in range(max_minutes):
        if cron_matches(schedule, probe):
            return probe
        probe += timedelta(minutes=1)

    raise ValueError("No matching cron time found within max_minutes window")

