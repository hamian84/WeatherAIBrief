from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from scripts.common.date_utils import format_run_date, parse_run_date

UTC = ZoneInfo("UTC")
DATE_INPUT_PATTERN = re.compile(r"^(?:\d{8}|\d{4}-\d{2}-\d{2})$")
UTC_CYCLE_INPUT_PATTERN = re.compile(r"^\d{10}$")


@dataclass(frozen=True)
class CollectionTarget:
    raw_input: str
    storage_date: date
    storage_date_text: str
    reference_utc: datetime
    mode: str


def _normalize_now_utc(now: datetime | None = None) -> datetime:
    current = now or datetime.now(tz=UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def _build_explicit_target(reference_utc: datetime, mode: str) -> CollectionTarget:
    if reference_utc.tzinfo is None:
        reference_utc = reference_utc.replace(tzinfo=UTC)
    reference_utc = reference_utc.astimezone(UTC)
    if reference_utc.hour not in {0, 12}:
        raise ValueError("explicit UTC cycle must end with 00 or 12 hour")
    storage_date = reference_utc.date()
    return CollectionTarget(
        raw_input=reference_utc.strftime("%Y%m%d%H"),
        storage_date=storage_date,
        storage_date_text=format_run_date(storage_date),
        reference_utc=reference_utc,
        mode=mode,
    )


def _targets_for_date(target_date: date) -> list[CollectionTarget]:
    hours = (0, 12)
    return [
        _build_explicit_target(
            datetime(target_date.year, target_date.month, target_date.day, hour, tzinfo=UTC),
            mode="date_all_synoptic_cycles",
        )
        for hour in hours
    ]


def resolve_completed_synoptic_reference_utc(now: datetime | None = None) -> datetime:
    """Return the latest completed 12-hour synoptic boundary.

    The boundary is chosen from 00 UTC / 12 UTC using the half-open window
    [now - 12h, now). This means that when the job starts exactly at 00/12 UTC,
    it still uses the previously completed cycle.
    """
    current_utc = _normalize_now_utc(now)
    probe = current_utc - timedelta(microseconds=1)
    boundary_hour = 0 if probe.hour < 12 else 12
    return probe.replace(hour=boundary_hour, minute=0, second=0, microsecond=0)


def resolve_collection_targets(value: str, now: datetime | None = None) -> list[CollectionTarget]:
    raw_input = str(value).strip()
    if not raw_input:
        raise ValueError("collection target input must not be empty")

    if UTC_CYCLE_INPUT_PATTERN.match(raw_input):
        reference_utc = datetime.strptime(raw_input, "%Y%m%d%H").replace(tzinfo=UTC)
        return [_build_explicit_target(reference_utc, mode="explicit_utc_cycle")]

    if DATE_INPUT_PATTERN.match(raw_input):
        storage_date = parse_run_date(raw_input)
        return _targets_for_date(storage_date)

    raise ValueError("collection target must be YYYYMMDD or YYYYMMDDHH")


def resolve_collection_target(value: str, now: datetime | None = None) -> CollectionTarget:
    targets = resolve_collection_targets(value, now)
    return targets[-1]
