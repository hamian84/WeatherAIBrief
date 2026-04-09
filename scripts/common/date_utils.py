from __future__ import annotations

import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
COMPACT_DATE_FORMAT = "%Y%m%d"
LEGACY_DATE_FORMAT = "%Y-%m-%d"
COMPACT_DATE_PATTERN = re.compile(r"^\d{8}$")
LEGACY_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_run_date(value: str) -> date:
    raw_value = str(value).strip()
    if COMPACT_DATE_PATTERN.match(raw_value):
        return datetime.strptime(raw_value, COMPACT_DATE_FORMAT).date()
    if LEGACY_DATE_PATTERN.match(raw_value):
        return datetime.strptime(raw_value, LEGACY_DATE_FORMAT).date()
    raise ValueError("run_date must be YYYYMMDD")


def format_run_date(value: date) -> str:
    return value.strftime(COMPACT_DATE_FORMAT)


def normalize_run_date(value: str) -> str:
    return format_run_date(parse_run_date(value))


def today_run_date_kst() -> str:
    return datetime.now(tz=KST).strftime(COMPACT_DATE_FORMAT)
