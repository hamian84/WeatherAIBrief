"""
ASOS 시간자료 수집.

실행 예시:
  python -m scripts.collect_asos --date 2026-01-19 --dry-run
  python -m scripts.collect_asos --date 2026-01-19
  python -m scripts.collect_asos --date 2026-01-19 --overwrite
"""

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from scripts.common.collection_schedule import resolve_collection_target
from scripts.common.config import get_env_value, load_project_env
from scripts.common.logging import configure_logging

KST = ZoneInfo("Asia/Seoul")
DATE_FORMAT = "%Y%m%d"

BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"
RAW_FILENAME = "asos_hourly.csv"
CURATED_FILENAME = "asos_daily_summary.csv"
SERVICE_KEY_ENV = "KMA_APIHUB_AUTH_KEY"
DEFAULT_SLEEP_SECONDS = 1.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_RETRY_BACKOFF_MAX_SECONDS = 20.0
DEFAULT_RETRY_WAIT_SECONDS = 60
DEFAULT_MAX_RECOVER_ROUNDS = 1
DEFAULT_TIMEOUT_SECONDS = 600
VERIFY_REPORT_FILENAME = "verify_result.json"


def _setup_logging(run_date: str) -> Path:
    return configure_logging("collect_asos", run_date)


def _window_for_reference(reference_utc: datetime) -> tuple[str, str, str, str, str, str]:
    reference_kst = reference_utc.astimezone(KST)
    reference_dt = reference_kst.strftime("%Y%m%d")
    reference_hh = reference_kst.strftime("%H")
    window_text = reference_kst.strftime("%Y%m%d %H:00")
    start_dt = reference_dt
    end_dt = reference_dt
    start_hh = reference_hh
    end_hh = reference_hh
    window_start = window_text
    window_end = window_text
    return start_dt, start_hh, end_dt, end_hh, window_start, window_end


def _window_for_secondary(target_date: date) -> tuple[str, str, str, str, str, str]:
    start_dt = target_date.strftime("%Y%m%d")
    end_dt = target_date.strftime("%Y%m%d")
    start_hh = "08"
    end_hh = "08"
    window_start = f"{target_date.strftime(DATE_FORMAT)} 08:00"
    window_end = f"{target_date.strftime(DATE_FORMAT)} 08:00"
    return start_dt, start_hh, end_dt, end_hh, window_start, window_end


def _read_station_ids(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"관측소 목록 파일 없음: {path}")
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value:
            continue
        ids.append(value)
    if not ids:
        raise ValueError(f"관측소 목록이 비어 있음: {path}")
    return ids


def _encode_query(params: dict[str, Any]) -> str:
    return urlencode(params, doseq=True, safe=",%")


def _fetch_text(url: str, timeout_seconds: int) -> str:
    request = Request(url, headers={"Accept": "text/plain"})
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read().decode("utf-8")
    return payload


def _mask_service_key(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "****" + value[-2:]


def _build_request_params(
    service_key: str,
    stn_id: str,
    tm1: str,
    tm2: str,
) -> dict[str, Any]:
    return {
        "authKey": service_key,
        "tm1": tm1,
        "tm2": tm2,
        "stn": stn_id,
    }


def _build_batch_request_params(
    service_key: str,
    stn_ids: list[str],
    tm1: str,
    tm2: str,
) -> dict[str, Any]:
    return {
        "authKey": service_key,
        "tm1": tm1,
        "tm2": tm2,
        "stn_id": ",".join(stn_ids),
    }


def _mask_params(params: dict[str, Any]) -> dict[str, Any]:
    masked = dict(params)
    masked["authKey"] = _mask_service_key(str(params.get("authKey", "")))
    return masked


def _build_request_url(params: dict[str, Any]) -> str:
    return f"{BASE_URL}?{_encode_query(params)}"


def _normalize_header_tokens(
    header_tokens: list[str],
    header_map: dict[str, str],
    unit_tokens: list[str] | None = None,
) -> list[str]:
    expected_tokens = [
        "YYMMDDHHMI", "STN", "WD", "WS", "GST", "GST", "GST", "PA", "PS", "PT",
        "PR", "TA", "TD", "HM", "PV", "RN", "RN", "RN", "RN", "SD", "SD", "SD",
        "WC", "WP", "WW", "CA", "CA", "CH", "CT", "CT", "CT", "CT", "VS", "SS",
        "SI", "ST", "TS", "TE", "TE", "TE", "TE", "ST", "WH", "BF", "IR", "IX",
    ]
    expected_aliases = [
        "tm", "stnId", "WD", "WS", "GST_WD", "GST_WS", "GST_TM", "PA", "PS", "PT",
        "PR", "TA", "TD", "HM", "PV", "RN", "RN_DAY", "RN_JUN", "RN_INT", "SD_HR3",
        "SD_DAY", "SD_TOT", "WC", "WP", "WW", "CA_TOT", "CA_MID", "CH_MIN", "CT",
        "CT_TOP", "CT_MID", "CT_LOW", "VS", "SS", "SI", "ST_GD", "TS", "TE_005",
        "TE_01", "TE_02", "TE_03", "ST_SEA", "WH", "BF", "IR", "IX",
    ]
    upper_tokens = [token.upper() for token in header_tokens]
    if upper_tokens == expected_tokens:
        return expected_aliases[:]

    unit_alias_map = {
        ("GST", "WD"): "GST_WD",
        ("GST", "WS"): "GST_WS",
        ("GST", "TM"): "GST_TM",
        ("RN", "DAY"): "RN_DAY",
        ("RN", "JUN"): "RN_JUN",
        ("RN", "INT"): "RN_INT",
        ("SD", "HR3"): "SD_HR3",
        ("SD", "DAY"): "SD_DAY",
        ("SD", "TOT"): "SD_TOT",
        ("CA", "TOT"): "CA_TOT",
        ("CA", "MID"): "CA_MID",
        ("CH", "MIN"): "CH_MIN",
        ("CT", "TOP"): "CT_TOP",
        ("CT", "MID"): "CT_MID",
        ("CT", "LOW"): "CT_LOW",
        ("ST", "GD"): "ST_GD",
        ("ST", "SEA"): "ST_SEA",
        ("TE", "5"): "TE_005",
        ("TE", "10"): "TE_01",
        ("TE", "20"): "TE_02",
        ("TE", "30"): "TE_03",
    }
    normalized: list[str] = []
    counts: dict[str, int] = {}

    for idx, token in enumerate(header_tokens):
        base = header_map.get(token.upper(), token)
        count = counts.get(base, 0) + 1
        counts[base] = count
        if count == 1:
            normalized.append(base)
            continue

        alias = None
        if unit_tokens and idx < len(unit_tokens):
            unit_key = unit_tokens[idx].strip().upper().replace("CM", "").replace("/", "")
            alias = unit_alias_map.get((base.upper(), unit_key))

        if alias and alias not in normalized:
            normalized.append(alias)
        else:
            normalized.append(f"{base}_{count}")
    return normalized


def _parse_text_payload(
    payload: str,
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    lines = [line.strip() for line in payload.splitlines() if line.strip()]
    if not lines:
        return [], "EMPTY_RESPONSE", "응답 없음"
    comment_lines = [line for line in lines if line.startswith("#")]
    filtered = [line for line in lines if not line.startswith("#")]
    if not filtered:
        return [], "EMPTY_RESPONSE", "응답 없음"
    header_tokens: list[str] = []
    header_map = {
        "YYMMDDHHMI": "tm",
        "STN": "stnId",
        "STN_ID": "stnId",
        "STNID": "stnId",
    }
    preferred_tokens: list[str] = []
    preferred_index: int | None = None
    header_from_comment = False
    for idx, line in enumerate(comment_lines):
        candidate = line.lstrip("#").strip()
        if not candidate:
            continue
        tokens = candidate.split()
        if not tokens:
            continue
        if "YYMMDDHHMI" in tokens:
            preferred_tokens = tokens
            preferred_index = idx
            header_from_comment = True
            break
    if preferred_tokens:
        header_tokens = preferred_tokens
    else:
        for line in comment_lines:
            candidate = line.lstrip("#").strip()
            if not candidate:
                continue
            tokens = candidate.split()
            if len(tokens) < 3:
                continue
            if any(ch.isalpha() for ch in candidate):
                header_tokens = tokens
                header_from_comment = True
                break
    if not header_tokens:
        header_tokens = filtered[0].split()
    if not header_tokens:
        return [], "PARSE_ERROR", "헤더 없음"
    unit_tokens: list[str] | None = None
    if header_from_comment and preferred_index is not None:
        unit_line_index = preferred_index + 1
        if unit_line_index < len(comment_lines):
            candidate = comment_lines[unit_line_index].lstrip("#").strip()
            tokens = candidate.split()
            if len(tokens) == len(header_tokens):
                unit_tokens = tokens
    header = _normalize_header_tokens(header_tokens, header_map, unit_tokens)
    if header[0].upper().startswith("ERROR"):
        return [], "ERROR", filtered[0]
    rows: list[dict[str, Any]] = []
    data_lines = filtered if header_from_comment else filtered[1:]
    for line in data_lines:
        parts = line.split()
        if not parts:
            continue
        if len(parts) < len(header):
            parts = parts + [""] * (len(header) - len(parts))
        row: dict[str, Any] = {}
        for idx, key in enumerate(header):
            row[key] = parts[idx] if idx < len(parts) else ""
        if "stnId" not in row and "stn" in row:
            row["stnId"] = row.get("stn", "")
        rows.append(row)
    if not rows:
        return [], "EMPTY_RESPONSE", "데이터 없음"
    return rows, "00", None


def _build_tm_value(dt_str: str, hh: str, mm: str = "00") -> str:
    return f"{dt_str}{hh}{mm}"


def _fetch_request_items(
    service_key: str,
    stn_id: str,
    start_dt: str,
    start_hh: str,
    end_dt: str,
    end_hh: str,
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str | None, str | None, str]:
    tm1 = _build_tm_value(start_dt, start_hh)
    tm2 = _build_tm_value(end_dt, end_hh)
    params = _build_request_params(
        service_key,
        stn_id,
        tm1,
        tm2,
    )
    masked_url = _build_request_url(_mask_params(params))
    logging.info("요청 주소: %s", masked_url)

    try:
        payload = _fetch_text(_build_request_url(params), timeout_seconds=timeout_seconds)
    except HTTPError as exc:
        return [], f"HTTP_{exc.code}", str(exc), masked_url
    except URLError as exc:
        return [], "URL_ERROR", str(exc.reason), masked_url
    except Exception as exc:
        return [], "EXCEPTION", str(exc), masked_url

    items, result_code, result_msg = _parse_text_payload(payload)
    if result_code not in (None, "00"):
        return [], result_code, result_msg, masked_url

    return items, result_code, result_msg, masked_url


def _should_retry(result_code: str | None) -> bool:
    return result_code not in (None, "00")


def _fetch_with_retry(
    service_key: str,
    stn_id: str,
    start_dt: str,
    start_hh: str,
    end_dt: str,
    end_hh: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    last_code: str | None = None
    last_msg: str | None = None
    last_url: str = ""
    for attempt in range(max_retries + 1):
        items, result_code, result_msg, masked_url = _fetch_request_items(
            service_key,
            stn_id,
            start_dt,
            start_hh,
            end_dt,
            end_hh,
            timeout_seconds,
        )
        if not _should_retry(result_code):
            return items, []
        last_code = result_code
        last_msg = result_msg
        last_url = masked_url
        if attempt < max_retries:
            wait_seconds = min(backoff_seconds * (2**attempt), backoff_max_seconds)
            logging.warning(
                "호출 재시도 대기: stnId=%s, attempt=%d/%d, wait=%.1fs, resultCode=%s",
                stn_id,
                attempt + 1,
                max_retries,
                wait_seconds,
                result_code,
            )
            time.sleep(wait_seconds)
    return (
        [],
        [
            {
                "stnId": stn_id,
                "startDt": start_dt,
                "startHh": start_hh,
                "endDt": end_dt,
                "endHh": end_hh,
                "resultCode": last_code,
                "resultMsg": last_msg,
                "url": last_url,
            }
        ],
    )


def _fetch_all_items(
    service_key: str,
    stn_ids: list[str],
    start_dt: str,
    start_hh: str,
    end_dt: str,
    end_hh: str,
    timeout_seconds: int,
    sleep_seconds: float,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    tm1 = _build_tm_value(start_dt, start_hh)
    tm2 = _build_tm_value(end_dt, end_hh)

    logging.info(
        "일괄 호출 시작: stn_id 개수=%d, tm1=%s, tm2=%s",
        len(stn_ids),
        tm1,
        tm2,
    )
    batch_items, batch_failures = _fetch_batch_with_retry(
        service_key=service_key,
        stn_ids=stn_ids,
        tm1=tm1,
        tm2=tm2,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        backoff_max_seconds=backoff_max_seconds,
    )
    if not batch_failures:
        logging.info("일괄 호출 성공: 수집 건수=%d", len(batch_items))
        return batch_items, []

    logging.warning(
        "일괄 호출 실패로 지점별 폴백 실행: resultCode=%s, resultMsg=%s",
        batch_failures[0].get("resultCode"),
        batch_failures[0].get("resultMsg"),
    )
    if batch_failures[0].get("url"):
        logging.warning("일괄 호출 URL(마스킹): %s", batch_failures[0].get("url"))
    return _fetch_all_items_by_station(
        service_key=service_key,
        stn_ids=stn_ids,
        start_dt=start_dt,
        start_hh=start_hh,
        end_dt=end_dt,
        end_hh=end_hh,
        timeout_seconds=timeout_seconds,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
        backoff_max_seconds=backoff_max_seconds,
    )


def _fetch_all_items_by_station(
    service_key: str,
    stn_ids: list[str],
    start_dt: str,
    start_hh: str,
    end_dt: str,
    end_hh: str,
    timeout_seconds: int,
    sleep_seconds: float,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    all_items: list[dict[str, Any]] = []
    failures: list[dict[str, str | None]] = []

    for stn_id in stn_ids:
        logging.info(
            "지점 호출 시작: stnId=%s, startDt=%s,startHh=%s,endDt=%s,endHh=%s",
            stn_id,
            start_dt,
            start_hh,
            end_dt,
            end_hh,
        )
        items, stn_failures = _fetch_with_retry(
            service_key,
            stn_id,
            start_dt,
            start_hh,
            end_dt,
            end_hh,
            timeout_seconds,
            max_retries,
            backoff_seconds,
            backoff_max_seconds,
        )
        if stn_failures:
            logging.warning(
                "호출 실패: stnId=%s, startDt=%s,startHh=%s,endDt=%s,endHh=%s, resultCode=%s, resultMsg=%s",
                stn_id,
                start_dt,
                start_hh,
                end_dt,
                end_hh,
                stn_failures[0].get("resultCode"),
                stn_failures[0].get("resultMsg"),
            )
            if stn_failures[0].get("url"):
                logging.warning("요청 URL(마스킹): %s", stn_failures[0].get("url"))
            failures.extend(stn_failures)
        else:
            all_items.extend(items)
            logging.info("누적 건수=%d", len(all_items))
        logging.info("지점 호출 종료: stnId=%s", stn_id)
        time.sleep(sleep_seconds)

    return all_items, failures


def _fetch_batch_request_items(
    service_key: str,
    stn_ids: list[str],
    tm1: str,
    tm2: str,
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str | None, str | None, str]:
    params = _build_batch_request_params(
        service_key=service_key,
        stn_ids=stn_ids,
        tm1=tm1,
        tm2=tm2,
    )
    masked_url = _build_request_url(_mask_params(params))
    logging.info("요청 주소: %s", masked_url)

    try:
        payload = _fetch_text(_build_request_url(params), timeout_seconds=timeout_seconds)
    except HTTPError as exc:
        return [], f"HTTP_{exc.code}", str(exc), masked_url
    except URLError as exc:
        return [], "URL_ERROR", str(exc.reason), masked_url
    except Exception as exc:
        return [], "EXCEPTION", str(exc), masked_url

    items, result_code, result_msg = _parse_text_payload(payload)
    if result_code not in (None, "00"):
        return [], result_code, result_msg, masked_url

    return items, result_code, result_msg, masked_url


def _fetch_batch_with_retry(
    service_key: str,
    stn_ids: list[str],
    tm1: str,
    tm2: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    last_code: str | None = None
    last_msg: str | None = None
    last_url: str = ""
    for attempt in range(max_retries + 1):
        items, result_code, result_msg, masked_url = _fetch_batch_request_items(
            service_key=service_key,
            stn_ids=stn_ids,
            tm1=tm1,
            tm2=tm2,
            timeout_seconds=timeout_seconds,
        )
        if not _should_retry(result_code):
            return items, []
        last_code = result_code
        last_msg = result_msg
        last_url = masked_url
        if attempt < max_retries:
            wait_seconds = min(backoff_seconds * (2**attempt), backoff_max_seconds)
            logging.warning(
                "일괄 호출 재시도 대기: attempt=%d/%d, wait=%.1fs, resultCode=%s",
                attempt + 1,
                max_retries,
                wait_seconds,
                result_code,
            )
            time.sleep(wait_seconds)

    return (
        [],
        [
            {
                "stnId": "BATCH",
                "tm1": tm1,
                "tm2": tm2,
                "resultCode": last_code,
                "resultMsg": last_msg,
                "url": last_url,
            }
        ],
    )


def _fetch_request_items_by_tm(
    service_key: str,
    stn_id: str,
    tm1: str,
    tm2: str,
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str | None, str | None, str]:
    params = _build_request_params(
        service_key,
        stn_id,
        tm1,
        tm2,
    )
    masked_url = _build_request_url(_mask_params(params))
    logging.info("요청 주소: %s", masked_url)

    try:
        payload = _fetch_text(_build_request_url(params), timeout_seconds=timeout_seconds)
    except HTTPError as exc:
        return [], f"HTTP_{exc.code}", str(exc), masked_url
    except URLError as exc:
        return [], "URL_ERROR", str(exc.reason), masked_url
    except Exception as exc:
        return [], "EXCEPTION", str(exc), masked_url

    items, result_code, result_msg = _parse_text_payload(payload)
    if result_code not in (None, "00"):
        return [], result_code, result_msg, masked_url

    return items, result_code, result_msg, masked_url


def _fetch_with_retry_by_tm(
    service_key: str,
    stn_id: str,
    tm1: str,
    tm2: str,
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    last_code: str | None = None
    last_msg: str | None = None
    last_url: str = ""
    for attempt in range(max_retries + 1):
        items, result_code, result_msg, masked_url = _fetch_request_items_by_tm(
            service_key,
            stn_id,
            tm1,
            tm2,
            timeout_seconds,
        )
        if not _should_retry(result_code):
            return items, []
        last_code = result_code
        last_msg = result_msg
        last_url = masked_url
        if attempt < max_retries:
            wait_seconds = min(backoff_seconds * (2**attempt), backoff_max_seconds)
            logging.warning(
                "복구 호출 재시도 대기: stnId=%s, attempt=%d/%d, wait=%.1fs, resultCode=%s",
                stn_id,
                attempt + 1,
                max_retries,
                wait_seconds,
                result_code,
            )
            time.sleep(wait_seconds)
    return (
        [],
        [
            {
                "stnId": stn_id,
                "tm1": tm1,
                "tm2": tm2,
                "resultCode": last_code,
                "resultMsg": last_msg,
                "url": last_url,
            }
        ],
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    if not fieldnames:
        fieldnames = []
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


def _filter_rows_by_station_ids(
    rows: list[dict[str, Any]],
    stn_ids: list[str],
) -> tuple[list[dict[str, Any]], int]:
    allowed = {value.strip() for value in stn_ids if value.strip()}
    filtered: list[dict[str, Any]] = []
    dropped = 0
    for row in rows:
        stn_id = str(row.get("stnId", "")).strip()
        if not stn_id:
            dropped += 1
            continue
        if stn_id not in allowed:
            dropped += 1
            continue
        filtered.append(row)
    return filtered, dropped


def _merge_rows(
    existing_rows: list[dict[str, Any]],
    new_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    before_count = len(existing_rows)
    new_count = len(new_rows)
    combo_map: dict[tuple[str, str], dict[str, Any]] = {}
    order_keys: list[tuple[str, str]] = []
    missing_existing: list[dict[str, Any]] = []
    missing_new: list[dict[str, Any]] = []
    for row in existing_rows:
        stn_id = (row.get("stnId") or "").strip()
        tm = (row.get("tm") or "").strip()
        if not stn_id or not tm:
            missing_existing.append(row)
            continue
        key = (stn_id, tm)
        if key not in combo_map:
            order_keys.append(key)
        combo_map[key] = row
    replaced = 0
    for row in new_rows:
        stn_id = (row.get("stnId") or "").strip()
        tm = (row.get("tm") or "").strip()
        if not stn_id or not tm:
            missing_new.append(row)
            continue
        key = (stn_id, tm)
        if key in combo_map:
            replaced += 1
        else:
            order_keys.append(key)
        combo_map[key] = row
    merged_rows = [combo_map[key] for key in order_keys] + missing_existing + missing_new
    after_count = len(merged_rows)
    dedup_removed = (before_count + new_count) - after_count
    stats = {
        "before": before_count,
        "new": new_count,
        "after": after_count,
        "dedup_removed": dedup_removed,
        "replaced": replaced,
    }
    return merged_rows, stats


def _derive_window_text_from_rows(rows: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    valid_tms = sorted(
        str(row.get("tm") or "").strip()
        for row in rows
        if str(row.get("tm") or "").strip()
    )
    if not valid_tms:
        return None, None

    def _format_tm(tm: str) -> str:
        return datetime.strptime(tm, "%Y%m%d%H%M").strftime("%Y%m%d %H:%M")

    return _format_tm(valid_tms[0]), _format_tm(valid_tms[-1])


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def _build_curated_rows(
    rows: list[dict[str, Any]],
    stn_ids: list[str],
    report_date: str,
    window_start: str,
    window_end: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    numeric_keys: set[str] = set()
    stats: dict[str, dict[str, Any]] = {}

    for row in rows:
        stn_id = row.get("stnId")
        if not stn_id:
            continue
        entry = stats.setdefault(stn_id, {"count": 0, "sums": {}, "counts": {}})
        entry["count"] += 1
        for key, value in row.items():
            if key == "stnId":
                continue
            num = _to_float(value)
            if num is None:
                continue
            numeric_keys.add(key)
            entry["sums"][key] = entry["sums"].get(key, 0.0) + num
            entry["counts"][key] = entry["counts"].get(key, 0) + 1

    sorted_numeric_keys = sorted(numeric_keys)
    fieldnames = [
        "report_date",
        "window_start",
        "window_end",
        "stnId",
        "obs_count",
    ] + [f"mean_{key}" for key in sorted_numeric_keys]

    curated_rows: list[dict[str, Any]] = []
    for stn_id in stn_ids:
        entry = stats.get(stn_id)
        row_out: dict[str, Any] = {
            "report_date": report_date,
            "window_start": window_start,
            "window_end": window_end,
            "stnId": stn_id,
            "obs_count": 0 if entry is None else entry["count"],
        }
        for key in sorted_numeric_keys:
            if entry is None:
                row_out[f"mean_{key}"] = ""
                continue
            value_count = entry["counts"].get(key, 0)
            if value_count <= 0:
                row_out[f"mean_{key}"] = ""
                continue
            mean_value = entry["sums"][key] / value_count
            row_out[f"mean_{key}"] = f"{mean_value:.3f}"
        curated_rows.append(row_out)

    return curated_rows, fieldnames


def _has_csv_data(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for _ in reader:
                return True
        return False
    except OSError:
        return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ASOS 시간자료 수집")
    parser.add_argument(
        "--date",
        required=True,
        help="collection target (YYYYMMDD or YYYYMMDDHH, HH in 00/12)",
    )
    parser.add_argument(
        "--phase",
        choices=("primary", "secondary"),
        default="primary",
        help="수집 단계(primary=1차 수집, secondary=복구 실행)",
    )
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 로그만 출력")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="동일 날짜 산출물이 존재할 때 덮어쓰기",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="API 호출 간격(초)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="지점별 재시도 횟수",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
        help="재시도 대기 시작값(초)",
    )
    parser.add_argument(
        "--retry-backoff-max-seconds",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_MAX_SECONDS,
        help="재시도 대기 최대값(초)",
    )
    parser.add_argument(
        "--retry-wait-seconds",
        type=int,
        default=DEFAULT_RETRY_WAIT_SECONDS,
        help="복구 전 대기 시간(초)",
    )
    parser.add_argument(
        "--verify-report-path",
        help="검증 결과 JSON 경로 (기본: logs/YYYY-MM-DD/verify_result.json)",
    )
    parser.add_argument(
        "--max-recover-rounds",
        type=int,
        default=DEFAULT_MAX_RECOVER_ROUNDS,
        help="복구 재시도 횟수",
    )
    return parser


def _default_verify_report_path(base_dir: Path, target_date: date) -> Path:
    return base_dir / "logs" / target_date.strftime(DATE_FORMAT) / VERIFY_REPORT_FILENAME


def _run_verify(
    base_dir: Path,
    target_date: date,
    report_path: Path,
    stations_file: Path | None,
) -> int:
    env = dict(os.environ)
    cmd = [
        sys.executable,
        str(base_dir / "scripts" / "verify_asos_outputs.py"),
        "--date",
        target_date.strftime(DATE_FORMAT),
        "--report-path",
        str(report_path),
    ]
    if stations_file is not None and stations_file.exists():
        cmd.extend(["--stations-file", str(stations_file)])
    result = subprocess.run(cmd, cwd=base_dir, env=env)
    return result.returncode


def run_verify(
    base_dir: Path,
    target_date: date,
    report_path: Path,
    stations_file: Path | None,
) -> dict[str, Any]:
    _run_verify(base_dir, target_date, report_path, stations_file)
    return _load_verify_report(report_path)


def _load_verify_report(report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        return {}
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _extract_missing_keys(report: dict[str, Any]) -> list[tuple[str, str]]:
    raw_keys = report.get("missing_keys", [])
    missing: list[tuple[str, str]] = []
    if not isinstance(raw_keys, list):
        return missing
    for entry in raw_keys:
        if not isinstance(entry, list) or len(entry) != 2:
            continue
        stn_id = str(entry[0]).strip()
        tm = str(entry[1]).strip()
        if stn_id and tm:
            missing.append((stn_id, tm))
    return missing


def _extract_duplicate_keys(report: dict[str, Any]) -> list[tuple[str, str]]:
    raw_keys = report.get("duplicate_keys", [])
    dupes: list[tuple[str, str]] = []
    if not isinstance(raw_keys, list):
        return dupes
    for entry in raw_keys:
        if not isinstance(entry, list) or len(entry) != 2:
            continue
        stn_id = str(entry[0]).strip()
        tm = str(entry[1]).strip()
        if stn_id and tm:
            dupes.append((stn_id, tm))
    return dupes


def _fetch_missing_items(
    service_key: str,
    missing_keys: list[tuple[str, str]],
    timeout_seconds: int,
    sleep_seconds: float,
    max_retries: int,
    backoff_seconds: float,
    backoff_max_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, str | None]]]:
    missing_set = set(missing_keys)
    grouped: dict[str, list[str]] = defaultdict(list)
    for stn_id, tm in missing_keys:
        grouped[stn_id].append(tm)

    recovered_items: list[dict[str, Any]] = []
    failures: list[dict[str, str | None]] = []

    for stn_id, tms in grouped.items():
        tm1 = min(tms)
        tm2 = max(tms)
        logging.info("복구 호출: stnId=%s, tm1=%s, tm2=%s", stn_id, tm1, tm2)
        items, stn_failures = _fetch_with_retry_by_tm(
            service_key=service_key,
            stn_id=stn_id,
            tm1=tm1,
            tm2=tm2,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            backoff_max_seconds=backoff_max_seconds,
        )
        if stn_failures:
            failures.extend(stn_failures)
            continue
        for row in items:
            row_stn = (row.get("stnId") or "").strip()
            row_tm = (row.get("tm") or "").strip()
            if (row_stn, row_tm) in missing_set:
                recovered_items.append(row)
        time.sleep(sleep_seconds)

    return recovered_items, failures


def _recover_missing_keys(
    service_key: str,
    missing_keys: list[tuple[str, str]],
    raw_path: Path,
    curated_path: Path,
    stn_ids: list[str],
    report_date: str,
    window_start: str,
    window_end: str,
    sleep_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_backoff_max_seconds: float,
) -> bool:
    recovered_items, recover_failures = _fetch_missing_items(
        service_key=service_key,
        missing_keys=missing_keys,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_seconds=retry_backoff_seconds,
        backoff_max_seconds=retry_backoff_max_seconds,
    )
    if recover_failures:
        logging.warning("복구 호출 실패: %d건", len(recover_failures))
    if not recovered_items:
        logging.error("복구 결과 0건")
        return False
    existing_rows = _read_csv_rows(raw_path)
    merged_rows, stats = _merge_rows(existing_rows, recovered_items)
    logging.info(
        "복구 병합 전/후: before=%d, new=%d, after=%d, 중복 제거=%d (신규 행 우선)",
        stats["before"],
        stats["new"],
        stats["after"],
        stats["dedup_removed"],
    )
    _write_csv(raw_path, merged_rows)
    logging.info("raw 병합 저장 완료: %s", raw_path)
    curated_rows, fieldnames = _build_curated_rows(
        merged_rows,
        stn_ids,
        report_date=report_date,
        window_start=window_start,
        window_end=window_end,
    )
    with curated_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(curated_rows)
    logging.info("curated 재생성 완료: %s", curated_path)
    return True


def auto_recover_after_primary(
    base_dir: Path,
    target_date: date,
    report_path: Path,
    recover_report_path: Path,
    stn_ids_path: Path | None,
    service_key: str,
    raw_path: Path,
    curated_path: Path,
    stn_ids: list[str],
    window_start: str,
    window_end: str,
    sleep_seconds: float,
    max_retries: int,
    retry_backoff_seconds: float,
    retry_backoff_max_seconds: float,
    retry_wait_seconds: int,
) -> int:
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report = run_verify(base_dir, target_date, report_path, stn_ids_path)
        if report.get("status") == "PASS":
            logging.info("primary 검증 PASS")
            return 0

        missing_keys = _extract_missing_keys(report)
        duplicate_keys = _extract_duplicate_keys(report)
        if duplicate_keys:
            logging.warning("중복 키 감지(복구 건너뜀): %d", len(duplicate_keys))
        if not missing_keys:
            logging.error("missing_keys 없음: 복구 대상이 비어 있음")
            return 1

        logging.info("복구 대기: %d초", retry_wait_seconds)
        time.sleep(retry_wait_seconds)
        recovered = _recover_missing_keys(
            service_key=service_key,
            missing_keys=missing_keys,
            raw_path=raw_path,
            curated_path=curated_path,
            stn_ids=stn_ids,
            report_date=target_date.strftime(DATE_FORMAT),
            window_start=window_start,
            window_end=window_end,
            sleep_seconds=sleep_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_backoff_max_seconds=retry_backoff_max_seconds,
        )
        if not recovered:
            return 1

        report_after = run_verify(
            base_dir, target_date, recover_report_path, stn_ids_path
        )
        if report_after.get("status") == "PASS":
            logging.info("복구 완료")
            return 0

        logging.error("복구 후에도 검증 FAIL")
        return 1
    except Exception as exc:
        logging.exception("자동 복구 실패: %s", exc)
        return 1

def main() -> int:
    args = build_parser().parse_args()

    try:
        target = resolve_collection_target(args.date)
        log_path = _setup_logging(target.storage_date_text)
        logging.info(
            "수집 시작: input=%s storage_date=%s",
            args.date,
            target.storage_date_text,
        )
        logging.info("로그 파일: %s", log_path)

        target_date = target.storage_date
        reference_utc = target.reference_utc
        start_dt, start_hh, end_dt, end_hh, window_start, window_end = _window_for_reference(reference_utc)

        base_dir = Path(__file__).resolve().parents[1]
        load_project_env(base_dir)
        service_key = get_env_value(SERVICE_KEY_ENV)
        if not service_key:
            raise ValueError(f"환경변수 없음: {SERVICE_KEY_ENV}")

        stn_ids_path = base_dir / "config" / "asos_stn_ids.txt"
        stn_ids = _read_station_ids(stn_ids_path)

        raw_dir = base_dir / "dain" / target_date.strftime(DATE_FORMAT) / "asos"
        curated_dir = base_dir / "dain" / target_date.strftime(DATE_FORMAT) / "curated" / "asos"
        raw_path = raw_dir / RAW_FILENAME
        curated_path = curated_dir / CURATED_FILENAME
        report_path = (
            Path(args.verify_report_path)
            if args.verify_report_path
            else _default_verify_report_path(base_dir, target_date)
        )
        recover_report_path = report_path.with_name("verify_result_after_recover.json")

        logging.info("수집 단계=%s", args.phase)
        logging.info(
            "기준 synoptic UTC 선택: %s",
            reference_utc.strftime("%Y-%m-%d %H:%M UTC"),
        )
        logging.info("기준 선택 방식: %s", target.mode)
        logging.info(
            "수집 시간창: startDt=%s,startHh=%s,endDt=%s,endHh=%s (%s ~ %s)",
            start_dt,
            start_hh,
            end_dt,
            end_hh,
            window_start,
            window_end,
        )
        logging.info("관측소 ID 개수: %d", len(stn_ids))
        logging.info("관측소 목록 파일: %s", stn_ids_path)

        if args.dry_run:
            logging.info("[DRY-RUN] API 호출 스킵")
            logging.info(
                "[DRY-RUN] 기준 synoptic UTC: %s",
                reference_utc.strftime("%Y-%m-%d %H:%M UTC"),
            )
            logging.info("[DRY-RUN] 기준 선택 방식: %s", target.mode)
            logging.info("[DRY-RUN] 호출 URL: %s", BASE_URL)
            logging.info(
                "[DRY-RUN] 호출 파라미터(일괄): tm1=%s%s00, tm2=%s%s00, stn_id=<지점ID,콤마구분>, authKey=***",
                start_dt,
                start_hh,
                end_dt,
                end_hh,
            )
            logging.info("[DRY-RUN] 지점 개수: %d", len(stn_ids))
            logging.info("[DRY-RUN] 예상 호출 수: 1 (일괄 호출, 실패 시 지점별 폴백)")
            logging.info("[DRY-RUN] raw 저장 예정: %s", raw_path)
            logging.info("[DRY-RUN] curated 저장 예정: %s", curated_path)
            logging.info("[DRY-RUN] 호출 간격(초): %.1f", args.sleep_seconds)
            logging.info("[DRY-RUN] verify report 경로: %s", report_path)
            return 0

        curated_window_start = window_start
        curated_window_end = window_end

        if (
            args.phase == "primary"
            and not args.overwrite
            and target.mode != "explicit_utc_cycle"
        ):
            raw_ready = _has_csv_data(raw_path)
            curated_ready = _has_csv_data(curated_path)
            if raw_ready and curated_ready:
                logging.info(
                    "동일 날짜 산출물이 존재하여 스킵: %s",
                    target_date.strftime(DATE_FORMAT),
                )
                return 0

        if args.phase == "secondary":
            if not raw_path.exists():
                logging.warning("secondary 전제 불충족: raw 파일 없음: %s", raw_path)
                return 1
            report = _load_verify_report(report_path)
            missing_keys = _extract_missing_keys(report)
            duplicate_keys = _extract_duplicate_keys(report)
            if duplicate_keys:
                logging.warning("중복 키 감지(복구 건너뜀): %d", len(duplicate_keys))
            if not missing_keys:
                logging.error("missing_keys 없음: 복구 대상이 비어 있음")
                return 1
            logging.info("복구 대기: %d초", args.retry_wait_seconds)
            time.sleep(args.retry_wait_seconds)
            recovered = _recover_missing_keys(
                service_key=service_key,
                missing_keys=missing_keys,
                raw_path=raw_path,
                curated_path=curated_path,
                stn_ids=stn_ids,
                report_date=target_date.strftime(DATE_FORMAT),
                window_start=window_start,
                window_end=window_end,
                sleep_seconds=args.sleep_seconds,
                max_retries=args.max_retries,
                retry_backoff_seconds=args.retry_backoff_seconds,
                retry_backoff_max_seconds=args.retry_backoff_max_seconds,
            )
            if not recovered:
                return 1
            items_for_curated = _read_csv_rows(raw_path)
            items_for_curated, dropped = _filter_rows_by_station_ids(items_for_curated, stn_ids)
            derived_window_start, derived_window_end = _derive_window_text_from_rows(items_for_curated)
            if derived_window_start and derived_window_end:
                curated_window_start = derived_window_start
                curated_window_end = derived_window_end
            if dropped > 0:
                logging.warning("요청 목록 외/무효 지점 행 제외(secondary): %d건", dropped)
        else:
            items, failures = _fetch_all_items(
                service_key=service_key,
                stn_ids=stn_ids,
                start_dt=start_dt,
                start_hh=start_hh,
                end_dt=end_dt,
                end_hh=end_hh,
                timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                sleep_seconds=args.sleep_seconds,
                max_retries=args.max_retries,
                backoff_seconds=args.retry_backoff_seconds,
                backoff_max_seconds=args.retry_backoff_max_seconds,
            )
            logging.info("수집 결과 건수: %d", len(items))
            if failures:
                logging.warning("실패 목록 요약: %d건", len(failures))
                for failure in failures:
                    logging.warning(
                        "실패: stnId=%s, startDt=%s,startHh=%s,endDt=%s,endHh=%s, resultCode=%s, resultMsg=%s",
                        failure.get("stnId"),
                        failure.get("startDt"),
                        failure.get("startHh"),
                        failure.get("endDt"),
                        failure.get("endHh"),
                        failure.get("resultCode"),
                        failure.get("resultMsg"),
                    )
            items, dropped = _filter_rows_by_station_ids(items, stn_ids)
            if dropped > 0:
                logging.warning("요청 목록 외/무효 지점 행 제외(primary): %d건", dropped)
            logging.info("필터 적용 후 수집 건수: %d", len(items))

            raw_dir.mkdir(parents=True, exist_ok=True)
            curated_dir.mkdir(parents=True, exist_ok=True)

            items_for_curated = items
            if raw_path.exists() and target.mode == "explicit_utc_cycle" and not args.overwrite:
                existing_rows = _read_csv_rows(raw_path)
                merged_rows, stats = _merge_rows(existing_rows, items)
                logging.info(
                    "explicit cycle merge: before=%d, new=%d, after=%d, dedup_removed=%d",
                    stats["before"],
                    stats["new"],
                    stats["after"],
                    stats["dedup_removed"],
                )
                _write_csv(raw_path, merged_rows)
                items_for_curated = merged_rows
                derived_window_start, derived_window_end = _derive_window_text_from_rows(merged_rows)
                if derived_window_start and derived_window_end:
                    curated_window_start = derived_window_start
                    curated_window_end = derived_window_end
            else:
                _write_csv(raw_path, items)
            logging.info("raw 저장 완료: %s", raw_path)

        curated_rows, fieldnames = _build_curated_rows(
            items_for_curated,
            stn_ids,
            report_date=target_date.strftime(DATE_FORMAT),
            window_start=curated_window_start,
            window_end=curated_window_end,
        )
        with curated_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(curated_rows)
        if args.phase == "secondary":
            logging.info("curated 재생성 완료: %s", curated_path)
            report_after = run_verify(base_dir, target_date, report_path, stn_ids_path)
            if report_after.get("status") == "PASS":
                logging.info("secondary 복구 검증 PASS")
                return 0
            logging.error("secondary 복구 검증 FAIL")
            return 1

        logging.info("curated 저장 완료: %s", curated_path)
        return auto_recover_after_primary(
            base_dir=base_dir,
            target_date=target_date,
            report_path=report_path,
            recover_report_path=recover_report_path,
            stn_ids_path=stn_ids_path,
            service_key=service_key,
            raw_path=raw_path,
            curated_path=curated_path,
            stn_ids=stn_ids,
            window_start=window_start,
            window_end=window_end,
            sleep_seconds=args.sleep_seconds,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            retry_backoff_max_seconds=args.retry_backoff_max_seconds,
            retry_wait_seconds=args.retry_wait_seconds,
        )
    except Exception:
        logging.exception("수집 실패")
        return 1

if __name__ == "__main__":
    sys.exit(main())


