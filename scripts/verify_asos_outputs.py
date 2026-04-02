#!/usr/bin/env python
import argparse
import csv
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.logging import configure_logging

RAW_FILENAME = "asos_hourly.csv"
CURATED_FILENAME = "asos_daily_summary.csv"
DATE_FORMAT = "%Y-%m-%d"

STATION_CANDIDATES = [
    "stnId",
    "stnid",
    "stn_id",
    "stn",
    "station",
    "station_id",
]

TIME_CANDIDATES = [
    "tm",
    "time",
    "datetime",
    "dt",
    "date_time",
    "obs_time",
]


def _normalize_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _select_column(headers: list[str], candidates: list[str]) -> tuple[str | None, list[str]]:
    normalized_headers = {_normalize_name(h): h for h in headers}
    matches: list[str] = []
    for candidate in candidates:
        key = _normalize_name(candidate)
        if key in normalized_headers:
            matches.append(normalized_headers[key])
    if not matches:
        return None, []
    chosen = matches[0]
    return chosen, matches


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames or []
        rows = [row for row in reader]
    return headers, rows


def _read_station_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"관측소 파일 없음: {path}")
    stations: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value:
            stations.append(value)
    if not stations:
        raise ValueError(f"관측소 파일이 비어 있음: {path}")
    return stations


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ASOS 산출물 검증")
    parser.add_argument("--date", help="기준일 (YYYY-MM-DD)")
    parser.add_argument("--raw", help="raw CSV 경로")
    parser.add_argument("--curated", help="curated CSV 경로")
    parser.add_argument("--expected-stations", type=int, help="예상 관측소 개수")
    parser.add_argument(
        "--report-path",
        help="JSON 리포트 출력 경로 (기본: logs/YYYY-MM-DD/verify_result.json)",
    )
    parser.add_argument("--stations-file", help="관측소 ID 목록 파일")
    parser.add_argument("--spotcheck-count", type=int, default=3, help="스팟체크 관측소 수")
    return parser


def _resolve_paths(args: argparse.Namespace, base_dir: Path) -> tuple[Path, Path, str | None]:
    if args.raw or args.curated:
        if not args.raw or not args.curated:
            raise ValueError("raw/curated는 함께 제공되어야 합니다")
        return Path(args.raw), Path(args.curated), None
    if not args.date:
        raise ValueError("date 또는 raw/curated 경로가 필요합니다")
    raw = base_dir / "dain" / args.date / "asos" / RAW_FILENAME
    curated = base_dir / "dain" / args.date / "curated" / "asos" / CURATED_FILENAME
    return raw, curated, args.date


def _format_bool(value: bool) -> str:
    return "성공" if value else "실패"


def _build_expected_times(date_str: str) -> list[str]:
    target_date = datetime.strptime(date_str, DATE_FORMAT).date()
    start_dt = datetime.combine(target_date - timedelta(days=1), datetime.min.time())
    start_dt = start_dt.replace(hour=8)
    return [
        (start_dt + timedelta(hours=offset)).strftime("%Y%m%d%H%M")
        for offset in range(24)
    ]


def _resolve_report_path(
    args: argparse.Namespace,
    base_dir: Path,
    date_label: str | None,
) -> Path:
    if args.report_path:
        return Path(args.report_path)
    if date_label:
        return base_dir / "logs" / date_label / "verify_result.json"
    return base_dir / "logs" / "verify_result.json"


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    args = _build_parser().parse_args()
    log_path = configure_logging("verify", args.date)
    logging.info("검증 시작")
    logging.info("로그 파일: %s", log_path)
    base_dir = BASE_DIR

    failures: list[str] = []
    missing_keys: list[list[str]] = []
    duplicate_keys: list[list[str]] = []
    status = "FAIL"
    date_value = args.date or ""

    try:
        raw_path, curated_path, date_label = _resolve_paths(args, base_dir)
    except ValueError as exc:
        failures.append(f"경로 해석 오류: {exc}")
        report_path = _resolve_report_path(args, base_dir, args.date)
        _write_report(
            report_path,
            {
                "status": status,
                "date": date_value,
                "missing_keys": missing_keys,
                "duplicate_keys": duplicate_keys,
                "failures": failures,
            },
        )
        logging.error("실패: 경로 해석 오류: %s", exc)
        return 1

    if date_label:
        date_value = date_label
    report_path = _resolve_report_path(args, base_dir, date_value or None)

    if not raw_path.exists():
        failures.append(f"raw 파일 없음: {raw_path}")
    if not curated_path.exists():
        failures.append(f"curated 파일 없음: {curated_path}")
    if failures:
        _write_report(
            report_path,
            {
                "status": status,
                "date": date_value,
                "missing_keys": missing_keys,
                "duplicate_keys": duplicate_keys,
                "failures": failures,
            },
        )
        for item in failures:
            logging.error("실패: %s", item)
        return 1

    if raw_path.stat().st_size <= 0:
        failures.append("raw 파일 크기 0 bytes")
    if curated_path.stat().st_size <= 0:
        failures.append("curated 파일 크기 0 bytes")

    stations_from_file: list[str] = []
    expected_stations = args.expected_stations
    if args.stations_file:
        stations_from_file = _read_station_file(Path(args.stations_file))
        if expected_stations is None:
            expected_stations = len(stations_from_file)
    if expected_stations is None:
        failures.append("expected-stations 누락")

    raw_headers, raw_rows = _read_csv(raw_path)
    curated_headers, curated_rows = _read_csv(curated_path)

    raw_station_col, raw_station_matches = _select_column(raw_headers, STATION_CANDIDATES)
    raw_time_col, raw_time_matches = _select_column(raw_headers, TIME_CANDIDATES)
    curated_station_col, curated_station_matches = _select_column(
        curated_headers, STATION_CANDIDATES
    )

    if raw_station_col is None:
        failures.append("raw 관측소 컬럼 없음")
    if raw_time_col is None:
        failures.append("raw 시간 컬럼 없음")
    if curated_station_col is None:
        failures.append("curated 관측소 컬럼 없음")

    logging.info("컬럼 선택")
    logging.info(
        "raw 관측소 컬럼: %s (매칭: %s)", raw_station_col, raw_station_matches
    )
    logging.info("raw 시간 컬럼: %s (매칭: %s)", raw_time_col, raw_time_matches)
    logging.info(
        "curated 관측소 컬럼: %s (매칭: %s)",
        curated_station_col,
        curated_station_matches,
    )

    if failures:
        _write_report(
            report_path,
            {
                "status": status,
                "date": date_value,
                "missing_keys": missing_keys,
                "duplicate_keys": duplicate_keys,
                "failures": failures,
            },
        )
        for item in failures:
            logging.error("실패: %s", item)
        return 1

    raw_station_values: list[str] = []
    combo_counter: Counter[tuple[str, str]] = Counter()
    missing_station = 0
    missing_time = 0

    for row in raw_rows:
        stn = (row.get(raw_station_col) or "").strip()
        tm = (row.get(raw_time_col) or "").strip()
        if not stn:
            missing_station += 1
            continue
        if not tm:
            missing_time += 1
            continue
        raw_station_values.append(stn)
        combo_counter[(stn, tm)] += 1

    raw_station_unique = len(set(raw_station_values))
    combo_unique = len(combo_counter)
    combo_max = max(combo_counter.values(), default=0)

    if missing_station > 0:
        failures.append(f"raw 관측소 값 누락: {missing_station}")
    if missing_time > 0:
        failures.append(f"raw 시간 값 누락: {missing_time}")

    if expected_stations is not None and raw_station_unique != expected_stations:
        failures.append(
            f"raw 관측소 고유 수 불일치: {raw_station_unique} (기대 {expected_stations})"
        )

    if combo_max > 1:
        duplicate_keys = [list(key) for key, count in combo_counter.items() if count > 1]

    curated_station_values = [
        (row.get(curated_station_col) or "").strip() for row in curated_rows
    ]
    curated_station_unique = len({v for v in curated_station_values if v})
    if expected_stations is not None and curated_station_unique != expected_stations:
        failures.append(
            f"curated 관측소 고유 수 불일치: {curated_station_unique} (기대 {expected_stations})"
        )

    curated_by_station: dict[str, dict[str, str]] = {}
    for row in curated_rows:
        stn = (row.get(curated_station_col) or "").strip()
        if not stn:
            continue
        curated_by_station[stn] = row

    mean_columns = [col for col in curated_headers if col.startswith("mean_")]
    spotcheck_count = max(1, min(args.spotcheck_count, 3))

    if stations_from_file:
        station_pool = stations_from_file
    else:
        station_pool = sorted(set(raw_station_values))

    spotcheck_targets = station_pool[:spotcheck_count]
    spotcheck_failures: list[str] = []
    spotcheck_total = 0

    sums: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    if mean_columns:
        for row in raw_rows:
            stn = (row.get(raw_station_col) or "").strip()
            if stn not in spotcheck_targets:
                continue
            for mean_col in mean_columns:
                raw_key = mean_col.replace("mean_", "", 1)
                value = _to_float(row.get(raw_key))
                if value is None:
                    continue
                sums[stn][raw_key] += value
                counts[stn][raw_key] += 1

        for stn in spotcheck_targets:
            curated_row = curated_by_station.get(stn)
            if curated_row is None:
                spotcheck_failures.append(f"curated 관측소 없음: {stn}")
                continue
            for mean_col in mean_columns:
                raw_key = mean_col.replace("mean_", "", 1)
                curated_value = _to_float(curated_row.get(mean_col))
                count = counts[stn].get(raw_key, 0)
                raw_mean = None if count == 0 else sums[stn][raw_key] / count
                spotcheck_total += 1
                if curated_value is None and raw_mean is None:
                    continue
                if curated_value is None or raw_mean is None:
                    spotcheck_failures.append(
                        f"스팟체크 불일치 {stn} {mean_col} (curated={curated_value}, raw_mean={raw_mean})"
                    )
                    continue
                if abs(curated_value - raw_mean) > 0.0015:
                    spotcheck_failures.append(
                        f"스팟체크 불일치 {stn} {mean_col} (curated={curated_value:.3f}, raw_mean={raw_mean:.3f})"
                    )
    else:
        failures.append("curated mean_ 컬럼 없음")

    if spotcheck_failures:
        failures.extend(spotcheck_failures)

    if date_value:
        expected_times = _build_expected_times(date_value)
    else:
        expected_times = []
        failures.append("기대 키 생성을 위해 date 필요")

    if station_pool and expected_times:
        expected_keys = {(stn, tm) for stn in station_pool for tm in expected_times}
        observed_keys = set(combo_counter.keys())
        missing_keys = [list(key) for key in sorted(expected_keys - observed_keys)]
    elif not station_pool:
        failures.append("관측소 목록이 비어 있음")

    if missing_keys:
        failures.append(f"누락 키: {len(missing_keys)}")

    logging.info("검증 요약")
    logging.info("기준 날짜: %s", date_label or "사용자 지정 경로")
    logging.info("raw 경로: %s", raw_path)
    logging.info("curated 경로: %s", curated_path)
    logging.info("예상 관측소 수: %s", expected_stations)
    logging.info("raw 행 수(헤더 제외): %d", len(raw_rows))
    logging.info("raw 관측소 고유 수: %d", raw_station_unique)
    logging.info("관측소/시간 조합 수: %d", combo_unique)
    logging.info("조합 최대 중복 수: %d", combo_max)
    logging.info("curated 관측소 고유 수: %d", curated_station_unique)
    logging.info("스팟체크 관측소: %s", spotcheck_targets)
    logging.info("스팟체크 비교 수: %d", spotcheck_total)
    logging.info("스팟체크 결과: %s", _format_bool(not spotcheck_failures))

    if failures:
        logging.info("검증 결과: 실패")
        status = "FAIL"
    else:
        status = "PASS"
        logging.info("검증 결과: 성공")

    _write_report(
        report_path,
        {
            "status": status,
            "date": date_value,
            "missing_keys": missing_keys,
            "duplicate_keys": duplicate_keys,
            "failures": failures,
        },
    )

    if failures:
        for item in failures:
            logging.error("실패: %s", item)
        return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logging.exception("검증 실패")
        sys.exit(1)


