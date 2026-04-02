#!/usr/bin/env python
import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.logging import configure_logging

DATE_FORMAT = "%Y-%m-%d"
VERIFY_REPORT_FILENAME = "verify_satellite_result.json"
PRODUCTS_FILENAME = "satellite_le1b_products.txt"
AREAS_FILENAME = "satellite_areas.txt"
UTC_TIMES = ("0000", "0600", "1200", "1800")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GK2A LE1B 위성자료 검증")
    parser.add_argument("--date", required=True, help="기준일 (YYYY-MM-DD)")
    parser.add_argument(
        "--report-path",
        help="JSON 리포트 출력 경로 (기본: logs/YYYY-MM-DD/verify_satellite_result.json)",
    )
    return parser


def _resolve_report_path(args: argparse.Namespace, base_dir: Path, date_label: str) -> Path:
    if args.report_path:
        return Path(args.report_path)
    return base_dir / "logs" / date_label / VERIFY_REPORT_FILENAME


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_list_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"목록 파일 없음: {path}")
    items = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    items = [item for item in items if item]
    if not items:
        raise ValueError(f"목록이 비어 있음: {path}")
    return items


def _target_utc_date(run_date: str) -> str:
    value = datetime.strptime(run_date, DATE_FORMAT).date()
    return (value - timedelta(days=1)).strftime("%Y%m%d")


def _build_utc_times(target_date: str) -> list[str]:
    return [f"{target_date}{hhmm}" for hhmm in UTC_TIMES]


def _has_valid_file(dir_path: Path, product: str, area: str, t: str) -> bool:
    pattern = f"gk2a_le1b_{product}_{area}_{t}.*"
    for path in dir_path.glob(pattern):
        if path.is_file() and path.stat().st_size > 0:
            return True
    return False


def main() -> int:
    args = _build_parser().parse_args()
    log_path = configure_logging("verify_satellite", args.date)
    logging.info("위성 검증 시작")
    logging.info("로그 파일: %s", log_path)

    failures: list[str] = []
    missing_items: list[dict[str, str]] = []
    status = "FAIL"

    base_dir = BASE_DIR
    report_path = _resolve_report_path(args, base_dir, args.date)

    try:
        products = _read_list_file(base_dir / "config" / PRODUCTS_FILENAME)
        areas = _read_list_file(base_dir / "config" / AREAS_FILENAME)
    except Exception as exc:
        failures.append(f"목록 로딩 실패: {exc}")
        _write_report(
            report_path,
            {
                "status": status,
                "date": args.date,
                "missing_items": missing_items,
                "failures": failures,
            },
        )
        logging.error("실패: %s", exc)
        return 1

    target_date = _target_utc_date(args.date)
    target_times = _build_utc_times(target_date)

    raw_root = base_dir / "dain" / args.date / "satellite" / "LE1B"
    expected_total = len(products) * len(areas) * len(target_times)
    found_total = 0

    for product in products:
        for area in areas:
            dir_path = raw_root / product / area
            for t in target_times:
                if _has_valid_file(dir_path, product, area, t):
                    found_total += 1
                else:
                    missing_items.append(
                        {
                            "product": product,
                            "area": area,
                            "date": t,
                        }
                    )

    logging.info("검증 요약")
    logging.info("기준 날짜: %s", args.date)
    logging.info("UTC 대상 날짜: %s", target_date)
    logging.info("product 개수: %d", len(products))
    logging.info("area 개수: %d", len(areas))
    logging.info("대상 시간: %s", ", ".join(target_times))
    logging.info("예상 조합 수: %d", expected_total)
    logging.info("성공 조합 수: %d", found_total)
    logging.info("누락 조합 수: %d", len(missing_items))

    if not failures and not missing_items:
        status = "PASS"
        logging.info("검증 결과: 성공")
    else:
        logging.info("검증 결과: 실패")

    _write_report(
        report_path,
        {
            "status": status,
            "date": args.date,
            "utc_date": target_date,
            "expected_total": expected_total,
            "found_total": found_total,
            "missing_items": missing_items,
            "failures": failures,
        },
    )

    if failures or missing_items:
        for item in failures:
            logging.error("실패: %s", item)
        return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        logging.exception("위성 검증 실패")
        sys.exit(1)


