"""
GK2A LE1B 위성자료 수집.

실행 예시:
  python -m scripts.collect_satellite --date 2026-01-19 --dry-run
  python -m scripts.collect_satellite --date 2026-01-19
  python -m scripts.collect_satellite --date 2026-01-19 --overwrite
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.collect_asos import DEFAULT_RETRY_WAIT_SECONDS
from scripts.common.collection_schedule import resolve_collection_target
from scripts.common.config import get_env_value, load_project_env
from scripts.common.logging import configure_logging

DATE_FORMAT = "%Y%m%d"
SERVICE_KEY_ENV = "KMA_APIHUB_AUTH_KEY"
VERIFY_REPORT_FILENAME = "verify_satellite_result.json"

BASE_URL = "https://apihub.kma.go.kr/api/typ05/api/GK2A/LE1B"

PRODUCTS_FILENAME = "satellite_le1b_products.txt"
AREAS_FILENAME = "satellite_areas.txt"

DEFAULT_TIMEOUT_SECONDS = 600


def _setup_logging(run_date: str) -> Path:
    return configure_logging("collect_satellite", run_date)


def _encode_query(params: dict[str, Any]) -> str:
    return urlencode(params, doseq=True, safe=",%")


def _mask_service_key(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "****" + value[-2:]


def _mask_params(params: dict[str, Any]) -> dict[str, Any]:
    masked = dict(params)
    masked["authKey"] = _mask_service_key(str(params.get("authKey", "")))
    return masked


def _read_list_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"목록 파일 없음: {path}")
    items = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    items = [item for item in items if item]
    if not items:
        raise ValueError(f"목록이 비어 있음: {path}")
    return items


def _request_json(url: str, timeout_seconds: int) -> dict[str, Any] | None:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read().decode("utf-8")
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _write_meta_json(path: Path, payload: dict[str, Any] | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if payload is None:
        path.write_text("{}", encoding="utf-8")
    else:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _extract_date_tokens(value: str) -> Iterable[str]:
    for match in re.findall(r"\b\d{12}\b", value):
        yield match


def _collect_dates_from_json(payload: Any) -> set[str]:
    dates: set[str] = set()
    if isinstance(payload, dict):
        for value in payload.values():
            dates.update(_collect_dates_from_json(value))
    elif isinstance(payload, list):
        for value in payload:
            dates.update(_collect_dates_from_json(value))
    elif isinstance(payload, str):
        for token in _extract_date_tokens(payload):
            dates.add(token)
    return dates


def _build_imagelist_url(product: str, area: str, t: str, service_key: str) -> str:
    params = {
        "sDate": t,
        "eDate": t,
        "format": "json",
        "authKey": service_key,
    }
    return f"{BASE_URL}/{product}/{area}/imageList?{_encode_query(params)}"


def _build_image_url(product: str, area: str, t: str, service_key: str) -> str:
    params = {
        "date": t,
        "authKey": service_key,
    }
    return f"{BASE_URL}/{product}/{area}/image?{_encode_query(params)}"


def _extension_from_content_type(content_type: str | None) -> str:
    if not content_type:
        return ".bin"
    main_type = content_type.split(";", 1)[0].strip().lower()
    if main_type == "image/png":
        return ".png"
    if main_type in ("image/jpeg", "image/jpg"):
        return ".jpg"
    if main_type == "image/gif":
        return ".gif"
    if "netcdf" in main_type:
        return ".nc"
    return ".bin"


def _build_filename(product: str, area: str, t: str, extension: str) -> str:
    return f"gk2a_le1b_{product}_{area}_{t}{extension}"


def _existing_file(dest_dir: Path, product: str, area: str, t: str) -> Path | None:
    pattern = f"gk2a_le1b_{product}_{area}_{t}.*"
    for path in dest_dir.glob(pattern):
        if path.is_file() and path.stat().st_size > 0:
            return path
    return None


def _clear_existing_files(dest_dir: Path, product: str, area: str, t: str) -> None:
    pattern = f"gk2a_le1b_{product}_{area}_{t}.*"
    for path in dest_dir.glob(pattern):
        if path.is_file():
            path.unlink()


def _download_image(url: str, dest_path: Path, timeout_seconds: int) -> bool:
    request = Request(url, headers={"Accept": "*/*"})
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                logging.warning("다운로드 실패: status=%s url=%s", response.status, url)
                return False
            with tmp_path.open("wb") as file:
                file.write(response.read())
    except Exception as exc:
        logging.warning("다운로드 예외: %s (%s)", url, exc)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False

    if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logging.warning("다운로드 결과 0 bytes: %s", url)
        return False

    tmp_path.replace(dest_path)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GK2A LE1B 위성자료 수집")
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
        "--retry-wait-seconds",
        type=int,
        default=DEFAULT_RETRY_WAIT_SECONDS,
        help="복구 전 대기 시간(초)",
    )
    parser.add_argument(
        "--verify-report-path",
        help="검증 결과 JSON 경로 (기본: logs/YYYY-MM-DD/verify_satellite_result.json)",
    )
    return parser


def _default_verify_report_path(base_dir: Path, target_date: date) -> Path:
    return base_dir / "logs" / target_date.strftime(DATE_FORMAT) / VERIFY_REPORT_FILENAME


def _run_verify(
    base_dir: Path,
    target_date: date,
    report_path: Path,
    cycle: str | None = None,
) -> int:
    env = dict(os.environ)
    cmd = [
        sys.executable,
        str(base_dir / "scripts" / "verify_satellite_outputs.py"),
        "--date",
        target_date.strftime(DATE_FORMAT),
        "--report-path",
        str(report_path),
    ]
    if cycle:
        cmd.extend(["--cycle", cycle])
    result = subprocess.run(cmd, cwd=base_dir, env=env)
    return result.returncode


def _load_verify_report(report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        return {}
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _extract_missing_items(report: dict[str, Any]) -> list[dict[str, str]]:
    raw_items = report.get("missing_items", [])
    items: list[dict[str, str]] = []
    if not isinstance(raw_items, list):
        return items
    for entry in raw_items:
        if not isinstance(entry, dict):
            continue
        product = str(entry.get("product", "")).strip()
        area = str(entry.get("area", "")).strip()
        date_value = str(entry.get("date", "")).strip()
        if product and area and date_value:
            items.append({"product": product, "area": area, "date": date_value})
    return items


def _download_one(
    product: str,
    area: str,
    t: str,
    service_key: str,
    dest_dir: Path,
    meta_dir: Path,
    overwrite: bool,
    phase: str,
) -> bool:
    if overwrite and phase == "primary":
        _clear_existing_files(dest_dir, product, area, t)

    existing = _existing_file(dest_dir, product, area, t)
    if existing:
        logging.info("이미 존재하는 파일 스킵: %s", existing)
        return True

    image_list_url = _build_imagelist_url(product, area, t, service_key)
    masked_url = _build_imagelist_url(product, area, t, _mask_service_key(service_key))
    logging.info("요청 주소: %s", masked_url)
    try:
        payload = _request_json(image_list_url, timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
    except HTTPError as exc:
        logging.warning("imageList HTTP 오류: %s (%s)", exc.code, masked_url)
        payload = None
    except URLError as exc:
        logging.warning("imageList URL 오류: %s (%s)", exc.reason, masked_url)
        payload = None
    except Exception as exc:
        logging.warning("imageList 예외: %s (%s)", exc, masked_url)
        payload = None

    meta_path = meta_dir / f"le1b_{product}_{area}_{t}_{phase}.json"
    _write_meta_json(meta_path, payload)

    if payload is None:
        return False

    dates = _collect_dates_from_json(payload)
    if t not in dates:
        logging.warning("imageList에 대상 시간 없음: product=%s, area=%s, date=%s", product, area, t)
        return False

    image_url = _build_image_url(product, area, t, service_key)
    masked_image_url = _build_image_url(product, area, t, _mask_service_key(service_key))
    logging.info("다운로드 주소: %s", masked_image_url)
    request = Request(image_url, headers={"Accept": "*/*"})
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        with urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
            content_type = response.headers.get("Content-Type")
            extension = _extension_from_content_type(content_type)
            filename = _build_filename(product, area, t, extension)
            dest_path = dest_dir / filename
            tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
            with tmp_path.open("wb") as file:
                file.write(response.read())
    except Exception as exc:
        logging.warning("image 다운로드 실패: %s (%s)", image_url, exc)
        return False

    if tmp_path.exists() and tmp_path.stat().st_size > 0:
        tmp_path.replace(dest_path)
        return True
    if tmp_path.exists():
        tmp_path.unlink(missing_ok=True)
    logging.warning("다운로드 결과 0 bytes: %s", image_url)
    return False


def main() -> int:
    args = build_parser().parse_args()

    try:
        target = resolve_collection_target(args.date)
        log_path = _setup_logging(target.storage_date_text)
        logging.info("위성 수집 시작: input=%s storage_date=%s", args.date, target.storage_date_text)
        logging.info("로그 파일: %s", log_path)

        run_date = target.storage_date
        reference_utc = target.reference_utc
        target_times = [reference_utc.strftime("%Y%m%d%H%M")]

        base_dir = Path(__file__).resolve().parents[1]
        load_project_env(base_dir)
        service_key = get_env_value(SERVICE_KEY_ENV)
        if not service_key:
            raise ValueError(f"환경변수 없음: {SERVICE_KEY_ENV}")

        products = _read_list_file(base_dir / "config" / PRODUCTS_FILENAME)
        areas = _read_list_file(base_dir / "config" / AREAS_FILENAME)

        raw_root = base_dir / "dain" / run_date.strftime(DATE_FORMAT) / "satellite"
        meta_dir = raw_root / "_meta"
        report_path = (
            Path(args.verify_report_path)
            if args.verify_report_path
            else _default_verify_report_path(base_dir, run_date)
        )
        recover_report_path = report_path.with_name(
            "verify_satellite_result_after_recover.json"
        )

        if args.dry_run:
            logging.info(
                "[DRY-RUN] 기준 synoptic UTC: %s",
                reference_utc.strftime("%Y-%m-%d %H:%M UTC"),
            )
            logging.info("[DRY-RUN] 기준 선택 방식: %s", target.mode)
            logging.info("[DRY-RUN] 대상 시간: %s", ", ".join(target_times))
            logging.info("[DRY-RUN] product 개수: %d", len(products))
            logging.info("[DRY-RUN] area 개수: %d", len(areas))
            logging.info("[DRY-RUN] meta 저장 경로: %s", meta_dir)
            return 0

        logging.info(
            "기준 synoptic UTC 선택: %s",
            reference_utc.strftime("%Y-%m-%d %H:%M UTC"),
        )
        logging.info("기준 선택 방식: %s", target.mode)

        targets: list[dict[str, str]] = []
        if args.phase == "secondary":
            report = _load_verify_report(report_path)
            missing_items = _extract_missing_items(report)
            if not missing_items:
                logging.error("missing_items 없음: 복구 대상이 비어 있음")
                return 1
            logging.info("복구 대기: %d초", args.retry_wait_seconds)
            time.sleep(args.retry_wait_seconds)
            targets = missing_items
        else:
            for product in products:
                for area in areas:
                    for t in target_times:
                        targets.append({"product": product, "area": area, "date": t})

        failures: list[dict[str, str]] = []
        for item in targets:
            product = item["product"]
            area = item["area"]
            t = item["date"]
            dest_dir = raw_root / "LE1B" / product / area
            ok = _download_one(
                product=product,
                area=area,
                t=t,
                service_key=service_key,
                dest_dir=dest_dir,
                meta_dir=meta_dir,
                overwrite=args.overwrite,
                phase=args.phase,
            )
            if not ok:
                failures.append(item)

        logging.info("수집 실패 건수: %d", len(failures))

        report_path.parent.mkdir(parents=True, exist_ok=True)
        _run_verify(base_dir, run_date, report_path, cycle=target_times[0])
        report = _load_verify_report(report_path)
        if report.get("status") == "PASS":
            logging.info("primary 검증 PASS")
            return 0

        if args.phase == "secondary":
            logging.error("secondary 복구 검증 FAIL")
            return 1

        missing_items = _extract_missing_items(report)
        if not missing_items:
            logging.error("missing_items 없음: 복구 대상이 비어 있음")
            return 1

        logging.info("복구 대기: %d초", args.retry_wait_seconds)
        time.sleep(args.retry_wait_seconds)
        for item in missing_items:
            product = item["product"]
            area = item["area"]
            t = item["date"]
            dest_dir = raw_root / "LE1B" / product / area
            ok = _download_one(
                product=product,
                area=area,
                t=t,
                service_key=service_key,
                dest_dir=dest_dir,
                meta_dir=meta_dir,
                overwrite=False,
                phase="secondary",
            )
            if not ok:
                failures.append(item)

        _run_verify(base_dir, run_date, recover_report_path, cycle=target_times[0])
        report_after = _load_verify_report(recover_report_path)
        if report_after.get("status") == "PASS":
            logging.info("복구 완료")
            return 0

        logging.error("복구 후에도 검증 FAIL")
        return 1
    except Exception:
        logging.exception("위성 수집 실패")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


