"""
Radar composite image collection.

Examples:
  python -m scripts.collect_radar --date 2026-01-19 --dry-run
  python -m scripts.collect_radar --date 2026011912
  python -m scripts.collect_radar --date 2026-01-19 --overwrite
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.collect_asos import DEFAULT_RETRY_WAIT_SECONDS
from scripts.common.collection_schedule import resolve_collection_target
from scripts.common.config import get_env_value, load_project_env
from scripts.common.logging import configure_logging

DATE_FORMAT = "%Y%m%d"
SERVICE_KEY_ENV = "KMA_APIHUB_AUTH_KEY"
VERIFY_REPORT_FILENAME = "verify_radar_result.json"
DEFAULT_TIMEOUT_SECONDS = 300

BASE_URL = "https://apihub.kma.go.kr/api/typ04/url/rdr_cmp_file.php"
RADAR_DATA = "img"
RADAR_CMP = "cmi"


def _setup_logging(run_date: str) -> Path:
    return configure_logging("collect_radar", run_date)


def _encode_query(params: dict[str, Any]) -> str:
    return urlencode(params, doseq=True, safe=",%")


def _mask_service_key(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "****" + value[-2:]


def _build_request_url(tm: str, service_key: str) -> str:
    params = {
        "tm": tm,
        "data": RADAR_DATA,
        "cmp": RADAR_CMP,
        "authKey": service_key,
    }
    return f"{BASE_URL}?{_encode_query(params)}"


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
    return ".bin"


def _is_valid_image(payload: bytes, ext: str) -> bool:
    if not payload:
        return False
    if ext == "gif":
        return payload.startswith(b"GIF87a") or payload.startswith(b"GIF89a")
    if ext == "png":
        return payload.startswith(b"\x89PNG\r\n\x1a\n")
    if ext in ("jpg", "jpeg"):
        return payload.startswith(b"\xff\xd8\xff")
    return False


def _extension_from_payload(payload: bytes) -> str | None:
    if payload.startswith(b"GIF87a") or payload.startswith(b"GIF89a"):
        return ".gif"
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if payload.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    return None


def _build_filename(tm: str, extension: str) -> str:
    return f"rdr_cmp_{RADAR_CMP}_{tm}{extension}"


def _existing_file(dest_dir: Path, tm: str) -> Path | None:
    pattern = f"rdr_cmp_{RADAR_CMP}_{tm}.*"
    for path in dest_dir.glob(pattern):
        if path.is_file() and path.stat().st_size > 0:
            return path
    return None


def _clear_existing_files(dest_dir: Path, tm: str) -> None:
    pattern = f"rdr_cmp_{RADAR_CMP}_{tm}.*"
    for path in dest_dir.glob(pattern):
        if path.is_file():
            path.unlink()


def _download_one(
    tm: str,
    service_key: str,
    dest_dir: Path,
    overwrite: bool,
) -> bool:
    if overwrite:
        _clear_existing_files(dest_dir, tm)

    existing = _existing_file(dest_dir, tm)
    if existing:
        logging.info("already exists, skip: %s", existing)
        return True

    request_url = _build_request_url(tm, service_key)
    masked_url = _build_request_url(tm, _mask_service_key(service_key))
    request = Request(request_url, headers={"Accept": "*/*"})
    dest_dir.mkdir(parents=True, exist_ok=True)

    logging.info("download request: %s", masked_url)
    tmp_path: Path | None = None
    dest_path: Path | None = None
    try:
        with urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                logging.warning("download failed: status=%s url=%s", response.status, masked_url)
                return False
            content_type = response.headers.get("Content-Type")
            payload = response.read()
            extension = _extension_from_content_type(content_type)
            if extension == ".bin":
                detected_extension = _extension_from_payload(payload)
                if detected_extension is not None:
                    extension = detected_extension
            if not _is_valid_image(payload, extension.lstrip(".")):
                logging.warning("invalid image payload: content-type=%s url=%s", content_type, masked_url)
                return False
            dest_path = dest_dir / _build_filename(tm, extension)
            tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
            with tmp_path.open("wb") as file:
                file.write(payload)
    except HTTPError as exc:
        logging.warning("download HTTP error: %s (%s)", exc.code, masked_url)
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except URLError as exc:
        logging.warning("download URL error: %s (%s)", exc.reason, masked_url)
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except Exception as exc:
        logging.warning("download exception: %s (%s)", exc, masked_url)
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False

    if tmp_path is None or not tmp_path.exists() or tmp_path.stat().st_size <= 0:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logging.warning("download result empty: %s", masked_url)
        return False

    assert dest_path is not None
    tmp_path.replace(dest_path)
    return True


def _verify_item(dest_dir: Path, tm: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    existing = _existing_file(dest_dir, tm)
    missing: list[dict[str, str]] = []
    if existing is None:
        missing.append({"tm": tm, "cmp": RADAR_CMP, "data": RADAR_DATA})
    report = {
        "status": "PASS" if not missing else "FAIL",
        "expected_total": 1,
        "found_total": 0 if missing else 1,
        "missing_items": missing,
    }
    return report, missing


def _load_verify_report(report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        return {}
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_missing_tms(report: dict[str, Any]) -> list[str]:
    raw_items = report.get("missing_items", [])
    tms: list[str] = []
    if not isinstance(raw_items, list):
        return tms
    for entry in raw_items:
        if not isinstance(entry, dict):
            continue
        tm = str(entry.get("tm", "")).strip()
        if tm:
            tms.append(tm)
    return tms


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Radar composite image collection")
    parser.add_argument(
        "--date",
        required=True,
        help="collection target (YYYYMMDD or YYYYMMDDHH, HH in 00/12)",
    )
    parser.add_argument(
        "--phase",
        choices=("primary", "secondary"),
        default="primary",
        help="collection phase",
    )
    parser.add_argument("--dry-run", action="store_true", help="log only without downloading")
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing outputs")
    parser.add_argument(
        "--retry-wait-seconds",
        type=int,
        default=DEFAULT_RETRY_WAIT_SECONDS,
        help="wait before recovery retry",
    )
    parser.add_argument(
        "--verify-report-path",
        help="verify report JSON path (default: logs/YYYY-MM-DD/verify_radar_result.json)",
    )
    return parser


def _default_verify_report_path(base_dir: Path, target_date: date) -> Path:
    return base_dir / "logs" / target_date.strftime(DATE_FORMAT) / VERIFY_REPORT_FILENAME


def main() -> int:
    args = build_parser().parse_args()

    try:
        target = resolve_collection_target(args.date)
        log_path = _setup_logging(target.storage_date_text)
        logging.info(
            "radar collection start: input=%s storage_date=%s",
            args.date,
            target.storage_date_text,
        )
        logging.info("log_file: %s", log_path)

        base_dir = Path(__file__).resolve().parents[1]
        load_project_env(base_dir)
        service_key = get_env_value(SERVICE_KEY_ENV)
        if not service_key:
            raise ValueError(f"missing env value: {SERVICE_KEY_ENV}")

        target_date = target.storage_date
        target_tm = target.reference_utc.strftime("%Y%m%d%H%M")
        raw_dir = base_dir / "dain" / target_date.strftime(DATE_FORMAT) / "radar" / RADAR_CMP
        report_path = (
            Path(args.verify_report_path)
            if args.verify_report_path
            else _default_verify_report_path(base_dir, target_date)
        )
        recover_report_path = report_path.with_name("verify_radar_result_after_recover.json")

        logging.info("reference synoptic UTC: %s", target.reference_utc.strftime("%Y-%m-%d %H:%M UTC"))
        logging.info("selection mode: %s", target.mode)
        logging.info("radar target tm: %s", target_tm)
        logging.info("radar cmp=%s data=%s", RADAR_CMP, RADAR_DATA)

        if args.dry_run:
            logging.info("[DRY-RUN] request URL: %s", _build_request_url(target_tm, _mask_service_key(service_key)))
            logging.info("[DRY-RUN] save dir: %s", raw_dir)
            logging.info("[DRY-RUN] verify report path: %s", report_path)
            return 0

        targets = [target_tm]
        if args.phase == "secondary":
            report = _load_verify_report(report_path)
            targets = _extract_missing_tms(report)
            if not targets:
                logging.error("no missing_items found for secondary phase")
                return 1
            logging.info("wait before recovery: %ds", args.retry_wait_seconds)
            time.sleep(args.retry_wait_seconds)

        for tm in targets:
            ok = _download_one(
                tm=tm,
                service_key=service_key,
                dest_dir=raw_dir,
                overwrite=args.overwrite and args.phase == "primary",
            )
            if not ok:
                logging.warning("download failed for tm=%s", tm)

        report, missing = _verify_item(raw_dir, target_tm)
        _write_report(report_path if args.phase == "primary" else recover_report_path, report)
        if report.get("status") == "PASS":
            logging.info("verification PASS")
            return 0

        if args.phase == "secondary":
            logging.error("secondary verification FAIL")
            return 1

        if not missing:
            logging.error("verification failed without missing list")
            return 1

        logging.info("wait before recovery: %ds", args.retry_wait_seconds)
        time.sleep(args.retry_wait_seconds)
        for tm in _extract_missing_tms(report):
            ok = _download_one(
                tm=tm,
                service_key=service_key,
                dest_dir=raw_dir,
                overwrite=False,
            )
            if not ok:
                logging.warning("recovery download failed for tm=%s", tm)

        report_after, _ = _verify_item(raw_dir, target_tm)
        _write_report(recover_report_path, report_after)
        if report_after.get("status") == "PASS":
            logging.info("recovery complete")
            return 0

        logging.error("verification still FAIL after recovery")
        return 1
    except Exception:
        logging.exception("radar collection failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
