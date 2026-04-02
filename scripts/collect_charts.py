"""
날씨누리 일기도 직접 다운로드 수집.
"""

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.collect_asos import DEFAULT_RETRY_WAIT_SECONDS
from scripts.common.logging import configure_logging

DATE_FORMAT = "%Y-%m-%d"
CONFIG_FILENAME = "nuri_charts.txt"
VERIFY_REPORT_FILENAME = "verify_nuri_charts_result.json"
DEFAULT_TIMEOUT_SECONDS = 300


def _setup_logging(run_date: str) -> Path:
    return configure_logging("collect_charts", run_date)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, DATE_FORMAT).date()


def _load_config(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"설정 파일 없음: {path}")
    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) != 4:
            raise ValueError(f"설정 형식 오류: {line}")
        name, interval_text, ext, url_template = parts
        try:
            interval_hours = int(interval_text)
        except ValueError as exc:
            raise ValueError(f"간격 시간 오류: {interval_text}") from exc
        if interval_hours <= 0:
            raise ValueError(f"간격 시간 오류: {interval_text}")
        ext = ext.lstrip(".").lower()
        if "{yyyymmddHH}" not in url_template:
            raise ValueError(f"주소 템플릿 오류: {url_template}")
        items.append(
            {
                "name": name,
                "interval_hours": interval_hours,
                "ext": ext,
                "url_template": url_template,
            }
        )
    if not items:
        raise ValueError(f"설정 항목 없음: {path}")
    return items


def _build_time_tokens(target_date: date, interval_hours: int) -> list[str]:
    base_date = target_date - timedelta(days=1)
    base = base_date.strftime("%Y%m%d")
    tokens: list[str] = []

    if interval_hours == 1:
        previous_date = (base_date - timedelta(days=1)).strftime("%Y%m%d")
        tokens.append(f"{previous_date}23")
    elif interval_hours == 6:
        previous_date = (base_date - timedelta(days=1)).strftime("%Y%m%d")
        tokens.append(f"{previous_date}18")

    for hour in range(0, 24, interval_hours):
        tokens.append(f"{base}{hour:02d}")
    return tokens


def _build_expected_items(
    config_items: list[dict[str, Any]], target_date: date
) -> list[dict[str, Any]]:
    expected: list[dict[str, Any]] = []
    for entry in config_items:
        tokens = _build_time_tokens(target_date, entry["interval_hours"])
        for token in tokens:
            url = entry["url_template"].replace("{yyyymmddHH}", token)
            filename = f"{entry['name']}_{token}.{entry['ext']}"
            expected.append(
                {
                    "name": entry["name"],
                    "yyyymmddHH": token,
                    "ext": entry["ext"],
                    "url": url,
                    "filename": filename,
                }
            )
    return expected


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _download_url(url: str, dest_path: Path, timeout_seconds: int) -> bool:
    request = Request(url, headers={"Accept": "*/*"})
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    logging.info("다운로드 요청: 파일=%s, 주소=%s", dest_path.name, url)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                logging.warning("다운로드 실패: 코드=%s 주소=%s", response.status, url)
                return False
            content_type = response.headers.get("Content-Type", "")
            if not content_type.startswith("image/"):
                logging.warning("이미지 아님: content-type=%s 주소=%s", content_type, url)
                return False
            payload = response.read()
            if not _is_valid_image(payload, dest_path.suffix.lstrip(".").lower()):
                logging.warning("이미지 시그니처 오류: 파일=%s 주소=%s", dest_path.name, url)
                return False
            with tmp_path.open("wb") as file:
                file.write(payload)
    except HTTPError as exc:
        logging.warning("다운로드 응답코드 오류: %s (%s)", exc.code, url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except URLError as exc:
        logging.warning("다운로드 주소 오류: %s (%s)", exc.reason, url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except Exception as exc:
        logging.warning("다운로드 예외: %s (%s)", exc, url)
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


def _clear_directory_files(dir_path: Path) -> None:
    if not dir_path.exists():
        return
    for item in dir_path.iterdir():
        if item.is_file():
            item.unlink()


def _download_items(
    items: list[dict[str, Any]],
    dest_dir: Path,
    overwrite: bool,
    timeout_seconds: int,
) -> tuple[int, list[dict[str, Any]]]:
    downloaded = 0
    failures: list[dict[str, Any]] = []
    if overwrite:
        _clear_directory_files(dest_dir)
    for item in items:
        dest_path = dest_dir / item["filename"]
        if dest_path.exists() and dest_path.stat().st_size > 0 and not overwrite:
            continue
        ok = _download_url(item["url"], dest_path, timeout_seconds)
        if ok:
            downloaded += 1
            logging.info("다운로드 완료: 파일=%s, 크기=%d", dest_path.name, dest_path.stat().st_size)
        else:
            failures.append(item)
    return downloaded, failures


def _verify_items(
    dest_dir: Path, expected_items: list[dict[str, Any]]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    missing: list[dict[str, Any]] = []
    for item in expected_items:
        path = dest_dir / item["filename"]
        if not path.exists():
            missing.append(item)
            continue
        if path.stat().st_size <= 0:
            missing.append(item)
    expected_total = len(expected_items)
    found_total = expected_total - len(missing)
    status = "PASS" if not missing else "FAIL"
    report = {
        "status": status,
        "expected_total": expected_total,
        "found_total": found_total,
        "missing_items": [
            {
                "name": item["name"],
                "yyyymmddHH": item["yyyymmddHH"],
                "filename": item["filename"],
                "url": item["url"],
            }
            for item in missing
        ],
    }
    return report, missing


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
        name = str(entry.get("name", "")).strip()
        token = str(entry.get("yyyymmddHH", "")).strip()
        if name and token:
            items.append({"name": name, "yyyymmddHH": token})
    return items


def _build_items_from_missing(
    config_items: list[dict[str, Any]], missing: list[dict[str, str]]
) -> list[dict[str, Any]]:
    config_map = {entry["name"]: entry for entry in config_items}
    items: list[dict[str, Any]] = []
    for entry in missing:
        name = entry["name"]
        token = entry["yyyymmddHH"]
        if name not in config_map:
            logging.warning("설정 누락: %s", name)
            continue
        config = config_map[name]
        url = config["url_template"].replace("{yyyymmddHH}", token)
        filename = f"{name}_{token}.{config['ext']}"
        items.append(
            {
                "name": name,
                "yyyymmddHH": token,
                "ext": config["ext"],
                "url": url,
                "filename": filename,
            }
        )
    return items


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="날씨누리 일기도 수집")
    parser.add_argument("--date", required=True, help="기준일(YYYY-MM-DD)")
    parser.add_argument(
        "--phase",
        choices=("primary", "secondary"),
        default="primary",
        help="수집 단계(1차/복구)",
    )
    parser.add_argument(
        "--retry-wait-seconds",
        type=int,
        default=DEFAULT_RETRY_WAIT_SECONDS,
        help="복구 전 대기 시간(초)",
    )
    parser.add_argument("--overwrite", action="store_true", help="기존 파일 덮어쓰기")
    parser.add_argument(
        "--verify-report-path",
        help="검증 결과 제이슨 경로 (기본: logs/YYYY-MM-DD/verify_nuri_charts_result.json)",
    )
    return parser


def _default_verify_report_path(base_dir: Path, target_date: date) -> Path:
    return base_dir / "logs" / target_date.strftime(DATE_FORMAT) / VERIFY_REPORT_FILENAME


def main() -> int:
    args = build_parser().parse_args()
    log_path = _setup_logging(args.date)
    logging.info("날씨누리 수집 시작: date=%s", args.date)
    logging.info("로그 파일: %s", log_path)

    try:
        target_date = _parse_date(args.date)
        base_dir = Path(__file__).resolve().parents[1]

        config_items = _load_config(base_dir / "config" / CONFIG_FILENAME)
        expected_items = _build_expected_items(config_items, target_date)

        raw_dir = (
            base_dir
            / "dain"
            / target_date.strftime(DATE_FORMAT)

            / "charts"
            / "nuri"
        )

        report_path = (
            Path(args.verify_report_path)
            if args.verify_report_path
            else _default_verify_report_path(base_dir, target_date)
        )
        recover_report_path = report_path.with_name(
            "verify_nuri_charts_result_after_recover.json"
        )

        if args.phase == "secondary":
            report = _load_verify_report(report_path)
            missing_items = _extract_missing_items(report)
            if not missing_items:
                logging.error("누락 목록 없음: 복구 대상 없음")
                return 1
            logging.info("복구 대기: %d초", args.retry_wait_seconds)
            time.sleep(args.retry_wait_seconds)
            retry_items = _build_items_from_missing(config_items, missing_items)
            _download_items(retry_items, raw_dir, overwrite=False, timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
            report_after, _ = _verify_items(raw_dir, expected_items)
            _write_report(recover_report_path, report_after)
            if report_after.get("status") == "PASS":
                logging.info("복구 완료")
                return 0
            logging.error("복구 후에도 검증 실패")
            return 1

        downloaded, failures = _download_items(
            expected_items,
            raw_dir,
            overwrite=args.overwrite,
            timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        )
        logging.info("다운로드 완료: %d", downloaded)
        if failures:
            logging.warning("다운로드 실패: %d건", len(failures))

        report, missing = _verify_items(raw_dir, expected_items)
        _write_report(report_path, report)
        if report.get("status") == "PASS":
            logging.info("1차 검증 성공")
            return 0

        if not missing:
            logging.error("누락 목록 없음: 복구 대상 없음")
            return 1

        logging.info("복구 대기: %d초", args.retry_wait_seconds)
        time.sleep(args.retry_wait_seconds)
        _download_items(missing, raw_dir, overwrite=False, timeout_seconds=DEFAULT_TIMEOUT_SECONDS)
        report_after, _ = _verify_items(raw_dir, expected_items)
        _write_report(recover_report_path, report_after)
        if report_after.get("status") == "PASS":
            logging.info("복구 완료")
            return 0
        logging.error("복구 후에도 검증 실패")
        return 1
    except Exception:
        logging.exception("날씨누리 수집 실패")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


