"""
Download Weather Nuri chart images directly.
"""

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.collect_asos import DEFAULT_RETRY_WAIT_SECONDS
from scripts.common.collection_schedule import resolve_collection_target
from scripts.common.logging import configure_logging

DATE_FORMAT = "%Y%m%d"
CONFIG_FILENAME = "nuri_charts.txt"
VERIFY_REPORT_FILENAME = "verify_nuri_charts_result.json"
DEFAULT_TIMEOUT_SECONDS = 300


def _setup_logging(run_date: str) -> Path:
    return configure_logging("collect_charts", run_date)


def _parse_required_flag(value: str) -> bool:
    token = value.strip().lower()
    if token in {"required", "true", "yes", "y", "1"}:
        return True
    if token in {"optional", "false", "no", "n", "0"}:
        return False
    raise ValueError(f"invalid required flag: {value}")


def _load_config(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")

    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        parts = [part.strip() for part in stripped.split("|")]
        if len(parts) not in (4, 5):
            raise ValueError(f"invalid config row: {line}")

        name, interval_text, ext, url_template = parts[:4]
        required = _parse_required_flag(parts[4] if len(parts) == 5 else "required")

        try:
            interval_hours = int(interval_text)
        except ValueError as exc:
            raise ValueError(f"invalid interval_hours: {interval_text}") from exc
        if interval_hours <= 0:
            raise ValueError(f"invalid interval_hours: {interval_text}")

        ext = ext.lstrip(".").lower()
        if "{yyyymmddHH}" not in url_template:
            raise ValueError(f"invalid url template: {url_template}")

        items.append(
            {
                "name": name,
                "interval_hours": interval_hours,
                "ext": ext,
                "url_template": url_template,
                "required": required,
            }
        )

    if not items:
        raise ValueError(f"no config rows found: {path}")
    return items


def _build_expected_items(
    config_items: list[dict[str, Any]], reference_token: str
) -> list[dict[str, Any]]:
    expected: list[dict[str, Any]] = []
    for entry in config_items:
        url = entry["url_template"].replace("{yyyymmddHH}", reference_token)
        filename = f"{entry['name']}_{reference_token}.{entry['ext']}"
        expected.append(
            {
                "name": entry["name"],
                "yyyymmddHH": reference_token,
                "ext": entry["ext"],
                "url": url,
                "filename": filename,
                "required": bool(entry.get("required", True)),
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
    logging.info("download request: file=%s url=%s", dest_path.name, url)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                logging.warning("download failed: status=%s url=%s", response.status, url)
                return False
            content_type = response.headers.get("Content-Type", "")
            if not content_type.startswith("image/"):
                logging.warning("non-image response: content-type=%s url=%s", content_type, url)
                return False
            payload = response.read()
            if not _is_valid_image(payload, dest_path.suffix.lstrip(".").lower()):
                logging.warning("invalid image signature: file=%s url=%s", dest_path.name, url)
                return False
            with tmp_path.open("wb") as file:
                file.write(payload)
    except HTTPError as exc:
        logging.warning("download http error: code=%s url=%s", exc.code, url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except URLError as exc:
        logging.warning("download url error: reason=%s url=%s", exc.reason, url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False
    except Exception as exc:
        logging.warning("download exception: %s (%s)", exc, url)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False

    if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        logging.warning("download result is empty: %s", url)
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
            logging.info("download complete: file=%s size=%d", dest_path.name, dest_path.stat().st_size)
        else:
            failures.append(item)
    return downloaded, failures


def _verify_items(
    dest_dir: Path, expected_items: list[dict[str, Any]]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    missing_required: list[dict[str, Any]] = []
    missing_optional: list[dict[str, Any]] = []

    for item in expected_items:
        path = dest_dir / item["filename"]
        is_missing = not path.exists() or path.stat().st_size <= 0
        if not is_missing:
            continue
        if item.get("required", True):
            missing_required.append(item)
        else:
            missing_optional.append(item)

    expected_total = len(expected_items)
    required_total = sum(1 for item in expected_items if item.get("required", True))
    optional_total = expected_total - required_total
    found_total = expected_total - len(missing_required) - len(missing_optional)
    status = "PASS" if not missing_required else "FAIL"

    report = {
        "status": status,
        "expected_total": expected_total,
        "found_total": found_total,
        "expected_required_total": required_total,
        "found_required_total": required_total - len(missing_required),
        "expected_optional_total": optional_total,
        "found_optional_total": optional_total - len(missing_optional),
        "missing_items": [
            {
                "name": item["name"],
                "yyyymmddHH": item["yyyymmddHH"],
                "filename": item["filename"],
                "url": item["url"],
            }
            for item in missing_required
        ],
        "optional_missing_items": [
            {
                "name": item["name"],
                "yyyymmddHH": item["yyyymmddHH"],
                "filename": item["filename"],
                "url": item["url"],
            }
            for item in missing_optional
        ],
    }
    return report, missing_required


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
            logging.warning("missing config entry for retry: %s", name)
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
                "required": bool(config.get("required", True)),
            }
        )
    return items


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect Weather Nuri charts")
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
    parser.add_argument(
        "--retry-wait-seconds",
        type=int,
        default=DEFAULT_RETRY_WAIT_SECONDS,
        help="wait time before secondary retry",
    )
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing files")
    parser.add_argument(
        "--verify-report-path",
        help="verification report path (default: logs/YYYYMMDD/verify_nuri_charts_result.json)",
    )
    return parser


def _default_verify_report_path(base_dir: Path, target_date: date) -> Path:
    return base_dir / "logs" / target_date.strftime(DATE_FORMAT) / VERIFY_REPORT_FILENAME


def _log_optional_missing(report: dict[str, Any], recovered: bool = False) -> None:
    count = len(report.get("optional_missing_items", []))
    if count <= 0:
        return
    if recovered:
        logging.warning("optional charts missing after recovery: %d", count)
    else:
        logging.warning("optional charts missing: %d", count)


def main() -> int:
    args = build_parser().parse_args()

    try:
        target = resolve_collection_target(args.date)
        log_path = _setup_logging(target.storage_date_text)
        logging.info(
            "chart collection start: input=%s storage_date=%s",
            args.date,
            target.storage_date_text,
        )
        logging.info("log file: %s", log_path)

        target_date = target.storage_date
        base_dir = Path(__file__).resolve().parents[1]
        reference_utc = target.reference_utc
        reference_token = reference_utc.strftime("%Y%m%d%H")

        config_items = _load_config(base_dir / "config" / CONFIG_FILENAME)
        expected_items = _build_expected_items(config_items, reference_token)

        raw_dir = base_dir / "dain" / target_date.strftime(DATE_FORMAT) / "charts" / "nuri"

        report_path = (
            Path(args.verify_report_path)
            if args.verify_report_path
            else _default_verify_report_path(base_dir, target_date)
        )
        recover_report_path = report_path.with_name(
            "verify_nuri_charts_result_after_recover.json"
        )

        logging.info(
            "selected synoptic UTC: %s",
            reference_utc.strftime("%Y-%m-%d %H:%M UTC"),
        )
        logging.info("selection mode: %s", target.mode)

        if args.phase == "secondary":
            report = _load_verify_report(report_path)
            missing_items = _extract_missing_items(report)
            if not missing_items:
                logging.error("no missing required items found for retry")
                return 1
            logging.info("retry wait: %ds", args.retry_wait_seconds)
            time.sleep(args.retry_wait_seconds)
            retry_items = _build_items_from_missing(config_items, missing_items)
            _download_items(
                retry_items,
                raw_dir,
                overwrite=False,
                timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
            )
            report_after, _ = _verify_items(raw_dir, expected_items)
            _write_report(recover_report_path, report_after)
            if report_after.get("status") == "PASS":
                _log_optional_missing(report_after, recovered=True)
                logging.info("recovery complete")
                return 0
            logging.error("verification failed after recovery")
            return 1

        downloaded, failures = _download_items(
            expected_items,
            raw_dir,
            overwrite=args.overwrite,
            timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        )
        logging.info("downloads completed: %d", downloaded)
        if failures:
            logging.warning("download failures: %d", len(failures))

        report, missing = _verify_items(raw_dir, expected_items)
        _write_report(report_path, report)
        if report.get("status") == "PASS":
            _log_optional_missing(report)
            logging.info("primary verification passed")
            return 0

        if not missing:
            logging.error("verification failed but no required missing items were listed")
            return 1

        logging.info("retry wait: %ds", args.retry_wait_seconds)
        time.sleep(args.retry_wait_seconds)
        _download_items(
            missing,
            raw_dir,
            overwrite=False,
            timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        )
        report_after, _ = _verify_items(raw_dir, expected_items)
        _write_report(recover_report_path, report_after)
        if report_after.get("status") == "PASS":
            _log_optional_missing(report_after, recovered=True)
            logging.info("recovery complete")
            return 0
        logging.error("verification failed after recovery")
        return 1
    except Exception:
        logging.exception("chart collection failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
