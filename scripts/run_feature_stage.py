from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.config import load_project_env
from scripts.common.date_utils import normalize_run_date
from scripts.common.feature_manifest_loader import load_manifest, load_manifests_from_dir
from scripts.common.logging import configure_logging
from scripts.feature_stage_runner_bundle import run_manifest

DATE_FORMAT_HINT = "YYYYMMDD"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run feature stage manifests.")
    parser.add_argument("--date", required=True, help=DATE_FORMAT_HINT)
    manifest_group = parser.add_mutually_exclusive_group(required=True)
    manifest_group.add_argument("--manifest", help="Single manifest path")
    manifest_group.add_argument("--manifest-dir", help="Manifest directory for batch execution")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and gating without calling OpenAI")
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Include manifests where enabled=false during batch execution",
    )
    parser.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop batch execution on the first error",
    )
    return parser


def _collect_manifest_jobs(base_dir: Path, args: argparse.Namespace) -> list[Path]:
    if args.manifest:
        return [Path(args.manifest)]
    loaded = load_manifests_from_dir(base_dir / args.manifest_dir)
    jobs: list[Path] = []
    for manifest_path, manifest in loaded:
        if not manifest.get("enabled", True) and not args.include_disabled:
            logging.info("manifest_skip_disabled: %s", manifest["id"])
            continue
        jobs.append(manifest_path)
    if not jobs:
        raise RuntimeError("no enabled manifests selected")
    return jobs


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)
    load_project_env(BASE_DIR)
    log_path = configure_logging("run_feature_stage", args.date)
    logging.info("run_feature_stage_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    try:
        manifest_jobs = _collect_manifest_jobs(BASE_DIR, args)
    except Exception as exc:
        logging.exception("manifest_collection_failed")
        print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    results: list[dict[str, object]] = []
    for manifest_path in manifest_jobs:
        try:
            manifest = load_manifest(manifest_path)
            logging.info(
                "feature_stage_start: manifest=%s stage=%s domain=%s",
                manifest["id"],
                manifest["stage"],
                manifest["domain"],
            )
            summary = run_manifest(
                BASE_DIR,
                args.date,
                manifest,
                dry_run=args.dry_run,
            )
            results.append(summary)
        except Exception as exc:
            logging.exception("feature_stage_failed: manifest=%s", manifest_path)
            results.append(
                {
                    "manifest_path": str(manifest_path),
                    "status": "error",
                    "message": str(exc),
                }
            )
            if args.stop_on_error:
                print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
                return 1

    print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
    return 0 if all(result.get("status") != "error" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
