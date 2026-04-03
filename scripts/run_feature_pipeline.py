from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.config import load_project_env
from scripts.common.feature_manifest_loader import load_manifest
from scripts.common.logging import configure_logging


def _collect_feature_manifests(manifest_dir: Path, include_disabled: bool) -> list[tuple[Path, dict[str, object]]]:
    if not manifest_dir.exists():
        raise FileNotFoundError(f"feature manifest directory not found: {manifest_dir}")
    manifest_paths = sorted(manifest_dir.glob("synoptic_*.yaml"), key=lambda path: path.name.lower())
    if not manifest_paths:
        raise FileNotFoundError(f"no synoptic feature manifests found in: {manifest_dir}")

    loaded: list[tuple[Path, dict[str, object]]] = []
    stage_order = {"stage1": 0, "stage2": 1}
    for manifest_path in manifest_paths:
        manifest = load_manifest(manifest_path)
        if not manifest.get("enabled", True) and not include_disabled:
            logging.info("feature_manifest_skip_disabled: %s", manifest["id"])
            continue
        loaded.append((manifest_path, manifest))
    loaded.sort(key=lambda item: (stage_order[str(item[1]["stage"])], str(item[1]["domain"]), str(item[1]["id"])))
    if not loaded:
        raise RuntimeError("no enabled synoptic feature manifests selected")
    return loaded


def _run_manifest(manifest_path: Path, run_date: str, dry_run: bool) -> int:
    cmd = [sys.executable, "-m", "scripts.run_feature_stage", "--date", run_date, "--manifest", str(manifest_path)]
    if dry_run:
        cmd.append("--dry-run")

    logging.info("feature_manifest_start: %s", manifest_path.name)
    started = time.monotonic()
    try:
        result = subprocess.run(cmd, cwd=BASE_DIR, check=False)
    except Exception:
        logging.exception("feature_manifest_exception: %s", manifest_path.name)
        return 1
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        logging.info("feature_manifest_done: %s (elapsed=%.2fs)", manifest_path.name, elapsed)
    else:
        logging.error("feature_manifest_fail: %s (code=%s, elapsed=%.2fs)", manifest_path.name, result.returncode, elapsed)
    return int(result.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="전체 feature 단계를 순차 실행합니다.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--manifest-dir", default="prompts/manifests", help="feature manifest 디렉토리")
    parser.add_argument("--dry-run", action="store_true", help="feature 단계 dry-run 실행")
    parser.add_argument("--include-disabled", action="store_true", help="enabled=false manifest도 포함")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os_manifest_dir = BASE_DIR / args.manifest_dir
    load_project_env(BASE_DIR)
    log_path = configure_logging("run_feature_pipeline", args.date)
    logging.info("run_feature_pipeline_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    try:
        manifest_jobs = _collect_feature_manifests(os_manifest_dir, args.include_disabled)
    except Exception as exc:
        logging.exception("feature_manifest_collection_failed")
        print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    results: list[dict[str, object]] = []
    for manifest_path, manifest in manifest_jobs:
        rc = _run_manifest(manifest_path, args.date, args.dry_run)
        results.append(
            {
                "manifest_id": manifest["id"],
                "manifest_path": str(manifest_path),
                "stage": manifest["stage"],
                "domain": manifest["domain"],
                "returncode": rc,
            }
        )
        if rc != 0:
            print(json.dumps({"status": "error", "failed_manifest": manifest["id"], "results": results}, ensure_ascii=False, indent=2))
            return rc

    print(json.dumps({"status": "ok", "run_date": args.date, "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
