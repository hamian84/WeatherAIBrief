from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.config import load_project_env
from scripts.common.logging import configure_logging
from scripts.direct_grounded_briefing_runner import run_direct_grounded_briefing_stage

DEFAULT_MANIFEST = BASE_DIR / "prompts" / "manifests" / "direct_grounded_briefing_writer_manifest.yaml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run direct grounded briefing stage from feature_bundle and image_feature_cards.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="manifest 경로")
    parser.add_argument("--dry-run", action="store_true", help="Build direct grounded briefing prompt input only")
    parser.add_argument("--model", help="Override model name")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    log_path = configure_logging("run_grounded_briefing", args.date)
    logging.info("run_grounded_briefing_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)
    try:
        result = run_direct_grounded_briefing_stage(BASE_DIR, args.date, args.manifest, model=args.model, dry_run=args.dry_run)
    except Exception as exc:
        logging.exception("run_grounded_briefing_failed")
        print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
