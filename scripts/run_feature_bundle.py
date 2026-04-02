from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.logging import configure_logging
from scripts.feature_bundle_builder import build_feature_bundle, write_feature_bundle_outputs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build consolidated feature bundle from stage outputs.")
    parser.add_argument("--date", required=True, help="Run date in YYYY-MM-DD")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    log_path = configure_logging("run_feature_bundle", args.date)
    logging.info("run_feature_bundle_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    bundle = build_feature_bundle(BASE_DIR, args.date)
    output_paths = write_feature_bundle_outputs(BASE_DIR, args.date, bundle)

    summary = {
        "run_date": args.date,
        "status": "completed",
        "domain_count": bundle["summary"]["domain_count"],
        "image_feature_card_count": bundle["summary"]["image_feature_card_count"],
        "total_stage1_record_count": bundle["summary"]["total_stage1_record_count"],
        "total_stage2_record_count": bundle["summary"]["total_stage2_record_count"],
        "image_feature_cards": str(output_paths["image_feature_cards"]),
        "domain_sequence_features": str(output_paths["domain_sequence_features"]),
        "feature_bundle": str(output_paths["feature_bundle"]),
    }
    logging.info(
        "run_feature_bundle_done: domains=%s image_cards=%s",
        summary["domain_count"],
        summary["image_feature_card_count"],
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
