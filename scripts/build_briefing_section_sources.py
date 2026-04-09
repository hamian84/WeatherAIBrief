from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.briefing_section_source_builder import (
    build_section_source_path,
    build_section_source_payload,
    load_yaml,
)
from scripts.common.date_utils import normalize_run_date
from scripts.common.feature_artifact_writer import write_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="feature compact 산출물로 브리핑 섹션 원자료를 생성합니다.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--section", help="특정 section_id만 생성합니다.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)

    section_map = load_yaml(BASE_DIR / "config" / "briefing" / "section_map.yaml")
    display_labels = load_yaml(BASE_DIR / "config" / "briefing" / "display_labels.yaml")
    rule_pack = load_yaml(BASE_DIR / "daba" / "rules" / "hands37_rule_pack.yaml")

    sections = section_map.get("sections") or []
    if not isinstance(sections, list):
        raise ValueError("section_map.yaml must contain a sections list")

    results: list[dict[str, object]] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_id = section.get("section_id")
        if not isinstance(section_id, str):
            continue
        if args.section and section_id != args.section:
            continue

        source_type = section.get("source_type")
        if source_type != "feature_compact":
            results.append(
                {
                    "section_id": section_id,
                    "status": "skipped",
                    "reason": f"unsupported source_type: {source_type}",
                }
            )
            continue

        payload = build_section_source_payload(
            base_dir=BASE_DIR,
            run_date=args.date,
            section_config=section,
            display_labels=display_labels,
            rule_pack=rule_pack,
        )
        output_path = build_section_source_path(BASE_DIR, args.date, section_id)
        write_json(output_path, payload)
        results.append(
            {
                "section_id": section_id,
                "status": "ok",
                "output_path": str(output_path),
                "row_count": payload["row_count"],
                "rule_count": len(payload["rule_catalog"]),
            }
        )

    print(json.dumps({"status": "ok", "run_date": args.date, "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
