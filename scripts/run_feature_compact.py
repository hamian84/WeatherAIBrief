from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.feature_artifact_writer import build_stage_artifact_paths, write_json_compact
from scripts.common.feature_compactor import compact_normalized_payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="기존 normalized feature 산출물로 compact 파일을 재생성합니다.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--domain", help="특정 도메인만 처리합니다.")
    parser.add_argument("--stage", choices=("stage1", "stage2"), help="특정 stage만 처리합니다.")
    return parser


def _collect_jobs(base_dir: Path, run_date: str, domain: str | None, stage: str | None) -> list[tuple[str, str, Path, Path]]:
    features_dir = base_dir / "daio" / run_date / "features"
    if not features_dir.exists():
        raise FileNotFoundError(f"features directory not found: {features_dir}")

    jobs: list[tuple[str, str, Path, Path]] = []
    target_domains = [domain] if domain else sorted(path.name for path in features_dir.iterdir() if path.is_dir())
    target_stages = [stage] if stage else ["stage1", "stage2"]
    for current_domain in target_domains:
        for current_stage in target_stages:
            paths = build_stage_artifact_paths(base_dir, run_date, current_domain, current_stage)
            if paths["normalized"].exists():
                jobs.append((current_domain, current_stage, paths["normalized"], paths["compact"]))
    if not jobs:
        raise FileNotFoundError("no normalized artifacts found for the requested selection")
    return jobs


def main() -> int:
    args = build_parser().parse_args()
    jobs = _collect_jobs(BASE_DIR, args.date, args.domain, args.stage)
    results: list[dict[str, object]] = []
    for domain, stage, normalized_path, compact_path in jobs:
        payload = json.loads(normalized_path.read_text(encoding="utf-8"))
        compact_payload = compact_normalized_payload(payload)
        write_json_compact(compact_path, compact_payload)
        results.append(
            {
                "domain": domain,
                "stage": stage,
                "normalized": str(normalized_path),
                "compact": str(compact_path),
                "row_count": compact_payload["row_count"],
            }
        )

    print(json.dumps({"status": "ok", "run_date": args.date, "results": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
