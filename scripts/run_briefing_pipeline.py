from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.config import load_project_env
from scripts.common.date_utils import normalize_run_date
from scripts.common.logging import configure_logging


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"yaml root must be a mapping: {path}")
    return payload


def _run_step(cmd: list[str], label: str) -> int:
    logging.info("briefing_step_start: %s", label)
    started = time.monotonic()
    try:
        result = subprocess.run(cmd, cwd=BASE_DIR, check=False)
    except Exception:
        logging.exception("briefing_step_exception: %s", label)
        return 1
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        logging.info("briefing_step_done: %s (elapsed=%.2fs)", label, elapsed)
    else:
        logging.error(
            "briefing_step_fail: %s (code=%s, elapsed=%.2fs)",
            label,
            result.returncode,
            elapsed,
        )
    return int(result.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the briefing pipeline.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    return parser


def _feature_sections(section_map: dict[str, Any]) -> list[str]:
    section_ids: list[str] = []
    for section in section_map.get("sections", []):
        if not isinstance(section, dict):
            continue
        if section.get("source_type") != "feature_compact":
            continue
        section_id = section.get("section_id")
        if isinstance(section_id, str):
            section_ids.append(section_id)
    if not section_ids:
        raise RuntimeError("no feature_compact sections found in section_map")
    return section_ids


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)

    load_project_env(BASE_DIR)
    log_path = configure_logging("run_briefing_pipeline", args.date)
    logging.info("run_briefing_pipeline_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    section_map = _load_yaml(BASE_DIR / "config" / "briefing" / "section_map.yaml")
    feature_sections = _feature_sections(section_map)
    py = sys.executable

    commands: list[tuple[str, list[str]]] = [
        (
            "build_briefing_section_sources",
            [py, "scripts/build_briefing_section_sources.py", "--date", args.date],
        )
    ]
    for section_id in feature_sections:
        commands.append(
            (
                f"extract_timeline_events:{section_id}",
                [py, "scripts/extract_timeline_events.py", "--date", args.date, "--section", section_id],
            )
        )
        commands.append(
            (
                f"write_briefing_from_events:{section_id}",
                [py, "scripts/write_briefing_from_events.py", "--date", args.date, "--section", section_id],
            )
        )
    commands.append(("write_briefing_draft", [py, "scripts/write_briefing_draft.py", "--date", args.date]))
    commands.append(("compose_weather_briefing", [py, "scripts/compose_weather_briefing.py", "--date", args.date]))

    results: list[dict[str, object]] = []
    for label, cmd in commands:
        rc = _run_step(cmd, label)
        results.append({"step": label, "command": cmd, "returncode": rc})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "run_date": args.date,
                        "failed_step": label,
                        "results": results,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return rc

    print(
        json.dumps(
            {
                "status": "ok",
                "run_date": args.date,
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
