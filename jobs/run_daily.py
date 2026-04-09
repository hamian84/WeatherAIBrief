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

from scripts.common.date_utils import normalize_run_date, today_run_date_kst
from scripts.common.logging import configure_logging

STAGE_ORDER = (
    "collect",
    "feature",
    "briefing",
)
STAGE_REQUIREMENTS: dict[str, dict[str, object]] = {
    "feature": {
        "required_paths": (
            "dain/{date}/charts/nuri",
            "dain/{date}/satellite/LE1B/WV063/EA",
            "dain/{date}/radar/cmi",
        ),
        "upstream_stages": ("collect",),
    },
    "briefing": {
        "required_paths": ("daio/{date}/features",),
        "upstream_stages": ("feature",),
    },
}


def _setup_logging(run_date: str) -> Path:
    return configure_logging("run_daily", run_date)


def _run_step(cmd: list[str], base_dir: Path, label: str) -> int:
    logging.info("step_start: %s", label)
    started = time.monotonic()
    try:
        result = subprocess.run(cmd, cwd=base_dir, check=False)
    except Exception:
        logging.exception("step_exception: %s", label)
        return 1
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        logging.info("step_done: %s (elapsed=%.2fs)", label, elapsed)
    else:
        logging.error("step_fail: %s (code=%s, elapsed=%.2fs)", label, result.returncode, elapsed)
    return int(result.returncode)


def _selected_stages(stage_args: list[str] | None) -> list[str]:
    if not stage_args:
        return list(STAGE_ORDER)
    selected = set(stage_args)
    return [stage for stage in STAGE_ORDER if stage in selected]


def _stage_requirement_paths(base_dir: Path, run_date: str, stage: str) -> list[Path]:
    requirement = STAGE_REQUIREMENTS.get(stage)
    if not requirement:
        return []
    required_paths = requirement.get("required_paths", ())
    return [base_dir / str(relative_path).format(date=run_date) for relative_path in required_paths]


def _validate_single_stage_prerequisites(base_dir: Path, run_date: str, stage: str) -> None:
    requirement = STAGE_REQUIREMENTS.get(stage)
    if not requirement:
        return

    missing_paths = [path for path in _stage_requirement_paths(base_dir, run_date, stage) if not path.exists()]
    if not missing_paths:
        return

    upstream_stages = [str(item) for item in requirement.get("upstream_stages", ())]
    hint_text = ", ".join(upstream_stages) or "check upstream stages"
    missing_path_lines = "\n".join(f"- {path}" for path in missing_paths)
    raise FileNotFoundError(
        f"stage prerequisite missing for '{stage}'.\n"
        f"Required upstream outputs are missing. Run these stages first: {hint_text}\n"
        f"Missing paths:\n{missing_path_lines}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the daily collect, feature, and briefing pipeline.")
    parser.add_argument("--date", help="YYYYMMDD (default: current KST date)")
    parser.add_argument(
        "--stage",
        action="append",
        choices=STAGE_ORDER,
        help="Run only selected stages. Can be provided multiple times.",
    )
    parser.add_argument("--phase", choices=("primary", "secondary"), default="primary", help="Collection phase")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    parser.add_argument(
        "--asos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable ASOS collection in the collect stage",
    )
    parser.add_argument(
        "--nuri-charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable Nuri chart collection in the collect stage",
    )
    parser.add_argument(
        "--satellite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable satellite collection in the collect stage",
    )
    parser.add_argument(
        "--radar",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable radar collection in the collect stage",
    )
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="ASOS API sleep interval (seconds)")
    parser.add_argument("--max-retries", type=int, default=3, help="ASOS retry count")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0, help="ASOS initial backoff (seconds)")
    parser.add_argument("--retry-backoff-max-seconds", type=float, default=20.0, help="ASOS max backoff (seconds)")
    parser.add_argument(
        "--feature-manifest-dir",
        default="daba/manifests",
        help="Feature stage manifest directory",
    )
    parser.add_argument("--feature-dry-run", action="store_true", help="Run the feature stage in dry-run mode")
    parser.add_argument(
        "--feature-include-disabled",
        action="store_true",
        help="Include disabled manifests in the feature stage",
    )
    return parser


def _build_stage_commands(args: argparse.Namespace, python_executable: str) -> dict[str, list[str]]:
    commands: dict[str, list[str]] = {
        "collect": [python_executable, "scripts/run_collection_stage.py", "--date", args.date, "--phase", args.phase],
        "feature": [
            python_executable,
            "scripts/run_feature_pipeline.py",
            "--date",
            args.date,
            "--manifest-dir",
            args.feature_manifest_dir,
        ],
        "briefing": [
            python_executable,
            "scripts/run_briefing_pipeline.py",
            "--date",
            args.date,
        ],
    }

    if args.overwrite:
        commands["collect"].append("--overwrite")
    if not args.asos:
        commands["collect"].append("--no-asos")
    if not args.nuri_charts:
        commands["collect"].append("--no-nuri-charts")
    if not args.satellite:
        commands["collect"].append("--no-satellite")
    if not args.radar:
        commands["collect"].append("--no-radar")
    commands["collect"].extend(
        [
            "--sleep-seconds",
            str(args.sleep_seconds),
            "--max-retries",
            str(args.max_retries),
            "--retry-backoff-seconds",
            str(args.retry_backoff_seconds),
            "--retry-backoff-max-seconds",
            str(args.retry_backoff_max_seconds),
        ]
    )

    if args.feature_dry_run:
        commands["feature"].append("--dry-run")
    if args.feature_include_disabled:
        commands["feature"].append("--include-disabled")

    return commands


def main() -> int:
    args = build_parser().parse_args()
    if not args.date:
        args.date = today_run_date_kst()
    else:
        args.date = normalize_run_date(args.date)

    log_path = _setup_logging(args.date)
    logging.info("run_daily_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    selected_stages = _selected_stages(args.stage)
    logging.info("selected_stages: %s", ",".join(selected_stages))

    commands = _build_stage_commands(args, sys.executable)
    results: list[dict[str, object]] = []
    for stage in selected_stages:
        try:
            _validate_single_stage_prerequisites(BASE_DIR, args.date, stage)
        except Exception as exc:
            logging.error("prerequisite_check_failed: %s", exc)
            print(
                json.dumps(
                    {
                        "status": "error",
                        "failed_stage": "prerequisite-check",
                        "target_stage": stage,
                        "message": str(exc),
                        "selected_stages": selected_stages,
                        "results": results,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 1

        cmd = commands[stage]
        rc = _run_step(cmd, BASE_DIR, stage)
        results.append({"stage": stage, "command": cmd, "returncode": rc})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "failed_stage": stage,
                        "selected_stages": selected_stages,
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
                "stages": selected_stages,
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
