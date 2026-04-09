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

from scripts.common.collection_schedule import resolve_collection_targets
from scripts.common.config import load_project_env
from scripts.common.logging import configure_logging


def _run_step(cmd: list[str], label: str) -> int:
    logging.info("collection_step_start: %s", label)
    started = time.monotonic()
    try:
        result = subprocess.run(cmd, cwd=BASE_DIR, check=False)
    except Exception:
        logging.exception("collection_step_exception: %s", label)
        return 1
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        logging.info("collection_step_done: %s (elapsed=%.2fs)", label, elapsed)
    else:
        logging.error(
            "collection_step_fail: %s (code=%s, elapsed=%.2fs)",
            label,
            result.returncode,
            elapsed,
        )
    return int(result.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run collection scripts.")
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
        "--overwrite",
        action="store_true",
        help="overwrite existing outputs when supported",
    )
    parser.add_argument(
        "--asos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run ASOS collection",
    )
    parser.add_argument(
        "--nuri-charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run Nuri chart collection",
    )
    parser.add_argument(
        "--satellite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run satellite collection",
    )
    parser.add_argument(
        "--radar",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run radar collection",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="ASOS API sleep interval",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="ASOS retry count",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="ASOS retry backoff start",
    )
    parser.add_argument(
        "--retry-backoff-max-seconds",
        type=float,
        default=20.0,
        help="ASOS retry backoff max",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    targets = resolve_collection_targets(args.date)
    primary_target = targets[0]
    expanded_inputs = [target.raw_input for target in targets]

    load_project_env(BASE_DIR)
    log_path = configure_logging("run_collection_stage", primary_target.storage_date_text)
    logging.info(
        "run_collection_stage_start: input=%s storage_date=%s expanded_inputs=%s",
        args.date,
        primary_target.storage_date_text,
        ",".join(expanded_inputs),
    )
    logging.info("log_file: %s", log_path)

    py = sys.executable
    commands: list[tuple[str, list[str]]] = []

    for target_index, target in enumerate(targets):
        target_input = target.raw_input
        target_suffix = f":{target_input}"
        target_overwrite = args.overwrite and target_index == 0

        if args.asos:
            cmd = [
                py,
                "-m",
                "scripts.collect_asos",
                "--date",
                target_input,
                "--phase",
                args.phase,
                "--sleep-seconds",
                str(args.sleep_seconds),
                "--max-retries",
                str(args.max_retries),
                "--retry-backoff-seconds",
                str(args.retry_backoff_seconds),
                "--retry-backoff-max-seconds",
                str(args.retry_backoff_max_seconds),
            ]
            if target_overwrite:
                cmd.append("--overwrite")
            commands.append((f"collect_asos{target_suffix}", cmd))

        if args.nuri_charts:
            cmd = [py, "-m", "scripts.collect_charts", "--date", target_input, "--phase", args.phase]
            if target_overwrite:
                cmd.append("--overwrite")
            commands.append((f"collect_charts{target_suffix}", cmd))

        if args.satellite:
            cmd = [py, "-m", "scripts.collect_satellite", "--date", target_input, "--phase", args.phase]
            if target_overwrite:
                cmd.append("--overwrite")
            commands.append((f"collect_satellite{target_suffix}", cmd))

        if args.radar:
            cmd = [py, "-m", "scripts.collect_radar", "--date", target_input, "--phase", args.phase]
            if target_overwrite:
                cmd.append("--overwrite")
            commands.append((f"collect_radar{target_suffix}", cmd))

    if not commands:
        print(
            json.dumps(
                {
                    "status": "ok",
                    "input": args.date,
                    "storage_date": primary_target.storage_date_text,
                    "expanded_inputs": expanded_inputs,
                    "executed_steps": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    results: list[dict[str, object]] = []
    for label, cmd in commands:
        rc = _run_step(cmd, label)
        results.append({"step": label, "command": cmd, "returncode": rc})
        if rc != 0:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "input": args.date,
                        "storage_date": primary_target.storage_date_text,
                        "expanded_inputs": expanded_inputs,
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
                "input": args.date,
                "storage_date": primary_target.storage_date_text,
                "expanded_inputs": expanded_inputs,
                "executed_steps": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
