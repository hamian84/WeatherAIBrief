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
        logging.error("collection_step_fail: %s (code=%s, elapsed=%.2fs)", label, result.returncode, elapsed)
    return int(result.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="수집 단계를 순차 실행합니다.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--phase", choices=("primary", "secondary"), default="primary", help="수집 phase")
    parser.add_argument("--overwrite", action="store_true", help="기존 산출물이 있어도 덮어씁니다.")
    parser.add_argument(
        "--asos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="ASOS 수집 여부",
    )
    parser.add_argument(
        "--nuri-charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="일기도 수집 여부",
    )
    parser.add_argument(
        "--satellite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="위성 수집 여부",
    )
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="ASOS 호출 간격(초)")
    parser.add_argument("--max-retries", type=int, default=3, help="ASOS 재시도 횟수")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0, help="ASOS 초기 backoff 초")
    parser.add_argument("--retry-backoff-max-seconds", type=float, default=20.0, help="ASOS 최대 backoff 초")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_project_env(BASE_DIR)
    log_path = configure_logging("run_collection_stage", args.date)
    logging.info("run_collection_stage_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    py = sys.executable
    commands: list[tuple[str, list[str]]] = []

    if args.asos:
        cmd = [
            py,
            "-m",
            "scripts.collect_asos",
            "--date",
            args.date,
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
        if args.overwrite:
            cmd.append("--overwrite")
        commands.append(("collect_asos", cmd))

    if args.nuri_charts:
        cmd = [py, "-m", "scripts.collect_charts", "--date", args.date, "--phase", args.phase]
        if args.overwrite:
            cmd.append("--overwrite")
        commands.append(("collect_charts", cmd))

    if args.satellite:
        cmd = [py, "-m", "scripts.collect_satellite", "--date", args.date, "--phase", args.phase]
        if args.overwrite:
            cmd.append("--overwrite")
        commands.append(("collect_satellite", cmd))

    if not commands:
        print(json.dumps({"status": "ok", "run_date": args.date, "executed_steps": []}, ensure_ascii=False, indent=2))
        return 0

    results: list[dict[str, object]] = []
    for label, cmd in commands:
        rc = _run_step(cmd, label)
        results.append({"step": label, "command": cmd, "returncode": rc})
        if rc != 0:
            print(json.dumps({"status": "error", "failed_step": label, "results": results}, ensure_ascii=False, indent=2))
            return rc

    print(json.dumps({"status": "ok", "run_date": args.date, "executed_steps": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
