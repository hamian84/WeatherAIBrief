from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.logging import LOG_PATH_ENV, configure_logging

KST = ZoneInfo("Asia/Seoul")
DATE_FORMAT = "%Y-%m-%d"
DEFAULT_RAW_KEEP_DAYS = 7
DEFAULT_CURATED_KEEP_DAYS = 7
DEFAULT_REPORT_KEEP_DAYS = 7


def _setup_logging(run_date: str) -> Path:
    return configure_logging("run_daily", run_date)


def _run_step(cmd: list[str], base_dir: Path, label: str, env: dict[str, str]) -> int:
    logging.info("step_start: %s", label)
    started = time.monotonic()
    try:
        result = subprocess.run(cmd, cwd=base_dir, env=env, check=False)
    except Exception:
        logging.exception("step_exception: %s", label)
        return 1
    elapsed = time.monotonic() - started
    if result.returncode == 0:
        logging.info("step_done: %s (elapsed=%.2fs)", label, elapsed)
    else:
        logging.warning("step_fail: %s (code=%s, elapsed=%.2fs)", label, result.returncode, elapsed)
    return int(result.returncode)


def _should_run(name: str, enabled: bool) -> bool:
    if enabled:
        return True
    logging.info("step_skip: %s", name)
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="일일 수집 및 observed 브리핑 실행")
    parser.add_argument("--date", help="YYYY-MM-DD (기본: KST 오늘)")
    parser.add_argument("--stations-file", help="ASOS 관측소 목록 파일")
    parser.add_argument("--expected-stations", type=int, help="ASOS 관측소 기대 개수")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="ASOS 호출 간격(초)")
    parser.add_argument("--max-retries", type=int, default=3, help="ASOS 재시도 횟수")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="ASOS 재시도 초기 backoff 초",
    )
    parser.add_argument(
        "--retry-backoff-max-seconds",
        type=float,
        default=20.0,
        help="ASOS 재시도 최대 backoff 초",
    )
    parser.add_argument("--overwrite", action="store_true", help="기존 산출물 덮어쓰기")

    parser.add_argument("--raw-keep-days", type=int, default=DEFAULT_RAW_KEEP_DAYS, help="raw 보관일수")
    parser.add_argument(
        "--curated-keep-days",
        type=int,
        default=DEFAULT_CURATED_KEEP_DAYS,
        help="curated 보관일수",
    )
    parser.add_argument(
        "--report-keep-days",
        type=int,
        default=DEFAULT_REPORT_KEEP_DAYS,
        help="report 보관일수",
    )

    parser.add_argument(
        "--retention",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="보관 정리 수행",
    )
    parser.add_argument(
        "--asos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="ASOS 수집 수행",
    )
    parser.add_argument(
        "--nuri-charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="nuri charts 수집 수행",
    )
    parser.add_argument(
        "--satellite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="satellite 수집 수행 (기본: 사용)",
    )
    parser.add_argument(
        "--observed-briefing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="observed 단일 브리핑 파이프라인 수행",
    )
    parser.add_argument(
        "--strict-fail",
        "--observed-strict-fail",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="observed 파이프라인 실패 시 전체 run_daily 실패 처리 (기본: 사용)",
    )
    parser.add_argument(
        "--allow-fallback",
        "--observed-allow-fallback",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="개발/디버깅 전용: LLM 호출 실패 또는 검증 실패 시 fallback 브리핑 생성을 허용",
    )
    return parser


def _run_observed_pipeline(args: argparse.Namespace, base_dir: Path, env: dict[str, str]) -> int:
    if not _should_run("observed_pipeline", args.observed_briefing):
        return 0

    if bool(getattr(args, "observed_allow_fallback", False)):
        logging.warning("run_daily_dev_mode: observed_allow_fallback=true (개발/디버깅 전용)")
        env["OBSERVED_FEATURE_ALLOW_CHART_SEMANTIC_FALLBACK"] = "1"

    py = sys.executable
    observed_steps = [
        (
            "observed_features",
            [py, "modules/observed_v1/scripts/build_observed_features.py", "--date", args.date],
        ),
        (
            "observed_synoptic",
            [py, "modules/observed_v1/scripts/build_synoptic_findings.py", "--date", args.date],
        ),
        (
            "observed_claims",
            [py, "modules/observed_v1/scripts/build_observed_claims.py", "--date", args.date],
        ),
        (
            "observed_evidence",
            [py, "modules/observed_v1/scripts/build_observed_evidence.py", "--date", args.date],
        ),
        (
            "observed_synth",
            [py, "modules/observed_v1/scripts/synth_observed_briefing.py", "--date", args.date]
            + (["--allow-fallback"] if bool(getattr(args, "observed_allow_fallback", False)) else []),
        ),
        (
            "observed_render",
            [
                py,
                "modules/observed_v1/scripts/render_observed_briefing.py",
                "--date",
                args.date,
                "--skip-validation",
            ],
        ),
        (
            "observed_validate",
            [py, "modules/observed_v1/scripts/validate_observed_briefing.py", "--date", args.date],
        ),
    ]

    for label, cmd in observed_steps:
        rc = _run_step(cmd, base_dir, label, env)
        if rc != 0:
            if args.observed_strict_fail:
                return rc
            logging.warning("observed 단계 실패(continue): %s", label)
            return 0
    return 0


def main() -> int:
    args = build_parser().parse_args()
    if not args.date:
        args.date = datetime.now(tz=KST).strftime(DATE_FORMAT)

    log_path = _setup_logging(args.date)
    logging.info("run_daily_start: date=%s", args.date)
    logging.info("log_file: %s", log_path)

    base_dir = Path(__file__).resolve().parents[1]
    py = sys.executable
    env = dict(os.environ)
    env[LOG_PATH_ENV] = str(log_path)

    if _should_run("retention", args.retention):
        cmd = [
            py,
            "-m",
            "jobs.retention",
            "--raw-keep-days",
            str(args.raw_keep_days),
            "--curated-keep-days",
            str(args.curated_keep_days),
            "--report-keep-days",
            str(args.report_keep_days),
        ]
        rc = _run_step(cmd, base_dir, "retention", env)
        if rc != 0:
            return rc

    if _should_run("asos", args.asos):
        cmd = [
            py,
            "-m",
            "scripts.collect_asos",
            "--date",
            args.date,
            "--phase",
            "primary",
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
        rc = _run_step(cmd, base_dir, "asos_collect", env)
        if rc != 0:
            logging.warning("ASOS 수집 실패(continue). verify 리포트에서 점검 필요.")

    if _should_run("nuri_charts", args.nuri_charts):
        cmd = [py, "-m", "scripts.collect_charts", "--date", args.date, "--phase", "primary"]
        if args.overwrite:
            cmd.append("--overwrite")
        rc = _run_step(cmd, base_dir, "nuri_charts_collect", env)
        if rc != 0:
            return rc

    if _should_run("satellite", args.satellite):
        cmd = [py, "-m", "scripts.collect_satellite", "--date", args.date, "--phase", "primary"]
        if args.overwrite:
            cmd.append("--overwrite")
        rc = _run_step(cmd, base_dir, "satellite_collect", env)
        if rc != 0:
            return rc

    observed_rc = _run_observed_pipeline(args, base_dir, env)
    if observed_rc != 0:
        return observed_rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
