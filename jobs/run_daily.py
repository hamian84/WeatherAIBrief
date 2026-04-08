from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.logging import configure_logging

KST = ZoneInfo("Asia/Seoul")
DATE_FORMAT = "%Y-%m-%d"
STAGE_ORDER = (
    "collect",
    "feature",
)
STAGE_REQUIREMENTS: dict[str, dict[str, object]] = {
    "feature": {
        "required_paths": ("dain/{date}/charts/nuri",),
        "upstream_stages": ("collect",),
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
    hint_text = ", ".join(upstream_stages) or "선행 단계 확인 필요"
    missing_path_lines = "\n".join(f"- {path}" for path in missing_paths)
    raise FileNotFoundError(
        f"stage prerequisite missing for '{stage}'.\n"
        f"필요한 선행 산출물이 없습니다. 먼저 다음 단계를 실행하세요: {hint_text}\n"
        f"누락 경로:\n{missing_path_lines}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="수집부터 feature compact 산출물까지 일일 파이프라인을 실행합니다.")
    parser.add_argument("--date", help="YYYY-MM-DD (기본: KST 오늘)")
    parser.add_argument(
        "--stage",
        action="append",
        choices=STAGE_ORDER,
        help="특정 단계만 실행합니다. 여러 번 지정할 수 있습니다.",
    )
    parser.add_argument("--phase", choices=("primary", "secondary"), default="primary", help="수집 단계 phase")
    parser.add_argument("--overwrite", action="store_true", help="기존 산출물이 있어도 덮어씁니다.")
    parser.add_argument(
        "--asos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="수집 단계에서 ASOS 수집 여부",
    )
    parser.add_argument(
        "--nuri-charts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="수집 단계에서 일기도 수집 여부",
    )
    parser.add_argument(
        "--satellite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="수집 단계에서 위성 수집 여부",
    )
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="ASOS 호출 간격(초)")
    parser.add_argument("--max-retries", type=int, default=3, help="ASOS 재시도 횟수")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0, help="ASOS 초기 backoff(초)")
    parser.add_argument("--retry-backoff-max-seconds", type=float, default=20.0, help="ASOS 최대 backoff(초)")
    parser.add_argument(
        "--feature-manifest-dir",
        default="daba/manifests",
        help="feature 단계 manifest 디렉터리",
    )
    parser.add_argument("--feature-dry-run", action="store_true", help="feature 단계만 dry-run으로 실행")
    parser.add_argument(
        "--feature-include-disabled",
        action="store_true",
        help="feature 단계에서 enabled=false manifest도 포함",
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
    }

    if args.overwrite:
        commands["collect"].append("--overwrite")
    if not args.asos:
        commands["collect"].append("--no-asos")
    if not args.nuri_charts:
        commands["collect"].append("--no-nuri-charts")
    if not args.satellite:
        commands["collect"].append("--no-satellite")
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
        args.date = datetime.now(tz=KST).strftime(DATE_FORMAT)

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
