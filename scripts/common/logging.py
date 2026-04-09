import logging
import os
from pathlib import Path

from scripts.common.date_utils import normalize_run_date, today_run_date_kst

LOG_PATH_ENV = "WEATHERAI_LOG_PATH"
LOG_DATE_ENV = "WEATHERAI_LOG_DATE"


def _resolve_run_date(run_date: str | None) -> str:
    if run_date:
        return normalize_run_date(run_date)
    env_date = os.environ.get(LOG_DATE_ENV)
    if env_date:
        return normalize_run_date(env_date)
    return today_run_date_kst()


def build_log_path(run_name: str, run_date: str | None = None) -> Path:
    resolved_date = _resolve_run_date(run_date)
    return Path("logs") / resolved_date / f"{run_name}.log"


def configure_logging(run_name: str, run_date: str | None = None) -> Path:
    log_path_value = os.environ.get(LOG_PATH_ENV)
    if log_path_value:
        log_path = Path(log_path_value)
    else:
        log_path = build_log_path(run_name, run_date)

    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_path
