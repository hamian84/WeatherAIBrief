import os
from pathlib import Path


KEYS_ENV_PATH = Path("config") / "keys.env"


def load_dotenv(dotenv_path: Path, override: bool = False) -> None:
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not override and key in os.environ:
            continue
        os.environ[key] = value


def _normalize_value(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped


def load_project_env(base_dir: Path) -> None:
    dotenv_path = base_dir / ".env"
    load_dotenv(dotenv_path, override=False)
    keys_env_path = base_dir / KEYS_ENV_PATH
    load_dotenv(keys_env_path, override=False)


def get_env_value(key: str) -> str | None:
    return _normalize_value(os.environ.get(key))
