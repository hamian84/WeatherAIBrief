from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

COMMON_REQUIRED_FIELDS = (
    "id",
    "domain",
    "stage",
    "image_root_template",
    "image_glob",
    "image_ref_regex",
    "response_schema_path",
    "artifact_subdir",
    "model",
    "enabled",
)
ROW_MODE_REQUIRED_FIELDS = ("prompt_table_path",)
STAGE2_REQUIRED_FIELDS = (
    "gating_source",
    "gating_match_keys",
    "gating_answer",
)
STAGE2_BUNDLE_REQUIRED_FIELDS = (
    "stage2_bundle_header_path",
    "stage2_bundle_targets_path",
)
PROMPT_TABLE_MODES = {"row", "stage1_bundle", "stage2_bundle"}
_STAGE_ORDER = {"stage1": 0, "stage2": 1}


def _read_manifest_payload(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        raise ValueError(f"empty manifest file: {path}")
    if yaml is not None:
        payload = yaml.safe_load(text)
    else:
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be an object: {path}")
    return payload


def _normalize_optional_path(payload: dict[str, Any], key: str) -> None:
    if key in payload and payload.get(key) is not None:
        payload[key] = str(payload[key]).strip()


def _validate_manifest(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in COMMON_REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"manifest missing required fields {missing}: {path}")
    stage = str(payload["stage"]).strip()
    if stage not in _STAGE_ORDER:
        raise ValueError(f"unsupported stage '{stage}': {path}")

    prompt_table_mode = str(payload.get("prompt_table_mode") or "row").strip()
    if prompt_table_mode not in PROMPT_TABLE_MODES:
        raise ValueError(f"unsupported prompt_table_mode '{prompt_table_mode}': {path}")
    if stage == "stage1" and prompt_table_mode == "stage2_bundle":
        raise ValueError(f"stage1 manifest cannot use prompt_table_mode=stage2_bundle: {path}")
    if stage == "stage2" and prompt_table_mode == "stage1_bundle":
        raise ValueError(f"stage2 manifest cannot use prompt_table_mode=stage1_bundle: {path}")
    if prompt_table_mode in {"row", "stage1_bundle"}:
        missing_row = [field for field in ROW_MODE_REQUIRED_FIELDS if field not in payload]
        if missing_row:
            raise ValueError(f"manifest missing required fields {missing_row}: {path}")
    if prompt_table_mode == "stage2_bundle":
        missing_stage2_bundle = [field for field in STAGE2_BUNDLE_REQUIRED_FIELDS if field not in payload]
        if missing_stage2_bundle:
            raise ValueError(f"stage2 bundle manifest missing required fields {missing_stage2_bundle}: {path}")
    if stage == "stage2":
        missing_stage2 = [field for field in STAGE2_REQUIRED_FIELDS if field not in payload]
        if missing_stage2:
            raise ValueError(f"stage2 manifest missing required fields {missing_stage2}: {path}")
    payload["stage"] = stage
    payload["prompt_table_mode"] = prompt_table_mode
    payload["enabled"] = bool(payload.get("enabled", True))
    payload["id"] = str(payload["id"]).strip()
    payload["domain"] = str(payload["domain"]).strip()
    payload["artifact_subdir"] = str(payload["artifact_subdir"]).strip()
    payload["model"] = str(payload.get("model") or "gemini-2.5-flash").strip()
    payload["allow_multi_select"] = bool(payload.get("allow_multi_select", prompt_table_mode == "stage1_bundle"))
    payload["bundle_fail_fast"] = bool(payload.get("bundle_fail_fast", True))
    if prompt_table_mode in {"row", "stage1_bundle"}:
        payload["prompt_table_path"] = str(payload["prompt_table_path"]).strip()
    payload["response_schema_path"] = str(payload["response_schema_path"]).strip()
    payload["image_root_template"] = str(payload["image_root_template"]).strip()
    payload["image_glob"] = str(payload["image_glob"]).strip()
    payload["image_ref_regex"] = str(payload["image_ref_regex"]).strip()
    _normalize_optional_path(payload, "stage2_bundle_header_path")
    _normalize_optional_path(payload, "stage2_bundle_targets_path")
    if "gating_match_keys" in payload and not isinstance(payload["gating_match_keys"], list):
        raise ValueError(f"gating_match_keys must be a list: {path}")
    return payload


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest file not found: {manifest_path}")
    payload = _read_manifest_payload(manifest_path)
    return _validate_manifest(manifest_path, payload)


def load_manifests_from_dir(path: str | Path) -> list[tuple[Path, dict[str, Any]]]:
    manifest_dir = Path(path)
    if not manifest_dir.exists():
        raise FileNotFoundError(f"manifest directory not found: {manifest_dir}")
    manifest_paths = sorted(
        [*manifest_dir.glob("*.yaml"), *manifest_dir.glob("*.yml")],
        key=lambda item: item.name.lower(),
    )
    if not manifest_paths:
        raise FileNotFoundError(f"no manifest files found in: {manifest_dir}")
    loaded: list[tuple[Path, dict[str, Any]]] = []
    for manifest_path in manifest_paths:
        loaded.append((manifest_path, load_manifest(manifest_path)))
    loaded.sort(key=lambda item: (_STAGE_ORDER[item[1]["stage"]], item[1]["domain"], item[1]["id"]))
    return loaded

