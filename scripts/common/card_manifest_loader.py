from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

REQUIRED_FIELDS = (
    'id',
    'stage',
    'input_paths',
    'output_paths',
    'schema_path',
)


def _read_yaml_or_json(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding='utf-8-sig').strip()
    if not text:
        raise ValueError(f'empty manifest file: {path}')
    if yaml is not None:
        payload = yaml.safe_load(text)
    else:
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f'manifest payload must be an object: {path}')
    return payload


def _normalize_mapping(value: Any, field_name: str, path: Path) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f'{field_name} must be a non-empty mapping: {path}')
    normalized: dict[str, str] = {}
    for key, item in value.items():
        text = str(item).strip()
        if not text:
            raise ValueError(f'{field_name}.{key} must not be empty: {path}')
        normalized[str(key).strip()] = text
    return normalized


def load_card_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f'manifest file not found: {manifest_path}')
    payload = _read_yaml_or_json(manifest_path)
    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f'manifest missing required fields {missing}: {manifest_path}')
    payload['id'] = str(payload['id']).strip()
    payload['stage'] = str(payload['stage']).strip()
    payload['schema_path'] = str(payload['schema_path']).strip()
    payload['input_paths'] = _normalize_mapping(payload['input_paths'], 'input_paths', manifest_path)
    payload['output_paths'] = _normalize_mapping(payload['output_paths'], 'output_paths', manifest_path)
    payload['policy_path'] = str(payload.get('policy_path', '')).strip()
    payload['model'] = str(payload.get('model') or 'gpt-4.1-mini').strip()
    payload['allow_new_claims'] = bool(payload.get('allow_new_claims', False))
    payload['prompt_instructions'] = str(payload.get('prompt_instructions') or '').strip()
    return payload


def resolve_manifest_path(base_dir: Path, template: str, run_date: str) -> Path:
    rendered = str(template).format(date=run_date)
    path = Path(rendered)
    if not path.is_absolute():
        path = base_dir / path
    return path


def load_policy_mapping(path: str | Path) -> dict[str, Any]:
    policy_path = Path(path)
    if not policy_path.exists():
        raise FileNotFoundError(f'policy file not found: {policy_path}')
    payload = _read_yaml_or_json(policy_path)
    if not isinstance(payload, dict):
        raise ValueError(f'policy payload must be an object: {policy_path}')
    return payload
