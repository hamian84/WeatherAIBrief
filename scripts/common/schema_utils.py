from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from jsonschema import validate as jsonschema_validate  # type: ignore
except ImportError:  # pragma: no cover
    jsonschema_validate = None


def load_json_schema(path: str | Path) -> dict[str, Any]:
    schema_path = Path(path)
    if not schema_path.exists():
        raise FileNotFoundError(f'schema file not found: {schema_path}')
    payload = json.loads(schema_path.read_text(encoding='utf-8-sig'))
    if not isinstance(payload, dict):
        raise ValueError(f'schema payload must be an object: {schema_path}')
    return payload


def _manual_validate(instance: Any, schema: dict[str, Any], label: str) -> None:
    if schema.get('type') == 'object' and not isinstance(instance, dict):
        raise ValueError(f'{label} must be an object')
    required = schema.get('required', [])
    if isinstance(instance, dict):
        for key in required:
            if key not in instance:
                raise ValueError(f'{label} missing required key: {key}')


def validate_instance(instance: Any, schema: dict[str, Any], label: str) -> None:
    if jsonschema_validate is not None:
        jsonschema_validate(instance, schema)
        return
    _manual_validate(instance, schema, label)
