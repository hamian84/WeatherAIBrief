from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.common.feature_prompt_table_loader import (
    load_prompt_table,
    load_stage1_bundle_table,
    load_stage2_bundle_tables,
)


def _resolve_project_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = base_dir / path
    return path


def load_prompt_spec(base_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    mode = str(manifest.get("prompt_table_mode") or "row").strip()
    schema_path = _resolve_project_path(base_dir, str(manifest["response_schema_path"]))
    if not schema_path.exists():
        raise FileNotFoundError(f"response schema not found: {schema_path}")

    if mode == "row":
        prompt_table_path = _resolve_project_path(base_dir, str(manifest["prompt_table_path"]))
        prompt_rows = load_prompt_table(prompt_table_path)
        return {
            "mode": mode,
            "prompt_rows": prompt_rows,
            "prompt_row_count": len(prompt_rows),
            "paths": {
                "prompt_table_path": prompt_table_path,
                "response_schema_path": schema_path,
            },
        }

    if mode == "stage1_bundle":
        prompt_table_path = _resolve_project_path(base_dir, str(manifest["prompt_table_path"]))
        prompt_rows = load_stage1_bundle_table(prompt_table_path)
        return {
            "mode": mode,
            "prompt_rows": prompt_rows,
            "prompt_row_count": len(prompt_rows),
            "paths": {
                "prompt_table_path": prompt_table_path,
                "response_schema_path": schema_path,
            },
        }

    if mode == "stage2_bundle":
        header_path = _resolve_project_path(base_dir, str(manifest["stage2_bundle_header_path"]))
        targets_path = _resolve_project_path(base_dir, str(manifest["stage2_bundle_targets_path"]))
        bundle_spec = load_stage2_bundle_tables(header_path, targets_path)
        return {
            "mode": mode,
            "prompt_rows": bundle_spec["bundles"],
            "prompt_row_count": len(bundle_spec["targets"]),
            "paths": {
                "stage2_bundle_header_path": header_path,
                "stage2_bundle_targets_path": targets_path,
                "response_schema_path": schema_path,
            },
            "bundle_headers": bundle_spec["headers"],
            "bundle_targets": bundle_spec["targets"],
            "bundle_count": len(bundle_spec["bundles"]),
            "target_count": len(bundle_spec["targets"]),
        }

    raise ValueError(f"unsupported prompt_table_mode '{mode}'")
