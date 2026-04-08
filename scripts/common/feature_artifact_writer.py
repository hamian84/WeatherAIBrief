from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.common.feature_compactor import compact_normalized_payload


def build_feature_output_dir(base_dir: Path, run_date: str, artifact_subdir: str) -> Path:
    return base_dir / "daio" / run_date / "features" / artifact_subdir


def build_stage_artifact_paths(base_dir: Path, run_date: str, artifact_subdir: str, stage: str) -> dict[str, Path]:
    output_dir = build_feature_output_dir(base_dir, run_date, artifact_subdir)
    return {
        "output_dir": output_dir,
        "raw": output_dir / f"{stage}_raw.json",
        "normalized": output_dir / f"{stage}_normalized.json",
        "compact": output_dir / f"{stage}_compact.json",
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_json_compact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def write_stage_artifacts(
    base_dir: Path,
    run_date: str,
    artifact_subdir: str,
    stage: str,
    raw_payload: dict[str, Any],
    normalized_payload: dict[str, Any],
) -> dict[str, Path]:
    paths = build_stage_artifact_paths(base_dir, run_date, artifact_subdir, stage)
    write_json(paths["raw"], raw_payload)
    write_json(paths["normalized"], normalized_payload)
    write_json_compact(paths["compact"], compact_normalized_payload(normalized_payload))
    return paths


def load_normalized_records(base_dir: Path, run_date: str, artifact_subdir: str, stage: str) -> list[dict[str, Any]]:
    path = build_stage_artifact_paths(base_dir, run_date, artifact_subdir, stage)["normalized"]
    if not path.exists():
        raise FileNotFoundError(f"normalized artifact not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"normalized artifact missing records list: {path}")
    return records
