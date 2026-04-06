from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from scripts.common.feature_artifact_writer import load_normalized_records, write_stage_artifacts
from scripts.common.feature_image_resolver import resolve_images
from scripts.common.feature_llm_client import call_feature_llm
from scripts.common.feature_normalizer import normalize_stage_response
from scripts.common.feature_preflight_validator import load_prompt_spec
from scripts.common.feature_stage_gating import select_stage2_targets


def _resolve_project_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = base_dir / path
    return path


def _load_schema(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"response schema not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"response schema must be an object: {path}")
    return payload


def _serialize_prompt_rows(prompt_rows: list[dict[str, Any]], prompt_table_mode: str) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    if prompt_table_mode == "stage2_bundle":
        for row in prompt_rows:
            serialized.append(
                {
                    "bundle_id": row["bundle_id"],
                    "region_id": row["region_id"],
                    "region_label": row["region_label"],
                    "gate_signals": row["gate_signals"],
                    "question_text": row["question_text"],
                    "targets": [
                        {
                            "target_order": target["target_order"],
                            "target_label": target["target_label"],
                            "signal_key": target["signal_key"],
                            "attribute_key": target["attribute_key"],
                            "question_text": target["question_text"],
                            "allowed_answers": target["allowed_answers"],
                            "gate_on_stage1": target["gate_on_stage1"],
                            "gate_rule": target["gate_rule"],
                            "tier": target["tier"],
                            "gate_answer": target.get("gate_answer"),
                        }
                        for target in row["targets"]
                    ],
                }
            )
        return serialized
    for row in prompt_rows:
        serialized.append(
            {
                "question_id": row["question_id"],
                "region_id": row["region_id"],
                "region_label": row["region_label"],
                "signal_key": row["signal_key"],
                "attribute_key": row["attribute_key"],
                "question_text": row["question_text"],
                "allowed_answers": row["allowed_answers"],
            }
        )
    return serialized


def _build_row_prompt_text(manifest: dict[str, Any], image_info: dict[str, str], prompt_rows: list[dict[str, Any]]) -> str:
    question_lines: list[str] = []
    for index, row in enumerate(prompt_rows, start=1):
        allowed_answers = "|".join(str(item) for item in row["allowed_answers"])
        question_lines.append(
            "{idx}. question_id={question_id} | region_id={region_id} | region_label={region_label} | "
            "signal_key={signal_key} | attribute_key={attribute_key} | lat_range={lat_range} | lon_range={lon_range} | "
            "allowed_answers={allowed_answers} | question_text={question_text}".format(
                idx=index,
                question_id=row["question_id"],
                region_id=row["region_id"],
                region_label=row["region_label"],
                signal_key=row["signal_key"],
                attribute_key=row["attribute_key"],
                lat_range=row["lat_range"],
                lon_range=row["lon_range"],
                allowed_answers=allowed_answers,
                question_text=row["question_text"],
            )
        )
    return "\n".join(
        [
            "당신은 기상 feature 추출 보조자이다.",
            "첨부된 종관장 이미지를 보고 아래 질문에 답하라.",
            "각 question_id마다 allowed_answers 중 정확히 하나만 answer에 넣어라.",
            "answer는 allowed_answers 값을 그대로 복사해야 하며 변형하면 안 된다.",
            "불확실하고 unknown이 허용되면 unknown만 사용하라.",
            "JSON만 반환하고 schema를 정확히 따라라.",
            f"manifest_id={manifest['id']}",
            f"domain={manifest['domain']}",
            f"stage={manifest['stage']}",
            f"image_ref={image_info['image_ref']}",
            f"valid_time={image_info['valid_time']}",
            "Questions:",
            *question_lines,
        ]
    )


def _build_stage1_bundle_prompt_text(
    manifest: dict[str, Any],
    image_info: dict[str, str],
    prompt_rows: list[dict[str, Any]],
) -> str:
    question_lines: list[str] = []
    for index, row in enumerate(prompt_rows, start=1):
        allowed_answers = "|".join(str(item) for item in row["allowed_answers"])
        question_lines.append(
            "{idx}. question_id={question_id} | region_id={region_id} | region_label={region_label} | "
            "lat_range={lat_range} | lon_range={lon_range} | allowed_answers={allowed_answers} | "
            "question_text={question_text}".format(
                idx=index,
                question_id=row["question_id"],
                region_id=row["region_id"],
                region_label=row["region_label"],
                lat_range=row["lat_range"],
                lon_range=row["lon_range"],
                allowed_answers=allowed_answers,
                question_text=row["question_text"],
            )
        )
    return "\n".join(
        [
            "당신은 기상 feature 추출 보조자이다.",
            "첨부된 종관장 이미지를 보고 각 영역 내부에 존재하는 신호를 모두 선택하라.",
            "각 question_id마다 selected_answers 배열을 반드시 반환하라.",
            "selected_answers의 각 원소는 allowed_answers 값과 정확히 일치해야 한다.",
            "allowed_answers 목록에 없는 값은 절대로 만들지 마라.",
            "신호가 없으면 none만 단독으로 선택하라.",
            "none과 다른 신호를 함께 선택하면 안 된다.",
            "하나 이상의 실제 신호를 선택했다면 none은 절대로 포함하지 마라.",
            "selected_answers를 만들기 전에 'none 단독' 또는 '실제 신호 목록' 둘 중 하나만 가능한지 스스로 다시 확인하라.",
            "잘못된 예: ['none', 'ridge_axis']",
            "올바른 예 1: ['none']",
            "올바른 예 2: ['ridge_axis', 'warm_advection']",
            "JSON만 반환하고 schema를 정확히 따라라.",
            f"manifest_id={manifest['id']}",
            f"domain={manifest['domain']}",
            f"stage={manifest['stage']}",
            f"image_ref={image_info['image_ref']}",
            f"valid_time={image_info['valid_time']}",
            "Bundles:",
            *question_lines,
        ]
    )


def _build_stage2_bundle_prompt_text(
    manifest: dict[str, Any],
    image_info: dict[str, str],
    prompt_rows: list[dict[str, Any]],
) -> str:
    bundle_lines: list[str] = []
    for index, bundle in enumerate(prompt_rows, start=1):
        bundle_lines.append(
            "{idx}. bundle_id={bundle_id} | region_id={region_id} | region_label={region_label} | "
            "lat_range={lat_range} | lon_range={lon_range} | question_text={question_text} | gate_summary={gate_summary}".format(
                idx=index,
                bundle_id=bundle["bundle_id"],
                region_id=bundle["region_id"],
                region_label=bundle["region_label"],
                lat_range=bundle["lat_range"],
                lon_range=bundle["lon_range"],
                question_text=bundle["question_text"],
                gate_summary=json.dumps(bundle.get("gate_summary", {}), ensure_ascii=False, sort_keys=True),
            )
        )
        for target in bundle["targets"]:
            allowed_answers = "|".join(str(item) for item in target["allowed_answers"])
            bundle_lines.append(
                "   - target_label={target_label} | signal_key={signal_key} | attribute_key={attribute_key} | "
                "target_order={target_order} | tier={tier} | gate_on_stage1={gate_on_stage1} | "
                "gate_rule={gate_rule} | allowed_answers={allowed_answers} | question_text={question_text}".format(
                    target_label=target["target_label"],
                    signal_key=target["signal_key"],
                    attribute_key=target["attribute_key"],
                    target_order=target["target_order"],
                    tier=target["tier"],
                    gate_on_stage1=target["gate_on_stage1"],
                    gate_rule=target["gate_rule"],
                    allowed_answers=allowed_answers,
                    question_text=target["question_text"],
                )
            )
    return "\n".join(
        [
            "당신은 기상 feature 추출 보조자이다.",
            "첨부된 종관장 이미지를 보고 각 bundle_id에 속한 targets 전체에 대해 한 번에 답하라.",
            "반드시 bundle_answers 배열을 반환하라.",
            "각 bundle_answers 항목은 bundle_id를 그대로 유지하고, targets 배열에 target_label별 answer를 모두 포함해야 한다.",
            "answer는 각 target의 allowed_answers 값 중 하나를 정확히 복사해야 한다.",
            "JSON만 반환하고 schema를 정확히 따라라.",
            f"manifest_id={manifest['id']}",
            f"domain={manifest['domain']}",
            f"stage={manifest['stage']}",
            f"image_ref={image_info['image_ref']}",
            f"valid_time={image_info['valid_time']}",
            "Bundles:",
            *bundle_lines,
        ]
    )


def _build_prompt_text(manifest: dict[str, Any], image_info: dict[str, str], prompt_rows: list[dict[str, Any]]) -> str:
    prompt_table_mode = str(manifest.get("prompt_table_mode") or "row").strip()
    if prompt_table_mode == "stage1_bundle":
        return _build_stage1_bundle_prompt_text(manifest, image_info, prompt_rows)
    if prompt_table_mode == "stage2_bundle":
        return _build_stage2_bundle_prompt_text(manifest, image_info, prompt_rows)
    return _build_row_prompt_text(manifest, image_info, prompt_rows)


def _build_stage_summary(
    manifest: dict[str, Any],
    run_date: str,
    images: list[dict[str, str]],
    prompt_rows: list[dict[str, Any]],
    status: str,
    records: list[dict[str, Any]],
    raw_entries: list[dict[str, Any]],
    artifact_paths: dict[str, Path] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "manifest_id": manifest["id"],
        "domain": manifest["domain"],
        "stage": manifest["stage"],
        "run_date": run_date,
        "status": status,
        "image_count": len(images),
        "prompt_row_count": len(prompt_rows),
        "record_count": len(records),
        "invalid_answer_count": sum(0 if record.get("is_valid_answer") else 1 for record in records),
        "raw_entry_count": len(raw_entries),
        "prompt_table_mode": manifest.get("prompt_table_mode", "row"),
    }
    if artifact_paths:
        summary["raw_artifact"] = str(artifact_paths["raw"])
        summary["normalized_artifact"] = str(artifact_paths["normalized"])
    if extra:
        summary.update(extra)
    return summary


def _chunk_prompt_rows(prompt_rows: list[dict[str, Any]], max_items_per_request: int) -> list[list[dict[str, Any]]]:
    if max_items_per_request <= 0 or len(prompt_rows) <= max_items_per_request:
        return [prompt_rows]
    return [
        prompt_rows[index:index + max_items_per_request]
        for index in range(0, len(prompt_rows), max_items_per_request)
    ]


def run_manifest_bundle(
    base_dir: Path,
    run_date: str,
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    images = resolve_images(base_dir, run_date, manifest)
    prompt_spec = load_prompt_spec(base_dir, manifest)
    schema_path = _resolve_project_path(base_dir, str(manifest["response_schema_path"]))
    prompt_rows = prompt_spec["prompt_rows"]
    schema = _load_schema(schema_path)
    prompt_table_mode = str(manifest.get("prompt_table_mode") or "row").strip()

    if str(manifest["stage"]) == "stage2":
        stage1_records = load_normalized_records(base_dir, run_date, str(manifest["artifact_subdir"]), "stage1")
        match_keys = [str(key) for key in manifest["gating_match_keys"]]
        selected_rows_by_image: dict[str, list[dict[str, Any]]] = {}
        selected_question_count = 0
        selected_bundle_count = 0
        selected_image_count = 0
        for image_info in images:
            rows_for_image = select_stage2_targets(
                stage1_records,
                prompt_rows,
                match_keys,
                str(manifest["gating_answer"]),
                image_info["source_image"],
            )
            if rows_for_image:
                selected_rows_by_image[image_info["source_image"]] = rows_for_image
                if prompt_table_mode == "stage2_bundle":
                    selected_bundle_count += len(rows_for_image)
                    selected_question_count += sum(len(row["targets"]) for row in rows_for_image)
                else:
                    selected_question_count += len(rows_for_image)
                selected_image_count += 1
        if not selected_rows_by_image:
            raw_payload = {
                "manifest_id": manifest["id"],
                "domain": manifest["domain"],
                "stage": manifest["stage"],
                "run_date": run_date,
                "model": manifest["model"],
                "status": "no_targets",
                "gating_source": manifest["gating_source"],
                "prompt_table_mode": prompt_table_mode,
                "images": [],
            }
            normalized_payload = {
                "manifest_id": manifest["id"],
                "domain": manifest["domain"],
                "stage": manifest["stage"],
                "run_date": run_date,
                "model": manifest["model"],
                "status": "no_targets",
                "records": [],
            }
            artifact_paths = None if dry_run else write_stage_artifacts(
                base_dir,
                run_date,
                str(manifest["artifact_subdir"]),
                str(manifest["stage"]),
                raw_payload,
                normalized_payload,
            )
            return _build_stage_summary(
                manifest,
                run_date,
                images,
                prompt_rows,
                "no_targets" if not dry_run else "dry_run_no_targets",
                [],
                [],
                artifact_paths,
                {
                    "gating_source": manifest["gating_source"],
                    "selected_image_count": 0,
                    "selected_question_count": 0,
                    "selected_bundle_count": 0,
                },
            )
    else:
        selected_rows_by_image = {image_info["source_image"]: prompt_rows for image_info in images}
        selected_question_count = len(images) * (
            sum(len(row["targets"]) for row in prompt_rows) if prompt_table_mode == "stage2_bundle" else len(prompt_rows)
        )
        selected_bundle_count = len(images) * len(prompt_rows) if prompt_table_mode == "stage2_bundle" else 0
        selected_image_count = len(images)

    if dry_run:
        return _build_stage_summary(
            manifest,
            run_date,
            images,
            prompt_rows,
            "dry_run",
            [],
            [],
            None,
            {
                "gating_source": manifest.get("gating_source"),
                "selected_image_count": selected_image_count,
                "selected_question_count": selected_question_count,
                "selected_bundle_count": selected_bundle_count,
                "preflight_paths": {key: str(value) for key, value in prompt_spec["paths"].items()},
            },
        )

    raw_entries: list[dict[str, Any]] = []
    normalized_records: list[dict[str, Any]] = []
    raw_max_items = (
        manifest.get("max_bundles_per_request")
        if prompt_table_mode in {"stage1_bundle", "stage2_bundle"}
        else manifest.get("max_questions_per_request")
    )
    max_items_per_request = int(raw_max_items or 0)

    for image_info in images:
        rows_for_image = selected_rows_by_image.get(image_info["source_image"], [])
        if not rows_for_image:
            continue
        prompt_row_batches = _chunk_prompt_rows(rows_for_image, max_items_per_request)
        for batch_index, prompt_row_batch in enumerate(prompt_row_batches, start=1):
            prompt_text = _build_prompt_text(manifest, image_info, prompt_row_batch)
            llm_result = call_feature_llm(
                prompt_text=prompt_text,
                image_path=image_info["image_path"],
                schema=schema,
                model=str(manifest["model"]),
            )
            raw_entries.append(
                {
                    "source_image": image_info["source_image"],
                    "image_ref": image_info["image_ref"],
                    "valid_time": image_info["valid_time"],
                    "batch_index": batch_index,
                    "batch_count": len(prompt_row_batches),
                    "prompt_text_length": len(prompt_text),
                    "prompt_table_mode": prompt_table_mode,
                    "prompt_rows": _serialize_prompt_rows(prompt_row_batch, prompt_table_mode),
                    "llm_usage": llm_result["response"].get("usage", {}),
                    "llm_result": llm_result,
                }
            )
            normalized_records.extend(
                normalize_stage_response(
                    manifest=manifest,
                    image_info=image_info,
                    prompt_rows=prompt_row_batch,
                    parsed_output=llm_result["parsed_output"],
                )
            )

    raw_payload = {
        "manifest_id": manifest["id"],
        "domain": manifest["domain"],
        "stage": manifest["stage"],
        "run_date": run_date,
        "model": manifest["model"],
        "status": "completed",
        "gating_source": manifest.get("gating_source"),
        "prompt_table_mode": prompt_table_mode,
        "images": raw_entries,
    }
    normalized_payload = {
        "manifest_id": manifest["id"],
        "domain": manifest["domain"],
        "stage": manifest["stage"],
        "run_date": run_date,
        "model": manifest["model"],
        "status": "completed",
        "records": normalized_records,
    }
    artifact_paths = write_stage_artifacts(
        base_dir,
        run_date,
        str(manifest["artifact_subdir"]),
        str(manifest["stage"]),
        raw_payload,
        normalized_payload,
    )
    logging.info(
        "feature_stage_done: manifest=%s stage=%s domain=%s records=%s",
        manifest["id"],
        manifest["stage"],
        manifest["domain"],
        len(normalized_records),
    )
    return _build_stage_summary(
        manifest,
        run_date,
        images,
        prompt_rows,
        "completed",
        normalized_records,
        raw_entries,
        artifact_paths,
        {
            "gating_source": manifest.get("gating_source"),
            "selected_image_count": selected_image_count,
            "selected_question_count": selected_question_count,
            "selected_bundle_count": selected_bundle_count,
            "preflight_paths": {key: str(value) for key, value in prompt_spec["paths"].items()},
        },
    )


def run_manifest(
    base_dir: Path,
    run_date: str,
    manifest: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    return run_manifest_bundle(base_dir, run_date, manifest, dry_run=dry_run)
