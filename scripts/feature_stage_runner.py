from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from scripts.common.feature_artifact_writer import load_normalized_records, write_stage_artifacts
from scripts.common.feature_image_resolver import resolve_images
from scripts.common.feature_llm_client import call_feature_llm
from scripts.common.feature_manifest_loader import load_manifest
from scripts.common.feature_normalizer import normalize_stage_response
from scripts.common.feature_prompt_table_loader import load_prompt_table
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


def _serialize_prompt_rows(prompt_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
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


def _build_prompt_text(manifest: dict[str, Any], image_info: dict[str, str], prompt_rows: list[dict[str, Any]]) -> str:
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
            "You are a weather feature extraction assistant.",
            "Analyze the attached synoptic chart image.",
            "Answer every question below with exactly one value from the allowed_answers list.",
            "The answer field must copy one allowed_answers value verbatim.",
            "Do not paraphrase, translate, inflect, pluralize, add adverbs, or add explanatory words.",
            "If the best label is 'continuous', return 'continuous' exactly, not 'continuously'.",
            "If you are uncertain, return 'unknown' exactly when 'unknown' is allowed.",
            "Before returning JSON, verify that every answer value exactly matches one of the allowed_answers values for that question.",
            "Keep question_id unchanged.",
            "Return JSON only that matches the provided schema.",
            f"manifest_id={manifest['id']}",
            f"domain={manifest['domain']}",
            f"stage={manifest['stage']}",
            f"image_ref={image_info['image_ref']}",
            f"valid_time={image_info['valid_time']}",
            "Questions:",
            *question_lines,
        ]
    )


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
    }
    if artifact_paths:
        summary["raw_artifact"] = str(artifact_paths["raw"])
        summary["normalized_artifact"] = str(artifact_paths["normalized"])
    if extra:
        summary.update(extra)
    return summary




def _chunk_prompt_rows(prompt_rows: list[dict[str, Any]], max_questions_per_request: int) -> list[list[dict[str, Any]]]:
    if max_questions_per_request <= 0 or len(prompt_rows) <= max_questions_per_request:
        return [prompt_rows]
    return [
        prompt_rows[index:index + max_questions_per_request]
        for index in range(0, len(prompt_rows), max_questions_per_request)
    ]

def run_manifest(base_dir: Path, run_date: str, manifest_path: str | Path, dry_run: bool = False) -> dict[str, Any]:
    manifest_file = Path(manifest_path)
    manifest = load_manifest(manifest_file)
    images = resolve_images(base_dir, run_date, manifest)
    prompt_table_path = _resolve_project_path(base_dir, str(manifest["prompt_table_path"]))
    schema_path = _resolve_project_path(base_dir, str(manifest["response_schema_path"]))
    prompt_rows = load_prompt_table(prompt_table_path)
    schema = _load_schema(schema_path)

    if str(manifest["stage"]) == "stage2":
        stage1_records = load_normalized_records(base_dir, run_date, str(manifest["artifact_subdir"]), "stage1")
        match_keys = [str(key) for key in manifest["gating_match_keys"]]
        selected_rows_by_image: dict[str, list[dict[str, Any]]] = {}
        selected_question_count = 0
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
                },
            )
    else:
        selected_rows_by_image = {image_info["source_image"]: prompt_rows for image_info in images}
        selected_question_count = len(images) * len(prompt_rows)
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
            },
        )

    raw_entries: list[dict[str, Any]] = []
    normalized_records: list[dict[str, Any]] = []
    max_questions_per_request = int(manifest.get("max_questions_per_request") or 0)

    for image_info in images:
        rows_for_image = selected_rows_by_image.get(image_info["source_image"], [])
        if not rows_for_image:
            continue
        prompt_row_batches = _chunk_prompt_rows(rows_for_image, max_questions_per_request)
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
                    "prompt_rows": _serialize_prompt_rows(prompt_row_batch),
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
        },
    )

