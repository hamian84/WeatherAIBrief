from __future__ import annotations

from datetime import datetime
from typing import Any


def _format_valid_time(value: str) -> str:
    if not value.isdigit():
        return value
    if len(value) == 10:
        dt = datetime.strptime(value, "%Y%m%d%H")
        return dt.strftime("%Y-%m-%d %H:00")
    if len(value) == 12:
        dt = datetime.strptime(value, "%Y%m%d%H%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    return value


def _normalize_answer(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _build_answer_map(parsed_output: dict[str, Any]) -> dict[str, dict[str, Any]]:
    answers = parsed_output.get("answers")
    if not isinstance(answers, list):
        raise ValueError("parsed output missing answers list")
    mapped: dict[str, dict[str, Any]] = {}
    for item in answers:
        if not isinstance(item, dict):
            raise ValueError("answer item must be an object")
        question_id = str(item.get("question_id", "")).strip()
        if not question_id:
            raise ValueError("answer item missing question_id")
        if question_id in mapped:
            raise ValueError(f"duplicate answer question_id: {question_id}")
        mapped[question_id] = item
    return mapped


def normalize_stage_response(
    manifest: dict[str, Any],
    image_info: dict[str, str],
    prompt_rows: list[dict[str, Any]],
    parsed_output: dict[str, Any],
) -> list[dict[str, Any]]:
    answer_map = _build_answer_map(parsed_output)
    expected_question_ids = {str(row["question_id"]) for row in prompt_rows}
    response_question_ids = set(answer_map)
    missing = sorted(expected_question_ids - response_question_ids)
    extra = sorted(response_question_ids - expected_question_ids)
    if missing:
        raise ValueError(f"response missing question_ids: {missing}")
    if extra:
        raise ValueError(f"response has unexpected question_ids: {extra}")

    records: list[dict[str, Any]] = []
    for row in prompt_rows:
        question_id = str(row["question_id"])
        answer_item = answer_map[question_id]
        model_answer_raw = str(answer_item.get("answer", "")).strip()
        model_answer_normalized = _normalize_answer(model_answer_raw)
        allowed_answers = [str(item).strip() for item in row["allowed_answers"]]
        allowed_answer_set = {item.lower() for item in allowed_answers}
        record: dict[str, Any] = {
            "manifest_id": str(manifest["id"]),
            "domain": str(manifest["domain"]),
            "stage": str(manifest["stage"]),
            "valid_time": _format_valid_time(image_info.get("valid_time", "")),
            "question_id": question_id,
            "region_id": str(row["region_id"]),
            "region_label": str(row["region_label"]),
            "signal_key": str(row["signal_key"]),
            "attribute_key": str(row["attribute_key"]),
            "question_text": str(row["question_text"]),
            "allowed_answers": allowed_answers,
            "model_answer_raw": model_answer_raw,
            "model_answer_normalized": model_answer_normalized,
            "is_valid_answer": model_answer_normalized in allowed_answer_set,
            "source_image": image_info.get("source_image", ""),
            "image_ref": image_info.get("image_ref", ""),
            "note": str(answer_item.get("note", "")).strip(),
        }
        if str(manifest["stage"]) == "stage2":
            record["gating_selected"] = True
        records.append(record)
    return records
