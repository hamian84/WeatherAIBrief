from __future__ import annotations

from datetime import datetime
from typing import Any


def format_valid_time(value: str) -> str:
    if not value.isdigit():
        return value
    if len(value) == 10:
        dt = datetime.strptime(value, "%Y%m%d%H")
        return dt.strftime("%Y-%m-%d %H:00")
    if len(value) == 12:
        dt = datetime.strptime(value, "%Y%m%d%H%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    return value


def normalize_answer(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def build_stage1_bundle_records(
    manifest: dict[str, Any],
    image_info: dict[str, str],
    prompt_rows: list[dict[str, Any]],
    parsed_output: dict[str, Any],
) -> list[dict[str, Any]]:
    answers = parsed_output.get("answers")
    if not isinstance(answers, list):
        raise ValueError("parsed output missing answers list")
    answer_map: dict[str, dict[str, Any]] = {}
    for item in answers:
        if not isinstance(item, dict):
            raise ValueError("stage1 bundle answer item must be an object")
        question_id = str(item.get("question_id", "")).strip()
        if not question_id:
            raise ValueError("stage1 bundle answer item missing question_id")
        if question_id in answer_map:
            raise ValueError(f"duplicate stage1 bundle question_id: {question_id}")
        selected_answers = item.get("selected_answers")
        if not isinstance(selected_answers, list) or not selected_answers:
            raise ValueError(f"stage1 bundle answer '{question_id}' missing selected_answers list")
        if any(not isinstance(value, str) or not str(value).strip() for value in selected_answers):
            raise ValueError(f"stage1 bundle answer '{question_id}' has invalid selected_answers value")
        answer_map[question_id] = item

    expected_question_ids = {str(row["question_id"]) for row in prompt_rows}
    response_question_ids = set(answer_map)
    missing = sorted(expected_question_ids - response_question_ids)
    extra = sorted(response_question_ids - expected_question_ids)
    if missing:
        raise ValueError(f"stage1 bundle response missing question_ids: {missing}")
    if extra:
        raise ValueError(f"stage1 bundle response has unexpected question_ids: {extra}")

    records: list[dict[str, Any]] = []
    valid_time = format_valid_time(image_info.get("valid_time", ""))
    for row in prompt_rows:
        question_id = str(row["question_id"])
        answer_item = answer_map[question_id]
        raw_selected_answers = [str(item).strip() for item in answer_item.get("selected_answers", [])]
        selected_answers: list[str] = []
        seen_selected: set[str] = set()
        for answer in raw_selected_answers:
            normalized = answer.lower()
            if normalized not in seen_selected:
                seen_selected.add(normalized)
                selected_answers.append(answer)
        allowed_answers = [str(item).strip() for item in row["allowed_answers"]]
        allowed_answer_set = {item.lower() for item in allowed_answers}
        invalid_answers = [item for item in selected_answers if item.lower() not in allowed_answer_set]
        if invalid_answers:
            raise ValueError(f"stage1 bundle response has invalid selected_answers {invalid_answers} for {question_id}")
        if "none" in {item.lower() for item in selected_answers} and len(selected_answers) > 1:
            raise ValueError(f"stage1 bundle response must not mix 'none' with other answers for {question_id}")
        selected_signal_keys = {
            item.lower()
            for item in selected_answers
            if item.lower() != "none"
        }
        for signal_key in [str(item).strip() for item in row["signal_options"]]:
            model_answer_raw = "yes" if signal_key.lower() in selected_signal_keys else "no"
            records.append(
                {
                    "manifest_id": str(manifest["id"]),
                    "domain": str(manifest["domain"]),
                    "stage": str(manifest["stage"]),
                    "valid_time": valid_time,
                    "question_id": f"{question_id}__{signal_key}",
                    "bundle_question_id": question_id,
                    "region_id": str(row["region_id"]),
                    "region_label": str(row["region_label"]),
                    "signal_key": signal_key,
                    "attribute_key": "presence",
                    "question_text": str(row["question_text"]),
                    "allowed_answers": ["yes", "no"],
                    "bundle_allowed_answers": allowed_answers,
                    "model_answer_raw": model_answer_raw,
                    "model_answer_normalized": model_answer_raw,
                    "is_valid_answer": True,
                    "source_image": image_info.get("source_image", ""),
                    "image_ref": image_info.get("image_ref", ""),
                    "note": str(answer_item.get("note", "")).strip(),
                    "selected_answers": selected_answers,
                    "selected_signal_keys": sorted(selected_signal_keys),
                    "none_selected": "none" in {item.lower() for item in selected_answers},
                }
            )
    return records


def build_stage2_bundle_records(
    manifest: dict[str, Any],
    image_info: dict[str, str],
    prompt_rows: list[dict[str, Any]],
    parsed_output: dict[str, Any],
) -> list[dict[str, Any]]:
    bundle_answers = parsed_output.get("bundle_answers")
    if not isinstance(bundle_answers, list):
        raise ValueError("parsed output missing bundle_answers list")

    answer_map: dict[str, dict[str, Any]] = {}
    for item in bundle_answers:
        if not isinstance(item, dict):
            raise ValueError("stage2 bundle answer item must be an object")
        bundle_id = str(item.get("bundle_id", "")).strip()
        if not bundle_id:
            raise ValueError("stage2 bundle answer item missing bundle_id")
        if bundle_id in answer_map:
            raise ValueError(f"duplicate stage2 bundle_id: {bundle_id}")
        targets = item.get("targets")
        if not isinstance(targets, list):
            raise ValueError(f"stage2 bundle '{bundle_id}' missing targets list")
        answer_map[bundle_id] = item

    expected_bundle_ids = {str(bundle["bundle_id"]) for bundle in prompt_rows}
    response_bundle_ids = set(answer_map)
    missing = sorted(expected_bundle_ids - response_bundle_ids)
    extra = sorted(response_bundle_ids - expected_bundle_ids)
    if missing:
        raise ValueError(f"stage2 bundle response missing bundle_ids: {missing}")
    if extra:
        raise ValueError(f"stage2 bundle response has unexpected bundle_ids: {extra}")

    valid_time = format_valid_time(image_info.get("valid_time", ""))
    records: list[dict[str, Any]] = []
    for bundle in prompt_rows:
        bundle_id = str(bundle["bundle_id"])
        answer_bundle = answer_map[bundle_id]
        target_map: dict[str, dict[str, Any]] = {}
        for target in answer_bundle.get("targets", []):
            if not isinstance(target, dict):
                raise ValueError(f"stage2 bundle '{bundle_id}' target must be an object")
            target_label = str(target.get("target_label", "")).strip()
            if not target_label:
                raise ValueError(f"stage2 bundle '{bundle_id}' target missing target_label")
            if target_label in target_map:
                raise ValueError(f"duplicate target_label '{target_label}' in bundle '{bundle_id}'")
            answer = target.get("answer")
            if not isinstance(answer, str) or not str(answer).strip():
                raise ValueError(f"stage2 bundle '{bundle_id}' target '{target_label}' missing answer")
            target_map[target_label] = target
        expected_target_labels = {str(target["target_label"]) for target in bundle.get("targets", [])}
        response_target_labels = set(target_map)
        missing_targets = sorted(expected_target_labels - response_target_labels)
        extra_targets = sorted(response_target_labels - expected_target_labels)
        if missing_targets:
            raise ValueError(f"stage2 bundle '{bundle_id}' missing target_labels: {missing_targets}")
        if extra_targets:
            raise ValueError(f"stage2 bundle '{bundle_id}' has unexpected target_labels: {extra_targets}")
        for target in bundle.get("targets", []):
            target_label = str(target["target_label"])
            answer_item = target_map[target_label]
            answer_raw = str(answer_item.get("answer", "")).strip()
            allowed_answers = [str(item).strip() for item in target["allowed_answers"]]
            record = {
                    "manifest_id": str(manifest["id"]),
                    "domain": str(manifest["domain"]),
                    "stage": str(manifest["stage"]),
                    "valid_time": valid_time,
                    "question_id": f"{bundle_id}__{target_label}",
                    "bundle_id": bundle_id,
                    "region_id": str(bundle["region_id"]),
                    "region_label": str(bundle["region_label"]),
                    "signal_key": str(target["signal_key"]),
                    "attribute_key": str(target["attribute_key"]),
                    "target_label": target_label,
                    "question_text": str(target["question_text"]),
                    "bundle_question_text": str(bundle["question_text"]),
                    "allowed_answers": allowed_answers,
                    "model_answer_raw": answer_raw,
                    "model_answer_normalized": normalize_answer(answer_raw),
                    "is_valid_answer": normalize_answer(answer_raw) in {item.lower() for item in allowed_answers},
                    "source_image": image_info.get("source_image", ""),
                    "image_ref": image_info.get("image_ref", ""),
                    "note": str(answer_item.get("note", "")).strip(),
                    "bundle_note": str(answer_bundle.get("note", "")).strip(),
                    "target_order": int(target["target_order"]),
                    "tier": str(target["tier"]),
                    "gate_on_stage1": str(target["gate_on_stage1"]),
                    "gate_rule": str(target["gate_rule"]),
                    "gate_answer": str(target.get("gate_answer", "")),
                    "gating_selected": True,
                }
            if not bool(record["is_valid_answer"]):
                raise ValueError(
                    f"stage2 bundle response has invalid answer '{record['model_answer_raw']}' for "
                    f"bundle_id={bundle_id}, target_label={target_label}"
                )
            records.append(record)
    return records
