from __future__ import annotations

from typing import Any, Iterable


def _split_answers(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().lower() for item in str(value).split("|") if item.strip()}


def select_stage2_targets(
    stage1_records: Iterable[dict[str, Any]],
    stage2_prompt_rows: Iterable[dict[str, Any]],
    match_keys: list[str],
    accepted_answers_text: str | None,
    source_image: str,
) -> list[dict[str, Any]]:
    if not match_keys:
        raise ValueError("gating_match_keys must not be empty")
    accepted_answers = _split_answers(accepted_answers_text)
    if not accepted_answers:
        raise ValueError("gating_answer must not be empty")
    selected_keys: set[tuple[str, ...]] = set()
    for record in stage1_records:
        if str(record.get("source_image", "")) != source_image:
            continue
        if not bool(record.get("is_valid_answer", False)):
            continue
        answer = str(record.get("model_answer_normalized", "")).strip().lower()
        if answer not in accepted_answers:
            continue
        selected_keys.add(tuple(str(record.get(key, "")).strip() for key in match_keys))
    selected_rows: list[dict[str, Any]] = []
    for row in stage2_prompt_rows:
        row_key = tuple(str(row.get(key, "")).strip() for key in match_keys)
        if row_key in selected_keys:
            selected_rows.append(dict(row))
    return selected_rows
