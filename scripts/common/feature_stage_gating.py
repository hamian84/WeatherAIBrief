from __future__ import annotations

from typing import Any, Iterable

ALLOWED_GATE_RULES = {
    "full_if_yes_core_if_unknown_skip_if_no",
    "full_if_yes_extended_if_unknown_skip_if_no",
}


def _normalize_gate_answer(value: str | None) -> str:
    return str(value or "").strip().lower()


def _resolve_gate_decision(stage1_answer: str, gate_rule: str, tier: str) -> bool:
    normalized_rule = str(gate_rule).strip()
    if normalized_rule not in ALLOWED_GATE_RULES:
        raise ValueError(f"unsupported gate_rule '{gate_rule}'")
    normalized_answer = _normalize_gate_answer(stage1_answer)
    normalized_tier = str(tier).strip().lower()
    if normalized_answer == "yes":
        return True
    if normalized_answer == "unknown":
        return normalized_tier == "core"
    if normalized_answer == "no":
        return False
    raise ValueError(f"unsupported stage1 gate answer '{stage1_answer}'")


def _build_stage1_gate_map(
    stage1_records: Iterable[dict[str, Any]],
    source_image: str,
) -> dict[tuple[str, str], str]:
    gate_map: dict[tuple[str, str], str] = {}
    for record in stage1_records:
        if str(record.get("source_image", "")) != source_image:
            continue
        if not bool(record.get("is_valid_answer", False)):
            continue
        key = (str(record.get("region_id", "")).strip(), str(record.get("signal_key", "")).strip())
        answer = _normalize_gate_answer(str(record.get("model_answer_normalized", "")))
        if not key[0] or not key[1]:
            continue
        gate_map[key] = answer
    return gate_map


def _resolve_composite_gate_answer(gate_map: dict[tuple[str, str], str], region_id: str, signals: list[str]) -> str:
    answers = [
        gate_map.get((region_id, str(signal).strip()), "no")
        for signal in signals
    ]
    if any(answer == "yes" for answer in answers):
        return "yes"
    if any(answer == "unknown" for answer in answers):
        return "unknown"
    return "no"


def select_stage2_targets(
    stage1_records: Iterable[dict[str, Any]],
    stage2_prompt_rows: Iterable[dict[str, Any]],
    match_keys: list[str],
    accepted_answers_text: str | None,
    source_image: str,
) -> list[dict[str, Any]]:
    if not match_keys:
        raise ValueError("gating_match_keys must not be empty")
    stage2_rows = [dict(row) for row in stage2_prompt_rows]
    if not stage2_rows:
        return []

    first_row = stage2_rows[0]
    if "targets" not in first_row:
        accepted_answers = {
            item.strip().lower()
            for item in str(accepted_answers_text or "").split("|")
            if item.strip()
        }
        if not accepted_answers:
            raise ValueError("gating_answer must not be empty")
        selected_keys: set[tuple[str, ...]] = set()
        for record in stage1_records:
            if str(record.get("source_image", "")) != source_image:
                continue
            if not bool(record.get("is_valid_answer", False)):
                continue
            answer = _normalize_gate_answer(str(record.get("model_answer_normalized", "")))
            if answer not in accepted_answers:
                continue
            selected_keys.add(tuple(str(record.get(key, "")).strip() for key in match_keys))
        selected_rows: list[dict[str, Any]] = []
        for row in stage2_rows:
            row_key = tuple(str(row.get(key, "")).strip() for key in match_keys)
            if row_key in selected_keys:
                selected_rows.append(row)
        return selected_rows

    gate_map = _build_stage1_gate_map(stage1_records, source_image)
    selected_bundles: list[dict[str, Any]] = []
    for bundle in stage2_rows:
        region_id = str(bundle.get("region_id", "")).strip()
        selected_targets: list[dict[str, Any]] = []
        gate_summary: dict[str, str] = {}
        for target in bundle.get("targets", []):
            gate_signals = [str(signal).strip() for signal in target.get("gate_on_stage1_signals", [])]
            signal_key = str(target.get("gate_on_stage1", "")).strip()
            gate_answer = _resolve_composite_gate_answer(gate_map, region_id, gate_signals or [signal_key])
            gate_summary[signal_key] = gate_answer
            if _resolve_gate_decision(gate_answer, str(target.get("gate_rule", "")), str(target.get("tier", ""))):
                enriched_target = dict(target)
                enriched_target["gate_answer"] = gate_answer
                selected_targets.append(enriched_target)
        if selected_targets:
            selected_bundle = dict(bundle)
            selected_bundle["targets"] = selected_targets
            selected_bundle["gate_summary"] = gate_summary
            selected_bundle["selected_target_count"] = len(selected_targets)
            selected_bundles.append(selected_bundle)
    return selected_bundles
