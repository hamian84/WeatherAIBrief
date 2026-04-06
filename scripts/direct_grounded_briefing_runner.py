from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from scripts.common.card_manifest_loader import load_card_manifest, resolve_manifest_path
from scripts.common.briefing_priority_builder import build_briefing_priority_summary
from scripts.common.direct_briefing_inputs import build_direct_briefing_inputs
from scripts.common.findings_llm_client import call_findings_llm
from scripts.common.rule_pack_loader import load_rule_pack
from scripts.common.schema_utils import load_json_schema, validate_instance

KST = ZoneInfo("Asia/Seoul")
SECTION_IDS = (
    "overall_summary",
    "synoptic_overview",
    "precipitation_structure",
    "surface_marine_impacts",
    "review_draft",
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines: list[str] = [f"# Direct Grounded Briefing Draft ({payload['run_date']})", ""]
    for section in payload.get("sections", []) or []:
        if not isinstance(section, dict):
            continue
        lines.append(f"## {section['title']}")
        lines.append(str(section["text"]).strip())
        lines.append("")
        lines.append(f"- focus_regions: {', '.join(section['focus_regions'])}")
        lines.append(f"- evidence_ids: {', '.join(section['evidence_ids'])}")
        lines.append(f"- evidence_refs: {', '.join(section['evidence_refs'])}")
        lines.append(f"- rule_refs: {', '.join(section['rule_refs'])}")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _collect_evidence_index(prompt_input: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = [item for item in prompt_input.get("image_feature_signal_summary", []) if isinstance(item, dict)]
    evidence_by_id: dict[str, dict[str, Any]] = {}
    for item in items:
        evidence_id = str(item.get("evidence_id", "")).strip()
        if evidence_id:
            evidence_by_id[evidence_id] = item
    if not evidence_by_id:
        raise ValueError("image_feature_signal_summary does not contain usable evidence items")
    return evidence_by_id


def _collect_rule_index(rule_pack: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rules = [item for item in rule_pack.get("rules", []) if isinstance(item, dict)]
    rule_by_id: dict[str, dict[str, Any]] = {}
    for item in rules:
        rule_id = str(item.get("rule_id", "")).strip()
        if rule_id:
            rule_by_id[rule_id] = item
    if not rule_by_id:
        raise ValueError("rule_pack does not contain usable rules")
    return rule_by_id


def _render_prompt(template_text: str, prompt_input: dict[str, Any]) -> str:
    replacements = {
        "{{RUN_DATE}}": str(prompt_input["run_date"]),
        "{{ALLOWED_EVIDENCE_IDS_JSON}}": json.dumps(prompt_input["allowed_evidence_ids"], ensure_ascii=False, indent=2),
        "{{ALLOWED_EVIDENCE_REFS_JSON}}": json.dumps(prompt_input["allowed_evidence_refs"], ensure_ascii=False, indent=2),
        "{{ALLOWED_RULE_REFS_JSON}}": json.dumps(prompt_input["allowed_rule_refs"], ensure_ascii=False, indent=2),
        "{{ALLOWED_REGIONS_JSON}}": json.dumps(prompt_input["allowed_regions"], ensure_ascii=False, indent=2),
        "{{FEATURE_BUNDLE_SUMMARY_JSON}}": json.dumps(prompt_input["feature_bundle_summary"], ensure_ascii=False, indent=2),
        "{{IMAGE_FEATURE_SIGNAL_SUMMARY_JSON}}": json.dumps(prompt_input["image_feature_signal_summary"], ensure_ascii=False, indent=2),
        "{{BRIEFING_PRIORITY_SUMMARY_JSON}}": json.dumps(prompt_input["briefing_priority_summary"], ensure_ascii=False, indent=2),
        "{{RULE_PACK_JSON}}": json.dumps(prompt_input["rule_pack"], ensure_ascii=False, indent=2),
    }
    rendered = template_text
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def _require_nonempty_string(field_name: str, value: Any, context: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{context}: required field is empty: {field_name}")
    return text


def _require_allowed_strings(field_name: str, values: Any, allowed: set[str], context: str) -> list[str]:
    if not isinstance(values, list):
        raise ValueError(f"{context}: field must be a list: {field_name}")
    items: list[str] = []
    invalid: list[str] = []
    for index, value in enumerate(values):
        text = str(value).strip()
        if not text:
            raise ValueError(f"{context}: empty string is not allowed: {field_name}[{index}]")
        if text not in allowed:
            invalid.append(text)
        if text not in items:
            items.append(text)
    if invalid:
        raise ValueError(f"{context}: unsupported values in {field_name}: {invalid}")
    if not items:
        raise ValueError(f"{context}: field must not be empty: {field_name}")
    return items


def _collect_allowed_section_refs(
    evidence_ids: list[str],
    evidence_by_id: dict[str, dict[str, Any]],
) -> tuple[set[str], set[str]]:
    refs: set[str] = set()
    regions: set[str] = set()
    for evidence_id in evidence_ids:
        evidence = evidence_by_id.get(evidence_id)
        if not isinstance(evidence, dict):
            raise ValueError(f"selected evidence_id not found: {evidence_id}")
        for ref in evidence.get("active_image_refs", []) or []:
            text = str(ref).strip()
            if text:
                refs.add(text)
        for region in evidence.get("region_labels", []) or []:
            text = str(region).strip()
            if text:
                regions.add(text)
    if not refs:
        raise ValueError("selected evidence_ids do not provide evidence_refs")
    if not regions:
        raise ValueError("selected evidence_ids do not provide regions")
    return refs, regions


def _augment_evidence_ids_for_refs(
    evidence_ids: list[str],
    evidence_refs: list[str],
    evidence_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    covered_refs, _ = _collect_allowed_section_refs(evidence_ids, evidence_by_id)
    augmented = list(evidence_ids)
    missing_refs = [ref for ref in evidence_refs if ref not in covered_refs]
    if not missing_refs:
        return augmented

    for missing_ref in missing_refs:
        candidates: list[str] = []
        for candidate_id, evidence in evidence_by_id.items():
            refs = {
                str(ref).strip()
                for ref in evidence.get("active_image_refs", []) or []
                if str(ref).strip()
            }
            if missing_ref in refs and candidate_id not in augmented:
                candidates.append(candidate_id)
        for candidate_id in sorted(candidates):
            augmented.append(candidate_id)
            covered_refs, _ = _collect_allowed_section_refs(augmented, evidence_by_id)
            if missing_ref in covered_refs:
                break
    return augmented


def _sanitize_section(
    parsed: dict[str, Any],
    section_id: str,
    evidence_by_id: dict[str, dict[str, Any]],
    rule_by_id: dict[str, dict[str, Any]],
    allowed_regions: set[str],
    allowed_evidence_refs: set[str],
) -> dict[str, Any]:
    context = f"direct_grounded_briefing[{section_id}]"
    evidence_ids = _require_allowed_strings("evidence_ids", parsed.get("evidence_ids", []), set(evidence_by_id.keys()), context)
    evidence_refs = _require_allowed_strings("evidence_refs", parsed.get("evidence_refs", []), allowed_evidence_refs, context)
    evidence_ids = _augment_evidence_ids_for_refs(evidence_ids, evidence_refs, evidence_by_id)
    _, selected_regions = _collect_allowed_section_refs(evidence_ids, evidence_by_id)
    focus_regions = _require_allowed_strings("focus_regions", parsed.get("focus_regions", []), allowed_regions, context)
    invalid_focus_regions = [item for item in focus_regions if item not in selected_regions]
    if invalid_focus_regions:
        raise ValueError(f"{context}: focus_regions must be covered by selected evidence_ids: {invalid_focus_regions}")
    rule_refs = _require_allowed_strings("rule_refs", parsed.get("rule_refs", []), set(rule_by_id.keys()), context)
    return {
        "section_id": section_id,
        "title": _require_nonempty_string("title", parsed.get("title"), context),
        "text": _require_nonempty_string("text", parsed.get("text"), context),
        "focus_regions": focus_regions,
        "evidence_ids": evidence_ids,
        "evidence_refs": evidence_refs,
        "rule_refs": rule_refs,
    }


def run_direct_grounded_briefing_stage(
    base_dir: Path,
    run_date: str,
    manifest_path: str | Path,
    model: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    manifest = load_card_manifest(manifest_path)
    output_schema = load_json_schema(resolve_manifest_path(base_dir, manifest["schema_path"], run_date))

    feature_bundle_path = resolve_manifest_path(base_dir, manifest["input_paths"]["feature_bundle"], run_date)
    image_feature_cards_path = resolve_manifest_path(base_dir, manifest["input_paths"]["image_feature_cards"], run_date)
    rule_pack = load_rule_pack(
        base_dir,
        manifest["input_paths"]["rule_pack"],
        manifest["input_paths"]["rule_pack_schema"],
    )
    direct_inputs = build_direct_briefing_inputs(base_dir, feature_bundle_path, image_feature_cards_path)
    evidence_by_id = _collect_evidence_index(direct_inputs)
    rule_by_id = _collect_rule_index(rule_pack)
    allowed_evidence_ids = sorted(evidence_by_id.keys())
    allowed_evidence_refs = sorted({ref for item in evidence_by_id.values() for ref in item.get("active_image_refs", []) if str(ref).strip()})
    allowed_rule_refs = sorted(rule_by_id.keys())
    allowed_regions = sorted({region for item in evidence_by_id.values() for region in item.get("region_labels", []) if str(region).strip()})

    briefing_priority_summary = build_briefing_priority_summary(
        direct_inputs["feature_bundle_summary"],
        direct_inputs["image_feature_signal_summary"],
    )
    prompt_input = {
        "run_date": run_date,
        "allow_new_claims": False,
        "feature_bundle_path": str(feature_bundle_path.relative_to(base_dir)).replace("\\", "/"),
        "image_feature_cards_path": str(image_feature_cards_path.relative_to(base_dir)).replace("\\", "/"),
        "rule_pack_path": str(Path(rule_pack["_meta"]["rule_pack_path"]).relative_to(base_dir)).replace("\\", "/"),
        "feature_bundle_summary": direct_inputs["feature_bundle_summary"],
        "image_feature_signal_summary": direct_inputs["image_feature_signal_summary"],
        "briefing_priority_summary": briefing_priority_summary,
        "rule_pack": rule_pack,
        "allowed_evidence_ids": allowed_evidence_ids,
        "allowed_evidence_refs": allowed_evidence_refs,
        "allowed_rule_refs": allowed_rule_refs,
        "allowed_regions": allowed_regions,
    }
    priority_summary_path = resolve_manifest_path(base_dir, manifest["output_paths"]["priority_summary"], run_date)
    prompt_input_path = resolve_manifest_path(base_dir, manifest["output_paths"]["prompt_input"], run_date)
    _write_json(priority_summary_path, briefing_priority_summary)
    _write_json(prompt_input_path, prompt_input)

    if dry_run:
        return {
            "status": "dry_run",
            "run_date": run_date,
            "priority_summary_path": str(priority_summary_path.relative_to(base_dir)).replace("\\", "/"),
            "prompt_input_path": str(prompt_input_path.relative_to(base_dir)).replace("\\", "/"),
            "evidence_item_count": len(allowed_evidence_ids),
            "rule_count": len(allowed_rule_refs),
            "region_count": len(allowed_regions),
        }

    prompt_template_path = base_dir / "prompts" / "templates" / "direct_grounded_briefing_prompt.txt"
    prompt_text = _render_prompt(prompt_template_path.read_text(encoding="utf-8-sig"), prompt_input)
    effective_model = (model or manifest.get("model") or "gpt-5.4-mini").strip()
    llm_result = call_findings_llm(prompt_text=prompt_text, schema=output_schema, model=effective_model, max_output_tokens=9000)

    parsed_sections = [item for item in llm_result["parsed_output"].get("sections", []) if isinstance(item, dict)]
    parsed_by_id = {str(item.get("section_id", "")).strip(): item for item in parsed_sections if str(item.get("section_id", "")).strip()}
    extra_sections = sorted(section_id for section_id in parsed_by_id if section_id not in SECTION_IDS)
    if extra_sections:
        raise ValueError(f"unexpected section_id in direct grounded briefing output: {extra_sections}")

    section_item_schema = output_schema["properties"]["sections"]["items"]
    sections: list[dict[str, Any]] = []
    for section_id in SECTION_IDS:
        parsed = parsed_by_id.get(section_id)
        if parsed is None:
            raise ValueError(f"direct grounded briefing missing section_id={section_id}")
        section = _sanitize_section(
            parsed,
            section_id,
            evidence_by_id,
            rule_by_id,
            set(allowed_regions),
            set(allowed_evidence_refs),
        )
        validate_instance(section, section_item_schema, f"direct_grounded_briefing[{section_id}]")
        sections.append(section)

    payload = {
        "version": "direct_grounded_briefing_draft.v1",
        "run_date": run_date,
        "allow_new_claims": False,
        "sections": sections,
    }
    validate_instance(payload, output_schema, "direct_grounded_briefing")

    raw_path = resolve_manifest_path(base_dir, manifest["output_paths"]["raw"], run_date)
    draft_path = resolve_manifest_path(base_dir, manifest["output_paths"]["draft"], run_date)
    markdown_path = resolve_manifest_path(base_dir, manifest["output_paths"]["markdown"], run_date)
    _write_json(
        raw_path,
        {
            "run_date": run_date,
            "generated_at_kst": datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S"),
            "model": effective_model,
            "prompt_input_path": str(prompt_input_path.relative_to(base_dir)).replace("\\", "/"),
            "llm_result": llm_result,
        },
    )
    _write_json(draft_path, payload)
    _write_markdown(markdown_path, payload)
    return {
        "status": "ok",
        "run_date": run_date,
        "priority_summary_path": str(priority_summary_path.relative_to(base_dir)).replace("\\", "/"),
        "prompt_input_path": str(prompt_input_path.relative_to(base_dir)).replace("\\", "/"),
        "raw_path": str(raw_path.relative_to(base_dir)).replace("\\", "/"),
        "draft_path": str(draft_path.relative_to(base_dir)).replace("\\", "/"),
        "markdown_path": str(markdown_path.relative_to(base_dir)).replace("\\", "/"),
        "section_count": len(sections),
        "model": effective_model,
    }
