from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from scripts.common.card_manifest_loader import load_card_manifest, resolve_manifest_path
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
SECTION_TITLES = {
    "overall_summary": "전체 개황",
    "synoptic_overview": "종관 해석",
    "precipitation_structure": "강수 구조 해석",
    "surface_marine_impacts": "지상 및 해상 영향 해석",
    "review_draft": "검토용 초안",
}
SECTION_GUIDANCE = {
    "overall_summary": (
        "이번 사례를 하나의 종합 사건으로 규정한다. 상층 제트와 기압골, 중층 순환, 저층 습윤 수송, "
        "지상 전선과 저기압을 하나의 구조로 묶어 핵심 축을 먼저 제시한다."
    ),
    "synoptic_overview": (
        "상층-중층-하층 구조의 연결을 설명한다. domain_sequence_features에서 드러나는 지속성, 이동, "
        "강화/약화, 층간 결합을 중심으로 쓴다."
    ),
    "precipitation_structure": (
        "강수 배경과 상대적으로 유리한 강수 축을 설명한다. 수증기 수송, 저층제트, 수렴, 위성 구름 구조가 "
        "어떻게 결합되는지 공간적으로 정리한다."
    ),
    "surface_marine_impacts": (
        "지상 전선대, 저기압, 기압경도, 강풍대와 해상 영향 축을 설명한다. 어느 구역에서 지상·해상 영향이 "
        "상대적으로 큰지 분명히 적는다."
    ),
    "review_draft": (
        "앞선 해석 section을 바탕으로 예보관 검토용 2문단 초안을 작성한다. 첫 문단은 종관 사건과 강수 배경, "
        "둘째 문단은 영향 구역과 우선 점검 축을 중심으로 쓴다."
    ),
}
SECTION_SEQUENCE_DOMAINS = {
    "overall_summary": None,
    "synoptic_overview": None,
    "precipitation_structure": ("300hPa", "500hPa", "850hPa", "925hPa", "satellite_wv", "surface", "sfc12h_synoptic"),
    "surface_marine_impacts": ("850hPa", "925hPa", "satellite_wv", "surface", "sfc12h_synoptic"),
}
SECTION_CARD_DOMAINS = {
    "overall_summary": (),
    "synoptic_overview": ("300hPa", "500hPa", "850hPa", "925hPa"),
    "precipitation_structure": ("300hPa", "500hPa", "850hPa", "925hPa", "satellite_wv", "surface", "sfc12h_synoptic"),
    "surface_marine_impacts": ("850hPa", "925hPa", "satellite_wv", "surface", "sfc12h_synoptic"),
}
LIMITATION_LINES = (
    "1. 이 초안은 자동 생성된 grounded briefing 결과이며, 운영용 최종 예보문이 아니다.",
    "2. 현재 사실 판단은 feature_bundle과 image_feature_cards를 중심으로 수행했으며, 레이더·ASOS·수치모델 예측장까지 결합하지 않았다.",
    "3. hands37 rule pack은 해석 규칙 참고용으로만 사용했고, 현재 상황의 직접 증거로 사용하지 않았다.",
    "4. 정량 강수량, 경보 단계, 지역별 시간대 예측은 포함하지 않았다.",
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines: list[str] = [f"# {payload['run_date']} 자동 근거표시 브리핑 초안", ""]
    lines.append("## 문서 성격")
    lines.append("")
    lines.append(
        "이 문서는 자동화 파이프라인이 "
        f"`{payload['run_date']}` 사례의 feature 근거를 직접 읽고 작성한 `예보관 검토용 초안`이다. "
        "현재 관측 사실은 feature 산출물과 규칙 팩을 바탕으로 정리했고, 근거 연결은 evidence_id와 image_ref를 기준으로 유지한다."
    )
    lines.append("")
    for section in payload.get("sections", []) or []:
        if not isinstance(section, dict):
            continue
        lines.append(f"## {section['title']}")
        lines.append(str(section["text"]).strip())
        lines.append("")
        lines.append(f"- 관측 근거: {section['evidence_summary']}")
        lines.append(f"- 적용 규칙: {section['rule_summary']}")
        lines.append("")
    lines.append("## 한계")
    lines.append("")
    lines.extend(LIMITATION_LINES)
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _collect_evidence_index(prompt_input: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = [item for item in prompt_input.get("evidence_catalog", []) if isinstance(item, dict)]
    evidence_by_id: dict[str, dict[str, Any]] = {}
    for item in items:
        evidence_id = str(item.get("evidence_id", "")).strip()
        if evidence_id:
            evidence_by_id[evidence_id] = item
    if not evidence_by_id:
        raise ValueError("evidence_catalog does not contain usable evidence items")
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


def _filter_by_domains(items: list[dict[str, Any]], domains: tuple[str, ...] | None) -> list[dict[str, Any]]:
    if domains is None:
        return items
    domain_set = set(domains)
    return [item for item in items if str(item.get("domain", "")).strip() in domain_set]


def _build_minimal_rule_pack(
    rule_pack: dict[str, Any],
    selected_domains: set[str],
    selected_rule_refs: list[str] | None = None,
) -> dict[str, Any]:
    selected_rule_ref_set = {str(item).strip() for item in selected_rule_refs or [] if str(item).strip()}
    compact_rules: list[dict[str, Any]] = []
    for rule in rule_pack.get("rules", []) or []:
        if not isinstance(rule, dict):
            continue
        rule_id = str(rule.get("rule_id", "")).strip()
        if not rule_id:
            continue
        applicable_domains = [
            str(item).strip()
            for item in rule.get("applicable_domains", []) or []
            if str(item).strip()
        ]
        if selected_rule_ref_set:
            if rule_id not in selected_rule_ref_set:
                continue
        elif applicable_domains and not (selected_domains & set(applicable_domains)):
            continue
        compact_rules.append(
            {
                "rule_id": rule_id,
                "title": str(rule.get("title", "")).strip(),
                "applicable_domains": applicable_domains,
                "interpretation": str(rule.get("interpretation", "")).strip(),
                "forecast_use": str(rule.get("forecast_use", "")).strip(),
                "prohibited_use": [
                    str(item).strip()
                    for item in rule.get("prohibited_use", []) or []
                    if str(item).strip()
                ],
            }
        )
    return {
        "version": rule_pack.get("version"),
        "source_document": rule_pack.get("source_document"),
        "rules": compact_rules,
    }


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


def _derive_evidence_ids(
    evidence_refs: list[str],
    focus_regions: list[str],
    evidence_by_id: dict[str, dict[str, Any]],
    context: str,
) -> list[str]:
    ref_set = {item for item in evidence_refs if item}
    region_set = {item for item in focus_regions if item}
    if not ref_set:
        raise ValueError(f"{context}: evidence_refs must not be empty for evidence_id derivation")
    if not region_set:
        raise ValueError(f"{context}: focus_regions must not be empty for evidence_id derivation")

    matched: list[str] = []
    for evidence_id, evidence in evidence_by_id.items():
        refs = {str(ref).strip() for ref in evidence.get("active_image_refs", []) or [] if str(ref).strip()}
        regions = {str(region).strip() for region in evidence.get("region_labels", []) or [] if str(region).strip()}
        if refs & ref_set and regions & region_set:
            matched.append(evidence_id)
    if matched:
        return sorted(matched)

    for evidence_id, evidence in evidence_by_id.items():
        refs = {str(ref).strip() for ref in evidence.get("active_image_refs", []) or [] if str(ref).strip()}
        if refs & ref_set:
            matched.append(evidence_id)
    if matched:
        return sorted(dict.fromkeys(matched))

    raise ValueError(f"{context}: unable to derive evidence_ids from selected evidence_refs/focus_regions")


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


def _sanitize_section(
    parsed: dict[str, Any],
    section_id: str,
    evidence_by_id: dict[str, dict[str, Any]],
    rule_by_id: dict[str, dict[str, Any]],
    allowed_regions: set[str],
    allowed_evidence_refs: set[str],
) -> dict[str, Any]:
    context = f"direct_grounded_briefing[{section_id}]"
    focus_regions = _require_allowed_strings("focus_regions", parsed.get("focus_regions", []), allowed_regions, context)
    evidence_refs = _require_allowed_strings("evidence_refs", parsed.get("evidence_refs", []), allowed_evidence_refs, context)
    raw_evidence_ids = parsed.get("evidence_ids", [])
    if isinstance(raw_evidence_ids, list) and raw_evidence_ids:
        evidence_ids = _require_allowed_strings("evidence_ids", raw_evidence_ids, set(evidence_by_id.keys()), context)
        evidence_ids = _augment_evidence_ids_for_refs(evidence_ids, evidence_refs, evidence_by_id)
    else:
        evidence_ids = _derive_evidence_ids(evidence_refs, focus_regions, evidence_by_id, context)
    _, selected_regions = _collect_allowed_section_refs(evidence_ids, evidence_by_id)
    invalid_focus_regions = [item for item in focus_regions if item not in selected_regions]
    if invalid_focus_regions:
        raise ValueError(f"{context}: focus_regions must be covered by selected evidence_ids: {invalid_focus_regions}")
    rule_refs = _require_allowed_strings("rule_refs", parsed.get("rule_refs", []), set(rule_by_id.keys()), context)
    return {
        "section_id": section_id,
        "title": SECTION_TITLES[section_id],
        "text": _require_nonempty_string("text", parsed.get("text"), context),
        "evidence_summary": _require_nonempty_string("evidence_summary", parsed.get("evidence_summary"), context),
        "rule_summary": _require_nonempty_string("rule_summary", parsed.get("rule_summary"), context),
        "focus_regions": focus_regions,
        "evidence_ids": evidence_ids,
        "evidence_refs": evidence_refs,
        "rule_refs": rule_refs,
    }


def _build_single_section_schema(output_schema: dict[str, Any], section_id: str) -> dict[str, Any]:
    item_schema = copy.deepcopy(output_schema["properties"]["sections"]["items"])
    item_schema["properties"]["section_id"] = {"type": "string", "enum": [section_id]}
    return item_schema


def _build_section_prompt(
    template_text: str,
    section_prompt_input: dict[str, Any],
) -> str:
    replacements = {
        "{{RUN_DATE}}": str(section_prompt_input["run_date"]),
        "{{SECTION_ID}}": section_prompt_input["section_id"],
        "{{SECTION_TITLE}}": section_prompt_input["section_title"],
        "{{SECTION_GUIDANCE}}": section_prompt_input["section_guidance"],
        "{{ALLOWED_EVIDENCE_REFS_JSON}}": json.dumps(section_prompt_input["allowed_evidence_refs"], ensure_ascii=False, indent=2),
        "{{ALLOWED_RULE_REFS_JSON}}": json.dumps(section_prompt_input["allowed_rule_refs"], ensure_ascii=False, indent=2),
        "{{ALLOWED_REGIONS_JSON}}": json.dumps(section_prompt_input["allowed_regions"], ensure_ascii=False, indent=2),
        "{{FEATURE_BUNDLE_SUMMARY_JSON}}": json.dumps(section_prompt_input["feature_bundle_summary"], ensure_ascii=False, indent=2),
        "{{SELECTED_DOMAIN_SEQUENCE_FEATURES_JSON}}": json.dumps(section_prompt_input["selected_domain_sequence_features"], ensure_ascii=False, indent=2),
        "{{SELECTED_IMAGE_FEATURE_CARDS_JSON}}": json.dumps(section_prompt_input["selected_image_feature_cards"], ensure_ascii=False, indent=2),
        "{{GENERATED_SECTION_CONTEXT_JSON}}": json.dumps(section_prompt_input["generated_section_context"], ensure_ascii=False, indent=2),
        "{{RULE_PACK_JSON}}": json.dumps(section_prompt_input["rule_pack"], ensure_ascii=False, indent=2),
    }
    rendered = template_text
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def _build_section_prompt_input(
    prompt_input: dict[str, Any],
    rule_pack: dict[str, Any],
    section_id: str,
    completed_sections: list[dict[str, Any]],
) -> dict[str, Any]:
    if section_id == "review_draft":
        selected_evidence_ids = []
        for section in completed_sections:
            selected_evidence_ids.extend(str(item).strip() for item in section.get("evidence_ids", []) if str(item).strip())
        selected_evidence_ids = list(dict.fromkeys(selected_evidence_ids))
        evidence_index = {
            str(item.get("evidence_id", "")).strip(): item
            for item in prompt_input["evidence_catalog"]
            if isinstance(item, dict) and str(item.get("evidence_id", "")).strip()
        }
        selected_evidence_catalog = [evidence_index[evidence_id] for evidence_id in selected_evidence_ids if evidence_id in evidence_index]
        selected_evidence_refs = sorted(
            {
                str(ref).strip()
                for item in selected_evidence_catalog
                for ref in item.get("active_image_refs", []) or []
                if str(ref).strip()
            }
        )
        selected_regions = sorted(
            {
                str(region).strip()
                for item in selected_evidence_catalog
                for region in item.get("region_labels", []) or []
                if str(region).strip()
            }
        )
        selected_rule_refs = sorted(
            {
                str(rule_ref).strip()
                for section in completed_sections
                for rule_ref in section.get("rule_refs", []) or []
                if str(rule_ref).strip()
            }
        )
        selected_domains = {
            str(item.get("domain", "")).strip()
            for item in selected_evidence_catalog
            if isinstance(item, dict) and str(item.get("domain", "")).strip()
        }
        minimal_rule_pack = _build_minimal_rule_pack(rule_pack, selected_domains, selected_rule_refs)
        effective_rule_refs = [
            str(item.get("rule_id", "")).strip()
            for item in minimal_rule_pack.get("rules", [])
            if isinstance(item, dict) and str(item.get("rule_id", "")).strip()
        ]
        return {
            "run_date": prompt_input["run_date"],
            "section_id": section_id,
            "section_title": SECTION_TITLES[section_id],
            "section_guidance": SECTION_GUIDANCE[section_id],
            "feature_bundle_summary": prompt_input["feature_bundle_summary"],
            "selected_domain_sequence_features": [],
            "selected_image_feature_cards": [],
            "selected_evidence_catalog": selected_evidence_catalog,
            "generated_section_context": completed_sections,
            "rule_pack": minimal_rule_pack,
            "allowed_evidence_ids": selected_evidence_ids,
            "allowed_evidence_refs": selected_evidence_refs,
            "allowed_rule_refs": effective_rule_refs or selected_rule_refs or prompt_input["allowed_rule_refs"],
            "allowed_regions": selected_regions or prompt_input["allowed_regions"],
        }

    selected_domain_sequence_features = _filter_by_domains(
        prompt_input["domain_sequence_features"],
        SECTION_SEQUENCE_DOMAINS.get(section_id),
    )
    selected_image_feature_cards = _filter_by_domains(
        prompt_input["image_feature_cards"],
        SECTION_CARD_DOMAINS.get(section_id, ()),
    )
    selected_domains = {
        str(item.get("domain", "")).strip()
        for item in selected_domain_sequence_features + selected_image_feature_cards
        if str(item.get("domain", "")).strip()
    }
    selected_evidence_catalog = [
        item
        for item in prompt_input["evidence_catalog"]
        if isinstance(item, dict) and str(item.get("domain", "")).strip() in selected_domains
    ]
    selected_evidence_ids = [str(item["evidence_id"]).strip() for item in selected_evidence_catalog if str(item.get("evidence_id", "")).strip()]
    selected_evidence_refs = sorted(
        {
            str(ref).strip()
            for item in selected_evidence_catalog
            for ref in item.get("active_image_refs", []) or []
            if str(ref).strip()
        }
    )
    selected_regions = sorted(
        {
            str(region).strip()
            for item in selected_evidence_catalog
            for region in item.get("region_labels", []) or []
            if str(region).strip()
        }
    )
    minimal_rule_pack = _build_minimal_rule_pack(rule_pack, selected_domains)
    effective_rule_refs = [
        str(item.get("rule_id", "")).strip()
        for item in minimal_rule_pack.get("rules", [])
        if isinstance(item, dict) and str(item.get("rule_id", "")).strip()
    ]
    return {
        "run_date": prompt_input["run_date"],
        "section_id": section_id,
        "section_title": SECTION_TITLES[section_id],
        "section_guidance": SECTION_GUIDANCE[section_id],
        "feature_bundle_summary": prompt_input["feature_bundle_summary"],
        "selected_domain_sequence_features": selected_domain_sequence_features,
        "selected_image_feature_cards": selected_image_feature_cards,
        "selected_evidence_catalog": selected_evidence_catalog,
        "generated_section_context": [],
        "rule_pack": minimal_rule_pack,
        "allowed_evidence_ids": selected_evidence_ids,
        "allowed_evidence_refs": selected_evidence_refs,
        "allowed_rule_refs": effective_rule_refs or prompt_input["allowed_rule_refs"],
        "allowed_regions": selected_regions or prompt_input["allowed_regions"],
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

    prompt_input = {
        "run_date": run_date,
        "allow_new_claims": False,
        "feature_bundle_path": str(feature_bundle_path.relative_to(base_dir)).replace("\\", "/"),
        "image_feature_cards_path": str(image_feature_cards_path.relative_to(base_dir)).replace("\\", "/"),
        "rule_pack_path": str(Path(rule_pack["_meta"]["rule_pack_path"]).relative_to(base_dir)).replace("\\", "/"),
        "feature_bundle_summary": direct_inputs["feature_bundle_summary"],
        "image_feature_cards": direct_inputs["image_feature_cards"],
        "domain_sequence_features": direct_inputs["domain_sequence_features"],
        "evidence_catalog": direct_inputs["evidence_catalog"],
        "rule_pack": rule_pack,
        "allowed_evidence_ids": allowed_evidence_ids,
        "allowed_evidence_refs": allowed_evidence_refs,
        "allowed_rule_refs": allowed_rule_refs,
        "allowed_regions": allowed_regions,
    }
    priority_summary_path = resolve_manifest_path(base_dir, manifest["output_paths"]["priority_summary"], run_date)
    prompt_input_path = resolve_manifest_path(base_dir, manifest["output_paths"]["prompt_input"], run_date)
    _write_json(
        priority_summary_path,
        {
            "version": "direct_briefing_source_summary.v1",
            "run_date": run_date,
            "domain_count": len(prompt_input["feature_bundle_summary"].get("domains", []) or []),
            "image_feature_card_count": len(prompt_input["image_feature_cards"]),
            "domain_sequence_count": len(prompt_input["domain_sequence_features"]),
            "evidence_catalog_count": len(prompt_input["evidence_catalog"]),
        },
    )
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
    prompt_template_text = prompt_template_path.read_text(encoding="utf-8-sig")
    effective_model = (model or manifest.get("model") or "gemini-2.5-pro").strip()

    sections: list[dict[str, Any]] = []
    raw_section_outputs: list[dict[str, Any]] = []
    for section_id in SECTION_IDS:
        section_prompt_input = _build_section_prompt_input(prompt_input, rule_pack, section_id, sections)
        prompt_text = _build_section_prompt(prompt_template_text, section_prompt_input)
        section_schema = _build_single_section_schema(output_schema, section_id)
        llm_result = call_findings_llm(
            prompt_text=prompt_text,
            schema=section_schema,
            model=effective_model,
            max_output_tokens=2200,
        )
        parsed = llm_result["parsed_output"]
        if not isinstance(parsed, dict):
            raise ValueError(f"direct grounded briefing section output must be object: {section_id}")
        section = _sanitize_section(
            parsed,
            section_id,
            evidence_by_id,
            rule_by_id,
            set(allowed_regions),
            set(allowed_evidence_refs),
        )
        validate_instance(section, output_schema["properties"]["sections"]["items"], f"direct_grounded_briefing[{section_id}]")
        sections.append(section)
        raw_section_outputs.append(
            {
                "section_id": section_id,
                "section_prompt_input": section_prompt_input,
                "llm_result": llm_result,
            }
        )

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
            "section_outputs": raw_section_outputs,
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
