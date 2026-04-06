from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

from scripts.common.schema_utils import load_json_schema, validate_instance

ALLOWED_RULE_TYPES = {"operational", "reference"}
ALLOWED_FEATURE_MAPPING_STATUS = {"mapped", "partial", "not_mapped_yet"}


def _read_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load rule_pack yaml")
    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        raise ValueError(f"empty rule_pack file: {path}")
    payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"rule_pack payload must be an object: {path}")
    return payload


def _resolve_path(base_dir: Path, value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = base_dir / path
    return path


def _validate_rule_pack_semantics(payload: dict[str, Any], path: Path) -> dict[str, Any]:
    section_ranges = payload.get("section_ranges") or []
    sections = payload.get("sections") or []
    rules = payload.get("rules") or []
    if not isinstance(section_ranges, list) or not isinstance(sections, list) or not isinstance(rules, list):
        raise ValueError(f"rule_pack arrays are malformed: {path}")

    section_range_ids: list[int] = []
    for item in section_ranges:
        if not isinstance(item, dict):
            raise ValueError(f"section_ranges item must be object: {path}")
        section_no = int(item["section_no"])
        page_range = item["page_range"]
        if int(page_range["start"]) > int(page_range["end"]):
            raise ValueError(f"section_range start/end invalid for section_no={section_no}: {path}")
        section_range_ids.append(section_no)
    if len(section_range_ids) != len(set(section_range_ids)):
        raise ValueError(f"duplicate section_no in section_ranges: {path}")

    section_ids: list[int] = []
    section_rule_refs: set[str] = set()
    for item in sections:
        if not isinstance(item, dict):
            raise ValueError(f"sections item must be object: {path}")
        section_no = int(item["section_no"])
        page_range = item["page_range"]
        if int(page_range["start"]) > int(page_range["end"]):
            raise ValueError(f"section page_range start/end invalid for section_no={section_no}: {path}")
        section_ids.append(section_no)
        for ref in item.get("rule_refs") or []:
            text = str(ref).strip()
            if not text:
                raise ValueError(f"empty rule_ref in section_no={section_no}: {path}")
            section_rule_refs.add(text)
    if len(section_ids) != len(set(section_ids)):
        raise ValueError(f"duplicate section_no in sections: {path}")
    if set(section_range_ids) != set(section_ids):
        raise ValueError(f"section_ranges and sections mismatch: {path}")

    rule_ids: list[str] = []
    rule_section_ids: set[int] = set()
    for item in rules:
        if not isinstance(item, dict):
            raise ValueError(f"rules item must be object: {path}")
        rule_id = str(item["rule_id"]).strip()
        rule_type = str(item["rule_type"]).strip()
        feature_mapping_status = str(item["feature_mapping_status"]).strip()
        section_no = int(item["section_no"])
        if rule_type not in ALLOWED_RULE_TYPES:
            raise ValueError(f"unsupported rule_type '{rule_type}' in rule_id={rule_id}: {path}")
        if feature_mapping_status not in ALLOWED_FEATURE_MAPPING_STATUS:
            raise ValueError(
                f"unsupported feature_mapping_status '{feature_mapping_status}' in rule_id={rule_id}: {path}"
            )
        applicable_domains = item.get("applicable_domains") or []
        if not isinstance(applicable_domains, list) or not applicable_domains:
            raise ValueError(f"applicable_domains must be a non-empty list in rule_id={rule_id}: {path}")
        rule_ids.append(rule_id)
        rule_section_ids.add(section_no)
    if len(rule_ids) != len(set(rule_ids)):
        raise ValueError(f"duplicate rule_id in rules: {path}")
    unknown_rule_refs = sorted(section_rule_refs - set(rule_ids))
    if unknown_rule_refs:
        raise ValueError(f"section rule_refs without rules: {unknown_rule_refs}: {path}")
    unknown_rule_sections = sorted(rule_section_ids - set(section_ids))
    if unknown_rule_sections:
        raise ValueError(f"rules reference missing section_no values {unknown_rule_sections}: {path}")

    source_document = payload.get("source_document") or {}
    usage_policy = source_document.get("usage_policy") or {}
    if bool(usage_policy.get("direct_evidence_source")):
        raise ValueError(f"rule_pack direct_evidence_source must remain false: {path}")

    payload["summary"] = {
        "section_range_count": len(section_ranges),
        "section_count": len(sections),
        "rule_count": len(rules),
        "operational_rule_count": len([item for item in rules if item.get("rule_type") == "operational"]),
        "reference_rule_count": len([item for item in rules if item.get("rule_type") == "reference"]),
    }
    return payload


def load_rule_pack(base_dir: Path, rule_pack_path: str | Path, schema_path: str | Path) -> dict[str, Any]:
    resolved_rule_pack_path = _resolve_path(base_dir, rule_pack_path)
    resolved_schema_path = _resolve_path(base_dir, schema_path)
    if not resolved_rule_pack_path.exists():
        raise FileNotFoundError(f"rule_pack file not found: {resolved_rule_pack_path}")
    if not resolved_schema_path.exists():
        raise FileNotFoundError(f"rule_pack schema not found: {resolved_schema_path}")

    payload = _read_yaml(resolved_rule_pack_path)
    schema = load_json_schema(resolved_schema_path)
    validate_instance(payload, schema, "rule_pack")
    payload = _validate_rule_pack_semantics(payload, resolved_rule_pack_path)
    payload["_meta"] = {
        "rule_pack_path": str(resolved_rule_pack_path),
        "schema_path": str(resolved_schema_path),
    }
    return payload
