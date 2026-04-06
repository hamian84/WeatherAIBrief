from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.card_manifest_loader import load_card_manifest, resolve_manifest_path
from scripts.common.config import load_project_env
from scripts.common.direct_briefing_inputs import build_direct_briefing_inputs, read_json_object
from scripts.common.logging import configure_logging
from scripts.common.rule_pack_loader import load_rule_pack
from scripts.common.schema_utils import load_json_schema, validate_instance

DEFAULT_MANIFEST = BASE_DIR / "prompts" / "manifests" / "direct_grounded_briefing_validator_manifest.yaml"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _add_issue(issues: list[dict[str, Any]], issue_type: str, severity: str, section_id: str, message: str) -> None:
    issues.append({"issue_id": f"issue_{len(issues) + 1:03d}", "issue_type": issue_type, "severity": severity, "section_id": section_id, "message": message})


def _safe_validate(payload: dict[str, Any], schema: dict[str, Any], label: str) -> tuple[bool, str]:
    try:
        validate_instance(payload, schema, label)
        return True, ""
    except Exception as exc:
        return False, str(exc)


def validate_grounded_briefing_stage(base_dir: Path, run_date: str, manifest_path: str | Path) -> dict[str, Any]:
    manifest = load_card_manifest(manifest_path)
    validation_schema = load_json_schema(resolve_manifest_path(base_dir, manifest["schema_path"], run_date))
    draft_schema = load_json_schema(base_dir / "prompts" / "schemas" / "direct_grounded_briefing.schema.json")
    feature_bundle_path = resolve_manifest_path(base_dir, manifest["input_paths"]["feature_bundle"], run_date)
    image_feature_cards_path = resolve_manifest_path(base_dir, manifest["input_paths"]["image_feature_cards"], run_date)
    draft_path = resolve_manifest_path(base_dir, manifest["input_paths"]["draft"], run_date)
    report_path = resolve_manifest_path(base_dir, manifest["output_paths"]["report"], run_date)
    draft = read_json_object(draft_path)
    direct_inputs = build_direct_briefing_inputs(base_dir, feature_bundle_path, image_feature_cards_path)
    rule_pack = load_rule_pack(base_dir, manifest["input_paths"]["rule_pack"], manifest["input_paths"]["rule_pack_schema"])

    issues: list[dict[str, Any]] = []
    draft_schema_valid, draft_schema_error = _safe_validate(draft, draft_schema, f"direct_grounded_briefing[{draft_path}]")
    if not draft_schema_valid:
        _add_issue(issues, "schema_invalid", "error", "global", draft_schema_error)

    evidence_by_id = {
        str(item.get("evidence_id", "")).strip(): item
        for item in direct_inputs.get("image_feature_signal_summary", []) or []
        if isinstance(item, dict) and str(item.get("evidence_id", "")).strip()
    }
    rule_ids = {
        str(item.get("rule_id", "")).strip()
        for item in rule_pack.get("rules", []) or []
        if isinstance(item, dict) and str(item.get("rule_id", "")).strip()
    }

    for section in draft.get("sections", []) or []:
        if not isinstance(section, dict):
            continue
        section_id = str(section.get("section_id", "")).strip() or "unknown"
        focus_regions = [str(item).strip() for item in section.get("focus_regions", []) or [] if str(item).strip()]
        evidence_ids = [str(item).strip() for item in section.get("evidence_ids", []) or [] if str(item).strip()]
        evidence_refs = [str(item).strip() for item in section.get("evidence_refs", []) or [] if str(item).strip()]
        rule_refs = [str(item).strip() for item in section.get("rule_refs", []) or [] if str(item).strip()]
        if not focus_regions:
            _add_issue(issues, "missing_focus_regions", "error", section_id, f"section={section_id} focus_regions is empty")
        invalid_evidence_ids = [item for item in evidence_ids if item not in evidence_by_id]
        if invalid_evidence_ids:
            _add_issue(issues, "unsupported_claim", "error", section_id, f"section={section_id} has unsupported evidence_ids: {invalid_evidence_ids}")
            continue
        allowed_refs: set[str] = set()
        allowed_regions: set[str] = set()
        for evidence_id in evidence_ids:
            evidence = evidence_by_id[evidence_id]
            for ref in evidence.get("active_image_refs", []) or []:
                text = str(ref).strip()
                if text:
                    allowed_refs.add(text)
            for region in evidence.get("region_labels", []) or []:
                text = str(region).strip()
                if text:
                    allowed_regions.add(text)
        invalid_focus_regions = [item for item in focus_regions if item not in allowed_regions]
        invalid_evidence_refs = [item for item in evidence_refs if item not in allowed_refs]
        invalid_rule_refs = [item for item in rule_refs if item not in rule_ids]
        if invalid_focus_regions or invalid_evidence_refs or invalid_rule_refs:
            detail: list[str] = []
            if invalid_focus_regions:
                detail.append(f"focus_regions={invalid_focus_regions}")
            if invalid_evidence_refs:
                detail.append(f"evidence_refs={invalid_evidence_refs}")
            if invalid_rule_refs:
                detail.append(f"rule_refs={invalid_rule_refs}")
            _add_issue(issues, "unsupported_claim", "error", section_id, f"section={section_id} has unsupported refs: {', '.join(detail)}")

    report = {
        "version": "direct_grounded_briefing_validation.v1",
        "run_date": run_date,
        "status": "pass" if not issues else "warning",
        "issue_count": len(issues),
        "checks": {
            "schema_valid": draft_schema_valid,
            "missing_focus_regions_count": sum(1 for issue in issues if issue["issue_type"] == "missing_focus_regions"),
            "unsupported_claim_count": sum(1 for issue in issues if issue["issue_type"] in {"unsupported_claim", "schema_invalid"}),
        },
        "issues": issues,
    }
    validate_instance(report, validation_schema, "direct_grounded_briefing_validation")
    _write_json(report_path, report)
    return {
        "status": "ok",
        "report_path": str(report_path.relative_to(base_dir)).replace("\\", "/"),
        "issue_count": len(issues),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="direct grounded briefing draft를 feature_bundle과 image_feature_cards 기준으로 검증합니다.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="manifest 경로")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    configure_logging("validate_grounded_briefing", args.date)
    try:
        result = validate_grounded_briefing_stage(BASE_DIR, args.date, args.manifest)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if int(result["issue_count"]) == 0 else 1
    except Exception as exc:
        logging.exception("validate_grounded_briefing_failed")
        print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
