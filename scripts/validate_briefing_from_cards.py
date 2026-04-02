from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.card_manifest_loader import load_card_manifest, load_policy_mapping, resolve_manifest_path
from scripts.common.config import load_project_env
from scripts.common.logging import configure_logging
from scripts.common.schema_utils import load_json_schema, validate_instance

DEFAULT_MANIFEST = BASE_DIR / 'prompts' / 'manifests' / 'briefing_validator_manifest.yaml'
DOMAIN_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'domain_semantic_cards.schema.json'
UPPER_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'upper_reasoning_cards.schema.json'
DRAFT_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'briefing_draft.schema.json'
TOKEN_PATTERN = re.compile(r'[가-힣A-Za-z0-9]+')


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(payload, dict):
        raise ValueError(f'json payload must be an object: {path}')
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _require_policy_flags(policy: dict[str, Any], policy_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    if bool(manifest.get('allow_new_claims', False)):
        raise ValueError(f'manifest allow_new_claims must be false: {manifest["id"]}')
    if bool(policy.get('allow_new_claims', False)):
        raise ValueError(f'policy allow_new_claims must be false: {policy_path}')
    if not bool(policy.get('fail_fast', False)):
        raise ValueError(f'policy fail_fast must be true: {policy_path}')
    if bool(policy.get('auto_recovery', True)):
        raise ValueError(f'policy auto_recovery must be false: {policy_path}')
    if bool(policy.get('allow_partial_output', True)):
        raise ValueError(f'policy allow_partial_output must be false: {policy_path}')
    hard_fail_issue_types = [str(item).strip() for item in policy.get('hard_fail_issue_types', []) or [] if str(item).strip()]
    if not hard_fail_issue_types:
        raise ValueError(f'policy hard_fail_issue_types must not be empty: {policy_path}')
    return {'hard_fail_issue_types': set(hard_fail_issue_types)}


def _add_issue(issues: list[dict[str, Any]], issue_type: str, severity: str, section_id: str, message: str) -> None:
    issues.append(
        {
            'issue_id': f'issue_{len(issues) + 1:03d}',
            'issue_type': issue_type,
            'severity': severity,
            'section_id': section_id,
            'message': message,
        }
    )


def _safe_validate(payload: dict[str, Any], schema: dict[str, Any], label: str) -> tuple[bool, str]:
    try:
        validate_instance(payload, schema, label)
        return True, ''
    except Exception as exc:
        return False, str(exc)


def _token_set(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_PATTERN.findall(text) if token.strip()}


def _jaccard_similarity(left: str, right: str) -> float:
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _shares_context(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_sources = {str(item).strip() for item in left.get('source_card_ids', []) or [] if str(item).strip()}
    right_sources = {str(item).strip() for item in right.get('source_card_ids', []) or [] if str(item).strip()}
    left_refs = {str(item).strip() for item in left.get('evidence_refs', []) or [] if str(item).strip()}
    right_refs = {str(item).strip() for item in right.get('evidence_refs', []) or [] if str(item).strip()}
    return bool(left_sources & right_sources or left_refs & right_refs)


def _flatten_upper_refs(
    upper_cards_by_id: dict[str, dict[str, Any]],
    source_card_ids: list[str],
    section_id: str,
    upper_cards_path: Path,
) -> set[str]:
    allowed_refs: set[str] = set()
    for card_id in source_card_ids:
        card = upper_cards_by_id.get(card_id)
        if not isinstance(card, dict):
            raise ValueError(
                f'validator input mismatch: draft section={section_id} references missing upper card {card_id} in {upper_cards_path}'
            )
        for ref in card.get('evidence_refs', []) or []:
            text = str(ref).strip()
            if text:
                allowed_refs.add(text)
    return allowed_refs


def _check_required_sections(sections_by_id: dict[str, dict[str, Any]], required_sections: list[str], draft_path: Path, issues: list[dict[str, Any]]) -> None:
    for section_id in required_sections:
        if section_id not in sections_by_id:
            _add_issue(issues, 'missing_section', 'error', section_id, f'draft file={draft_path} is missing required section_id={section_id}')


def _check_allow_new_claims(
    domain_cards: dict[str, Any],
    upper_cards: dict[str, Any],
    draft: dict[str, Any],
    issues: list[dict[str, Any]],
    domain_path: Path,
    upper_path: Path,
    draft_path: Path,
) -> None:
    if bool(domain_cards.get('allow_new_claims', False)):
        _add_issue(issues, 'allow_new_claims_violation', 'error', 'global', f'domain cards file={domain_path} violates allow_new_claims=false')
    if bool(upper_cards.get('allow_new_claims', False)):
        _add_issue(issues, 'allow_new_claims_violation', 'error', 'global', f'upper cards file={upper_path} violates allow_new_claims=false')
    if bool(draft.get('allow_new_claims', False)):
        _add_issue(issues, 'allow_new_claims_violation', 'error', 'global', f'briefing draft file={draft_path} violates allow_new_claims=false')


def _check_missing_evidence(sections: list[dict[str, Any]], draft_path: Path, issues: list[dict[str, Any]]) -> None:
    for section in sections:
        section_id = str(section.get('section_id', '')).strip() or 'unknown'
        refs = [str(item).strip() for item in section.get('evidence_refs', []) or [] if str(item).strip()]
        if not refs:
            _add_issue(issues, 'missing_evidence_refs', 'error', section_id, f'draft file={draft_path} section={section_id} field=evidence_refs is empty')


def _check_unsupported_claims(
    sections: list[dict[str, Any]],
    upper_cards_by_id: dict[str, dict[str, Any]],
    upper_cards_path: Path,
    draft_path: Path,
    issues: list[dict[str, Any]],
) -> None:
    allowed_upper_ids = set(upper_cards_by_id.keys())
    for section in sections:
        section_id = str(section.get('section_id', '')).strip() or 'unknown'
        source_card_ids = [str(item).strip() for item in section.get('source_card_ids', []) or [] if str(item).strip()]
        if not source_card_ids:
            _add_issue(issues, 'missing_source_card_ids', 'error', section_id, f'draft file={draft_path} section={section_id} field=source_card_ids is empty')
            continue
        invalid_sources = [card_id for card_id in source_card_ids if card_id not in allowed_upper_ids]
        if invalid_sources:
            _add_issue(
                issues,
                'unsupported_claim',
                'error',
                section_id,
                f'draft file={draft_path} section={section_id} references missing upper cards in {upper_cards_path}: {invalid_sources}',
            )
            continue
        allowed_refs = _flatten_upper_refs(upper_cards_by_id, source_card_ids, section_id, upper_cards_path)
        if not allowed_refs:
            _add_issue(
                issues,
                'unsupported_claim',
                'error',
                section_id,
                f'draft file={draft_path} section={section_id} source cards do not provide evidence_refs',
            )
            continue
        evidence_refs = [str(item).strip() for item in section.get('evidence_refs', []) or [] if str(item).strip()]
        invalid_refs = [ref for ref in evidence_refs if ref not in allowed_refs]
        if invalid_refs:
            _add_issue(
                issues,
                'unsupported_claim',
                'error',
                section_id,
                f'draft file={draft_path} section={section_id} has evidence_refs not supported by source_card_ids: {invalid_refs}',
            )


def _check_duplicate_meaning(sections: list[dict[str, Any]], threshold: float, issues: list[dict[str, Any]], draft_path: Path) -> None:
    for index, left in enumerate(sections):
        for right in sections[index + 1:]:
            if not _shares_context(left, right):
                continue
            similarity = _jaccard_similarity(str(left.get('text', '')), str(right.get('text', '')))
            if similarity >= threshold:
                _add_issue(
                    issues,
                    'duplicate_meaning',
                    'warning',
                    f"{left.get('section_id')}|{right.get('section_id')}",
                    f'draft file={draft_path} has highly similar section text. similarity={similarity:.2f}',
                )


def _check_contradictions(sections: list[dict[str, Any]], contradiction_pairs: list[list[str]], issues: list[dict[str, Any]], draft_path: Path) -> None:
    for index, left in enumerate(sections):
        left_text = str(left.get('text', ''))
        for right in sections[index + 1:]:
            if not _shares_context(left, right):
                continue
            right_text = str(right.get('text', ''))
            for pair in contradiction_pairs:
                if not isinstance(pair, list) or len(pair) != 2:
                    continue
                positive = str(pair[0]).strip()
                negative = str(pair[1]).strip()
                if not positive or not negative:
                    continue
                if (positive in left_text and negative in right_text) or (negative in left_text and positive in right_text):
                    _add_issue(
                        issues,
                        'contradiction',
                        'error',
                        f"{left.get('section_id')}|{right.get('section_id')}",
                        f'draft file={draft_path} contains contradictory terms: {positive} / {negative}',
                    )
                    break


def _build_report(run_date: str, manifest: dict[str, Any], issues: list[dict[str, Any]], schema_valid: bool) -> dict[str, Any]:
    checks = {
        'schema_valid': schema_valid,
        'missing_evidence_count': sum(1 for issue in issues if issue['issue_type'] == 'missing_evidence_refs'),
        'unsupported_claim_count': sum(
            1
            for issue in issues
            if issue['issue_type'] in {'unsupported_claim', 'missing_source_card_ids', 'missing_section', 'allow_new_claims_violation', 'schema_invalid'}
        ),
        'contradiction_count': sum(1 for issue in issues if issue['issue_type'] == 'contradiction'),
        'duplicate_meaning_count': sum(1 for issue in issues if issue['issue_type'] == 'duplicate_meaning'),
    }
    return {
        'version': 'briefing_validation.v1',
        'run_date': run_date,
        'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
        'status': 'pass' if not issues else 'warning',
        'issue_count': len(issues),
        'checks': checks,
        'issues': issues,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='카드 기반 브리핑 초안을 검증합니다.')
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--manifest', default=str(DEFAULT_MANIFEST), help='manifest 경로')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    configure_logging('validate_briefing_from_cards', args.date)

    try:
        manifest = load_card_manifest(args.manifest)
        validation_schema = load_json_schema(resolve_manifest_path(BASE_DIR, manifest['schema_path'], args.date))
        domain_schema = load_json_schema(DOMAIN_SCHEMA_PATH)
        upper_schema = load_json_schema(UPPER_SCHEMA_PATH)
        draft_schema = load_json_schema(DRAFT_SCHEMA_PATH)
        policy_path = resolve_manifest_path(BASE_DIR, manifest['policy_path'], args.date)
        policy = load_policy_mapping(policy_path)
        runtime_flags = _require_policy_flags(policy, policy_path, manifest)

        domain_cards_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['domain_cards'], args.date)
        upper_cards_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['upper_cards'], args.date)
        draft_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['draft'], args.date)
        output_report_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['report'], args.date)

        domain_cards = _read_json(domain_cards_path)
        upper_cards = _read_json(upper_cards_path)
        draft = _read_json(draft_path)

        issues: list[dict[str, Any]] = []
        schema_valid = True
        for payload, schema, label, source_path in (
            (domain_cards, domain_schema, 'domain_semantic_cards', domain_cards_path),
            (upper_cards, upper_schema, 'upper_reasoning_cards', upper_cards_path),
            (draft, draft_schema, 'briefing_draft', draft_path),
        ):
            valid, message = _safe_validate(payload, schema, label)
            if not valid:
                schema_valid = False
                _add_issue(issues, 'schema_invalid', 'error', 'global', f'{label} file={source_path} schema validation failed: {message}')

        if not schema_valid and bool(policy.get('fail_fast', False)):
            report = _build_report(args.date, manifest, issues, schema_valid)
            report['status'] = 'fail'
            validate_instance(report, validation_schema, 'briefing_validation')
            _write_json(output_report_path, report)
            print(
                json.dumps(
                    {
                        'status': 'ok',
                        'run_date': args.date,
                        'validation_status': 'fail',
                        'issue_count': len(issues),
                        'report_path': output_report_path.relative_to(BASE_DIR).as_posix(),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 1

        _check_allow_new_claims(domain_cards, upper_cards, draft, issues, domain_cards_path, upper_cards_path, draft_path)

        upper_cards_by_id = {
            str(card.get('card_id', '')).strip(): card
            for card in upper_cards.get('cards', []) or []
            if isinstance(card, dict) and str(card.get('card_id', '')).strip()
        }
        sections = [section for section in draft.get('sections', []) or [] if isinstance(section, dict)]
        sections_by_id = {
            str(section.get('section_id', '')).strip(): section
            for section in sections
            if str(section.get('section_id', '')).strip()
        }

        required_sections = [str(item).strip() for item in policy.get('required_sections', []) or [] if str(item).strip()]
        _check_required_sections(sections_by_id, required_sections, draft_path, issues)
        _check_missing_evidence(sections, draft_path, issues)
        _check_unsupported_claims(sections, upper_cards_by_id, upper_cards_path, draft_path, issues)
        _check_duplicate_meaning(sections, float(policy.get('duplicate_similarity_threshold', 0.72)), issues, draft_path)
        _check_contradictions(sections, policy.get('contradiction_pairs', []) or [], issues, draft_path)

        report = _build_report(args.date, manifest, issues, schema_valid)
        hard_fail_issue_types = runtime_flags['hard_fail_issue_types']
        has_hard_fail = any(issue['issue_type'] in hard_fail_issue_types for issue in issues)
        if has_hard_fail:
            report['status'] = 'fail'
        elif issues:
            report['status'] = 'warning'
        else:
            report['status'] = 'pass'

        validate_instance(report, validation_schema, 'briefing_validation')
        _write_json(output_report_path, report)
        print(
            json.dumps(
                {
                    'status': 'ok',
                    'run_date': args.date,
                    'validation_status': report['status'],
                    'issue_count': len(issues),
                    'report_path': output_report_path.relative_to(BASE_DIR).as_posix(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1 if has_hard_fail else 0
    except Exception as exc:
        logging.exception('validate_briefing_from_cards_failed')
        print(json.dumps({'status': 'error', 'message': str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
