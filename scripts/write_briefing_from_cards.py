from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.card_manifest_loader import load_card_manifest, load_policy_mapping, resolve_manifest_path
from scripts.common.config import load_project_env
from scripts.common.findings_llm_client import call_findings_llm
from scripts.common.logging import configure_logging
from scripts.common.schema_utils import load_json_schema, validate_instance

KST = ZoneInfo('Asia/Seoul')
DEFAULT_MANIFEST = BASE_DIR / 'prompts' / 'manifests' / 'briefing_writer_manifest.yaml'
SECTION_IDS = ('overall_summary', 'synoptic_overview', 'surface_overview')
UPPER_CARDS_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'upper_reasoning_cards.schema.json'


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(payload, dict):
        raise ValueError(f'json payload must be an object: {path}')
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _require_policy_flags(policy: dict[str, Any], policy_path: Path, manifest: dict[str, Any]) -> None:
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


def _require_nonempty_string(field_name: str, value: Any, context: str) -> str:
    text = str(value).strip() if value is not None else ''
    if not text:
        raise ValueError(f'{context}: required field is empty: {field_name}')
    return text


def _require_list(field_name: str, value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f'{context}: field must be a list: {field_name}')
    return value


def _require_allowed_strings(field_name: str, values: Any, allowed: set[str], context: str) -> list[str]:
    raw_items = _require_list(field_name, values, context)
    items: list[str] = []
    invalid_items: list[str] = []
    for index, value in enumerate(raw_items):
        text = str(value).strip()
        if not text:
            raise ValueError(f'{context}: empty string is not allowed: {field_name}[{index}]')
        if text not in allowed:
            invalid_items.append(text)
        items.append(text)
    if invalid_items:
        raise ValueError(f'{context}: field contains unsupported values: {field_name} -> {invalid_items}')
    if not items:
        raise ValueError(f'{context}: field must not be empty: {field_name}')
    return items


def _build_prompt_text(manifest: dict[str, Any], run_date: str, prompt_input: dict[str, Any]) -> str:
    return "\n".join(
        [
            str(manifest.get('prompt_instructions') or '').strip(),
            f'run_date={run_date}',
            f"allow_new_claims={str(manifest.get('allow_new_claims', False)).lower()}",
            'briefing_writer_prompt_input_json=',
            json.dumps(prompt_input, ensure_ascii=False, indent=2),
        ]
    )


def _collect_allowed_refs(upper_cards: list[dict[str, Any]]) -> set[str]:
    refs: set[str] = set()
    for index, card in enumerate(upper_cards):
        if not isinstance(card, dict):
            raise ValueError(f'upper_reasoning_cards[{index}] must be an object')
        for ref in card.get('evidence_refs', []) or []:
            text = str(ref).strip()
            if text:
                refs.add(text)
    return refs


def _collect_source_refs(upper_cards_by_id: dict[str, dict[str, Any]], source_card_ids: list[str], context: str) -> set[str]:
    refs: set[str] = set()
    for card_id in source_card_ids:
        card = upper_cards_by_id.get(card_id)
        if not isinstance(card, dict):
            raise ValueError(f'{context}: source card not found in upper_reasoning_cards: {card_id}')
        for ref in card.get('evidence_refs', []) or []:
            text = str(ref).strip()
            if text:
                refs.add(text)
    if not refs:
        raise ValueError(f'{context}: source cards do not provide any evidence_refs')
    return refs


def _sanitize_section(parsed: dict[str, Any], policy_card: dict[str, Any], upper_cards_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    section_id = str(policy_card['card_type']).strip()
    context = f'briefing_section[{section_id}]'
    source_card_ids = _require_allowed_strings('source_card_ids', parsed.get('source_card_ids', []), set(upper_cards_by_id.keys()), context)
    allowed_refs = _collect_source_refs(upper_cards_by_id, source_card_ids, context)
    evidence_refs = _require_allowed_strings('evidence_refs', parsed.get('evidence_refs', []), allowed_refs, context)
    return {
        'section_id': section_id,
        'title': _require_nonempty_string('title', parsed.get('title'), context),
        'text': _require_nonempty_string('text', parsed.get('text'), context),
        'source_card_ids': source_card_ids,
        'evidence_refs': evidence_refs,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='카드 기반 브리핑 초안을 생성합니다.')
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--manifest', default=str(DEFAULT_MANIFEST), help='manifest 경로')
    parser.add_argument('--model', help='모델 override')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    configure_logging('write_briefing_from_cards', args.date)

    try:
        manifest = load_card_manifest(args.manifest)
        output_schema = load_json_schema(resolve_manifest_path(BASE_DIR, manifest['schema_path'], args.date))
        input_schema = load_json_schema(UPPER_CARDS_SCHEMA_PATH)
        policy_path = resolve_manifest_path(BASE_DIR, manifest['policy_path'], args.date)
        policy = load_policy_mapping(policy_path)
        _require_policy_flags(policy, policy_path, manifest)

        upper_cards_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['upper_cards'], args.date)
        upper_cards_payload = _read_json(upper_cards_path)
        validate_instance(upper_cards_payload, input_schema, f'input_upper_reasoning_cards[{upper_cards_path}]')

        output_prompt_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['prompt_input'], args.date)
        output_raw_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['raw'], args.date)
        output_draft_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['draft'], args.date)

        upper_cards = [card for card in upper_cards_payload.get('cards', []) if isinstance(card, dict)]
        if not upper_cards:
            raise ValueError(f'no upper reasoning cards found: {upper_cards_path}')
        upper_cards_by_id = {str(card.get('card_id', '')).strip(): card for card in upper_cards if str(card.get('card_id', '')).strip()}
        allowed_refs = sorted(_collect_allowed_refs(upper_cards))
        if not allowed_refs:
            raise ValueError(f'upper reasoning cards do not contain evidence_refs: {upper_cards_path}')

        policy_cards = _require_list('cards', policy.get('cards'), f'policy[{policy_path}]')
        expected_sections: list[str] = []
        for index, policy_card in enumerate(policy_cards):
            if not isinstance(policy_card, dict):
                raise ValueError(f'policy[{policy_path}] cards[{index}] must be an object')
            section_id = _require_nonempty_string('card_type', policy_card.get('card_type'), f'policy[{policy_path}] cards[{index}]')
            if section_id in SECTION_IDS:
                expected_sections.append(section_id)
        if len(expected_sections) != len(SECTION_IDS):
            raise ValueError(f'policy is missing required section definitions: {policy_path}')

        prompt_input = {
            'run_date': args.date,
            'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
            'sections_policy': policy_cards,
            'upper_reasoning_cards': upper_cards,
            'allowed_source_card_ids': sorted(upper_cards_by_id.keys()),
            'allowed_evidence_refs': allowed_refs,
        }
        prompt_text = _build_prompt_text(manifest, args.date, prompt_input)
        effective_model = (args.model or manifest.get('model') or 'gpt-4.1-mini').strip()
        llm_result = call_findings_llm(
            prompt_text=prompt_text,
            schema=output_schema,
            model=effective_model,
            max_output_tokens=3200,
        )
        parsed_sections = [item for item in llm_result['parsed_output'].get('sections', []) if isinstance(item, dict)]
        parsed_by_id = {str(item.get('section_id', '')).strip(): item for item in parsed_sections if str(item.get('section_id', '')).strip()}
        extra_sections = sorted(section_id for section_id in parsed_by_id if section_id not in expected_sections)
        if extra_sections:
            raise ValueError(f'briefing llm output contains unexpected section_id values: {extra_sections}')

        section_item_schema = output_schema['properties']['sections']['items']
        sections: list[dict[str, Any]] = []
        for policy_card in policy_cards:
            section_id = _require_nonempty_string('card_type', policy_card.get('card_type'), f'policy[{policy_path}]')
            if section_id not in SECTION_IDS:
                continue
            parsed = parsed_by_id.get(section_id)
            if parsed is None:
                raise ValueError(f'briefing llm output missing required section_id={section_id}')
            section = _sanitize_section(parsed, policy_card, upper_cards_by_id)
            validate_instance(section, section_item_schema, f'briefing_section[{section_id}]')
            sections.append(section)

        if len(sections) != len(expected_sections):
            raise ValueError(f'partial briefing output is not allowed: expected={len(expected_sections)}, actual={len(sections)}')

        draft_payload = {
            'version': 'briefing_draft.v1',
            'run_date': args.date,
            'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
            'sections': sections,
        }
        validate_instance(draft_payload, output_schema, 'briefing_draft')
        _write_json(output_prompt_path, prompt_input)
        _write_json(
            output_raw_path,
            {
                'run_date': args.date,
                'generated_at_kst': datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S'),
                'model': effective_model,
                'llm_result': llm_result,
            },
        )
        _write_json(output_draft_path, draft_payload)
        print(
            json.dumps(
                {
                    'status': 'ok',
                    'run_date': args.date,
                    'section_count': len(sections),
                    'draft_path': output_draft_path.relative_to(BASE_DIR).as_posix(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        logging.exception('write_briefing_from_cards_failed')
        print(json.dumps({'status': 'error', 'message': str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
