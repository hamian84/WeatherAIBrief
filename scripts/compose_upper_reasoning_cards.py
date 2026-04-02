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
DEFAULT_MANIFEST = BASE_DIR / 'prompts' / 'manifests' / 'upper_reasoning_cards_manifest.yaml'
CARD_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'upper_reasoning_card.schema.json'
DOMAIN_CARDS_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'domain_semantic_cards.schema.json'
CONFIDENCE_VALUES = {'high', 'medium', 'low'}


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
            'upper_reasoning_prompt_input_json=',
            json.dumps(prompt_input, ensure_ascii=False, indent=2),
        ]
    )


def _require_expected_domains(card_policy: dict[str, Any], available_domains: set[str], context: str) -> list[str]:
    source_domains = _require_allowed_strings('source_domains', card_policy.get('source_domains', []), available_domains, context)
    missing_domains = [domain for domain in source_domains if domain not in available_domains]
    if missing_domains:
        raise ValueError(f'{context}: required domain semantic cards are missing: {missing_domains}')
    return source_domains


def _sanitize_card(
    parsed: dict[str, Any],
    domain_cards_by_id: dict[str, dict[str, Any]],
    allowed_refs: set[str],
    available_domains: set[str],
) -> dict[str, Any]:
    card_type = _require_nonempty_string('card_type', parsed.get('card_type'), 'upper_reasoning_card')
    context = f'upper_reasoning_card[{card_type}]'
    source_card_ids = _require_allowed_strings('source_card_ids', parsed.get('source_card_ids', []), set(domain_cards_by_id.keys()), context)
    expected_domains = sorted({str(domain_cards_by_id[card_id].get('domain', '')).strip() for card_id in source_card_ids})
    supporting_domains = sorted(_require_allowed_strings('supporting_domains', parsed.get('supporting_domains', []), available_domains, context))
    if supporting_domains != expected_domains:
        raise ValueError(
            f'{context}: supporting_domains does not match source_card_ids. expected={expected_domains}, actual={supporting_domains}'
        )
    confidence = _require_nonempty_string('confidence', parsed.get('confidence'), context).lower()
    if confidence not in CONFIDENCE_VALUES:
        raise ValueError(f'{context}: confidence must be one of {sorted(CONFIDENCE_VALUES)}')
    return {
        'card_id': _require_nonempty_string('card_id', parsed.get('card_id'), context),
        'card_type': card_type,
        'title': _require_nonempty_string('title', parsed.get('title'), context),
        'summary': _require_nonempty_string('summary', parsed.get('summary'), context),
        'source_card_ids': source_card_ids,
        'supporting_domains': supporting_domains,
        'evidence_refs': _require_allowed_strings('evidence_refs', parsed.get('evidence_refs', []), allowed_refs, context),
        'confidence': confidence,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='상위 해석 카드를 생성합니다.')
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--manifest', default=str(DEFAULT_MANIFEST), help='manifest 경로')
    parser.add_argument('--model', help='모델 override')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    configure_logging('compose_upper_reasoning_cards', args.date)

    try:
        manifest = load_card_manifest(args.manifest)
        output_schema = load_json_schema(resolve_manifest_path(BASE_DIR, manifest['schema_path'], args.date))
        card_schema = load_json_schema(CARD_SCHEMA_PATH)
        domain_cards_schema = load_json_schema(DOMAIN_CARDS_SCHEMA_PATH)
        policy_path = resolve_manifest_path(BASE_DIR, manifest['policy_path'], args.date)
        policy = load_policy_mapping(policy_path)
        _require_policy_flags(policy, policy_path, manifest)

        domain_cards_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['domain_cards'], args.date)
        domain_cards_payload = _read_json(domain_cards_path)
        validate_instance(domain_cards_payload, domain_cards_schema, f'input_domain_cards[{domain_cards_path}]')

        output_prompt_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['prompt_input'], args.date)
        output_raw_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['raw'], args.date)
        output_cards_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['cards'], args.date)

        domain_cards = [card for card in domain_cards_payload.get('cards', []) if isinstance(card, dict)]
        if not domain_cards:
            raise ValueError(f'no domain semantic cards found: {domain_cards_path}')
        domain_cards_by_id = {str(card.get('card_id', '')).strip(): card for card in domain_cards if str(card.get('card_id', '')).strip()}
        available_domains = {str(card.get('domain', '')).strip() for card in domain_cards if str(card.get('domain', '')).strip()}
        if not available_domains:
            raise ValueError(f'domain semantic cards do not contain domain values: {domain_cards_path}')
        allowed_refs = {str(ref).strip() for card in domain_cards for ref in card.get('evidence_refs', []) or [] if str(ref).strip()}
        if not allowed_refs:
            raise ValueError(f'domain semantic cards do not contain evidence_refs: {domain_cards_path}')

        policy_cards = _require_list('cards', policy.get('cards'), f'policy[{policy_path}]')
        for index, card_policy in enumerate(policy_cards):
            if not isinstance(card_policy, dict):
                raise ValueError(f'policy[{policy_path}] cards[{index}] must be an object')
            card_type = _require_nonempty_string('card_type', card_policy.get('card_type'), f'policy[{policy_path}] cards[{index}]')
            _require_expected_domains(card_policy, available_domains, f'policy card_type={card_type}')

        prompt_input = {
            'run_date': args.date,
            'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
            'policy_cards': policy_cards,
            'domain_cards': domain_cards,
            'allowed_source_card_ids': sorted(domain_cards_by_id.keys()),
            'allowed_supporting_domains': sorted(available_domains),
            'allowed_evidence_refs': sorted(allowed_refs),
        }
        prompt_text = _build_prompt_text(manifest, args.date, prompt_input)
        effective_model = (args.model or manifest.get('model') or 'gpt-4.1-mini').strip()
        llm_result = call_findings_llm(
            prompt_text=prompt_text,
            schema=output_schema,
            model=effective_model,
            max_output_tokens=3000,
        )
        parsed_cards = [card for card in llm_result['parsed_output'].get('cards', []) if isinstance(card, dict)]
        parsed_by_type = {str(card.get('card_type', '')).strip(): card for card in parsed_cards if str(card.get('card_type', '')).strip()}
        expected_types = {
            str(card_policy.get('card_type', '')).strip()
            for card_policy in policy_cards
            if isinstance(card_policy, dict) and str(card_policy.get('card_type', '')).strip()
        }
        extra_types = sorted(card_type for card_type in parsed_by_type if card_type not in expected_types)
        if extra_types:
            raise ValueError(f'upper reasoning llm output contains unexpected card_type values: {extra_types}')

        cards: list[dict[str, Any]] = []
        for card_policy in policy_cards:
            card_type = _require_nonempty_string('card_type', card_policy.get('card_type'), f'policy[{policy_path}]')
            parsed = parsed_by_type.get(card_type)
            if parsed is None:
                raise ValueError(f'upper reasoning llm output missing card_type={card_type}')
            card = _sanitize_card(parsed, domain_cards_by_id, allowed_refs, available_domains)
            validate_instance(card, card_schema, f'upper_reasoning_card[{card_type}]')
            cards.append(card)

        if len(cards) != len(policy_cards):
            raise ValueError(f'partial upper reasoning output is not allowed: expected={len(policy_cards)}, actual={len(cards)}')

        cards_payload = {
            'version': 'upper_reasoning_cards.v1',
            'run_date': args.date,
            'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
            'cards': cards,
        }
        validate_instance(cards_payload, output_schema, 'upper_reasoning_cards')
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
        _write_json(output_cards_path, cards_payload)
        print(
            json.dumps(
                {
                    'status': 'ok',
                    'run_date': args.date,
                    'card_count': len(cards),
                    'cards_path': output_cards_path.relative_to(BASE_DIR).as_posix(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        logging.exception('compose_upper_reasoning_cards_failed')
        print(json.dumps({'status': 'error', 'message': str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
