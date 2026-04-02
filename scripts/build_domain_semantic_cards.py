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
DEFAULT_MANIFEST = BASE_DIR / 'prompts' / 'manifests' / 'domain_semantic_cards_manifest.yaml'
DOMAIN_CARD_SCHEMA_PATH = BASE_DIR / 'prompts' / 'schemas' / 'domain_semantic_card.schema.json'
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


def _require_string_list(field_name: str, values: Any, context: str, allow_empty: bool = True) -> list[str]:
    raw_items = _require_list(field_name, values, context)
    items: list[str] = []
    for index, value in enumerate(raw_items):
        text = str(value).strip()
        if not text:
            raise ValueError(f'{context}: empty string is not allowed: {field_name}[{index}]')
        items.append(text)
    if not allow_empty and not items:
        raise ValueError(f'{context}: field must not be empty: {field_name}')
    return items


def _require_allowed_refs(field_name: str, values: Any, allowed_refs: set[str], context: str) -> list[str]:
    refs = _require_string_list(field_name, values, context, allow_empty=False)
    invalid_refs = [ref for ref in refs if ref not in allowed_refs]
    if invalid_refs:
        raise ValueError(f'{context}: field contains refs outside allowed_evidence_refs: {field_name} -> {invalid_refs}')
    return refs


def _collect_domain_payload(
    feature_bundle: dict[str, Any],
    domain: str,
    feature_bundle_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], dict[str, str]]:
    sequence_items = _require_list('domain_sequence_features', feature_bundle.get('domain_sequence_features'), f'feature_bundle[{feature_bundle_path}]')
    image_items = _require_list('image_feature_cards', feature_bundle.get('image_feature_cards'), f'feature_bundle[{feature_bundle_path}]')

    domain_sequence: dict[str, Any] | None = None
    for index, item in enumerate(sequence_items):
        if not isinstance(item, dict):
            raise ValueError(f'feature_bundle[{feature_bundle_path}] domain_sequence_features[{index}] must be an object')
        if str(item.get('domain', '')).strip() == domain:
            domain_sequence = item
            break

    image_cards: list[dict[str, Any]] = []
    for index, item in enumerate(image_items):
        if not isinstance(item, dict):
            raise ValueError(f'feature_bundle[{feature_bundle_path}] image_feature_cards[{index}] must be an object')
        if str(item.get('domain', '')).strip() == domain:
            image_cards.append(item)

    if domain_sequence is None:
        raise ValueError(f'feature bundle missing domain_sequence_feature for domain={domain}: {feature_bundle_path}')
    if not image_cards:
        raise ValueError(f'feature bundle missing image_feature_cards for domain={domain}: {feature_bundle_path}')

    allowed_refs: list[str] = []
    valid_times: list[str] = []
    for index, card in enumerate(image_cards):
        image_ref = _require_nonempty_string('image_ref', card.get('image_ref'), f'image_feature_cards[{domain}][{index}]')
        valid_time = _require_nonempty_string('valid_time', card.get('valid_time'), f'image_feature_cards[{domain}][{index}]')
        allowed_refs.append(image_ref)
        valid_times.append(valid_time)

    return domain_sequence, image_cards, allowed_refs, {'start': min(valid_times), 'end': max(valid_times)}


def _build_prompt_text(manifest: dict[str, Any], run_date: str, domain_policy: dict[str, Any], prompt_input: dict[str, Any]) -> str:
    return "\n".join(
        [
            str(manifest.get('prompt_instructions') or '').strip(),
            f'run_date={run_date}',
            f"allow_new_claims={str(manifest.get('allow_new_claims', False)).lower()}",
            'domain_policy_json=',
            json.dumps(domain_policy, ensure_ascii=False, indent=2),
            'domain_prompt_input_json=',
            json.dumps(prompt_input, ensure_ascii=False, indent=2),
        ]
    )


def _sanitize_key_points(values: Any, allowed_refs: set[str], context: str) -> list[dict[str, Any]]:
    raw_items = _require_list('key_points', values, context)
    items: list[dict[str, Any]] = []
    for index, value in enumerate(raw_items):
        if not isinstance(value, dict):
            raise ValueError(f'{context}: key_points[{index}] must be an object')
        item_context = f'{context} key_points[{index}]'
        items.append(
            {
                'label': _require_nonempty_string('label', value.get('label'), item_context),
                'detail': _require_nonempty_string('detail', value.get('detail'), item_context),
                'regions': _require_string_list('regions', value.get('regions', []), item_context, allow_empty=True),
                'evidence_refs': _require_allowed_refs('evidence_refs', value.get('evidence_refs', []), allowed_refs, item_context),
            }
        )
    return items


def _sanitize_observed_links(values: Any, allowed_refs: set[str], context: str) -> list[dict[str, Any]]:
    raw_items = _require_list('observed_links', values, context)
    items: list[dict[str, Any]] = []
    for index, value in enumerate(raw_items):
        if not isinstance(value, dict):
            raise ValueError(f'{context}: observed_links[{index}] must be an object')
        item_context = f'{context} observed_links[{index}]'
        items.append(
            {
                'link_type': _require_nonempty_string('link_type', value.get('link_type'), item_context),
                'detail': _require_nonempty_string('detail', value.get('detail'), item_context),
                'evidence_refs': _require_allowed_refs('evidence_refs', value.get('evidence_refs', []), allowed_refs, item_context),
            }
        )
    return items


def _sanitize_domain_card(
    parsed_output: dict[str, Any],
    domain: str,
    allowed_refs: set[str],
    expected_time_window: dict[str, str],
) -> dict[str, Any]:
    context = f'domain_card[{domain}]'
    time_window = parsed_output.get('time_window')
    if not isinstance(time_window, dict):
        raise ValueError(f'{context}: time_window must be an object')
    start = _require_nonempty_string('time_window.start', time_window.get('start'), context)
    end = _require_nonempty_string('time_window.end', time_window.get('end'), context)
    confidence = _require_nonempty_string('confidence', parsed_output.get('confidence'), context).lower()
    if confidence not in CONFIDENCE_VALUES:
        raise ValueError(f'{context}: confidence must be one of {sorted(CONFIDENCE_VALUES)}')

    card = {
        'card_id': _require_nonempty_string('card_id', parsed_output.get('card_id'), context),
        'domain': domain,
        'title': _require_nonempty_string('title', parsed_output.get('title'), context),
        'summary': _require_nonempty_string('summary', parsed_output.get('summary'), context),
        'time_window': {'start': start, 'end': end},
        'key_points': _sanitize_key_points(parsed_output.get('key_points', []), allowed_refs, context),
        'observed_links': _sanitize_observed_links(parsed_output.get('observed_links', []), allowed_refs, context),
        'evidence_refs': _require_allowed_refs('evidence_refs', parsed_output.get('evidence_refs', []), allowed_refs, context),
        'source_image_refs': _require_allowed_refs('source_image_refs', parsed_output.get('source_image_refs', []), allowed_refs, context),
        'confidence': confidence,
    }
    if card['time_window'] != expected_time_window:
        raise ValueError(
            f'domain_card[{domain}]: time_window mismatch. expected={expected_time_window}, actual={card["time_window"]}'
        )
    return card


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='도메인 의미 카드를 생성합니다.')
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--manifest', default=str(DEFAULT_MANIFEST), help='manifest 경로')
    parser.add_argument('--model', help='모델 override')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    os.chdir(BASE_DIR)
    load_project_env(BASE_DIR)
    configure_logging('build_domain_semantic_cards', args.date)

    try:
        manifest = load_card_manifest(args.manifest)
        schema = load_json_schema(resolve_manifest_path(BASE_DIR, manifest['schema_path'], args.date))
        card_schema = load_json_schema(DOMAIN_CARD_SCHEMA_PATH)
        policy_path = resolve_manifest_path(BASE_DIR, manifest['policy_path'], args.date)
        policy = load_policy_mapping(policy_path)
        _require_policy_flags(policy, policy_path, manifest)

        feature_bundle_path = resolve_manifest_path(BASE_DIR, manifest['input_paths']['feature_bundle'], args.date)
        feature_bundle = _read_json(feature_bundle_path)
        output_prompt_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['prompt_input'], args.date)
        output_raw_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['raw'], args.date)
        output_cards_path = resolve_manifest_path(BASE_DIR, manifest['output_paths']['cards'], args.date)

        domain_policies = _require_list('domains', policy.get('domains'), f'policy[{policy_path}]')
        prompt_inputs: list[dict[str, Any]] = []
        raw_results: list[dict[str, Any]] = []
        cards: list[dict[str, Any]] = []
        effective_model = (args.model or manifest.get('model') or 'gpt-4.1-mini').strip()

        for index, domain_policy in enumerate(domain_policies):
            if not isinstance(domain_policy, dict):
                raise ValueError(f'policy[{policy_path}] domains[{index}] must be an object')
            domain = _require_nonempty_string('domain', domain_policy.get('domain'), f'policy[{policy_path}] domains[{index}]')
            domain_sequence, image_cards, allowed_refs, expected_time_window = _collect_domain_payload(feature_bundle, domain, feature_bundle_path)
            prompt_input = {
                'run_date': args.date,
                'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
                'domain': domain,
                'domain_policy': domain_policy,
                'feature_summary': feature_bundle.get('summary', {}),
                'domain_sequence_feature': domain_sequence,
                'image_feature_cards': image_cards,
                'expected_time_window': expected_time_window,
                'allowed_source_image_refs': allowed_refs,
                'allowed_evidence_refs': allowed_refs,
            }
            prompt_text = _build_prompt_text(manifest, args.date, domain_policy, prompt_input)
            llm_result = call_findings_llm(
                prompt_text=prompt_text,
                schema=card_schema,
                model=effective_model,
                max_output_tokens=2500,
            )
            card = _sanitize_domain_card(llm_result['parsed_output'], domain, set(allowed_refs), expected_time_window)
            validate_instance(card, card_schema, f'domain_semantic_card[{domain}]')
            prompt_inputs.append(prompt_input)
            raw_results.append({'domain': domain, 'prompt_input': prompt_input, 'llm_result': llm_result})
            cards.append(card)

        if len(cards) != len(domain_policies):
            raise ValueError(f'partial domain card output is not allowed: expected={len(domain_policies)}, actual={len(cards)}')

        cards_payload = {
            'version': 'domain_semantic_cards.v1',
            'run_date': args.date,
            'allow_new_claims': bool(manifest.get('allow_new_claims', False)),
            'cards': cards,
        }
        validate_instance(cards_payload, schema, 'domain_semantic_cards')
        _write_json(output_prompt_path, {'run_date': args.date, 'items': prompt_inputs})
        _write_json(
            output_raw_path,
            {
                'run_date': args.date,
                'generated_at_kst': datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S'),
                'model': effective_model,
                'results': raw_results,
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
        logging.exception('build_domain_semantic_cards_failed')
        print(json.dumps({'status': 'error', 'message': str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
