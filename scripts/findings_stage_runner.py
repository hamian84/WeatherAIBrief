from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from scripts.common.findings_llm_client import call_findings_llm

KST = ZoneInfo('Asia/Seoul')
TOP_TRACKS_PER_DOMAIN = 8
TOP_IMAGE_SAMPLES_PER_DOMAIN = 4
TOP_STATIONS_PER_METRIC = 5
ALLOWED_FINDINGS_ARRAY_KEYS = (
    'synoptic_primary_cards',
    'cross_layer_cards',
    'regional_response_cards',
    'time_change_cards',
)


def _read_json(path: Path) -> dict[str, Any]:
    for encoding in ('utf-8-sig', 'utf-8'):
        try:
            payload = json.loads(path.read_text(encoding=encoding))
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f'failed to read json with supported encodings: {path}')
    if not isinstance(payload, dict):
        raise ValueError(f'json payload must be an object: {path}')
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _safe_float(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if number <= -8.5:
        return None
    return number


def _build_station_metric_rows(rows: list[dict[str, str]], field_name: str, reverse: bool, limit: int) -> list[dict[str, Any]]:
    scored: list[tuple[float, dict[str, str]]] = []
    for row in rows:
        value = _safe_float(row.get(field_name))
        if value is None:
            continue
        scored.append((value, row))
    scored.sort(key=lambda item: item[0], reverse=reverse)
    results: list[dict[str, Any]] = []
    for value, row in scored[:limit]:
        results.append({'stn_id': str(row.get('stnId', '')).strip(), 'value': round(value, 3)})
    return results


def _mean_of_field(rows: list[dict[str, str]], field_name: str) -> float | None:
    values = [_safe_float(row.get(field_name)) for row in rows]
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 3)


def _build_asos_summary(base_dir: Path, run_date: str) -> dict[str, Any]:
    curated_path = base_dir / 'dain' / run_date / 'curated' / 'asos' / 'asos_daily_summary.csv'
    if not curated_path.exists():
        return {'status': 'missing', 'source_path': str(curated_path.relative_to(base_dir)).replace('\\', '/')}
    with curated_path.open('r', encoding='utf-8-sig', newline='') as handle:
        rows = [row for row in csv.DictReader(handle)]
    rainy_station_count = sum(1 for row in rows if (_safe_float(row.get('mean_RN_DAY')) or 0.0) > 0.0)
    return {
        'status': 'ok',
        'source_path': str(curated_path.relative_to(base_dir)).replace('\\', '/'),
        'station_count': len(rows),
        'rainy_station_count': rainy_station_count,
        'mean_surface_pressure_hpa': _mean_of_field(rows, 'mean_PS'),
        'mean_wind_speed_ms': _mean_of_field(rows, 'mean_WS'),
        'mean_humidity_pct': _mean_of_field(rows, 'mean_HM'),
        'top_rain_stations': _build_station_metric_rows(rows, 'mean_RN_DAY', True, TOP_STATIONS_PER_METRIC),
        'top_wind_stations': _build_station_metric_rows(rows, 'mean_WS', True, TOP_STATIONS_PER_METRIC),
        'lowest_pressure_stations': _build_station_metric_rows(rows, 'mean_PS', False, TOP_STATIONS_PER_METRIC),
    }


def _summarize_track(track: dict[str, Any]) -> dict[str, Any]:
    time_steps = [step for step in track.get('time_steps', []) if isinstance(step, dict)]
    active_steps = [step for step in time_steps if str(step.get('presence', '')) in {'yes', 'unknown'}]
    attribute_summary: dict[str, list[str]] = {}
    for step in active_steps:
        for attribute in step.get('attributes', []) or []:
            if not isinstance(attribute, dict):
                continue
            key = str(attribute.get('attribute_key', '')).strip()
            answer = str(attribute.get('answer', '')).strip()
            if not key or not answer:
                continue
            bucket = attribute_summary.setdefault(key, [])
            if answer not in bucket:
                bucket.append(answer)
    notes: list[str] = []
    for step in active_steps[:3]:
        note = str(step.get('note', '')).strip()
        if note and note not in notes:
            notes.append(note)
    return {
        'region_id': str(track.get('region_id', '')).strip(),
        'region_label': str(track.get('region_label', '')).strip(),
        'signal_key': str(track.get('signal_key', '')).strip(),
        'active_image_refs': [str(item).strip() for item in track.get('active_image_refs', []) if str(item).strip()],
        'active_step_count': len(active_steps),
        'attribute_summary': attribute_summary,
        'notes': notes,
    }


def _build_domain_briefs(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    image_cards = [card for card in bundle.get('image_feature_cards', []) if isinstance(card, dict)]
    cards_by_domain: dict[str, list[dict[str, Any]]] = {}
    for card in image_cards:
        domain = str(card.get('domain', '')).strip()
        if not domain:
            continue
        cards_by_domain.setdefault(domain, []).append(card)

    briefs: list[dict[str, Any]] = []
    for domain_item in bundle.get('domain_sequence_features', []) or []:
        if not isinstance(domain_item, dict):
            continue
        domain = str(domain_item.get('domain', '')).strip()
        signal_tracks = [track for track in domain_item.get('signal_tracks', []) if isinstance(track, dict)]
        summarized_tracks = [_summarize_track(track) for track in signal_tracks]
        summarized_tracks.sort(
            key=lambda item: (-int(item['active_step_count']), -len(item['active_image_refs']), item['region_id'], item['signal_key'])
        )
        image_samples: list[dict[str, Any]] = []
        for card in sorted(cards_by_domain.get(domain, []), key=lambda item: (str(item.get('valid_time', '')), str(item.get('image_ref', ''))))[:TOP_IMAGE_SAMPLES_PER_DOMAIN]:
            region_samples = []
            for region in card.get('regions', []) or []:
                if not isinstance(region, dict):
                    continue
                region_samples.append(
                    {
                        'region_id': str(region.get('region_id', '')).strip(),
                        'region_label': str(region.get('region_label', '')).strip(),
                        'signal_count': int(region.get('signal_count', 0) or 0),
                    }
                )
            image_samples.append(
                {
                    'image_ref': str(card.get('image_ref', '')).strip(),
                    'valid_time': str(card.get('valid_time', '')).strip(),
                    'active_signal_count': int(card.get('active_signal_count', 0) or 0),
                    'active_region_count': int(card.get('active_region_count', 0) or 0),
                    'region_samples': region_samples[:4],
                }
            )
        briefs.append(
            {
                'domain': domain,
                'summary': domain_item.get('summary', {}),
                'image_sequence': domain_item.get('image_sequence', []),
                'top_signal_tracks': summarized_tracks[:TOP_TRACKS_PER_DOMAIN],
                'image_card_samples': image_samples,
            }
        )
    return briefs


def _render_prompt(template_text: str, prompt_input: dict[str, Any]) -> str:
    replacements = {
        '{{RUN_DATE}}': str(prompt_input['run_date']),
        '{{FEATURE_BUNDLE_JSON}}': json.dumps(prompt_input['feature_bundle'], ensure_ascii=False, indent=2),
        '{{ASOS_SUMMARY_JSON}}': json.dumps(prompt_input['asos_observed_context'], ensure_ascii=False, indent=2),
        '{{ALLOWED_EVIDENCE_REFS_JSON}}': json.dumps(prompt_input['allowed_evidence_refs'], ensure_ascii=False, indent=2),
    }
    rendered = template_text
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def _sanitize_evidence_refs(values: Any, allowed_refs: set[str]) -> list[str]:
    cleaned: list[str] = []
    for value in values or []:
        text = str(value).strip()
        if not text or text not in allowed_refs or text in cleaned:
            continue
        cleaned.append(text)
    return cleaned


def _sanitize_findings_payload(parsed_output: dict[str, Any], allowed_refs: set[str]) -> dict[str, Any]:
    cleaned = {
        'summary': str(parsed_output.get('summary', '')).strip(),
        'synoptic_primary_cards': [],
        'cross_layer_cards': [],
        'regional_response_cards': [],
        'time_change_cards': [],
    }
    for key in ALLOWED_FINDINGS_ARRAY_KEYS:
        cards = parsed_output.get(key, [])
        if not isinstance(cards, list):
            continue
        for card in cards:
            if not isinstance(card, dict):
                continue
            item = dict(card)
            item['evidence_refs'] = _sanitize_evidence_refs(item.get('evidence_refs', []), allowed_refs)
            cleaned[key].append(item)
    return cleaned


def build_findings_prompt_input(base_dir: Path, run_date: str) -> dict[str, Any]:
    feature_bundle_path = base_dir / 'daio' / run_date / 'features' / 'feature_bundle.json'
    if not feature_bundle_path.exists():
        raise FileNotFoundError(f'feature bundle not found: {feature_bundle_path}')
    bundle = _read_json(feature_bundle_path)
    image_feature_cards = [card for card in bundle.get('image_feature_cards', []) if isinstance(card, dict)]
    allowed_evidence_refs = sorted({str(card.get('image_ref', '')).strip() for card in image_feature_cards if str(card.get('image_ref', '')).strip()})
    return {
        'run_date': run_date,
        'feature_bundle_path': str(feature_bundle_path.relative_to(base_dir)).replace('\\', '/'),
        'feature_bundle': bundle,
        'asos_observed_context': _build_asos_summary(base_dir, run_date),
        'allowed_evidence_refs': allowed_evidence_refs,
    }


def run_findings_stage(base_dir: Path, run_date: str, dry_run: bool = False, model: str | None = None) -> dict[str, Any]:
    findings_root = base_dir / 'daio' / run_date / 'findings'
    prompt_template_path = base_dir / 'prompts' / 'templates' / 'findings_bundle_prompt.txt'
    schema_path = base_dir / 'prompts' / 'schemas' / 'findings_bundle_response.schema.json'
    if not prompt_template_path.exists():
        raise FileNotFoundError(f'findings prompt template not found: {prompt_template_path}')
    if not schema_path.exists():
        raise FileNotFoundError(f'findings schema not found: {schema_path}')

    prompt_input = build_findings_prompt_input(base_dir, run_date)
    prompt_input_path = findings_root / 'findings_prompt_input.json'
    _write_json(prompt_input_path, prompt_input)

    if dry_run:
        return {
            'status': 'dry_run',
            'run_date': run_date,
            'prompt_input_path': str(prompt_input_path.relative_to(base_dir)).replace('\\', '/'),
            'domain_count': len(prompt_input['feature_bundle'].get('domains', [])),
            'allowed_evidence_ref_count': len(prompt_input['allowed_evidence_refs']),
        }

    prompt_template = prompt_template_path.read_text(encoding='utf-8-sig')
    schema = _read_json(schema_path)
    prompt_text = _render_prompt(prompt_template, prompt_input)
    effective_model = (model or 'gpt-4.1-mini').strip()
    llm_result = call_findings_llm(
        prompt_text=prompt_text,
        schema=schema,
        model=effective_model,
        max_output_tokens=5000,
    )

    raw_payload = {
        'run_date': run_date,
        'generated_at_kst': datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S'),
        'model': effective_model,
        'prompt_input_path': str(prompt_input_path.relative_to(base_dir)).replace('\\', '/'),
        'llm_result': llm_result,
    }
    raw_path = findings_root / 'findings_llm_raw.json'
    _write_json(raw_path, raw_payload)

    sanitized = _sanitize_findings_payload(llm_result['parsed_output'], set(prompt_input['allowed_evidence_refs']))
    findings_bundle = {
        'version': 'findings_bundle.v1',
        'date': run_date,
        'analysis_time_kst': datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S'),
        'summary': sanitized['summary'],
        'synoptic_primary_cards': sanitized['synoptic_primary_cards'],
        'cross_layer_cards': sanitized['cross_layer_cards'],
        'regional_response_cards': sanitized['regional_response_cards'],
        'time_change_cards': sanitized['time_change_cards'],
        'source_feature_bundle': prompt_input['feature_bundle_path'],
        'prompt_input_snapshot': {
            'feature_summary': prompt_input['feature_bundle'].get('summary', {}),
            'domain_count': len(prompt_input['feature_bundle'].get('domains', [])),
            'allowed_evidence_ref_count': len(prompt_input['allowed_evidence_refs']),
            'asos_status': prompt_input['asos_observed_context'].get('status'),
        },
        'meta': {
            'model': effective_model,
            'raw_response_path': str(raw_path.relative_to(base_dir)).replace('\\', '/'),
        },
    }
    findings_bundle_path = findings_root / 'findings_bundle.json'
    _write_json(findings_bundle_path, findings_bundle)

    return {
        'status': 'ok',
        'run_date': run_date,
        'prompt_input_path': str(prompt_input_path.relative_to(base_dir)).replace('\\', '/'),
        'raw_path': str(raw_path.relative_to(base_dir)).replace('\\', '/'),
        'findings_bundle_path': str(findings_bundle_path.relative_to(base_dir)).replace('\\', '/'),
        'model': effective_model,
        'synoptic_primary_card_count': len(findings_bundle['synoptic_primary_cards']),
        'cross_layer_card_count': len(findings_bundle['cross_layer_cards']),
        'regional_response_card_count': len(findings_bundle['regional_response_cards']),
        'time_change_card_count': len(findings_bundle['time_change_cards']),
    }
