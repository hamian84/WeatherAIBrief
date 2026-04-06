from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be an object: {path}")
    return payload


def load_region_label_map(base_dir: Path) -> dict[str, str]:
    tables_dir = base_dir / "prompts" / "tables"
    mapping: dict[str, str] = {}
    for csv_path in sorted(tables_dir.glob("*_feature_stage1_bundle_table.csv")):
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                region_id = str(row.get("region_id", "")).strip()
                region_label = str(row.get("region_label", "")).strip()
                if region_id and region_label and region_id not in mapping:
                    mapping[region_id] = region_label
    if not mapping:
        raise ValueError(f"region label map is empty: {tables_dir}")
    return mapping


def _normalize_region_label(region_id: str, raw_label: str, region_label_map: dict[str, str]) -> str:
    if raw_label and "?" not in raw_label:
        return raw_label
    mapped = region_label_map.get(region_id, "").strip()
    if mapped:
        return mapped
    return raw_label or region_id


def _extract_utc_hour(valid_time: str) -> str:
    text = valid_time.strip()
    if len(text) >= 13:
        return f"{text[11:13]}UTC"
    return "unknown"


def _sorted_strings(values: set[str]) -> list[str]:
    return sorted(item for item in values if str(item).strip())


def _build_observation_note(domain: str, signal_key: str, regions: list[str], utc_hours: list[str], occurrence_count: int) -> str:
    region_text = ", ".join(regions[:3]) if regions else "영역 미상"
    hour_text = ", ".join(utc_hours) if utc_hours else "시각 미상"
    return f"{domain}에서 {signal_key}가 {region_text}을 중심으로 {hour_text}에 걸쳐 {occurrence_count}회 반복 관측되었다."


def build_direct_briefing_inputs(
    base_dir: Path,
    feature_bundle_path: Path,
    image_feature_cards_path: Path,
) -> dict[str, Any]:
    feature_bundle = read_json_object(feature_bundle_path)
    image_feature_cards = read_json_object(image_feature_cards_path)
    region_label_map = load_region_label_map(base_dir)

    cards = image_feature_cards.get("cards")
    if not isinstance(cards, list) or not cards:
        cards = feature_bundle.get("image_feature_cards")
    if not isinstance(cards, list) or not cards:
        raise ValueError("image_feature_cards source does not contain cards")

    summary_index: dict[str, dict[str, Any]] = {}
    regional_index: dict[str, dict[str, Any]] = {}
    for card in cards:
        if not isinstance(card, dict):
            continue
        domain = str(card.get("domain", "")).strip()
        image_ref = str(card.get("image_ref", "")).strip()
        valid_time = str(card.get("valid_time", "")).strip()
        utc_hour = _extract_utc_hour(valid_time)
        for region in card.get("regions", []) or []:
            if not isinstance(region, dict):
                continue
            region_id = str(region.get("region_id", "")).strip()
            region_label = _normalize_region_label(region_id, str(region.get("region_label", "")).strip(), region_label_map)
            regional_evidence_id = f"{domain}__{region_id}" if domain and region_id else ""
            regional_entry = None
            if regional_evidence_id:
                regional_entry = regional_index.setdefault(
                    regional_evidence_id,
                    {
                        "evidence_id": regional_evidence_id,
                        "domain": domain,
                        "region_id": region_id,
                        "region_label": region_label,
                        "occurrence_count": 0,
                        "active_image_refs": set(),
                        "active_valid_times": set(),
                        "active_utc_hours": set(),
                        "active_signals": set(),
                    },
                )
            for signal in region.get("signals", []) or []:
                if not isinstance(signal, dict):
                    continue
                if str(signal.get("presence", "")).strip().lower() != "yes":
                    continue
                signal_key = str(signal.get("signal_key", "")).strip()
                if not domain or not signal_key:
                    continue
                if regional_entry is not None:
                    regional_entry["occurrence_count"] += 1
                    if image_ref:
                        regional_entry["active_image_refs"].add(image_ref)
                    if valid_time:
                        regional_entry["active_valid_times"].add(valid_time)
                    if utc_hour:
                        regional_entry["active_utc_hours"].add(utc_hour)
                    regional_entry["active_signals"].add(signal_key)
                evidence_id = f"{domain}.{signal_key}"
                entry = summary_index.setdefault(
                    evidence_id,
                    {
                        "evidence_id": evidence_id,
                        "domain": domain,
                        "signal_key": signal_key,
                        "occurrence_count": 0,
                        "active_image_refs": set(),
                        "active_valid_times": set(),
                        "active_utc_hours": set(),
                        "region_ids": set(),
                        "region_labels": set(),
                        "attribute_summary": {},
                    },
                )
                entry["occurrence_count"] += 1
                if image_ref:
                    entry["active_image_refs"].add(image_ref)
                if valid_time:
                    entry["active_valid_times"].add(valid_time)
                if utc_hour:
                    entry["active_utc_hours"].add(utc_hour)
                if region_id:
                    entry["region_ids"].add(region_id)
                if region_label:
                    entry["region_labels"].add(region_label)
                for attribute in signal.get("attributes", []) or []:
                    if not isinstance(attribute, dict):
                        continue
                    attribute_key = str(attribute.get("attribute_key", "")).strip()
                    answer = str(attribute.get("answer", "")).strip()
                    if not attribute_key or not answer or answer == "unknown":
                        continue
                    bucket = entry["attribute_summary"].setdefault(attribute_key, set())
                    bucket.add(answer)

    evidence_items: list[dict[str, Any]] = []
    for evidence_id in sorted(summary_index):
        item = summary_index[evidence_id]
        region_labels = _sorted_strings(item["region_labels"])
        utc_hours = _sorted_strings(item["active_utc_hours"])
        attribute_summary = {
            key: _sorted_strings(values)
            for key, values in sorted(item["attribute_summary"].items())
            if values
        }
        evidence_items.append(
            {
                "evidence_id": evidence_id,
                "domain": item["domain"],
                "signal_key": item["signal_key"],
                "occurrence_count": int(item["occurrence_count"]),
                "active_image_refs": _sorted_strings(item["active_image_refs"]),
                "active_valid_times": _sorted_strings(item["active_valid_times"]),
                "active_utc_hours": utc_hours,
                "region_ids": _sorted_strings(item["region_ids"]),
                "region_labels": region_labels,
                "attribute_summary": attribute_summary,
                "observation_note": _build_observation_note(
                    str(item["domain"]),
                    str(item["signal_key"]),
                    region_labels,
                    utc_hours,
                    int(item["occurrence_count"]),
                ),
            }
        )

    for evidence_id in sorted(regional_index):
        item = regional_index[evidence_id]
        region_label = str(item["region_label"]).strip()
        utc_hours = _sorted_strings(item["active_utc_hours"])
        active_signals = _sorted_strings(item["active_signals"])
        evidence_items.append(
            {
                "evidence_id": evidence_id,
                "domain": item["domain"],
                "region_id": item["region_id"],
                "region_labels": [region_label] if region_label else [],
                "signal_keys": active_signals,
                "occurrence_count": int(item["occurrence_count"]),
                "active_image_refs": _sorted_strings(item["active_image_refs"]),
                "active_valid_times": _sorted_strings(item["active_valid_times"]),
                "active_utc_hours": utc_hours,
                "observation_note": (
                    f"{item['domain']}에서 {region_label or item['region_id']} 영역은 "
                    f"{', '.join(active_signals[:6])} 신호가 {', '.join(utc_hours)}에 반복 관측된 핵심 구역이다."
                ),
            }
        )

    allowed_evidence_refs = sorted(
        {
            image_ref
            for item in evidence_items
            for image_ref in item.get("active_image_refs", [])
            if str(image_ref).strip()
        }
    )
    allowed_regions = sorted(
        {
            region
            for item in evidence_items
            for region in item.get("region_labels", [])
            if str(region).strip()
        }
    )

    return {
        "feature_bundle": feature_bundle,
        "feature_bundle_summary": {
            "run_date": str(feature_bundle.get("run_date", "")).strip(),
            "summary": feature_bundle.get("summary", {}),
            "domains": feature_bundle.get("domains", []),
        },
        "image_feature_cards": image_feature_cards,
        "image_feature_signal_summary": evidence_items,
        "allowed_evidence_ids": [item["evidence_id"] for item in evidence_items],
        "allowed_evidence_refs": allowed_evidence_refs,
        "allowed_regions": allowed_regions,
    }
