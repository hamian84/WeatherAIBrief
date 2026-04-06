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


def _resolve_image_cards(feature_bundle: dict[str, Any], image_feature_cards: dict[str, Any]) -> list[dict[str, Any]]:
    cards = image_feature_cards.get("cards")
    if not isinstance(cards, list) or not cards:
        cards = feature_bundle.get("image_feature_cards")
    if not isinstance(cards, list) or not cards:
        raise ValueError("image_feature_cards source does not contain cards")
    return [card for card in cards if isinstance(card, dict)]


def _resolve_domain_sequence_features(feature_bundle: dict[str, Any]) -> list[dict[str, Any]]:
    items = feature_bundle.get("domain_sequence_features")
    if not isinstance(items, list) or not items:
        raise ValueError("feature_bundle does not contain domain_sequence_features")
    return [item for item in items if isinstance(item, dict)]


def _build_semantic_image_feature_cards(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for card in cards:
        regions_output: list[dict[str, Any]] = []
        for region in card.get("regions", []) or []:
            if not isinstance(region, dict):
                continue
            signals_output: list[dict[str, Any]] = []
            for signal in region.get("signals", []) or []:
                if not isinstance(signal, dict):
                    continue
                presence = str(signal.get("presence", "")).strip().lower()
                if presence == "no":
                    continue
                attributes_output: dict[str, str] = {}
                for attribute in signal.get("attributes", []) or []:
                    if not isinstance(attribute, dict):
                        continue
                    answer = str(attribute.get("answer", "")).strip()
                    attribute_key = str(attribute.get("attribute_key", "")).strip()
                    if not attribute_key or not answer or answer == "unknown":
                        continue
                    attributes_output[attribute_key] = answer
                signals_output.append(
                    {
                        "signal_key": str(signal.get("signal_key", "")).strip(),
                        "presence": presence,
                        "attributes": attributes_output,
                    }
                )
            if not signals_output:
                continue
            regions_output.append(
                {
                    "region_label": str(region.get("region_label", "")).strip(),
                    "signals": signals_output,
                }
            )
        output.append(
            {
                "domain": str(card.get("domain", "")).strip(),
                "image_ref": str(card.get("image_ref", "")).strip(),
                "valid_time": str(card.get("valid_time", "")).strip(),
                "regions": regions_output,
            }
        )
    return output


def _build_semantic_domain_sequence_features(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in items:
        image_sequence_output: list[dict[str, Any]] = []
        for image_step in item.get("image_sequence", []) or []:
            if not isinstance(image_step, dict):
                continue
            image_sequence_output.append(
                {
                    "image_ref": str(image_step.get("image_ref", "")).strip(),
                    "valid_time": str(image_step.get("valid_time", "")).strip(),
                }
            )

        signal_tracks_output: list[dict[str, Any]] = []
        for track in item.get("signal_tracks", []) or []:
            if not isinstance(track, dict):
                continue
            time_steps_output: list[dict[str, Any]] = []
            for time_step in track.get("time_steps", []) or []:
                if not isinstance(time_step, dict):
                    continue
                presence = str(time_step.get("presence", "")).strip().lower()
                if presence == "no":
                    continue
                attributes_output: dict[str, str] = {}
                for attribute in time_step.get("attributes", []) or []:
                    if not isinstance(attribute, dict):
                        continue
                    attribute_key = str(attribute.get("attribute_key", "")).strip()
                    answer = str(attribute.get("answer", "")).strip()
                    if not attribute_key or not answer or answer == "unknown":
                        continue
                    attributes_output[attribute_key] = answer
                time_steps_output.append(
                    {
                        "image_ref": str(time_step.get("image_ref", "")).strip(),
                        "valid_time": str(time_step.get("valid_time", "")).strip(),
                        "presence": presence,
                        "attributes": attributes_output,
                    }
                )
            if not time_steps_output:
                continue
            signal_tracks_output.append(
                {
                    "region_label": str(track.get("region_label", "")).strip(),
                    "signal_key": str(track.get("signal_key", "")).strip(),
                    "time_steps": time_steps_output,
                }
            )

        output.append(
            {
                "domain": str(item.get("domain", "")).strip(),
                "summary": {
                    "image_count": int((item.get("summary", {}) or {}).get("image_count", 0) or 0),
                    "active_regions": list((item.get("summary", {}) or {}).get("active_regions", []) or []),
                    "signal_keys_present": list((item.get("summary", {}) or {}).get("signal_keys_present", []) or []),
                },
                "image_sequence": image_sequence_output,
                "signal_tracks": signal_tracks_output,
            }
        )
    return output


def _build_evidence_catalog(cards: list[dict[str, Any]], region_label_map: dict[str, str]) -> list[dict[str, Any]]:
    signal_index: dict[str, dict[str, Any]] = {}
    region_index: dict[str, dict[str, Any]] = {}

    for card in cards:
        domain = str(card.get("domain", "")).strip()
        image_ref = str(card.get("image_ref", "")).strip()
        valid_time = str(card.get("valid_time", "")).strip()
        utc_hour = _extract_utc_hour(valid_time)
        if not domain:
            continue

        for region in card.get("regions", []) or []:
            if not isinstance(region, dict):
                continue
            region_id = str(region.get("region_id", "")).strip()
            region_label = _normalize_region_label(region_id, str(region.get("region_label", "")).strip(), region_label_map)
            regional_evidence_id = f"{domain}__{region_id}" if region_id else ""
            regional_entry = None
            if regional_evidence_id:
                regional_entry = region_index.setdefault(
                    regional_evidence_id,
                    {
                        "evidence_id": regional_evidence_id,
                        "evidence_type": "region_axis",
                        "domain": domain,
                        "region_id": region_id,
                        "region_labels": set([region_label] if region_label else []),
                        "occurrence_count": 0,
                        "active_image_refs": set(),
                        "active_valid_times": set(),
                        "active_utc_hours": set(),
                        "signal_keys": set(),
                    },
                )

            for signal in region.get("signals", []) or []:
                if not isinstance(signal, dict):
                    continue
                if str(signal.get("presence", "")).strip().lower() != "yes":
                    continue

                signal_key = str(signal.get("signal_key", "")).strip()
                if not signal_key:
                    continue

                if regional_entry is not None:
                    regional_entry["occurrence_count"] += 1
                    if image_ref:
                        regional_entry["active_image_refs"].add(image_ref)
                    if valid_time:
                        regional_entry["active_valid_times"].add(valid_time)
                    if utc_hour:
                        regional_entry["active_utc_hours"].add(utc_hour)
                    regional_entry["signal_keys"].add(signal_key)

                evidence_id = f"{domain}.{signal_key}"
                signal_entry = signal_index.setdefault(
                    evidence_id,
                    {
                        "evidence_id": evidence_id,
                        "evidence_type": "signal_track",
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
                signal_entry["occurrence_count"] += 1
                if image_ref:
                    signal_entry["active_image_refs"].add(image_ref)
                if valid_time:
                    signal_entry["active_valid_times"].add(valid_time)
                if utc_hour:
                    signal_entry["active_utc_hours"].add(utc_hour)
                if region_id:
                    signal_entry["region_ids"].add(region_id)
                if region_label:
                    signal_entry["region_labels"].add(region_label)

                for attribute in signal.get("attributes", []) or []:
                    if not isinstance(attribute, dict):
                        continue
                    attribute_key = str(attribute.get("attribute_key", "")).strip()
                    answer = str(attribute.get("answer", "")).strip()
                    if not attribute_key or not answer or answer == "unknown":
                        continue
                    bucket = signal_entry["attribute_summary"].setdefault(attribute_key, set())
                    bucket.add(answer)

    evidence_catalog: list[dict[str, Any]] = []

    for evidence_id in sorted(signal_index):
        item = signal_index[evidence_id]
        evidence_catalog.append(
            {
                "evidence_id": evidence_id,
                "evidence_type": item["evidence_type"],
                "domain": item["domain"],
                "signal_key": item["signal_key"],
                "occurrence_count": int(item["occurrence_count"]),
                "active_image_refs": _sorted_strings(item["active_image_refs"]),
                "active_valid_times": _sorted_strings(item["active_valid_times"]),
                "active_utc_hours": _sorted_strings(item["active_utc_hours"]),
                "region_ids": _sorted_strings(item["region_ids"]),
                "region_labels": _sorted_strings(item["region_labels"]),
                "attribute_summary": {
                    key: _sorted_strings(values)
                    for key, values in sorted(item["attribute_summary"].items())
                    if values
                },
            }
        )

    for evidence_id in sorted(region_index):
        item = region_index[evidence_id]
        evidence_catalog.append(
            {
                "evidence_id": evidence_id,
                "evidence_type": item["evidence_type"],
                "domain": item["domain"],
                "region_id": item["region_id"],
                "region_labels": _sorted_strings(item["region_labels"]),
                "signal_keys": _sorted_strings(item["signal_keys"]),
                "occurrence_count": int(item["occurrence_count"]),
                "active_image_refs": _sorted_strings(item["active_image_refs"]),
                "active_valid_times": _sorted_strings(item["active_valid_times"]),
                "active_utc_hours": _sorted_strings(item["active_utc_hours"]),
            }
        )

    return evidence_catalog


def build_direct_briefing_inputs(
    base_dir: Path,
    feature_bundle_path: Path,
    image_feature_cards_path: Path,
) -> dict[str, Any]:
    feature_bundle = read_json_object(feature_bundle_path)
    image_feature_cards = read_json_object(image_feature_cards_path)
    region_label_map = load_region_label_map(base_dir)

    cards = _resolve_image_cards(feature_bundle, image_feature_cards)
    domain_sequence_features = _resolve_domain_sequence_features(feature_bundle)
    evidence_catalog = _build_evidence_catalog(cards, region_label_map)
    semantic_image_feature_cards = _build_semantic_image_feature_cards(cards)
    semantic_domain_sequence_features = _build_semantic_domain_sequence_features(domain_sequence_features)

    allowed_evidence_refs = sorted(
        {
            image_ref
            for item in evidence_catalog
            for image_ref in item.get("active_image_refs", [])
            if str(image_ref).strip()
        }
    )
    allowed_regions = sorted(
        {
            region
            for item in evidence_catalog
            for region in item.get("region_labels", [])
            if str(region).strip()
        }
    )

    return {
        "feature_bundle_summary": {
            "run_date": str(feature_bundle.get("run_date", "")).strip(),
            "summary": feature_bundle.get("summary", {}),
            "domains": feature_bundle.get("domains", []),
        },
        "image_feature_cards": semantic_image_feature_cards,
        "domain_sequence_features": semantic_domain_sequence_features,
        "evidence_catalog": evidence_catalog,
        "allowed_evidence_ids": [item["evidence_id"] for item in evidence_catalog],
        "allowed_evidence_refs": allowed_evidence_refs,
        "allowed_regions": allowed_regions,
    }
