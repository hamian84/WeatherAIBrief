from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ACTIVE_PRESENCE_ANSWERS = {"yes", "unknown"}


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be an object: {path}")
    return payload


def _load_stage_records(features_root: Path, domain: str, stage_name: str) -> list[dict[str, Any]]:
    path = features_root / domain / f"{stage_name}_normalized.json"
    if not path.exists():
        if stage_name == "stage2":
            return []
        raise FileNotFoundError(f"required normalized artifact not found: {path}")
    payload = _read_json(path)
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"normalized artifact missing records list: {path}")
    return [record for record in records if isinstance(record, dict)]


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(record.get("valid_time", "")),
        str(record.get("image_ref", "")),
        str(record.get("region_id", "")),
        str(record.get("signal_key", "")),
    )


def _attribute_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("attribute_key", "")),
        str(record.get("question_id", "")),
    )


def _build_signal_entry(stage1_record: dict[str, Any], stage2_records: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_stage2 = sorted(stage2_records, key=_attribute_sort_key)
    return {
        "signal_key": stage1_record["signal_key"],
        "presence": stage1_record["model_answer_normalized"],
        "presence_question_id": stage1_record["question_id"],
        "presence_question_text": stage1_record["question_text"],
        "presence_note": stage1_record.get("note", ""),
        "attributes": [
            {
                "attribute_key": row["attribute_key"],
                "question_id": row["question_id"],
                "question_text": row["question_text"],
                "answer": row["model_answer_normalized"],
                "note": row.get("note", ""),
                "allowed_answers": row["allowed_answers"],
            }
            for row in sorted_stage2
        ],
    }


def _build_image_feature_cards(
    domain: str,
    stage1_records: list[dict[str, Any]],
    stage2_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    stage1_by_image: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in sorted(stage1_records, key=_record_sort_key):
        stage1_by_image[str(record["image_ref"])].append(record)

    stage2_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in stage2_records:
        key = (str(record["image_ref"]), str(record["region_id"]), str(record["signal_key"]))
        stage2_by_key[key].append(record)

    cards: list[dict[str, Any]] = []
    for image_ref, image_stage1_records in stage1_by_image.items():
        first = image_stage1_records[0]
        answer_counts = Counter(str(record["model_answer_normalized"]) for record in image_stage1_records)
        active_stage1_records = [
            record
            for record in image_stage1_records
            if str(record["model_answer_normalized"]) in ACTIVE_PRESENCE_ANSWERS
        ]
        active_stage1_records.sort(key=lambda row: (str(row["region_id"]), str(row["signal_key"])))

        regions: list[dict[str, Any]] = []
        region_ids = sorted({str(record["region_id"]) for record in active_stage1_records})
        for region_id in region_ids:
            region_rows = [record for record in active_stage1_records if str(record["region_id"]) == region_id]
            region_label = str(region_rows[0]["region_label"])
            signals = [
                _build_signal_entry(
                    stage1_record=row,
                    stage2_records=stage2_by_key.get((image_ref, region_id, str(row["signal_key"])), []),
                )
                for row in region_rows
            ]
            regions.append(
                {
                    "region_id": region_id,
                    "region_label": region_label,
                    "signal_count": len(signals),
                    "signals": signals,
                }
            )

        cards.append(
            {
                "domain": domain,
                "image_ref": image_ref,
                "valid_time": str(first["valid_time"]),
                "source_image": str(first["source_image"]),
                "stage1_answer_counts": dict(answer_counts),
                "active_signal_count": len(active_stage1_records),
                "active_region_count": len(regions),
                "stage2_attribute_count": sum(
                    1 for record in stage2_records if str(record.get("image_ref", "")) == image_ref
                ),
                "regions": regions,
            }
        )

    cards.sort(key=lambda card: (str(card["valid_time"]), str(card["image_ref"])))
    return cards


def _build_domain_sequence_feature(
    domain: str,
    stage1_records: list[dict[str, Any]],
    stage2_records: list[dict[str, Any]],
    image_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    stage1_lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in stage1_records:
        key = (str(record["image_ref"]), str(record["region_id"]), str(record["signal_key"]))
        stage1_lookup[key] = record

    stage2_lookup: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in stage2_records:
        key = (str(record["image_ref"]), str(record["region_id"]), str(record["signal_key"]))
        stage2_lookup[key].append(record)

    ordered_images = [(str(card["image_ref"]), str(card["valid_time"])) for card in image_cards]
    active_track_keys = sorted(
        {
            (str(record["region_id"]), str(record["signal_key"]))
            for record in stage1_records
            if str(record["model_answer_normalized"]) in ACTIVE_PRESENCE_ANSWERS
        }
    )

    signal_tracks: list[dict[str, Any]] = []
    active_region_ids: set[str] = set()
    active_signal_keys: set[str] = set()
    active_signal_count_total = 0

    for region_id, signal_key in active_track_keys:
        track_stage1_records = [
            record
            for record in stage1_records
            if str(record["region_id"]) == region_id and str(record["signal_key"]) == signal_key
        ]
        if not track_stage1_records:
            continue
        track_stage1_records.sort(key=_record_sort_key)
        region_label = str(track_stage1_records[0]["region_label"])
        time_steps: list[dict[str, Any]] = []
        active_image_refs: list[str] = []
        for image_ref, valid_time in ordered_images:
            stage1_record = stage1_lookup.get((image_ref, region_id, signal_key))
            if stage1_record is None:
                continue
            presence = str(stage1_record["model_answer_normalized"])
            attributes = [
                {
                    "attribute_key": row["attribute_key"],
                    "answer": row["model_answer_normalized"],
                    "note": row.get("note", ""),
                }
                for row in sorted(stage2_lookup.get((image_ref, region_id, signal_key), []), key=_attribute_sort_key)
            ]
            if presence in ACTIVE_PRESENCE_ANSWERS:
                active_image_refs.append(image_ref)
            time_steps.append(
                {
                    "image_ref": image_ref,
                    "valid_time": valid_time,
                    "presence": presence,
                    "note": stage1_record.get("note", ""),
                    "attributes": attributes,
                }
            )
        signal_tracks.append(
            {
                "region_id": region_id,
                "region_label": region_label,
                "signal_key": signal_key,
                "active_image_refs": active_image_refs,
                "time_steps": time_steps,
            }
        )
        active_region_ids.add(region_id)
        active_signal_keys.add(signal_key)
        active_signal_count_total += 1

    return {
        "domain": domain,
        "summary": {
            "image_count": len(image_cards),
            "stage1_record_count": len(stage1_records),
            "stage2_record_count": len(stage2_records),
            "active_signal_track_count": active_signal_count_total,
            "active_regions": sorted(active_region_ids),
            "signal_keys_present": sorted(active_signal_keys),
        },
        "image_sequence": [
            {
                "image_ref": card["image_ref"],
                "valid_time": card["valid_time"],
                "source_image": card["source_image"],
                "stage1_answer_counts": card["stage1_answer_counts"],
                "active_signal_count": card["active_signal_count"],
                "active_region_count": card["active_region_count"],
            }
            for card in image_cards
        ],
        "signal_tracks": signal_tracks,
    }


def build_feature_bundle(base_dir: Path, run_date: str) -> dict[str, Any]:
    features_root = base_dir / "daio" / run_date / "features"
    if not features_root.exists():
        raise FileNotFoundError(f"features directory not found: {features_root}")

    generated_at = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    domains = sorted(path.name for path in features_root.iterdir() if path.is_dir())
    image_feature_cards: list[dict[str, Any]] = []
    domain_sequence_features: list[dict[str, Any]] = []
    total_stage1_records = 0
    total_stage2_records = 0

    for domain in domains:
        stage1_records = _load_stage_records(features_root, domain, "stage1")
        stage2_records = _load_stage_records(features_root, domain, "stage2")
        total_stage1_records += len(stage1_records)
        total_stage2_records += len(stage2_records)
        domain_cards = _build_image_feature_cards(domain, stage1_records, stage2_records)
        image_feature_cards.extend(domain_cards)
        domain_sequence_features.append(
            _build_domain_sequence_feature(
                domain=domain,
                stage1_records=stage1_records,
                stage2_records=stage2_records,
                image_cards=domain_cards,
            )
        )

    image_feature_cards.sort(key=lambda card: (str(card["domain"]), str(card["valid_time"]), str(card["image_ref"])))
    domain_sequence_features.sort(key=lambda item: str(item["domain"]))

    return {
        "run_date": run_date,
        "generated_at_kst": generated_at,
        "summary": {
            "domain_count": len(domains),
            "image_feature_card_count": len(image_feature_cards),
            "total_stage1_record_count": total_stage1_records,
            "total_stage2_record_count": total_stage2_records,
        },
        "domains": domains,
        "image_feature_cards": image_feature_cards,
        "domain_sequence_features": domain_sequence_features,
    }


def write_feature_bundle_outputs(base_dir: Path, run_date: str, bundle: dict[str, Any]) -> dict[str, Path]:
    features_root = base_dir / "daio" / run_date / "features"
    features_root.mkdir(parents=True, exist_ok=True)

    image_cards_payload = {
        "run_date": bundle["run_date"],
        "generated_at_kst": bundle["generated_at_kst"],
        "summary": {
            "card_count": len(bundle["image_feature_cards"]),
            "domains": bundle["domains"],
        },
        "cards": bundle["image_feature_cards"],
    }
    domain_sequence_payload = {
        "run_date": bundle["run_date"],
        "generated_at_kst": bundle["generated_at_kst"],
        "summary": {
            "domain_count": len(bundle["domain_sequence_features"]),
            "domains": bundle["domains"],
        },
        "domains": bundle["domain_sequence_features"],
    }

    image_cards_path = features_root / "image_feature_cards.json"
    domain_sequence_path = features_root / "domain_sequence_features.json"
    feature_bundle_path = features_root / "feature_bundle.json"

    image_cards_path.write_text(json.dumps(image_cards_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    domain_sequence_path.write_text(
        json.dumps(domain_sequence_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    feature_bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "image_feature_cards": image_cards_path,
        "domain_sequence_features": domain_sequence_path,
        "feature_bundle": feature_bundle_path,
    }
