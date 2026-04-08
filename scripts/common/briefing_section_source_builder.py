from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"yaml root must be a mapping: {path}")
    return payload


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json root must be an object: {path}")
    return payload


def build_section_source_path(base_dir: Path, run_date: str, section_id: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "section_sources" / f"{section_id}.json"


def _page_range_text(page_range: dict[str, Any]) -> str:
    start = page_range.get("start")
    end = page_range.get("end")
    if not isinstance(start, int) or not isinstance(end, int):
        return "p.?"
    if start == end:
        return f"p.{start}"
    return f"p.{start}-{end}"


def _extract_trigger_pairs(value: Any) -> set[str]:
    pairs: set[str] = set()
    if isinstance(value, str) and ":" in value:
        domain, signal = value.split(":", 1)
        if domain and signal:
            pairs.add(f"{domain}:{signal}")
        return pairs
    if isinstance(value, list):
        for item in value:
            pairs.update(_extract_trigger_pairs(item))
    elif isinstance(value, dict):
        for item in value.values():
            pairs.update(_extract_trigger_pairs(item))
    return pairs


def _build_rule_catalog(
    rule_pack: dict[str, Any],
    section_config: dict[str, Any],
    section_domains: list[str],
    active_signal_pairs: set[str],
) -> list[dict[str, Any]]:
    section_map: dict[int, dict[str, Any]] = {}
    for section in rule_pack.get("sections", []):
        if isinstance(section, dict) and isinstance(section.get("section_no"), int):
            section_map[section["section_no"]] = section

    selection = section_config.get("rule_selection") or {}
    mode = selection.get("mode", "all_relevant")
    hard_section_filter = bool(selection.get("hard_section_filter", False))
    allowed_sections = set(section_config.get("allowed_rule_sections") or [])

    rows: list[dict[str, Any]] = []
    for rule in rule_pack.get("rules", []):
        if not isinstance(rule, dict):
            continue
        rule_id = rule.get("rule_id")
        section_no = rule.get("section_no")
        applicable_domains = set(rule.get("applicable_domains") or [])
        if not isinstance(rule_id, str) or not rule_id:
            continue
        if not set(section_domains).intersection(applicable_domains):
            continue
        if hard_section_filter and allowed_sections and section_no not in allowed_sections:
            continue

        trigger_pairs = _extract_trigger_pairs(rule.get("trigger_conditions"))
        if mode == "all_relevant":
            if trigger_pairs and not active_signal_pairs.intersection(trigger_pairs):
                continue

        page_range = {}
        if isinstance(section_no, int) and section_no in section_map:
            page_range = section_map[section_no].get("page_range") or {}

        rows.append(
            {
                "rule_id": rule_id,
                "section_no": section_no,
                "page_range": _page_range_text(page_range),
            }
        )

    rows.sort(key=lambda item: (item["section_no"], item["rule_id"]))
    return rows


def _compact_row_index_map(payload: dict[str, Any]) -> dict[str, int]:
    row_fields = payload.get("row_fields")
    if not isinstance(row_fields, list):
        raise ValueError("compact payload missing row_fields")
    return {field: idx for idx, field in enumerate(row_fields)}


def _read_feature_compact(base_dir: Path, run_date: str, domain: str, stage: str) -> dict[str, Any]:
    path = base_dir / "daio" / run_date / "features" / domain / f"{stage}_compact.json"
    if not path.exists():
        raise FileNotFoundError(f"compact feature not found: {path}")
    return load_json(path)


def _normalize_answer_label(value: Any, answer_labels: dict[str, str]) -> str:
    if value is None:
        return ""
    text = str(value)
    return answer_labels.get(text, answer_labels.get(text.lower(), text))


def _collect_catalogs_for_section(
    compact_payloads: list[dict[str, Any]],
    display_labels: dict[str, Any],
) -> tuple[
    list[str],
    dict[str, int],
    list[dict[str, str]],
    dict[str, int],
    list[dict[str, str]],
    dict[str, int],
    list[str],
    dict[str, int],
]:
    time_values: list[str] = []
    region_values: list[dict[str, str]] = []
    signal_values: list[dict[str, str]] = []
    evidence_values: list[str] = []
    signal_labels = display_labels.get("signal_labels", {})

    for payload in compact_payloads:
        catalogs = payload.get("catalogs", {})
        for image in catalogs.get("images", []):
            valid_time = image.get("valid_time")
            image_ref = image.get("image_ref")
            if isinstance(valid_time, str) and valid_time not in time_values:
                time_values.append(valid_time)
            if isinstance(image_ref, str) and image_ref not in evidence_values:
                evidence_values.append(image_ref)

        for region in catalogs.get("regions", []):
            region_id = region.get("region_id")
            region_label = region.get("region_label")
            if not isinstance(region_id, str) or not isinstance(region_label, str):
                continue
            row = {"region_id": region_id, "region_label": region_label}
            if row not in region_values:
                region_values.append(row)

        for question in catalogs.get("questions", []):
            signal_key = question.get("signal_key")
            if not isinstance(signal_key, str):
                continue
            row = {"signal_key": signal_key, "signal_label": signal_labels.get(signal_key, signal_key)}
            if row not in signal_values:
                signal_values.append(row)

    time_values.sort()
    region_values.sort(key=lambda item: item["region_id"])
    signal_values.sort(key=lambda item: item["signal_key"])
    evidence_values.sort()

    return (
        time_values,
        {value: idx for idx, value in enumerate(time_values)},
        region_values,
        {item["region_id"]: idx for idx, item in enumerate(region_values)},
        signal_values,
        {item["signal_key"]: idx for idx, item in enumerate(signal_values)},
        evidence_values,
        {value: idx for idx, value in enumerate(evidence_values)},
    )


def _build_stage1_rows(
    payload: dict[str, Any],
    domain: str,
    time_index: dict[str, int],
    region_catalog: list[dict[str, str]],
    region_index: dict[str, int],
    signal_index: dict[str, int],
    evidence_index: dict[str, int],
    signal_labels: dict[str, str],
) -> list[list[Any]]:
    catalogs = payload["catalogs"]
    images = {item["image_idx"]: item for item in catalogs["images"]}
    questions = {item["question_idx"]: item for item in catalogs["questions"]}
    row_index = _compact_row_index_map(payload)
    output_rows: list[list[Any]] = []

    for row in payload.get("rows", []):
        normalized = str(row[row_index["model_answer_normalized"]]).lower()
        if normalized != "yes":
            continue

        image = images[row[row_index["image_idx"]]]
        question = questions[row[row_index["question_idx"]]]
        signal_key = question["signal_key"]
        signal_label = signal_labels.get(signal_key, signal_key)
        region_pos = question["region_idx"]

        output_rows.append(
            [
                time_index[image["valid_time"]],
                domain,
                region_index[region_catalog[region_pos]["region_id"]],
                signal_index[signal_key],
                "stage1",
                1,
                f"{signal_label} 존재",
                [evidence_index[image["image_ref"]]],
            ]
        )
    return output_rows


def _build_stage2_rows(
    payload: dict[str, Any],
    domain: str,
    time_index: dict[str, int],
    region_catalog: list[dict[str, str]],
    region_index: dict[str, int],
    signal_index: dict[str, int],
    evidence_index: dict[str, int],
    signal_labels: dict[str, str],
    attribute_labels: dict[str, str],
    answer_labels: dict[str, str],
) -> list[list[Any]]:
    catalogs = payload["catalogs"]
    images = {item["image_idx"]: item for item in catalogs["images"]}
    questions = {item["question_idx"]: item for item in catalogs["questions"]}
    row_index = _compact_row_index_map(payload)

    grouped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in payload.get("rows", []):
        normalized = str(row[row_index["model_answer_normalized"]]).strip()
        if not normalized or normalized.lower() == "unknown":
            continue
        if not bool(row[row_index["is_valid_answer"]]):
            continue

        image = images[row[row_index["image_idx"]]]
        question = questions[row[row_index["question_idx"]]]
        signal_key = question["signal_key"]
        region_pos = question["region_idx"]
        key = (image["valid_time"], region_pos, signal_key)

        item = grouped.setdefault(key, {"attrs": [], "evidence_refs": set()})
        item["attrs"].append((question["attribute_key"], normalized))
        item["evidence_refs"].add(image["image_ref"])

    output_rows: list[list[Any]] = []
    for (valid_time, region_pos, signal_key), item in grouped.items():
        signal_label = signal_labels.get(signal_key, signal_key)
        attr_parts: list[str] = []
        seen_pairs: set[tuple[str, str]] = set()
        for attr_key, raw_value in item["attrs"]:
            pair = (attr_key, raw_value)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            attr_label = attribute_labels.get(attr_key, attr_key)
            value_label = _normalize_answer_label(raw_value, answer_labels)
            attr_parts.append(f"{attr_label} {value_label}")
        attr_parts.sort()
        detail_text = f"{signal_label} 세부 판독: " + ", ".join(attr_parts)
        evidence_refs = sorted(item["evidence_refs"])
        output_rows.append(
            [
                time_index[valid_time],
                domain,
                region_index[region_catalog[region_pos]["region_id"]],
                signal_index[signal_key],
                "stage2",
                len(attr_parts) if attr_parts else 1,
                detail_text,
                [evidence_index[ref] for ref in evidence_refs],
            ]
        )
    return output_rows


def build_section_source_payload(
    *,
    base_dir: Path,
    run_date: str,
    section_config: dict[str, Any],
    display_labels: dict[str, Any],
    rule_pack: dict[str, Any],
) -> dict[str, Any]:
    domains = list(section_config.get("domains") or [])
    if not domains:
        raise ValueError(f"section missing domains: {section_config.get('section_id')}")

    compact_payloads: list[dict[str, Any]] = []
    for domain in domains:
        compact_payloads.append(_read_feature_compact(base_dir, run_date, domain, "stage1"))
        compact_payloads.append(_read_feature_compact(base_dir, run_date, domain, "stage2"))

    (
        time_catalog,
        time_index,
        region_catalog,
        region_index,
        signal_catalog,
        signal_index,
        evidence_catalog,
        evidence_index,
    ) = _collect_catalogs_for_section(compact_payloads, display_labels)

    signal_labels = display_labels.get("signal_labels", {})
    attribute_labels = display_labels.get("attribute_labels", {})
    answer_labels = display_labels.get("answer_labels", {})

    rows: list[list[Any]] = []
    for domain in domains:
        rows.extend(
            _build_stage1_rows(
                _read_feature_compact(base_dir, run_date, domain, "stage1"),
                domain,
                time_index,
                region_catalog,
                region_index,
                signal_index,
                evidence_index,
                signal_labels,
            )
        )
        rows.extend(
            _build_stage2_rows(
                _read_feature_compact(base_dir, run_date, domain, "stage2"),
                domain,
                time_index,
                region_catalog,
                region_index,
                signal_index,
                evidence_index,
                signal_labels,
                attribute_labels,
                answer_labels,
            )
        )

    domain_order = {domain: idx for idx, domain in enumerate(domains)}
    rows.sort(key=lambda row: (row[0], domain_order.get(row[1], 999), row[2], row[3], row[4]))

    active_signal_pairs = {
        f"{row[1]}:{signal_catalog[row[3]]['signal_key']}"
        for row in rows
    }

    return {
        "run_date": run_date,
        "section_id": section_config["section_id"],
        "section_title": section_config["title"],
        "source_domains": domains,
        "time_catalog": time_catalog,
        "region_catalog": region_catalog,
        "signal_catalog": signal_catalog,
        "evidence_catalog": evidence_catalog,
        "rule_catalog": _build_rule_catalog(
            rule_pack,
            section_config,
            domains,
            active_signal_pairs,
        ),
        "row_fields": [
            "time_idx",
            "domain",
            "region_idx",
            "signal_idx",
            "stage",
            "count",
            "detail_text",
            "evidence_idx_list",
        ],
        "rows": rows,
        "row_count": len(rows),
    }
