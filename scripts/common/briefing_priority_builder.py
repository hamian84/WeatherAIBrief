from __future__ import annotations

from typing import Any


DOMAIN_WEIGHTS = {
    "300hPa": 4,
    "500hPa": 4,
    "850hPa": 5,
    "925hPa": 5,
    "surface": 5,
    "sfc12h_synoptic": 4,
    "satellite_wv": 4,
}

MOISTURE_SIGNALS = {
    "low_level_jet",
    "moisture_transport_axis",
    "wet_area",
    "warm_advection",
    "moist_plume",
    "convective_cloud_cluster",
    "band_cloud",
}

SURFACE_HAZARD_SIGNALS = {
    "front",
    "frontal_zone",
    "low_center",
    "synoptic_low_center",
    "strong_wind_zone",
    "gale_wind_area",
    "tight_pressure_gradient",
    "surface_trough_axis",
}

UPPER_DYNAMIC_SIGNALS = {
    "jet_core",
    "jet_axis",
    "diffluent_flow",
    "upper_trough",
    "cold_core",
    "closed_cyclonic_circulation",
    "trough_axis",
}


def _score_item(item: dict[str, Any]) -> int:
    domain = str(item.get("domain", "")).strip()
    count = int(item.get("occurrence_count", 0) or 0)
    utc_count = len(item.get("active_utc_hours", []) or [])
    region_count = len(item.get("region_labels", []) or [])
    return count * DOMAIN_WEIGHTS.get(domain, 1) + utc_count * 3 + min(region_count, 4)


def _top_signal_items(items: list[dict[str, Any]], limit: int = 12) -> list[dict[str, Any]]:
    signal_items = [item for item in items if "signal_key" in item]
    ranked = sorted(signal_items, key=lambda item: (_score_item(item), item["evidence_id"]), reverse=True)
    output: list[dict[str, Any]] = []
    for item in ranked[:limit]:
        output.append(
            {
                "evidence_id": item["evidence_id"],
                "domain": item["domain"],
                "signal_key": item["signal_key"],
                "importance_score": _score_item(item),
                "occurrence_count": item["occurrence_count"],
                "active_utc_hours": item.get("active_utc_hours", []),
                "region_labels": item.get("region_labels", []),
            }
        )
    return output


def _build_region_axes(items: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    region_items = [item for item in items if "region_id" in item]
    ranked = sorted(region_items, key=lambda item: (_score_item(item), item["evidence_id"]), reverse=True)
    output: list[dict[str, Any]] = []
    for item in ranked[:limit]:
        signals = list(item.get("signal_keys", []) or [])
        signal_set = set(signals)
        supporting_rules: list[str] = []
        if signal_set & UPPER_DYNAMIC_SIGNALS and signal_set & MOISTURE_SIGNALS:
            supporting_rules.extend(["jet_coupling_heavy_rain", "upper_difffluence_heavy_rain"])
        if signal_set & MOISTURE_SIGNALS:
            supporting_rules.extend(["moisture_flux_convergence_focus", "llj_broad_rain"])
        if signal_set & SURFACE_HAZARD_SIGNALS:
            supporting_rules.extend(["front_boundary_identification", "explosive_cyclone_risk"])
        output.append(
            {
                "axis_id": item["evidence_id"],
                "domain": item["domain"],
                "region_label": (item.get("region_labels") or [""])[0],
                "importance_score": _score_item(item),
                "occurrence_count": item["occurrence_count"],
                "active_utc_hours": item.get("active_utc_hours", []),
                "active_signals": signals,
                "supporting_rule_ids": list(dict.fromkeys(supporting_rules)),
                "summary": item.get("observation_note", ""),
            }
        )
    return output


def _build_priority_axes(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_region: dict[str, dict[str, Any]] = {}
    for item in items:
        signal_key = str(item.get("signal_key", "")).strip()
        if not signal_key:
            continue
        for region in item.get("region_labels", []) or []:
            region_text = str(region).strip()
            if not region_text:
                continue
            entry = by_region.setdefault(
                region_text,
                {
                    "region_label": region_text,
                    "domains": set(),
                    "signals": set(),
                    "evidence_ids": set(),
                    "evidence_refs": set(),
                    "utc_hours": set(),
                    "score": 0,
                },
            )
            entry["domains"].add(str(item.get("domain", "")).strip())
            entry["signals"].add(signal_key)
            entry["evidence_ids"].add(str(item.get("evidence_id", "")).strip())
            entry["evidence_refs"].update(str(ref).strip() for ref in item.get("active_image_refs", []) or [] if str(ref).strip())
            entry["utc_hours"].update(str(hour).strip() for hour in item.get("active_utc_hours", []) or [] if str(hour).strip())
            entry["score"] += _score_item(item)

    ranked = sorted(
        by_region.values(),
        key=lambda item: (len(item["domains"]), item["score"], item["region_label"]),
        reverse=True,
    )
    output: list[dict[str, Any]] = []
    for item in ranked[:6]:
        domains = sorted(item["domains"])
        signals = sorted(item["signals"])
        supporting_rules: list[str] = []
        if any(signal in UPPER_DYNAMIC_SIGNALS for signal in signals) and any(signal in MOISTURE_SIGNALS for signal in signals):
            supporting_rules.extend(["jet_coupling_heavy_rain", "upper_difffluence_heavy_rain"])
        if any(signal in MOISTURE_SIGNALS for signal in signals):
            supporting_rules.extend(["moisture_flux_convergence_focus", "llj_broad_rain"])
        if any(signal in SURFACE_HAZARD_SIGNALS for signal in signals):
            supporting_rules.extend(["front_boundary_identification", "explosive_cyclone_risk"])
        output.append(
            {
                "axis_id": f"axis_{item['region_label']}",
                "region_label": item["region_label"],
                "supporting_domains": domains,
                "importance_score": item["score"] + len(domains) * 10,
                "active_utc_hours": sorted(item["utc_hours"]),
                "evidence_ids": sorted(item["evidence_ids"]),
                "evidence_refs": sorted(item["evidence_refs"]),
                "supporting_rule_ids": list(dict.fromkeys(supporting_rules)),
                "summary": (
                    f"{item['region_label']}을 중심으로 "
                    f"{', '.join(domains)} 신호가 겹치며 "
                    f"{', '.join(sorted(item['utc_hours']))}에 반복 관측된 중심 축"
                ),
            }
        )
    return output


def build_briefing_priority_summary(
    feature_bundle_summary: dict[str, Any],
    image_feature_signal_summary: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": "briefing_priority_summary.v1",
        "run_date": str(feature_bundle_summary.get("run_date", "")).strip(),
        "top_signal_items": _top_signal_items(image_feature_signal_summary),
        "top_region_axes": _build_region_axes(image_feature_signal_summary),
        "priority_axes": _build_priority_axes(image_feature_signal_summary),
    }
