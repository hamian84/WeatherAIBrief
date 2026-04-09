from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.config import load_project_env
from scripts.common.date_utils import normalize_run_date
from scripts.common.openai_structured_client import call_structured_text_llm
from scripts.common.schema_utils import load_json_schema, validate_instance


BODY_TIME_MARKERS = ("초반", "이후", "후반", "따라서")
INTERNAL_KEY_PATTERN = re.compile(r"\b[a-z0-9]+_[a-z0-9_]+\b")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="시간 변화 사건 목록에서 브리핑 섹션 본문을 작성합니다.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--section", required=True, help="section_id")
    parser.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model name")
    parser.add_argument("--max-output-tokens", type=int, default=3000, help="max_output_tokens")
    parser.add_argument("--max-attempts", type=int, default=3, help="semantic validation retry attempts")
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 prompt_input만 생성합니다.")
    return parser


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"yaml root must be a mapping: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json root must be an object: {path}")
    return payload


def _section_source_path(base_dir: Path, run_date: str, section_id: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "section_sources" / f"{section_id}.json"


def _events_path(base_dir: Path, run_date: str, section_id: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "events" / f"{section_id}.json"


def _sections_dir(base_dir: Path, run_date: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "sections"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _render_prompt(template: str, prompt_input: dict[str, Any]) -> str:
    events_json = json.dumps(prompt_input["events"], ensure_ascii=False, separators=(",", ":"))
    return template.format(
        run_date=prompt_input["run_date"],
        section_id=prompt_input["section_id"],
        section_title=prompt_input["section_title"],
        events_json=events_json,
    )


def _build_prompt_input(events_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_date": events_payload["run_date"],
        "section_id": events_payload["section_id"],
        "section_title": events_payload["section_title"],
        "events": events_payload["events"],
    }


def _validate_body_payload(parsed_output: dict[str, Any], schema: dict[str, Any], prompt_input: dict[str, Any]) -> None:
    validate_instance(parsed_output, schema, "briefing_section_body")
    title = parsed_output.get("title")
    body = parsed_output.get("body")
    source_event_ids = parsed_output.get("source_event_ids")

    if title != prompt_input["section_title"]:
        raise ValueError(f"title must equal section_title: {prompt_input['section_title']}")
    if not isinstance(body, str) or not body.strip():
        raise ValueError("body must be a non-empty string")
    if any(marker not in body for marker in BODY_TIME_MARKERS):
        raise ValueError("body must include 초반, 이후, 후반, 따라서")
    if re.search(r"\d+회", body):
        raise ValueError("body must not include counts like 5회")
    if INTERNAL_KEY_PATTERN.search(body):
        raise ValueError("body must not include internal underscore keys")
    if not isinstance(source_event_ids, list) or not source_event_ids:
        raise ValueError("source_event_ids must be a non-empty list")

    allowed_event_ids = {event["event_id"] for event in prompt_input["events"]}
    invalid_event_ids = [event_id for event_id in source_event_ids if event_id not in allowed_event_ids]
    if invalid_event_ids:
        raise ValueError(f"invalid source_event_ids: {invalid_event_ids}")


def _build_observation_basis(section_source: dict[str, Any], source_event_ids: list[str], events_payload: dict[str, Any]) -> list[str]:
    selected_events = [event for event in events_payload["events"] if event["event_id"] in source_event_ids]
    selected_domains = []
    for event in selected_events:
        for domain in event.get("domains", []):
            if domain not in selected_domains:
                selected_domains.append(domain)

    region_catalog = section_source["region_catalog"]
    signal_catalog = section_source["signal_catalog"]
    rows = section_source["rows"]
    aggregated_stage1: dict[str, dict[tuple[int, int], int]] = {}
    aggregated_fallback: dict[str, dict[tuple[int, int], int]] = {}
    for row in rows:
        domain = row[1]
        if selected_domains and domain not in selected_domains:
            continue
        region_idx = row[2]
        signal_idx = row[3]
        stage = row[4]
        aggregated_fallback.setdefault(domain, {})
        aggregated_fallback[domain][(region_idx, signal_idx)] = aggregated_fallback[domain].get((region_idx, signal_idx), 0) + 1
        if stage == "stage1":
            aggregated_stage1.setdefault(domain, {})
            aggregated_stage1[domain][(region_idx, signal_idx)] = aggregated_stage1[domain].get((region_idx, signal_idx), 0) + 1

    basis_lines: list[str] = []
    for domain in selected_domains:
        items = aggregated_stage1.get(domain) or aggregated_fallback.get(domain, {})
        if not items:
            continue
        ranked = sorted(
            items.items(),
            key=lambda item: (-item[1], region_catalog[item[0][0]]["region_label"], signal_catalog[item[0][1]]["signal_label"]),
        )[:4]
        parts = [
            f"{region_catalog[region_idx]['region_label']} {signal_catalog[signal_idx]['signal_label']} {count}회"
            for (region_idx, signal_idx), count in ranked
        ]
        if parts:
            basis_lines.append(f"{domain}: " + ", ".join(parts))
    return basis_lines


def _build_applied_patterns(section_source: dict[str, Any], source_event_ids: list[str], events_payload: dict[str, Any]) -> list[str]:
    selected_events = [event for event in events_payload["events"] if event["event_id"] in source_event_ids]
    selected_rule_ids: list[str] = []
    for event in selected_events:
        for rule_id in event.get("rule_refs", []):
            if rule_id not in selected_rule_ids:
                selected_rule_ids.append(rule_id)

    rule_lookup = {item["rule_id"]: item for item in section_source["rule_catalog"]}
    lines: list[str] = []
    for rule_id in selected_rule_ids:
        rule = rule_lookup.get(rule_id)
        if not rule:
            continue
        lines.append(f"{rule_id}, 섹션 {rule['section_no']}, {rule['page_range']}")
    return lines


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)
    load_project_env(BASE_DIR)

    section_map = _load_yaml(BASE_DIR / "config" / "briefing" / "section_map.yaml")
    section_lookup = {
        section["section_id"]: section
        for section in section_map.get("sections", [])
        if isinstance(section, dict) and isinstance(section.get("section_id"), str)
    }
    if args.section not in section_lookup:
        raise ValueError(f"section_id not found in section_map: {args.section}")

    events_payload = _load_json(_events_path(BASE_DIR, args.date, args.section))
    section_source = _load_json(_section_source_path(BASE_DIR, args.date, args.section))
    prompt_input = _build_prompt_input(events_payload)

    sections_dir = _sections_dir(BASE_DIR, args.date)
    prompt_input_path = sections_dir / f"{args.section}.prompt_input.json"
    _write_json(prompt_input_path, prompt_input)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "ok",
                    "run_date": args.date,
                    "section_id": args.section,
                    "prompt_input_path": str(prompt_input_path),
                    "event_count": len(prompt_input["events"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    schema = load_json_schema(BASE_DIR / "daba" / "schemas" / "briefing_section_body.schema.json")
    template = (BASE_DIR / "daba" / "templates" / "briefing_section_prompt.txt").read_text(encoding="utf-8")
    base_prompt = _render_prompt(template, prompt_input)

    llm_result: dict[str, Any] | None = None
    parsed_output: dict[str, Any] | None = None
    feedback_note = ""
    last_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        attempt_prompt = base_prompt
        if feedback_note:
            attempt_prompt = f"{base_prompt}\n\n이전 응답 오류:\n{feedback_note}\n위 오류를 모두 수정해 처음부터 다시 작성하라."
        try:
            llm_result = call_structured_text_llm(
                prompt_text=attempt_prompt,
                schema=schema,
                model=args.model,
                max_output_tokens=args.max_output_tokens,
            )
            parsed_output = llm_result["parsed_output"]
            _validate_body_payload(parsed_output, schema, prompt_input)
            break
        except Exception as exc:
            last_error = exc
            feedback_note = str(exc)
            llm_result = None
            parsed_output = None
            if attempt >= args.max_attempts:
                raise

    if llm_result is None or parsed_output is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("briefing section writing failed without a captured error")

    source_event_ids = parsed_output["source_event_ids"]
    final_payload = {
        "run_date": args.date,
        "section_id": args.section,
        "title": parsed_output["title"],
        "body": parsed_output["body"],
        "observation_basis": _build_observation_basis(section_source, source_event_ids, events_payload),
        "applied_patterns": _build_applied_patterns(section_source, source_event_ids, events_payload),
        "source_event_ids": source_event_ids,
    }

    final_schema = load_json_schema(BASE_DIR / "daba" / "schemas" / "briefing_section.schema.json")
    validate_instance(final_payload, final_schema, "briefing_section")

    raw_path = sections_dir / f"{args.section}.raw.json"
    final_path = sections_dir / f"{args.section}.json"
    _write_json(raw_path, llm_result)
    _write_json(final_path, final_payload)

    print(
        json.dumps(
            {
                "status": "ok",
                "run_date": args.date,
                "section_id": args.section,
                "prompt_input_path": str(prompt_input_path),
                "raw_path": str(raw_path),
                "section_path": str(final_path),
                "source_event_count": len(source_event_ids),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
