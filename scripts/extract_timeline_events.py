from __future__ import annotations

import argparse
import json
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="섹션 원자료에서 시간 변화 사건 목록을 추출합니다.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--section", required=True, help="section_id")
    parser.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model name")
    parser.add_argument("--max-output-tokens", type=int, default=4000, help="max_output_tokens")
    parser.add_argument("--max-attempts", type=int, default=3, help="semantic validation retry attempts")
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 prompt_input만 생성합니다.")
    return parser


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"yaml root must be a mapping: {path}")
    return payload


def _load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _events_dir(base_dir: Path, run_date: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "events"


def _section_source_path(base_dir: Path, run_date: str, section_id: str) -> Path:
    return base_dir / "daio" / run_date / "briefing" / "section_sources" / f"{section_id}.json"


def _write_json(path: Path, payload: dict[str, Any], compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if compact:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    else:
        text = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")


def _build_prompt_input(
    section_source: dict[str, Any],
    section_config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_date": section_source["run_date"],
        "section_id": section_source["section_id"],
        "section_title": section_source["section_title"],
        "max_events": section_config.get("max_events", 3),
        "source_domains": section_source["source_domains"],
        "time_catalog": section_source["time_catalog"],
        "region_catalog": section_source["region_catalog"],
        "signal_catalog": section_source["signal_catalog"],
        "evidence_catalog": section_source["evidence_catalog"],
        "rule_catalog": section_source["rule_catalog"],
        "row_fields": section_source["row_fields"],
        "rows": section_source["rows"],
    }


def _render_prompt(template: str, prompt_input: dict[str, Any]) -> str:
    section_source_json = json.dumps(prompt_input, ensure_ascii=False, separators=(",", ":"))
    return template.format(
        run_date=prompt_input["run_date"],
        section_id=prompt_input["section_id"],
        section_title=prompt_input["section_title"],
        max_events=prompt_input["max_events"],
        section_source_json=section_source_json,
    )


def _validate_event_payload(parsed_output: dict[str, Any], llm_schema: dict[str, Any], event_schema: dict[str, Any]) -> None:
    validate_instance(parsed_output, llm_schema, "timeline_event_list")
    events = parsed_output.get("events")
    if not isinstance(events, list):
        raise ValueError("timeline_event_list.events must be a list")
    for idx, event in enumerate(events):
        validate_instance(event, event_schema, f"timeline_event[{idx}]")


def _build_llm_schema(list_schema: dict[str, Any], event_schema: dict[str, Any]) -> dict[str, Any]:
    schema = dict(list_schema)
    properties = dict(schema.get("properties") or {})
    events_prop = dict(properties.get("events") or {})
    events_prop["items"] = event_schema
    properties["events"] = events_prop
    schema["properties"] = properties
    return schema


def _validate_event_references(parsed_output: dict[str, Any], prompt_input: dict[str, Any]) -> None:
    allowed_evidence = set(prompt_input["evidence_catalog"])
    allowed_rules = {item["rule_id"] for item in prompt_input["rule_catalog"]}
    allowed_domains = set(prompt_input["source_domains"])
    events = parsed_output.get("events", [])
    if not isinstance(events, list):
        raise ValueError("events must be a list")
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("event item must be an object")
        event_id = str(event.get("event_id", "unknown"))
        invalid_evidence = [ref for ref in event.get("evidence_refs", []) if ref not in allowed_evidence]
        if invalid_evidence:
            raise ValueError(f"{event_id}: invalid evidence_refs {invalid_evidence}")
        invalid_rules = [ref for ref in event.get("rule_refs", []) if ref not in allowed_rules]
        if invalid_rules:
            raise ValueError(f"{event_id}: invalid rule_refs {invalid_rules}")
        invalid_domains = [value for value in event.get("domains", []) if value not in allowed_domains]
        if invalid_domains:
            raise ValueError(f"{event_id}: invalid domains {invalid_domains}")


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

    section_source_path = _section_source_path(BASE_DIR, args.date, args.section)
    if not section_source_path.exists():
        raise FileNotFoundError(f"section source not found: {section_source_path}")

    section_source = json.loads(section_source_path.read_text(encoding="utf-8"))
    section_config = section_lookup[args.section]
    prompt_input = _build_prompt_input(section_source, section_config)

    events_dir = _events_dir(BASE_DIR, args.date)
    prompt_input_path = events_dir / f"{args.section}.prompt_input.json"
    _write_json(prompt_input_path, prompt_input)

    template = _load_template(BASE_DIR / "daba" / "templates" / "timeline_event_prompt.txt")
    prompt_text = _render_prompt(template, prompt_input)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "ok",
                    "run_date": args.date,
                    "section_id": args.section,
                    "prompt_input_path": str(prompt_input_path),
                    "row_count": len(prompt_input["rows"]),
                    "rule_count": len(prompt_input["rule_catalog"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    event_schema = load_json_schema(BASE_DIR / "daba" / "schemas" / "timeline_event.schema.json")
    list_schema = load_json_schema(BASE_DIR / "daba" / "schemas" / "timeline_event_list.schema.json")
    llm_schema = _build_llm_schema(list_schema, event_schema)
    llm_result: dict[str, Any] | None = None
    parsed_output: dict[str, Any] | None = None
    feedback_note = ""
    last_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        attempt_prompt = prompt_text
        if feedback_note:
            attempt_prompt = f"{prompt_text}\n\n이전 응답 오류:\n{feedback_note}\n위 오류를 모두 수정해 처음부터 다시 작성하라."
        try:
            llm_result = call_structured_text_llm(
                prompt_text=attempt_prompt,
                schema=llm_schema,
                model=args.model,
                max_output_tokens=args.max_output_tokens,
            )
            parsed_output = llm_result["parsed_output"]
            _validate_event_payload(parsed_output, llm_schema, event_schema)
            _validate_event_references(parsed_output, prompt_input)
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
        raise RuntimeError("timeline event extraction failed without a captured error")

    raw_path = events_dir / f"{args.section}.raw.json"
    parsed_path = events_dir / f"{args.section}.json"
    _write_json(raw_path, llm_result)
    _write_json(parsed_path, parsed_output)

    print(
        json.dumps(
            {
                "status": "ok",
                "run_date": args.date,
                "section_id": args.section,
                "prompt_input_path": str(prompt_input_path),
                "raw_path": str(raw_path),
                "events_path": str(parsed_path),
                "event_count": len(parsed_output.get("events", [])),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
