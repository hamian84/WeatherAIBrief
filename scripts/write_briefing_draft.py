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
    parser = argparse.ArgumentParser(description="앞선 섹션 출력으로 검토용 초안을 작성합니다.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model name")
    parser.add_argument("--max-output-tokens", type=int, default=2500, help="max_output_tokens")
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


def _briefing_dir(base_dir: Path, run_date: str) -> Path:
    return base_dir / "daio" / run_date / "briefing"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _render_prompt(template: str, prompt_input: dict[str, Any]) -> str:
    sections_json = json.dumps(prompt_input["sections"], ensure_ascii=False, separators=(",", ":"))
    return template.format(sections_json=sections_json)


def _build_prompt_input(section_map: dict[str, Any], base_dir: Path, run_date: str) -> dict[str, Any]:
    draft_config = next(
        section for section in section_map.get("sections", [])
        if isinstance(section, dict) and section.get("section_id") == "briefing_draft"
    )
    source_section_ids = draft_config.get("source_sections") or []
    sections: list[dict[str, Any]] = []
    for section_id in source_section_ids:
        section_path = _briefing_dir(base_dir, run_date) / "sections" / f"{section_id}.json"
        if not section_path.exists():
            raise FileNotFoundError(f"section output not found: {section_path}")
        payload = _load_json(section_path)
        sections.append(
            {
                "section_id": payload["section_id"],
                "title": payload["title"],
                "body": payload["body"],
                "observation_basis": payload["observation_basis"],
                "applied_patterns": payload["applied_patterns"],
            }
        )
    return {
        "run_date": run_date,
        "section_id": "briefing_draft",
        "section_title": "검토용 초안",
        "sections": sections,
    }


def _validate_payload(parsed_output: dict[str, Any], schema: dict[str, Any], prompt_input: dict[str, Any]) -> None:
    validate_instance(parsed_output, schema, "briefing_draft_body")
    if parsed_output.get("title") != "검토용 초안":
        raise ValueError("title must equal 검토용 초안")
    body = parsed_output.get("body")
    if not isinstance(body, str) or not body.strip():
        raise ValueError("body must be a non-empty string")
    if any(marker not in body for marker in BODY_TIME_MARKERS):
        raise ValueError("body must include 초반, 이후, 후반, 따라서")
    if re.search(r"\d+회", body):
        raise ValueError("body must not include counts like 5회")
    if INTERNAL_KEY_PATTERN.search(body):
        raise ValueError("body must not include internal underscore keys")
    allowed_ids = {section["section_id"] for section in prompt_input["sections"]}
    source_ids = parsed_output.get("source_section_ids")
    if not isinstance(source_ids, list) or not source_ids:
        raise ValueError("source_section_ids must be a non-empty list")
    invalid = [value for value in source_ids if value not in allowed_ids]
    if invalid:
        raise ValueError(f"invalid source_section_ids: {invalid}")


def _build_observation_basis(prompt_input: dict[str, Any], source_ids: list[str]) -> list[str]:
    selected = [section for section in prompt_input["sections"] if section["section_id"] in source_ids]
    lines: list[str] = []
    for section in selected:
        if section["observation_basis"]:
            lines.append(f"{section['title']}: {section['observation_basis'][0]}")
    return lines


def _build_applied_patterns(prompt_input: dict[str, Any], source_ids: list[str]) -> list[str]:
    selected = [section for section in prompt_input["sections"] if section["section_id"] in source_ids]
    patterns: list[str] = []
    for section in selected:
        for pattern in section["applied_patterns"]:
            if pattern not in patterns:
                patterns.append(pattern)
    return patterns[:6]


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)
    load_project_env(BASE_DIR)

    section_map = _load_yaml(BASE_DIR / "config" / "briefing" / "section_map.yaml")
    prompt_input = _build_prompt_input(section_map, BASE_DIR, args.date)

    briefing_dir = _briefing_dir(BASE_DIR, args.date)
    prompt_input_path = briefing_dir / "sections" / "briefing_draft.prompt_input.json"
    _write_json(prompt_input_path, prompt_input)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "status": "ok",
                    "run_date": args.date,
                    "section_id": "briefing_draft",
                    "prompt_input_path": str(prompt_input_path),
                    "section_count": len(prompt_input["sections"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    schema = load_json_schema(BASE_DIR / "daba" / "schemas" / "briefing_draft_body.schema.json")
    template = (BASE_DIR / "daba" / "templates" / "briefing_draft_prompt.txt").read_text(encoding="utf-8")
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
            _validate_payload(parsed_output, schema, prompt_input)
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
        raise RuntimeError("briefing draft writing failed without a captured error")

    source_ids = parsed_output["source_section_ids"]
    final_payload = {
        "run_date": args.date,
        "section_id": "briefing_draft",
        "title": parsed_output["title"],
        "body": parsed_output["body"],
        "observation_basis": _build_observation_basis(prompt_input, source_ids),
        "applied_patterns": _build_applied_patterns(prompt_input, source_ids),
        "source_section_ids": source_ids,
    }

    raw_path = briefing_dir / "sections" / "briefing_draft.raw.json"
    final_path = briefing_dir / "sections" / "briefing_draft.json"
    _write_json(raw_path, llm_result)
    _write_json(final_path, final_payload)

    print(
        json.dumps(
            {
                "status": "ok",
                "run_date": args.date,
                "section_id": "briefing_draft",
                "prompt_input_path": str(prompt_input_path),
                "raw_path": str(raw_path),
                "section_path": str(final_path),
                "source_section_count": len(source_ids),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
