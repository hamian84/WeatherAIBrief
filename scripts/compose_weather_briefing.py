from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.common.date_utils import normalize_run_date

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="생성된 브리핑 섹션들을 최종 Markdown으로 조합합니다.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
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


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _briefing_dir(base_dir: Path, run_date: str) -> Path:
    return base_dir / "daio" / run_date / "briefing"


def _render_markdown(title: str, sections: list[dict[str, Any]]) -> str:
    lines: list[str] = [f"# {title}", ""]
    for section in sections:
        lines.append(f"## {section['title']}")
        lines.append("")
        lines.append(section["body"])
        lines.append("")
        lines.append("관측 근거:")
        for item in section["observation_basis"]:
            lines.append(f"- {item}")
        lines.append("")
        lines.append("적용 패턴:")
        for item in section["applied_patterns"]:
            lines.append(f"- {item}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = build_parser().parse_args()
    args.date = normalize_run_date(args.date)
    section_map = _load_yaml(BASE_DIR / "config" / "briefing" / "section_map.yaml")
    title_template = section_map.get("document", {}).get("title_template", "{date} 기상 실황 브리핑")
    section_order = section_map.get("document", {}).get("section_order", [])
    if not isinstance(section_order, list):
        raise ValueError("section_order must be a list")

    briefing_dir = _briefing_dir(BASE_DIR, args.date)
    sections: list[dict[str, Any]] = []
    for section_id in section_order:
        path = briefing_dir / "sections" / f"{section_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"section output not found: {path}")
        sections.append(_load_json(path))

    briefing_payload = {
        "run_date": args.date,
        "title": title_template.format(date=args.date),
        "sections": sections,
    }

    json_path = briefing_dir / "weather_briefing.json"
    md_path = briefing_dir / "weather_briefing.md"
    _write_text(json_path, json.dumps(briefing_payload, ensure_ascii=False, indent=2))
    _write_text(md_path, _render_markdown(briefing_payload["title"], sections))

    print(
        json.dumps(
            {
                "status": "ok",
                "run_date": args.date,
                "json_path": str(json_path),
                "md_path": str(md_path),
                "section_count": len(sections),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
