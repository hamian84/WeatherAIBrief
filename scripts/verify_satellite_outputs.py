from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
MANIFEST_DIR = BASE_DIR / "daba" / "manifests"
SATELLITE_ROOT_PATTERN = re.compile(
    r'"image_root_template"\s*:\s*"dain/\{date\}/satellite/LE1B/(?P<product>[^/]+)/(?P<area>[^"]+)"'
)


def _read_list_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _has_non_empty_file(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def _write_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_required_pairs() -> list[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    if MANIFEST_DIR.exists():
        for path in sorted(MANIFEST_DIR.glob("*.yaml")):
            text = path.read_text(encoding="utf-8")
            for match in SATELLITE_ROOT_PATTERN.finditer(text):
                pairs.add((match.group("product"), match.group("area")))
    if pairs:
        return sorted(pairs)

    products = _read_list_file(BASE_DIR / "config" / "satellite_le1b_products.txt")
    areas = _read_list_file(BASE_DIR / "config" / "satellite_areas.txt")
    return sorted((product, area) for product in products for area in areas)


def _read_all_pairs() -> list[tuple[str, str]]:
    products = _read_list_file(BASE_DIR / "config" / "satellite_le1b_products.txt")
    areas = _read_list_file(BASE_DIR / "config" / "satellite_areas.txt")
    return sorted((product, area) for product in products for area in areas)


def _matching_files(
    raw_root: Path,
    product: str,
    area: str,
    cycle: str | None,
) -> list[Path]:
    target_dir = raw_root / product / area
    if not target_dir.exists():
        return []
    if cycle:
        pattern = f"gk2a_le1b_{product}_{area}_{cycle}.*"
    else:
        pattern = f"gk2a_le1b_{product}_{area}_*.*"
    return [path for path in target_dir.glob(pattern) if _has_non_empty_file(path)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify collected satellite outputs.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--cycle", help="YYYYMMDDHHMM")
    parser.add_argument("--report-path", required=True, help="JSON report output path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    raw_root = BASE_DIR / "dain" / args.date / "satellite" / "LE1B"

    required_pairs = _read_required_pairs()
    all_pairs = _read_all_pairs()
    optional_pairs = [pair for pair in all_pairs if pair not in required_pairs]

    missing_items: list[dict[str, str]] = []
    optional_missing_items: list[dict[str, str]] = []
    found_total = 0
    optional_found_total = 0

    for product, area in required_pairs:
        valid_files = _matching_files(raw_root, product, area, args.cycle)
        if valid_files:
            found_total += 1
            continue
        item = {"product": product, "area": area, "date": args.cycle or args.date}
        missing_items.append(item)

    for product, area in optional_pairs:
        valid_files = _matching_files(raw_root, product, area, args.cycle)
        if valid_files:
            optional_found_total += 1
            continue
        item = {"product": product, "area": area, "date": args.cycle or args.date}
        optional_missing_items.append(item)

    status = "PASS" if not missing_items else "FAIL"
    report = {
        "status": status,
        "date": args.date,
        "cycle": args.cycle,
        "expected_total": len(required_pairs),
        "found_total": found_total,
        "expected_optional_total": len(optional_pairs),
        "found_optional_total": optional_found_total,
        "missing_items": missing_items,
        "optional_missing_items": optional_missing_items,
    }
    _write_report(Path(args.report_path), report)
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
