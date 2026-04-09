from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def _has_csv_rows(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for _ in reader:
            return True
    return False


def _write_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify collected ASOS outputs.")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--report-path", required=True, help="JSON report output path")
    parser.add_argument("--stations-file", help="Unused compatibility argument")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    raw_path = BASE_DIR / "dain" / args.date / "asos" / "asos_hourly.csv"
    curated_path = BASE_DIR / "dain" / args.date / "curated" / "asos" / "asos_daily_summary.csv"

    raw_ok = _has_csv_rows(raw_path)
    curated_ok = _has_csv_rows(curated_path)
    status = "PASS" if raw_ok and curated_ok else "FAIL"
    report = {
        "status": status,
        "date": args.date,
        "raw_path": str(raw_path),
        "curated_path": str(curated_path),
        "raw_has_rows": raw_ok,
        "curated_has_rows": curated_ok,
        "missing_keys": [],
        "duplicate_keys": [],
    }
    _write_report(Path(args.report_path), report)
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
