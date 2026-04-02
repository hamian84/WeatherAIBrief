from __future__ import annotations

import csv
from pathlib import Path

REQUIRED_COLUMNS = (
    "stage",
    "question_id",
    "region_id",
    "region_label",
    "lat_range",
    "lon_range",
    "signal_key",
    "attribute_key",
    "question_text",
    "allowed_answers",
)


def _split_allowed_answers(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def load_prompt_table(path: str | Path) -> list[dict[str, object]]:
    table_path = Path(path)
    if not table_path.exists():
        raise FileNotFoundError(f"prompt table not found: {table_path}")
    with table_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"prompt table missing header: {table_path}")
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
        if missing_columns:
            raise ValueError(f"prompt table missing required columns {missing_columns}: {table_path}")
        rows: list[dict[str, object]] = []
        seen_question_ids: set[str] = set()
        for row_index, raw_row in enumerate(reader, start=2):
            row = {column: (raw_row.get(column) or "").strip() for column in REQUIRED_COLUMNS}
            blank_columns = [column for column, value in row.items() if not value]
            if blank_columns:
                raise ValueError(f"prompt table row {row_index} has blank required columns {blank_columns}: {table_path}")
            question_id = str(row["question_id"])
            if question_id in seen_question_ids:
                raise ValueError(f"duplicate question_id '{question_id}' in {table_path}")
            seen_question_ids.add(question_id)
            allowed_answers = _split_allowed_answers(str(row["allowed_answers"]))
            if not allowed_answers:
                raise ValueError(f"prompt table row {row_index} has empty allowed_answers: {table_path}")
            normalized_row: dict[str, object] = dict(row)
            normalized_row["row_index"] = row_index
            normalized_row["allowed_answers_text"] = str(row["allowed_answers"])
            normalized_row["allowed_answers"] = allowed_answers
            rows.append(normalized_row)
    return rows
