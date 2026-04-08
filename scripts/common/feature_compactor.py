from __future__ import annotations

from typing import Any


IMAGE_FIELDS = ("image_ref", "valid_time", "source_image")
REGION_FIELDS = ("region_id", "region_label")
QUESTION_FIELDS = ("question_id", "signal_key", "attribute_key", "question_text", "allowed_answers")
ROW_VALUE_FIELDS = ("model_answer_raw", "model_answer_normalized", "is_valid_answer")
OPTIONAL_NOTE_FIELD = "note"
BASE_RECORD_FIELDS = set(IMAGE_FIELDS + REGION_FIELDS + QUESTION_FIELDS + ROW_VALUE_FIELDS + (OPTIONAL_NOTE_FIELD,))


def _stable_unique(values: list[Any]) -> list[Any]:
    unique: list[Any] = []
    for value in values:
        if value not in unique:
            unique.append(value)
    return unique


def _freeze(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, dict):
        return tuple((str(key), _freeze(val)) for key, val in sorted(value.items(), key=lambda item: item[0]))
    return value


def compact_normalized_payload(payload: dict[str, Any]) -> dict[str, Any]:
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError("normalized payload missing records list")

    meta = {key: value for key, value in payload.items() if key != "records"}
    if not records:
        return {
            "meta": meta,
            "catalogs": {
                "images": [],
                "regions": [],
                "questions": [],
                "notes": [],
            },
            "shared_record_fields": {},
            "row_fields": [
                "image_idx",
                "question_idx",
                "model_answer_raw",
                "model_answer_normalized",
                "is_valid_answer",
                "note_idx",
            ],
            "rows": [],
            "row_count": 0,
        }

    image_values = _stable_unique(
        [
            tuple(str(record.get(field, "")) for field in IMAGE_FIELDS)
            for record in records
        ]
    )
    image_index = {value: idx for idx, value in enumerate(image_values)}

    region_values = _stable_unique(
        [
            tuple(str(record.get(field, "")) for field in REGION_FIELDS)
            for record in records
        ]
    )
    region_index = {value: idx for idx, value in enumerate(region_values)}

    question_values = _stable_unique(
        [
            (
                str(record.get("question_id", "")),
                region_index[tuple(str(record.get(field, "")) for field in REGION_FIELDS)],
                str(record.get("signal_key", "")),
                str(record.get("attribute_key", "")),
                str(record.get("question_text", "")),
                tuple(str(item) for item in record.get("allowed_answers", [])),
            )
            for record in records
        ]
    )
    question_index = {value: idx for idx, value in enumerate(question_values)}

    note_values = _stable_unique([str(record.get(OPTIONAL_NOTE_FIELD, "")) for record in records if str(record.get(OPTIONAL_NOTE_FIELD, ""))])
    note_index = {value: idx for idx, value in enumerate(note_values)}

    record_keys = _stable_unique([key for record in records for key in record.keys()])
    extra_fields = [key for key in record_keys if key not in BASE_RECORD_FIELDS]
    shared_record_fields: dict[str, Any] = {}
    row_extra_fields: list[str] = []
    for field in extra_fields:
        unique_values = _stable_unique([record.get(field) for record in records])
        if len(unique_values) == 1:
            shared_record_fields[field] = unique_values[0]
        else:
            row_extra_fields.append(field)

    row_fields = [
        "image_idx",
        "question_idx",
        "model_answer_raw",
        "model_answer_normalized",
        "is_valid_answer",
        "note_idx",
        *row_extra_fields,
    ]
    rows: list[list[Any]] = []
    for record in records:
        image_tuple = tuple(str(record.get(field, "")) for field in IMAGE_FIELDS)
        region_tuple = tuple(str(record.get(field, "")) for field in REGION_FIELDS)
        question_tuple = (
            str(record.get("question_id", "")),
            region_index[region_tuple],
            str(record.get("signal_key", "")),
            str(record.get("attribute_key", "")),
            str(record.get("question_text", "")),
            tuple(str(item) for item in record.get("allowed_answers", [])),
        )
        note = str(record.get(OPTIONAL_NOTE_FIELD, ""))
        row: list[Any] = [
            image_index[image_tuple],
            question_index[question_tuple],
            str(record.get("model_answer_raw", "")),
            str(record.get("model_answer_normalized", "")),
            bool(record.get("is_valid_answer", False)),
            note_index.get(note),
        ]
        for field in row_extra_fields:
            value = record.get(field)
            row.append(_freeze(value))
        rows.append(row)

    return {
        "meta": meta,
        "catalogs": {
            "images": [
                {
                    "image_idx": idx,
                    "image_ref": value[0],
                    "valid_time": value[1],
                    "source_image": value[2],
                }
                for idx, value in enumerate(image_values)
            ],
            "regions": [
                {
                    "region_idx": idx,
                    "region_id": value[0],
                    "region_label": value[1],
                }
                for idx, value in enumerate(region_values)
            ],
            "questions": [
                {
                    "question_idx": idx,
                    "question_id": value[0],
                    "region_idx": value[1],
                    "signal_key": value[2],
                    "attribute_key": value[3],
                    "question_text": value[4],
                    "allowed_answers": list(value[5]),
                }
                for idx, value in enumerate(question_values)
            ],
            "notes": note_values,
        },
        "shared_record_fields": shared_record_fields,
        "row_fields": row_fields,
        "rows": rows,
        "row_count": len(rows),
    }
