from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

ROW_REQUIRED_COLUMNS = (
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

STAGE1_BUNDLE_REQUIRED_COLUMNS = ROW_REQUIRED_COLUMNS

STAGE2_BUNDLE_HEADER_REQUIRED_COLUMNS = (
    "stage",
    "bundle_id",
    "region_id",
    "region_label",
    "lat_range",
    "lon_range",
    "gate_signals",
    "question_text",
)

STAGE2_BUNDLE_TARGET_REQUIRED_COLUMNS = (
    "bundle_id",
    "target_order",
    "signal_key",
    "attribute_key",
    "target_label",
    "question_text",
    "allowed_answers",
    "gate_on_stage1",
    "gate_rule",
    "tier",
)

ALLOWED_GATE_RULES = {
    "full_if_yes_core_if_unknown_skip_if_no",
    "full_if_yes_extended_if_unknown_skip_if_no",
}

ALLOWED_TARGET_TIERS = {"core", "extended"}


def split_pipe_values(value: str, *, field_name: str, path: Path, row_index: int) -> list[str]:
    values = [item.strip() for item in str(value).split("|") if item.strip()]
    if not values:
        raise ValueError(f"{path} row {row_index} has empty {field_name}")
    return values


def _read_csv_reader(path: Path) -> tuple[csv.DictReader, Any]:
    handle = path.open("r", encoding="utf-8-sig", newline="")
    reader = csv.DictReader(handle)
    if reader.fieldnames is None:
        handle.close()
        raise ValueError(f"prompt table missing header: {path}")
    return reader, handle


def _validate_required_columns(path: Path, fieldnames: list[str], required_columns: tuple[str, ...]) -> None:
    missing_columns = [column for column in required_columns if column not in fieldnames]
    if missing_columns:
        raise ValueError(f"prompt table missing required columns {missing_columns}: {path}")


def load_prompt_table(path: str | Path) -> list[dict[str, object]]:
    table_path = Path(path)
    if not table_path.exists():
        raise FileNotFoundError(f"prompt table not found: {table_path}")
    reader, handle = _read_csv_reader(table_path)
    try:
        _validate_required_columns(table_path, list(reader.fieldnames or []), ROW_REQUIRED_COLUMNS)
        rows: list[dict[str, object]] = []
        seen_question_ids: set[str] = set()
        for row_index, raw_row in enumerate(reader, start=2):
            row = {column: (raw_row.get(column) or "").strip() for column in ROW_REQUIRED_COLUMNS}
            blank_columns = [column for column, value in row.items() if not value]
            if blank_columns:
                raise ValueError(f"prompt table row {row_index} has blank required columns {blank_columns}: {table_path}")
            question_id = str(row["question_id"])
            if question_id in seen_question_ids:
                raise ValueError(f"duplicate question_id '{question_id}' in {table_path}")
            seen_question_ids.add(question_id)
            allowed_answers = split_pipe_values(
                str(row["allowed_answers"]),
                field_name="allowed_answers",
                path=table_path,
                row_index=row_index,
            )
            normalized_row: dict[str, object] = dict(row)
            normalized_row["row_index"] = row_index
            normalized_row["allowed_answers_text"] = str(row["allowed_answers"])
            normalized_row["allowed_answers"] = allowed_answers
            rows.append(normalized_row)
        return rows
    finally:
        handle.close()


def load_stage1_bundle_table(path: str | Path) -> list[dict[str, object]]:
    table_path = Path(path)
    if not table_path.exists():
        raise FileNotFoundError(f"stage1 bundle table not found: {table_path}")
    reader, handle = _read_csv_reader(table_path)
    try:
        _validate_required_columns(table_path, list(reader.fieldnames or []), STAGE1_BUNDLE_REQUIRED_COLUMNS)
        rows: list[dict[str, object]] = []
        seen_question_ids: set[str] = set()
        for row_index, raw_row in enumerate(reader, start=2):
            row = {column: (raw_row.get(column) or "").strip() for column in STAGE1_BUNDLE_REQUIRED_COLUMNS}
            blank_columns = [column for column, value in row.items() if not value]
            if blank_columns:
                raise ValueError(
                    f"stage1 bundle table row {row_index} has blank required columns {blank_columns}: {table_path}"
                )
            if str(row["stage"]).strip() != "stage1":
                raise ValueError(f"stage1 bundle table row {row_index} has invalid stage: {table_path}")
            question_id = str(row["question_id"])
            if question_id in seen_question_ids:
                raise ValueError(f"duplicate question_id '{question_id}' in {table_path}")
            seen_question_ids.add(question_id)
            allowed_answers = split_pipe_values(
                str(row["allowed_answers"]),
                field_name="allowed_answers",
                path=table_path,
                row_index=row_index,
            )
            normalized_row: dict[str, object] = dict(row)
            normalized_row["row_index"] = row_index
            normalized_row["allowed_answers_text"] = str(row["allowed_answers"])
            normalized_row["allowed_answers"] = allowed_answers
            normalized_row["signal_options"] = [item for item in allowed_answers if item.lower() != "none"]
            rows.append(normalized_row)
        return rows
    finally:
        handle.close()


def load_stage2_bundle_tables(header_path: str | Path, targets_path: str | Path) -> dict[str, list[dict[str, object]]]:
    header_file = Path(header_path)
    targets_file = Path(targets_path)
    if not header_file.exists():
        raise FileNotFoundError(f"stage2 bundle header not found: {header_file}")
    if not targets_file.exists():
        raise FileNotFoundError(f"stage2 bundle targets not found: {targets_file}")

    header_reader, header_handle = _read_csv_reader(header_file)
    target_reader, target_handle = _read_csv_reader(targets_file)
    try:
        _validate_required_columns(
            header_file,
            list(header_reader.fieldnames or []),
            STAGE2_BUNDLE_HEADER_REQUIRED_COLUMNS,
        )
        _validate_required_columns(
            targets_file,
            list(target_reader.fieldnames or []),
            STAGE2_BUNDLE_TARGET_REQUIRED_COLUMNS,
        )

        headers: list[dict[str, object]] = []
        seen_bundle_ids: set[str] = set()
        for row_index, raw_row in enumerate(header_reader, start=2):
            row = {column: (raw_row.get(column) or "").strip() for column in STAGE2_BUNDLE_HEADER_REQUIRED_COLUMNS}
            blank_columns = [column for column, value in row.items() if not value]
            if blank_columns:
                raise ValueError(
                    f"stage2 bundle header row {row_index} has blank required columns {blank_columns}: {header_file}"
                )
            if str(row["stage"]).strip() != "stage2":
                raise ValueError(f"stage2 bundle header row {row_index} has invalid stage: {header_file}")
            bundle_id = str(row["bundle_id"])
            if bundle_id in seen_bundle_ids:
                raise ValueError(f"duplicate bundle_id '{bundle_id}' in {header_file}")
            seen_bundle_ids.add(bundle_id)
            gate_signals = split_pipe_values(
                str(row["gate_signals"]),
                field_name="gate_signals",
                path=header_file,
                row_index=row_index,
            )
            normalized_row: dict[str, object] = dict(row)
            normalized_row["row_index"] = row_index
            normalized_row["gate_signals"] = gate_signals
            headers.append(normalized_row)

        targets: list[dict[str, object]] = []
        seen_target_keys: set[tuple[str, str]] = set()
        seen_target_orders: set[tuple[str, int]] = set()
        for row_index, raw_row in enumerate(target_reader, start=2):
            row = {column: (raw_row.get(column) or "").strip() for column in STAGE2_BUNDLE_TARGET_REQUIRED_COLUMNS}
            blank_columns = [column for column, value in row.items() if not value]
            if blank_columns:
                raise ValueError(
                    f"stage2 bundle targets row {row_index} has blank required columns {blank_columns}: {targets_file}"
                )
            try:
                target_order = int(str(row["target_order"]))
            except ValueError as exc:
                raise ValueError(f"stage2 bundle targets row {row_index} has invalid target_order: {targets_file}") from exc
            allowed_answers = split_pipe_values(
                str(row["allowed_answers"]),
                field_name="allowed_answers",
                path=targets_file,
                row_index=row_index,
            )
            gate_on_stage1_signals = split_pipe_values(
                str(row["gate_on_stage1"]),
                field_name="gate_on_stage1",
                path=targets_file,
                row_index=row_index,
            )
            gate_rule = str(row["gate_rule"]).strip()
            if gate_rule not in ALLOWED_GATE_RULES:
                raise ValueError(f"unsupported gate_rule '{gate_rule}' in {targets_file} row {row_index}")
            tier = str(row["tier"]).strip().lower()
            if tier not in ALLOWED_TARGET_TIERS:
                raise ValueError(f"unsupported tier '{tier}' in {targets_file} row {row_index}")
            bundle_id = str(row["bundle_id"]).strip()
            target_label = str(row["target_label"]).strip()
            target_key = (bundle_id, target_label)
            if target_key in seen_target_keys:
                raise ValueError(f"duplicate target_label '{target_label}' for bundle '{bundle_id}' in {targets_file}")
            seen_target_keys.add(target_key)
            target_order_key = (bundle_id, target_order)
            if target_order_key in seen_target_orders:
                raise ValueError(f"duplicate target_order '{target_order}' for bundle '{bundle_id}' in {targets_file}")
            seen_target_orders.add(target_order_key)
            normalized_row = dict(row)
            normalized_row["row_index"] = row_index
            normalized_row["target_order"] = target_order
            normalized_row["allowed_answers_text"] = str(row["allowed_answers"])
            normalized_row["allowed_answers"] = allowed_answers
            normalized_row["gate_on_stage1_signals"] = gate_on_stage1_signals
            normalized_row["gate_rule"] = gate_rule
            normalized_row["tier"] = tier
            targets.append(normalized_row)

        header_map = {str(header["bundle_id"]): header for header in headers}
        target_group_map: dict[str, list[dict[str, object]]] = defaultdict(list)
        for target in targets:
            target_group_map[str(target["bundle_id"])].append(target)

        header_bundle_ids = set(header_map)
        target_bundle_ids = set(target_group_map)
        missing_targets = sorted(header_bundle_ids - target_bundle_ids)
        if missing_targets:
            raise ValueError(f"stage2 bundle header has bundles without targets {missing_targets}: {header_file}")
        extra_targets = sorted(target_bundle_ids - header_bundle_ids)
        if extra_targets:
            raise ValueError(f"stage2 bundle targets reference unknown bundles {extra_targets}: {targets_file}")

        bundles: list[dict[str, object]] = []
        for bundle_id in sorted(header_map):
            header = dict(header_map[bundle_id])
            bundle_targets = sorted(target_group_map[bundle_id], key=lambda item: int(item["target_order"]))
            header_gate_signals = {str(value).strip() for value in header["gate_signals"]}
            for target in bundle_targets:
                declared_gate_signals = [
                    signal
                    for signal in target["gate_on_stage1_signals"]
                    if str(signal).strip() in header_gate_signals
                ]
                if not declared_gate_signals:
                    raise ValueError(
                        f"stage2 target '{target['target_label']}' references gate_on_stage1="
                        f"'{target['gate_on_stage1']}' not declared in header gate_signals: {targets_file}"
                    )
            header["targets"] = bundle_targets
            bundles.append(header)

        return {
            "headers": headers,
            "targets": targets,
            "bundles": bundles,
        }
    finally:
        header_handle.close()
        target_handle.close()
