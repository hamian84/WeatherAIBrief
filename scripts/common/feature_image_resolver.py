from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def _resolve_root(base_dir: Path, manifest: dict[str, Any], run_date: str) -> Path:
    template = str(manifest.get("image_root_template") or manifest.get("image_root") or "").strip()
    if not template:
        raise ValueError("manifest missing image_root_template")
    expanded = template.format(date=run_date)
    root = Path(expanded)
    if not root.is_absolute():
        root = base_dir / root
    return root


def resolve_images(base_dir: Path, run_date: str, manifest: dict[str, Any]) -> list[dict[str, str]]:
    root = _resolve_root(base_dir, manifest, run_date)
    if not root.exists():
        raise FileNotFoundError(f"image root not found: {root}")
    pattern = str(manifest["image_glob"])
    regex = re.compile(str(manifest["image_ref_regex"]))
    target_hours = {str(item) for item in manifest.get("target_hours", [])}
    images: list[dict[str, str]] = []
    for path in sorted(root.glob(pattern), key=lambda item: item.name):
        if not path.is_file():
            continue
        match = regex.match(path.name)
        if not match:
            continue
        image_ref = match.groupdict().get("image_ref") or path.stem
        valid_time = match.groupdict().get("valid_time") or ""
        if target_hours and valid_time and valid_time[-2:] not in target_hours:
            continue
        try:
            source_image = path.relative_to(base_dir).as_posix()
        except ValueError:
            source_image = str(path)
        images.append(
            {
                "source_image": source_image,
                "image_path": str(path),
                "image_ref": image_ref,
                "valid_time": valid_time,
                "filename": path.name,
            }
        )
    if not images:
        raise FileNotFoundError(f"no images matched manifest '{manifest['id']}' in {root} with pattern {pattern}")
    return images
