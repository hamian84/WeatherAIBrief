from __future__ import annotations

import base64
import json
import mimetypes
import re
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.config import get_env_value

GEMINI_API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_WAIT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_MAX_OUTPUT_TOKENS = 4000
RETRY_AFTER_PATTERN = re.compile(r"(?:try again in|retryDelay[^0-9]*)([0-9]+(?:\.[0-9]+)?)s", re.IGNORECASE)

try:
    from jsonschema import validate as jsonschema_validate  # type: ignore
except ImportError:  # pragma: no cover
    jsonschema_validate = None


def _guess_mime_type(path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(path.name)
    return mime_type or "application/octet-stream"


def _encode_image_base64(image_path: Path) -> str:
    payload = image_path.read_bytes()
    return base64.b64encode(payload).decode("ascii")


def _extract_retry_after_seconds(message: str) -> float | None:
    match = RETRY_AFTER_PATTERN.search(message)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _build_request_body(
    prompt_text: str,
    image_path: Path,
    schema: dict[str, Any],
    model: str,
    max_output_tokens: int,
) -> dict[str, Any]:
    mime_type = _guess_mime_type(image_path)
    return {
        "system_instruction": {
            "parts": [
                {
                    "text": (
                        "You must return strict JSON only. "
                        "For every answer item, the answer value must be exactly one of the "
                        "allowed_answers values provided in the user prompt. "
                        "Do not paraphrase, translate, inflect, pluralize, or output synonyms. "
                        "If the allowed value is 'continuous', do not output 'continuously'. "
                        "If uncertain and 'unknown' is allowed, return 'unknown' exactly."
                    )
                }
            ]
        },
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "inline_data": {
                            "mime_type": mime_type,
                            "data": _encode_image_base64(image_path),
                        }
                    },
                    {"text": prompt_text},
                ],
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseJsonSchema": schema,
            "maxOutputTokens": max_output_tokens,
            "temperature": 0,
        },
    }


def _extract_output_text(payload: dict[str, Any]) -> str:
    texts: list[str] = []
    for item in payload.get("candidates", []) or []:
        if not isinstance(item, dict):
            continue
        content = item.get("content", {})
        if not isinstance(content, dict):
            continue
        for content_part in content.get("parts", []) or []:
            if not isinstance(content_part, dict):
                continue
            text = content_part.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(text)
    return "\n".join(texts).strip()


def _extract_finish_reason(payload: dict[str, Any]) -> str:
    for item in payload.get("candidates", []) or []:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("finishReason", "")).strip()
        if reason:
            return reason
    return ""


def _extract_prompt_feedback(payload: dict[str, Any]) -> str:
    prompt_feedback = payload.get("promptFeedback")
    if not isinstance(prompt_feedback, dict):
        return ""
    return json.dumps(prompt_feedback, ensure_ascii=False)


def _manual_validate_schema(parsed_output: dict[str, Any]) -> None:
    if "bundle_answers" in parsed_output:
        bundle_answers = parsed_output.get("bundle_answers")
        if not isinstance(bundle_answers, list):
            raise ValueError("response payload missing bundle_answers list")
        for bundle in bundle_answers:
            if not isinstance(bundle, dict):
                raise ValueError("response bundle item must be an object")
            if not isinstance(bundle.get("bundle_id"), str) or not str(bundle.get("bundle_id")).strip():
                raise ValueError("response bundle item missing bundle_id")
            targets = bundle.get("targets")
            if not isinstance(targets, list):
                raise ValueError("response bundle item missing targets list")
            for target in targets:
                if not isinstance(target, dict):
                    raise ValueError("response target item must be an object")
                if not isinstance(target.get("target_label"), str) or not str(target.get("target_label")).strip():
                    raise ValueError("response target item missing target_label")
                if not isinstance(target.get("answer"), str) or not str(target.get("answer")).strip():
                    raise ValueError("response target item missing answer")
                note = target.get("note")
                if note is not None and not isinstance(note, str):
                    raise ValueError("response target note must be a string when present")
        return

    answers = parsed_output.get("answers")
    if not isinstance(answers, list):
        raise ValueError("response payload missing answers list")
    for item in answers:
        if not isinstance(item, dict):
            raise ValueError("response answer item must be an object")
        if not isinstance(item.get("question_id"), str) or not str(item.get("question_id")).strip():
            raise ValueError("response answer item missing question_id")
        if "selected_answers" in item:
            selected_answers = item.get("selected_answers")
            if not isinstance(selected_answers, list) or not selected_answers:
                raise ValueError("response answer item missing selected_answers")
            if any(not isinstance(value, str) or not str(value).strip() for value in selected_answers):
                raise ValueError("response selected_answers item must be a non-empty string")
        elif not isinstance(item.get("answer"), str) or not str(item.get("answer")).strip():
            raise ValueError("response answer item missing answer")
        note = item.get("note")
        if note is not None and not isinstance(note, str):
            raise ValueError("response answer item note must be a string when present")


def _validate_schema(parsed_output: dict[str, Any], schema: dict[str, Any]) -> None:
    if jsonschema_validate is not None:
        jsonschema_validate(parsed_output, schema)
        return
    _manual_validate_schema(parsed_output)


def call_feature_llm(
    prompt_text: str,
    image_path: str | Path,
    schema: dict[str, Any],
    model: str = "gemini-2.5-flash",
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_wait_seconds: float = DEFAULT_RETRY_WAIT_SECONDS,
) -> dict[str, Any]:
    api_key = get_env_value("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")

    image_file = Path(image_path)
    if not image_file.exists():
        raise FileNotFoundError(f"source image not found: {image_file}")

    request_body = _build_request_body(prompt_text, image_file, schema, model, max_output_tokens)
    request_payload = json.dumps(request_body).encode("utf-8")
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(
                GEMINI_API_URL_TEMPLATE.format(model=model),
                data=request_payload,
                headers={
                    "x-goog-api-key": api_key,
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urlopen(request, timeout=timeout_seconds) as response:
                http_status = response.status
                response_payload = json.loads(response.read().decode("utf-8"))
            if http_status != 200:
                raise RuntimeError(f"Gemini returned http_status={http_status}")
            output_text = _extract_output_text(response_payload)
            if not output_text:
                finish_reason = _extract_finish_reason(response_payload)
                prompt_feedback = _extract_prompt_feedback(response_payload)
                raise RuntimeError(
                    f"Gemini response missing output text"
                    f"{f' finishReason={finish_reason}' if finish_reason else ''}"
                    f"{f' promptFeedback={prompt_feedback}' if prompt_feedback else ''}"
                )
            parsed_output = json.loads(output_text)
            if not isinstance(parsed_output, dict):
                raise RuntimeError("Gemini parsed output must be an object")
            _validate_schema(parsed_output, schema)
            return {
                "request": {
                    "model": model,
                    "max_output_tokens": max_output_tokens,
                    "source_image": str(image_file),
                    "prompt_text": prompt_text,
                },
                "response": response_payload,
                "parsed_output": parsed_output,
            }
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            retryable = exc.code in {408, 409, 429, 500, 502, 503, 504}
            last_error = RuntimeError(f"Gemini HTTPError {exc.code}: {body}")
            if not retryable or attempt >= max_retries:
                break
            retry_after_seconds = _extract_retry_after_seconds(body)
        except URLError as exc:
            last_error = RuntimeError(f"Gemini URLError: {exc.reason}")
            if attempt >= max_retries:
                break
            retry_after_seconds = None
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt >= max_retries:
                break
            retry_after_seconds = None
        wait_seconds = retry_wait_seconds * attempt
        if retry_after_seconds is not None:
            wait_seconds = max(wait_seconds, retry_after_seconds + 1.0)
        time.sleep(wait_seconds)

    if last_error is None:
        raise RuntimeError("Gemini request failed without a captured error")
    raise RuntimeError(str(last_error))
