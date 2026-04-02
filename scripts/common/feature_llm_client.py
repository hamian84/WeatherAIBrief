from __future__ import annotations

import base64
import json
import mimetypes
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.config import get_env_value

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_WAIT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_MAX_OUTPUT_TOKENS = 4000

try:
    from jsonschema import validate as jsonschema_validate  # type: ignore
except ImportError:  # pragma: no cover
    jsonschema_validate = None


def _guess_mime_type(path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(path.name)
    return mime_type or "application/octet-stream"


def _encode_image_data_url(image_path: Path) -> str:
    payload = image_path.read_bytes()
    encoded = base64.b64encode(payload).decode("ascii")
    mime_type = _guess_mime_type(image_path)
    return f"data:{mime_type};base64,{encoded}"


def _build_request_body(
    prompt_text: str,
    image_path: Path,
    schema: dict[str, Any],
    model: str,
    max_output_tokens: int,
) -> dict[str, Any]:
    return {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You must return strict JSON only. "
                            "For every answer item, the answer value must be exactly one of the "
                            "allowed_answers values provided in the user prompt. "
                            "Do not paraphrase, translate, inflect, pluralize, or output synonyms. "
                            "If the allowed value is 'continuous', do not output 'continuously'. "
                            "If uncertain and 'unknown' is allowed, return 'unknown' exactly."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt_text},
                    {"type": "input_image", "image_url": _encode_image_data_url(image_path)},
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "feature_stage_response",
                "schema": schema,
                "strict": True,
            }
        },
        "max_output_tokens": max_output_tokens,
    }


def _extract_output_text(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    texts: list[str] = []
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(text)
    return "\n".join(texts).strip()


def _manual_validate_schema(parsed_output: dict[str, Any]) -> None:
    answers = parsed_output.get("answers")
    if not isinstance(answers, list):
        raise ValueError("response payload missing answers list")
    for item in answers:
        if not isinstance(item, dict):
            raise ValueError("response answer item must be an object")
        if not isinstance(item.get("question_id"), str) or not str(item.get("question_id")).strip():
            raise ValueError("response answer item missing question_id")
        if not isinstance(item.get("answer"), str) or not str(item.get("answer")).strip():
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
    model: str = "gpt-4.1-mini",
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_wait_seconds: float = DEFAULT_RETRY_WAIT_SECONDS,
) -> dict[str, Any]:
    api_key = get_env_value("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    image_file = Path(image_path)
    if not image_file.exists():
        raise FileNotFoundError(f"source image not found: {image_file}")

    request_body = _build_request_body(prompt_text, image_file, schema, model, max_output_tokens)
    request_payload = json.dumps(request_body).encode("utf-8")
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(
                OPENAI_RESPONSES_URL,
                data=request_payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urlopen(request, timeout=timeout_seconds) as response:
                http_status = response.status
                response_payload = json.loads(response.read().decode("utf-8"))
            if http_status != 200:
                raise RuntimeError(f"OpenAI returned http_status={http_status}")
            if str(response_payload.get("status", "")).lower() == "incomplete":
                raise RuntimeError(f"OpenAI response incomplete: {response_payload.get('incomplete_details')}")
            output_text = _extract_output_text(response_payload)
            if not output_text:
                raise RuntimeError("OpenAI response missing output text")
            parsed_output = json.loads(output_text)
            if not isinstance(parsed_output, dict):
                raise RuntimeError("OpenAI parsed output must be an object")
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
            last_error = RuntimeError(f"OpenAI HTTPError {exc.code}: {body}")
            if not retryable or attempt >= max_retries:
                break
        except URLError as exc:
            last_error = RuntimeError(f"OpenAI URLError: {exc.reason}")
            if attempt >= max_retries:
                break
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt >= max_retries:
                break
        time.sleep(retry_wait_seconds * attempt)

    if last_error is None:
        raise RuntimeError("OpenAI request failed without a captured error")
    raise RuntimeError(str(last_error))
