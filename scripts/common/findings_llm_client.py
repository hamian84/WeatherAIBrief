from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.config import get_env_value

GEMINI_API_URL_TEMPLATE = 'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_WAIT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 240
DEFAULT_MAX_OUTPUT_TOKENS = 5000

try:
    from jsonschema import validate as jsonschema_validate  # type: ignore
except ImportError:  # pragma: no cover
    jsonschema_validate = None


RETRY_AFTER_PATTERN = re.compile(r'try again in\s+([0-9]+(?:\.[0-9]+)?)s', re.IGNORECASE)


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
    schema: dict[str, Any],
    model: str,
    max_output_tokens: int,
) -> dict[str, Any]:
    return {
        'system_instruction': {
            'parts': [
                {
                    'text': (
                        'You generate structured synoptic findings for a weather briefing. '
                        'Return strict JSON only that matches the schema. '
                        'Use only the information provided in the user prompt. '
                        'Do not invent evidence references not included in allowed_evidence_refs. '
                        'Write narrative text in Korean.'
                    )
                }
            ]
        },
        'contents': [
            {
                'role': 'user',
                'parts': [
                    {'text': prompt_text},
                ],
            },
        ],
        'generationConfig': {
            'responseMimeType': 'application/json',
            'responseJsonSchema': schema,
            'maxOutputTokens': max_output_tokens,
            'temperature': 0,
        },
    }


def _extract_output_text(payload: dict[str, Any]) -> str:
    texts: list[str] = []
    for item in payload.get('candidates', []) or []:
        if not isinstance(item, dict):
            continue
        content = item.get('content', {})
        if not isinstance(content, dict):
            continue
        for part in content.get('parts', []) or []:
            if not isinstance(part, dict):
                continue
            text = part.get('text')
            if isinstance(text, str) and text.strip():
                texts.append(text)
    return '\n'.join(texts).strip()


def _extract_finish_reason(payload: dict[str, Any]) -> str:
    for item in payload.get('candidates', []) or []:
        if not isinstance(item, dict):
            continue
        reason = str(item.get('finishReason', '')).strip()
        if reason:
            return reason
    return ''


def _extract_prompt_feedback(payload: dict[str, Any]) -> str:
    prompt_feedback = payload.get('promptFeedback')
    if not isinstance(prompt_feedback, dict):
        return ''
    return json.dumps(prompt_feedback, ensure_ascii=False)


def _manual_validate_schema(parsed_output: dict[str, Any]) -> None:
    required_keys = (
        'summary',
        'synoptic_primary_cards',
        'cross_layer_cards',
        'regional_response_cards',
        'time_change_cards',
    )
    for key in required_keys:
        if key not in parsed_output:
            raise ValueError(f'findings response missing key: {key}')
    for array_key in required_keys[1:]:
        value = parsed_output.get(array_key)
        if not isinstance(value, list):
            raise ValueError(f'findings response key must be a list: {array_key}')


def _validate_schema(parsed_output: dict[str, Any], schema: dict[str, Any]) -> None:
    if jsonschema_validate is not None:
        jsonschema_validate(parsed_output, schema)
        return
    _manual_validate_schema(parsed_output)


def call_findings_llm(
    prompt_text: str,
    schema: dict[str, Any],
    model: str = 'gemini-2.5-flash',
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_wait_seconds: float = DEFAULT_RETRY_WAIT_SECONDS,
) -> dict[str, Any]:
    api_key = get_env_value('GEMINI_API_KEY')
    if not api_key:
        raise RuntimeError('GEMINI_API_KEY is not configured')

    request_body = _build_request_body(prompt_text, schema, model, max_output_tokens)
    request_payload = json.dumps(request_body).encode('utf-8')
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(
                GEMINI_API_URL_TEMPLATE.format(model=model),
                data=request_payload,
                headers={
                    'x-goog-api-key': api_key,
                    'Content-Type': 'application/json',
                },
                method='POST',
            )
            with urlopen(request, timeout=timeout_seconds) as response:
                http_status = response.status
                response_payload = json.loads(response.read().decode('utf-8'))
            if http_status != 200:
                raise RuntimeError(f'Gemini returned http_status={http_status}')
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
                raise RuntimeError('Gemini parsed output must be an object')
            _validate_schema(parsed_output, schema)
            return {
                'request': {
                    'model': model,
                    'max_output_tokens': max_output_tokens,
                    'prompt_text': prompt_text,
                },
                'response': response_payload,
                'parsed_output': parsed_output,
            }
        except HTTPError as exc:
            body = exc.read().decode('utf-8', errors='replace')
            retryable = exc.code in {408, 409, 429, 500, 502, 503, 504}
            last_error = RuntimeError(f'Gemini HTTPError {exc.code}: {body}')
            if not retryable or attempt >= max_retries:
                break
            retry_after_seconds = _extract_retry_after_seconds(body)
        except URLError as exc:
            last_error = RuntimeError(f'Gemini URLError: {exc.reason}')
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
        raise RuntimeError('Gemini request failed without a captured error')
    raise RuntimeError(str(last_error))
