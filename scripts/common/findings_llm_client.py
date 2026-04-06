from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.config import get_env_value

OPENAI_RESPONSES_URL = 'https://api.openai.com/v1/responses'
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
        'model': model,
        'input': [
            {
                'role': 'system',
                'content': [
                    {
                        'type': 'input_text',
                        'text': (
                            'You generate structured synoptic findings for a weather briefing. '
                            'Return strict JSON only that matches the schema. '
                            'Use only the information provided in the user prompt. '
                            'Do not invent evidence references not included in allowed_evidence_refs. '
                            'Write narrative text in Korean.'
                        ),
                    }
                ],
            },
            {
                'role': 'user',
                'content': [
                    {'type': 'input_text', 'text': prompt_text},
                ],
            },
        ],
        'text': {
            'format': {
                'type': 'json_schema',
                'name': 'findings_bundle_response',
                'schema': schema,
                'strict': True,
            }
        },
        'max_output_tokens': max_output_tokens,
    }


def _extract_output_text(payload: dict[str, Any]) -> str:
    output_text = payload.get('output_text')
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    texts: list[str] = []
    for item in payload.get('output', []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get('content', []) or []:
            if not isinstance(content, dict):
                continue
            text = content.get('text')
            if isinstance(text, str) and text.strip():
                texts.append(text)
    return '\n'.join(texts).strip()


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
    model: str = 'gpt-4.1-mini',
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_wait_seconds: float = DEFAULT_RETRY_WAIT_SECONDS,
) -> dict[str, Any]:
    api_key = get_env_value('OPENAI_API_KEY')
    if not api_key:
        raise RuntimeError('OPENAI_API_KEY is not configured')

    request_body = _build_request_body(prompt_text, schema, model, max_output_tokens)
    request_payload = json.dumps(request_body).encode('utf-8')
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            request = Request(
                OPENAI_RESPONSES_URL,
                data=request_payload,
                headers={
                    'Authorization': f'Bearer {api_key}',
                    'Content-Type': 'application/json',
                },
                method='POST',
            )
            with urlopen(request, timeout=timeout_seconds) as response:
                http_status = response.status
                response_payload = json.loads(response.read().decode('utf-8'))
            if http_status != 200:
                raise RuntimeError(f'OpenAI returned http_status={http_status}')
            if str(response_payload.get('status', '')).lower() == 'incomplete':
                raise RuntimeError(f"OpenAI response incomplete: {response_payload.get('incomplete_details')}")
            output_text = _extract_output_text(response_payload)
            if not output_text:
                raise RuntimeError('OpenAI response missing output text')
            parsed_output = json.loads(output_text)
            if not isinstance(parsed_output, dict):
                raise RuntimeError('OpenAI parsed output must be an object')
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
            last_error = RuntimeError(f'OpenAI HTTPError {exc.code}: {body}')
            if not retryable or attempt >= max_retries:
                break
            retry_after_seconds = _extract_retry_after_seconds(body)
        except URLError as exc:
            last_error = RuntimeError(f'OpenAI URLError: {exc.reason}')
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
        raise RuntimeError('OpenAI request failed without a captured error')
    raise RuntimeError(str(last_error))
