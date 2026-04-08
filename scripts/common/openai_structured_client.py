from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.common.config import get_env_value

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_WAIT_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_MAX_OUTPUT_TOKENS = 4000


def _build_request_body(
    *,
    prompt_text: str,
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
                            "Return strict JSON only. "
                            "Do not wrap JSON in markdown. "
                            "Do not add commentary. "
                            "Use only ids and references provided by the user input. "
                            "Never fabricate evidence_refs or rule_refs."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt_text},
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "structured_text_response",
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


def call_structured_text_llm(
    *,
    prompt_text: str,
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

    request_body = _build_request_body(
        prompt_text=prompt_text,
        schema=schema,
        model=model,
        max_output_tokens=max_output_tokens,
    )
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
            return {
                "request": {
                    "model": model,
                    "max_output_tokens": max_output_tokens,
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
