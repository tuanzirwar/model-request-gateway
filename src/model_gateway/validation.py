"""校验网关支持的Chat Completions子集，不静默忽略调用方参数。"""

import math

from fastapi import HTTPException

FORWARDED_FIELDS = {
    "messages",
    "stream",
    "stream_options",
    "tools",
    "tool_choice",
    "temperature",
    "max_tokens",
    "max_completion_tokens",
    "reasoning_effort",
    "response_format",
    "top_p",
    "presence_penalty",
    "frequency_penalty",
    "seed",
    "stop",
}


def validate_payload(payload):
    def reject(code):
        raise HTTPException(400, code)

    if not isinstance(payload, dict) or not isinstance(payload.get("model"), str):
        reject("invalid_request")
    if set(payload) - FORWARDED_FIELDS - {"model"}:
        reject("unsupported_parameter")
    messages = payload.get("messages")
    if not isinstance(messages, list) or not 1 <= len(messages) <= 256:
        reject("invalid_messages")
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in {
            "system",
            "developer",
            "user",
            "assistant",
            "tool",
            "function",
        }:
            reject("invalid_message")
        content = message.get("content")
        if not isinstance(content, (str, list)) and not (
            content is None and message["role"] == "assistant" and message.get("tool_calls")
        ):
            reject("invalid_message_content")
        if message["role"] == "tool" and not isinstance(message.get("tool_call_id"), str):
            reject("missing_tool_call_id")
    if type(payload.get("stream", False)) is not bool:
        reject("invalid_stream")
    for name, upper, lower in (
        ("temperature", 2, 0),
        ("top_p", 1, 0),
        ("presence_penalty", 2, -2),
        ("frequency_penalty", 2, -2),
    ):
        if name in payload:
            value = payload[name]
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not lower <= value <= upper
            ):
                reject("invalid_" + name)
    for name in ("max_tokens", "max_completion_tokens", "seed"):
        if name in payload and (
            type(payload[name]) is not int or (name != "seed" and payload[name] <= 0)
        ):
            reject("invalid_" + name)
    if "max_tokens" in payload and "max_completion_tokens" in payload:
        reject("conflicting_token_limits")
    for name, kind in (("tools", list), ("stream_options", dict), ("response_format", dict)):
        if name in payload and not isinstance(payload[name], kind):
            reject("invalid_" + name)
    if len(payload.get("tools", [])) > 128:
        reject("too_many_tools")
    if (
        "stream_options" in payload
        and type(payload["stream_options"].get("include_usage", False)) is not bool
    ):
        reject("invalid_stream_options")
    return payload


def forward_payload(payload, model):
    forwarded = {
        **model.request_defaults,
        **{key: payload[key] for key in FORWARDED_FIELDS if key in payload},
    }
    # 客户端显式设置另一种token上限时，替换默认上限，避免两者冲突。
    if "max_tokens" in payload:
        forwarded.pop("max_completion_tokens", None)
    elif "max_completion_tokens" in payload:
        forwarded.pop("max_tokens", None)
    forwarded["model"] = model.upstream_model
    return forwarded
