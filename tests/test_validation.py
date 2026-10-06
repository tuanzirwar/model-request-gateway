import pytest
from fastapi import HTTPException

from model_gateway.config import Model
from model_gateway.validation import forward_payload, validate_payload


@pytest.mark.parametrize(
    "changes",
    [
        {"messages": ["text"]},
        {"messages": [{"role": "bad", "content": "a"}]},
        {"messages": [{"role": "user", "content": 1}]},
        {"messages": [{"role": "tool", "content": "x"}]},
        {"stream": "true"},
        {"temperature": float("nan")},
        {"temperature": 3},
        {"top_p": True},
        {"max_tokens": -1},
        {"max_tokens": 10, "max_completion_tokens": 10},
        {"tools": "function"},
        {"stream_options": {"include_usage": "true"}},
        {"upstream_url": "https://evil.example"},
    ],
)
def test_invalid_chat_input_rejected_before_upstream(changes):
    payload = {"model": "coding", "messages": [{"role": "user", "content": "hello"}], **changes}
    with pytest.raises(HTTPException) as exc:
        validate_payload(payload)
    assert exc.value.status_code == 400


def test_assistant_tool_handoff_supported():
    payload = {
        "model": "coding",
        "messages": [
            {"role": "assistant", "content": None, "tool_calls": [{"id": "call_1"}]},
            {"role": "tool", "content": "result", "tool_call_id": "call_1"},
        ],
        "stream": True,
        "max_completion_tokens": 100,
    }
    assert validate_payload(payload) is payload


def test_model_defaults_allow_explicit_override_without_token_conflict():
    model = Model(
        "coding",
        "actual",
        "https://example.com",
        "",
        request_defaults={"reasoning_effort": "none", "max_tokens": 512},
    )
    forwarded = forward_payload(
        {
            "model": "coding",
            "messages": [],
            "reasoning_effort": "high",
            "max_completion_tokens": 100,
        },
        model,
    )
    assert forwarded["model"] == "actual" and forwarded["reasoning_effort"] == "high"
    assert forwarded["max_completion_tokens"] == 100 and "max_tokens" not in forwarded
