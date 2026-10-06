"""真实TCP故障注入上游；不冒充模型推理性能。"""

import asyncio
import json
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI()
stats = {"started": 0, "closed": 0, "active": 0, "max_active": 0, "active_modes": {}}


@app.get("/stats")
async def read_stats():
    return stats


@app.post("/v1/chat/completions")
async def chat(request: Request):
    payload = await request.json()
    mode = payload["messages"][-1]["content"]
    behavior = mode.split(":", 1)[0]
    stats["started"] += 1
    if mode == "http_error":
        return JSONResponse({"error": "private-provider-secret"}, status_code=500)
    if not payload.get("stream"):
        return {
            "id": "fixture",
            "object": "chat.completion",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "fixture answer"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2},
        }

    async def stream():
        stats["active"] += 1
        stats["active_modes"][mode] = stats["active_modes"].get(mode, 0) + 1
        stats["max_active"] = max(stats["max_active"], stats["active"])
        try:

            async def wait_connected(seconds):
                until = time.monotonic() + seconds
                while time.monotonic() < until:
                    if await request.is_disconnected():
                        return False
                    await asyncio.sleep(0.02)
                return True

            if behavior == "before_first":
                if not await wait_connected(30):
                    return
            if behavior == "oversize":
                yield b"data: " + b"x" * 70000
                return
            if behavior == "tool":
                chunks = [
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_fixture",
                                "type": "function",
                                "function": {"name": "echo", "arguments": '{"text":'},
                            }
                        ]
                    },
                    {"tool_calls": [{"index": 0, "function": {"arguments": '"ok"}'}}]},
                ]
            elif behavior == "xss":
                chunks = [{"content": '<img src=x onerror="window.gatewayXss=true">'}]
            else:
                chunks = [{"content": "第一段"}, {"content": "第二段"}]
            for index, delta in enumerate(chunks):
                data = {
                    "id": "fixture",
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
                }
                yield ("data: " + json.dumps(data, ensure_ascii=False) + "\n\n").encode()
                if index == 0:
                    if behavior == "truncated":
                        return
                    if not await wait_connected(30 if behavior in ("hold", "idle") else 0.05):
                        return
            yield (
                b'data: {"choices":[],"usage":'
                b'{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}\n\n'
            )
            yield b"data: [DONE]\n\n"
        finally:
            stats["active"] -= 1
            stats["active_modes"][mode] -= 1
            stats["closed"] += 1

    return StreamingResponse(stream(), media_type="text/event-stream")
