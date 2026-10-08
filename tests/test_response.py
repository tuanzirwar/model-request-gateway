import asyncio
import time

from model_gateway.app import DeadlineResponse


async def test_slow_asgi_consumer_cannot_hold_response_forever():
    class Execution:
        deadline = time.monotonic() + 0.05
        lost = asyncio.Event()
        terminal_override = None
        result = None

        def account_stream_frame(self, data):
            return data.encode()

        async def cleanup(self, status, error):
            self.result = self.terminal_override or (status, error)

    execution = Execution()

    async def content():
        yield b"first"

    async def send(message):
        if message["type"] == "http.response.body":
            await asyncio.sleep(10)

    async def receive():
        await asyncio.sleep(10)

    response = DeadlineResponse(content(), execution=execution)
    await asyncio.wait_for(
        response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send), 0.5
    )
    assert execution.result == ("failed", "deadline_exceeded")


async def test_lost_lease_completes_http_body_with_error_without_done():
    class Execution:
        deadline = time.monotonic() + 2
        lost = asyncio.Event()
        stop_reason = "lease_lost"
        terminal_override = None

        def account_stream_frame(self, data):
            return ("data: " + data + "\n\n").encode()

        async def cleanup(self, status, error):
            pass

    execution = Execution()
    sent = []

    async def content():
        yield b"data: first\n\n"
        await asyncio.Future()

    async def send(message):
        sent.append(message)
        if message["type"] == "http.response.body":
            execution.lost.set()

    async def receive():
        await asyncio.Future()

    response = DeadlineResponse(content(), execution=execution)
    await asyncio.wait_for(
        response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send), 1
    )
    assert sent[-1]["more_body"] is False
    assert b"lease_lost" in sent[-1]["body"] and b"[DONE]" not in sent[-1]["body"]
    assert execution.terminal_override == ("failed", "lease_lost")
