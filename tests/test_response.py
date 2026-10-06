import asyncio
import time

from model_gateway.app import DeadlineResponse


async def test_slow_asgi_consumer_cannot_hold_response_forever():
    class Execution:
        deadline = time.monotonic() + 0.05
        lost = asyncio.Event()
        terminal_override = None
        result = None

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
