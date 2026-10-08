"""验证压测客户端能复用连接并识别分块响应，不重试不确定请求。"""

import asyncio

import httpx
import pytest

from scripts.load_client import RawClient


async def test_raw_client_reuses_connection_and_reads_chunked():
    connections = 0

    async def serve(reader, writer):
        nonlocal connections
        connections += 1
        try:
            for _ in range(2):
                await reader.readuntil(b"\r\n\r\n")
                writer.write(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                    b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
                )
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
        url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
        async with RawClient() as client:
            assert (await client.get(url, {})).content == b"abcde"
            assert (await client.get(url, {})).status_code == 200
        assert connections == 1


async def test_raw_client_does_not_retry_truncated_response():
    connections = 0

    async def serve(reader, writer):
        nonlocal connections
        connections += 1
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async with await asyncio.start_server(serve, "127.0.0.1", 0) as server:
        url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
        async with RawClient() as client:
            with pytest.raises(httpx.TransportError):
                await client.post(url, {}, b"")
        assert connections == 1
