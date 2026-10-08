"""仅供本机压测的低开销HTTP/1.1客户端，不作为生产模型客户端。"""

import asyncio
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx


class RawClient:
    def __init__(self, timeout=10):
        self.timeout = timeout
        self.reader = None
        self.writer = None
        self.address = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                async with asyncio.timeout(2):
                    await self.writer.wait_closed()
            except (OSError, TimeoutError):
                pass
        self.reader = self.writer = self.address = None

    async def get(self, url, headers):
        return await self.request("GET", url, headers, b"")

    async def post(self, url, headers, content):
        return await self.request("POST", url, headers, content)

    async def request(self, method, url, headers, body):
        parsed = urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise ValueError("原始压测客户端只允许本机HTTP服务")
        address = (parsed.hostname, parsed.port or 80)
        try:
            async with asyncio.timeout(self.timeout):
                if self.address != address or self.writer is None:
                    await self.close()
                    self.reader, self.writer = await asyncio.open_connection(*address, limit=16384)
                    self.address = address
                path = parsed.path or "/"
                if parsed.query:
                    path += "?" + parsed.query
                request = [
                    f"{method} {path} HTTP/1.1",
                    f"Host: {address[0]}:{address[1]}",
                    "Connection: keep-alive",
                    f"Content-Length: {len(body)}",
                ]
                request.extend(f"{name}: {value}" for name, value in headers.items())
                self.writer.write(("\r\n".join(request) + "\r\n\r\n").encode("ascii") + body)
                await self.writer.drain()
                raw = await self.reader.readuntil(b"\r\n\r\n")
                lines = raw.split(b"\r\n")
                status = int(lines[0].split()[1])
                response_headers = {}
                for line in lines[1:]:
                    if b":" in line:
                        key, value = line.split(b":", 1)
                        response_headers[key.strip().lower()] = value.strip().lower()
                if b"content-length" in response_headers:
                    size = int(response_headers[b"content-length"])
                    if not 0 <= size <= 2 * 1048576:
                        raise ValueError("压测响应超过2MiB")
                    content = await self.reader.readexactly(size)
                elif response_headers.get(b"transfer-encoding") == b"chunked":
                    content = bytearray()
                    while True:
                        line = await self.reader.readuntil(b"\r\n")
                        size = int(line.split(b";", 1)[0], 16)
                        if size == 0:
                            while await self.reader.readuntil(b"\r\n") != b"\r\n":
                                pass
                            break
                        if len(content) + size > 2 * 1048576:
                            raise ValueError("压测响应超过2MiB")
                        content.extend(await self.reader.readexactly(size))
                        if await self.reader.readexactly(2) != b"\r\n":
                            raise ValueError("非法chunk边界")
                    content = bytes(content)
                else:
                    raise ValueError("压测响应需要明确长度或chunked编码")
                if response_headers.get(b"connection") == b"close":
                    await self.close()
                return SimpleNamespace(status_code=status, content=content)
        except (
            OSError,
            ValueError,
            TimeoutError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ) as exc:
            await self.close()
            # 不输出请求头、密钥和返回正文，也不自动重试不确定的POST。
            raise httpx.TransportError(type(exc).__name__) from exc
