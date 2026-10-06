"""有界SSE解析，拒绝残缺结束及超大帧。"""

import json


class StreamError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


async def frames(response, max_bytes):
    pending = bytearray()
    async for chunk in response.aiter_bytes():
        pending.extend(chunk)
        while True:
            # 兼容CRLF；截断仍按原始字节上限检查。
            lf = pending.find(b"\n\n")
            crlf = pending.find(b"\r\n\r\n")
            positions = [(pos, width) for pos, width in ((lf, 2), (crlf, 4)) if pos >= 0]
            if not positions:
                if len(pending) > max_bytes:
                    raise StreamError("frame_too_large")
                break
            end, width = min(positions)
            if end > max_bytes:
                raise StreamError("frame_too_large")
            raw = bytes(pending[:end])
            del pending[: end + width]
            try:
                lines = raw.decode("utf-8").splitlines()
            except UnicodeDecodeError as exc:
                raise StreamError("invalid_utf8") from exc
            data = "\n".join(line[5:].lstrip(" ") for line in lines if line.startswith("data:"))
            if data:
                yield data
    raise StreamError("upstream_incomplete")


def parse(data):
    if data == "[DONE]":
        return None
    try:
        value = json.loads(data)
    except json.JSONDecodeError as exc:
        raise StreamError("invalid_sse_json") from exc
    if (
        not isinstance(value, dict)
        or value.get("error")
        or not isinstance(value.get("choices"), list)
    ):
        raise StreamError("upstream_protocol_error")
    return value


def encode(data):
    return ("data: " + data.replace("\n", "\ndata: ") + "\n\n").encode()
