"""在鉴权和读取请求体之前限制本进程在途任务，过载时立即拒绝。"""

from fastapi.responses import JSONResponse


class AdmissionMiddleware:
    def __init__(self, app, settings):
        self.app = app
        self.settings = settings
        self.active = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not (
            scope["path"].startswith("/v1/")
            or scope["path"].startswith("/requests")
            or scope["path"] in {"/stats", "/health"}
        ):
            return await self.app(scope, receive, send)
        # 无await的检查和增加在本事件循环中连续执行；不创建无界等待队列。
        settings = self.settings or scope["app"].state.runtime.settings
        if self.active >= settings.max_inflight_requests:
            response = JSONResponse(
                {"error": {"code": "gateway_busy"}}, status_code=503, headers={"Retry-After": "1"}
            )
            return await response(scope, receive, send)
        self.active += 1
        try:
            await self.app(scope, receive, send)
        finally:
            # 包含整个SSE消费过程，取消路径也归还容量。
            self.active -= 1
