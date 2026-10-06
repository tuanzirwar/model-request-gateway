"""实时模型网关：失败关闭，流式输出后不重试，不恢复取消的生成。"""

import asyncio
import json
import logging
import math
import secrets
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from .config import load_settings
from .db import Database
from .observability import Metrics
from .quota import Quota
from .sse import StreamError, encode, frames, parse
from .validation import forward_payload, validate_payload

LOG = logging.getLogger("model_gateway")


def validate_filters(status, model, since):
    if status is not None and status not in {
        "accepted",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "rejected",
        "abandoned",
    }:
        raise HTTPException(400, "invalid_status")
    if model is not None and not 1 <= len(model) <= 100:
        raise HTTPException(400, "invalid_model")
    if since is not None and (not math.isfinite(since) or since < 0):
        raise HTTPException(400, "invalid_since")


class DeadlineResponse(StreamingResponse):
    def __init__(self, *args, execution, **kwargs):
        self.execution = execution
        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send):
        response_task = asyncio.create_task(super().__call__(scope, receive, send))
        lost = asyncio.create_task(self.execution.lost.wait())
        try:
            # 包括ASGI send等待；慢消费者不能无限持有接入租约。
            done, _ = await asyncio.wait(
                (response_task, lost),
                timeout=max(0, self.execution.deadline - time.monotonic()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if response_task in done:
                await response_task
            else:
                self.execution.terminal_override = (
                    "failed",
                    "lease_lost" if lost in done else "deadline_exceeded",
                )
                response_task.cancel()
                await asyncio.gather(response_task, return_exceptions=True)
        finally:
            for task in (response_task, lost):
                if not task.done():
                    task.cancel()
            await asyncio.gather(response_task, lost, return_exceptions=True)
            await self.execution.cleanup("cancelled", "client_disconnected")


class Runtime:
    def __init__(self, settings):
        self.settings = settings
        self.database = Database(settings.database_url, pool_size=settings.db_workers)
        self.db_executor = ThreadPoolExecutor(
            max_workers=settings.db_workers, thread_name_prefix="gateway-db"
        )
        self.db_slots = asyncio.Semaphore(settings.db_workers + settings.db_queue_size)
        self.db_pending = set()
        self.metrics = Metrics()
        self.redis = Redis.from_url(settings.redis_url, socket_timeout=2, socket_connect_timeout=2)
        self.quota = Quota(self.redis, settings.namespace, settings.lease_seconds, self.metrics)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.idle_seconds, connect=5),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            trust_env=False,
        )
        self.cleanups = set()

    async def db_call(self, function, *args, **kwargs):
        queued = time.monotonic()
        try:
            await asyncio.wait_for(self.db_slots.acquire(), self.settings.db_queue_seconds)
        except TimeoutError as exc:
            raise HTTPException(503, "database_busy", headers={"Retry-After": "1"}) from exc

        def execute():
            started = time.monotonic()
            self.metrics.stage.labels("db_" + function.__name__, "queue").observe(started - queued)
            try:
                return function(*args, **kwargs)
            finally:
                self.metrics.stage.labels("db_" + function.__name__, "execute").observe(
                    time.monotonic() - started
                )

        future = asyncio.get_running_loop().run_in_executor(self.db_executor, execute)
        self.db_pending.add(future)

        def settled(done):
            self.db_slots.release()
            self.db_pending.discard(done)
            # 调用方已取消时仍获取异常，避免后台Future异常无人消费。
            if not done.cancelled():
                done.exception()

        future.add_done_callback(settled)
        # 取消客户端不等于取消已运行的同步SQL；容量必须到真实执行结束后才归还。
        return await asyncio.shield(future)

    async def measure_lag(self):
        while True:
            deadline = time.monotonic() + 0.25
            await asyncio.sleep(0.25)
            self.metrics.lag.set(max(0, time.monotonic() - deadline))

    async def maintenance(self):
        while True:
            try:
                await self.db_call(self.database.reconcile)
            except (SQLAlchemyError, HTTPException):
                LOG.warning("请求对账失败")
            await asyncio.sleep(2)

    async def close(self):
        if self.cleanups:
            await asyncio.gather(*self.cleanups, return_exceptions=True)
        await self.client.aclose()
        await self.redis.aclose()
        if self.db_pending:
            await asyncio.gather(*self.db_pending, return_exceptions=True)
        self.db_executor.shutdown(wait=True)
        self.database.engine.dispose()


class Execution:
    def __init__(self, runtime, app, model):
        self.runtime, self.app, self.model = runtime, app, model
        self.id = str(uuid.uuid4())
        self.started = time.monotonic()
        self.deadline = self.started + runtime.settings.total_seconds
        self.response = None
        self.leased = False
        self.heartbeat = None
        self.lost = asyncio.Event()
        self.bytes_out = 0
        self.first_ms = None
        self.usage = {}
        self.finalized = False
        self.disconnect_task = None
        self.stop_reason = "lease_lost"
        self.terminal_override = None
        self.io_reapers = set()

    async def monitor_disconnect(self, request):
        while True:
            message = await request.receive()
            if message["type"] == "http.disconnect":
                self.stop_reason = "client_disconnected"
                self.lost.set()
                return

    async def renew_loop(self):
        while True:
            await asyncio.sleep(self.runtime.settings.lease_seconds / 3)
            try:
                renewed = await self.runtime.quota.renew(self.app["id"], self.model.alias, self.id)
            except RedisError:
                renewed = False
            if not renewed:
                self.lost.set()
                return

    async def wait(self, awaitable, timeout, operation=None):
        started = time.monotonic()
        task = asyncio.ensure_future(awaitable)
        lost = asyncio.create_task(self.lost.wait())
        try:
            done, _ = await asyncio.wait(
                (task, lost),
                timeout=max(0, min(timeout, self.deadline - time.monotonic())),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if lost in done:
                raise StreamError(self.stop_reason)
            if task not in done:
                raise StreamError("deadline_exceeded")
            return task.result()
        finally:
            if operation:
                self.runtime.metrics.stage.labels(operation, "execute").observe(
                    time.monotonic() - started
                )
            for item in (task, lost):
                if not item.done():
                    item.cancel()

            async def settle():
                await asyncio.gather(task, lost, return_exceptions=True)

            # ASGI取消作用域可能反复取消await；不要让gather将第二次取消传播到
            # HTTPX迭代器正在执行的aclose，否则会留下已标记关闭但未释放的连接。
            reaper = asyncio.create_task(settle())
            self.io_reapers.add(reaper)
            reaper.add_done_callback(self.io_reapers.discard)
            await asyncio.shield(reaper)

    async def cleanup(self, status, error=""):
        if self.finalized:
            return
        self.finalized = True
        if self.terminal_override:
            status, error = self.terminal_override

        # 独立清理任务避免ASGI取消作用域再次取消额度释放；记录失败由对账补终态。
        async def work():
            if self.io_reapers:
                await asyncio.gather(*self.io_reapers, return_exceptions=True)
            if self.disconnect_task:
                self.disconnect_task.cancel()
                await asyncio.gather(self.disconnect_task, return_exceptions=True)
            if self.heartbeat:
                self.heartbeat.cancel()
                await asyncio.gather(self.heartbeat, return_exceptions=True)
            if self.response:
                try:
                    await asyncio.wait_for(self.response.aclose(), 3)
                except Exception:
                    LOG.warning("上游响应关闭失败 request=%s", self.id)
            if self.leased:
                try:
                    await self.runtime.quota.release(self.app["id"], self.model.alias, self.id)
                except RedisError:
                    LOG.warning("租约释放失败，等待过期 request=%s", self.id)
                self.runtime.metrics.active.dec()
            try:
                await self.runtime.db_call(
                    self.runtime.database.update_record,
                    self.id,
                    status=status,
                    error=error,
                    finished_at=time.time(),
                    first_ms=self.first_ms,
                    bytes_out=self.bytes_out,
                    usage=self.usage,
                )
            except (SQLAlchemyError, HTTPException):
                LOG.warning("终态持久化失败 request=%s", self.id)
            self.runtime.metrics.terminal.labels(self.model.alias, status).inc()
            self.runtime.metrics.duration.labels(self.model.alias, status).observe(
                time.monotonic() - self.started
            )

        task = asyncio.create_task(work())
        self.runtime.cleanups.add(task)
        task.add_done_callback(self.runtime.cleanups.discard)
        await asyncio.shield(task)


def create_app(settings=None):
    @asynccontextmanager
    async def lifespan(app):
        runtime = Runtime(settings or load_settings())
        app.state.runtime = runtime
        task = asyncio.create_task(runtime.maintenance())
        lag_task = asyncio.create_task(runtime.measure_lag())
        try:
            yield
        finally:
            task.cancel()
            lag_task.cancel()
            await asyncio.gather(task, lag_task, return_exceptions=True)
            await runtime.close()

    app = FastAPI(title="模型请求网关", lifespan=lifespan)

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        # 不将SQL、连接口令或驱动错误正文暴露给客户端。
        LOG.warning("数据库操作失败 type=%s", type(exc).__name__)
        return JSONResponse({"error": {"code": "database_unavailable"}}, status_code=503)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def console():
        return HTMLResponse(
            (Path(__file__).parent / "static/index.html").read_text("utf-8"),
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "default-src 'self'; script-src 'self'; "
                "style-src 'self'; frame-ancestors 'none'; base-uri 'none'",
            },
        )

    @app.get("/static/{filename}", include_in_schema=False)
    async def asset(filename: str):
        if filename not in {"console.js", "console.css"}:
            raise HTTPException(404)
        return FileResponse(Path(__file__).parent / "static" / filename)

    @app.get("/live", include_in_schema=False)
    async def live():
        return {"alive": True}

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request):
        runtime = request.app.state.runtime
        key = runtime.settings.metrics_key
        if not key:
            raise HTTPException(404, "metrics_disabled")
        if not secrets.compare_digest(request.headers.get("authorization", ""), "Bearer " + key):
            raise HTTPException(401, "invalid_monitor_key")
        return Response(runtime.metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    async def identity(request: Request):
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(401, "invalid_api_key")
        try:
            subject = await request.app.state.runtime.db_call(
                request.app.state.runtime.database.authenticate, auth[7:]
            )
        except SQLAlchemyError as exc:
            raise HTTPException(503, "database_unavailable") from exc
        if subject is None:
            raise HTTPException(401, "invalid_api_key")
        return subject

    @app.get("/health")
    async def health(request: Request):
        runtime = request.app.state.runtime
        try:
            await runtime.redis.ping()
            from sqlalchemy import text

            def db_ping():
                with runtime.database.engine.connect() as connection:
                    connection.execute(text("SELECT 1"))

            await runtime.db_call(db_ping)
        except (RedisError, SQLAlchemyError, HTTPException):
            return JSONResponse({"ready": False}, status_code=503)
        return {"ready": True}

    @app.get("/v1/models")
    async def models(request: Request, subject=Depends(identity)):
        configured = request.app.state.runtime.settings.models
        return {
            "object": "list",
            "data": [
                {"id": name, "object": "model"} for name in subject["models"] if name in configured
            ],
        }

    @app.get("/requests")
    async def records(
        request: Request,
        before: str | None = None,
        limit: int = 25,
        status: str | None = None,
        model: str | None = None,
        since: float | None = None,
        subject=Depends(identity),
    ):
        if not 1 <= limit <= 100:
            raise HTTPException(400, "invalid_limit")
        validate_filters(status, model, since)
        result = await request.app.state.runtime.db_call(
            request.app.state.runtime.database.list_records,
            subject["id"],
            before,
            limit,
            status=status,
            model=model,
            since=since,
        )
        if result is None:
            raise HTTPException(400, "invalid_cursor")
        return result

    @app.get("/requests/{request_id}")
    async def record(request: Request, request_id: str, subject=Depends(identity)):
        result = await request.app.state.runtime.db_call(
            request.app.state.runtime.database.get_record, subject["id"], request_id
        )
        if result is None:
            raise HTTPException(404, "request_not_found")
        return result

    @app.get("/stats")
    async def stats(request: Request, hours: int = 24, subject=Depends(identity)):
        if not 1 <= hours <= 168:
            raise HTTPException(400, "invalid_window")
        return await request.app.state.runtime.db_call(
            request.app.state.runtime.database.statistics, subject["id"], hours
        )

    @app.post("/v1/chat/completions")
    async def chat(request: Request, subject=Depends(identity)):
        runtime = request.app.state.runtime
        config = runtime.settings
        buffer = bytearray()
        try:
            async with asyncio.timeout(10):
                async for chunk in request.stream():
                    buffer.extend(chunk)
                    if len(buffer) > config.max_body_bytes:
                        raise HTTPException(413, "request_too_large")
        except TimeoutError as exc:
            raise HTTPException(408, "request_body_timeout") from exc
        try:
            payload = json.loads(buffer)
        except (ValueError, UnicodeDecodeError) as exc:
            raise HTTPException(400, "invalid_json") from exc
        validate_payload(payload)
        model = config.models.get(payload["model"])
        if model is None or model.alias not in subject["models"]:
            raise HTTPException(403, "model_not_allowed")
        execution = Execution(runtime, subject, model)
        status, error = "failed", "internal_error"
        try:
            await runtime.db_call(
                runtime.database.create_record,
                execution.id,
                subject["id"],
                model.alias,
                config.total_seconds,
            )
            execution.leased = await runtime.quota.acquire(
                subject["id"], model.alias, execution.id, model.concurrency, subject["concurrency"]
            )
            if not execution.leased:
                await execution.cleanup("rejected", "concurrency_limit")
                return JSONResponse(
                    {"error": {"code": "concurrency_limit"}},
                    status_code=429,
                    headers={"Retry-After": "1", "X-Request-ID": execution.id},
                )
            runtime.metrics.active.inc()
            execution.heartbeat = asyncio.create_task(execution.renew_loop())
            execution.disconnect_task = asyncio.create_task(execution.monitor_disconnect(request))
            await runtime.db_call(runtime.database.update_record, execution.id, status="running")
            # 白名单转发字段，不接受调用方提供地址、密钥或任意上游请求头。
            forwarded = forward_payload(payload, model)
            upstream_request = runtime.client.build_request(
                "POST",
                model.endpoint,
                json=forwarded,
                headers={
                    **({"Authorization": f"Bearer {model.key}"} if model.key else {}),
                    "Accept": "text/event-stream" if payload.get("stream") else "application/json",
                    "Accept-Encoding": "identity",
                },
            )
            execution.response = await execution.wait(
                runtime.client.send(upstream_request, stream=True),
                execution.started + config.first_seconds - time.monotonic(),
                "upstream_headers",
            )
            if execution.response.status_code >= 400:
                raise StreamError("upstream_http_error")
            if not payload.get("stream"):
                body = bytearray()

                async def collect():
                    async for chunk in execution.response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > config.max_body_bytes:
                            raise StreamError("response_too_large")

                await execution.wait(collect(), config.total_seconds, "upstream_body")
                try:
                    result = json.loads(body)
                except ValueError as exc:
                    raise StreamError("invalid_response") from exc
                if not isinstance(result, dict) or not isinstance(result.get("choices"), list):
                    raise StreamError("invalid_response")
                execution.usage = usage_fields(result.get("usage"))
                execution.bytes_out = len(body)
                await execution.cleanup("succeeded")
                return JSONResponse(result, headers={"X-Request-ID": execution.id})
            if "text/event-stream" not in execution.response.headers.get("content-type", ""):
                raise StreamError("invalid_content_type")
            iterator = frames(execution.response, config.max_frame_bytes)
            first = await execution.wait(
                anext(iterator), execution.started + config.first_seconds - time.monotonic()
            )
            parse(first)
            execution.first_ms = int((time.monotonic() - execution.started) * 1000)
            # 交付响应后，由StreamingResponse独占ASGI断连消息。
            execution.disconnect_task.cancel()
            await asyncio.gather(execution.disconnect_task, return_exceptions=True)
            execution.disconnect_task = None
        except HTTPException as exc:
            await execution.cleanup("failed", str(exc.detail))
            raise
        except asyncio.CancelledError:
            await execution.cleanup("cancelled", "client_disconnected")
            raise
        except (StreamError, httpx.HTTPError, RedisError, SQLAlchemyError) as exc:
            error = (
                exc.code
                if isinstance(exc, StreamError)
                else "redis_unavailable"
                if isinstance(exc, RedisError)
                else "database_unavailable"
                if isinstance(exc, SQLAlchemyError)
                else "upstream_connection_error"
            )
            if error == "client_disconnected":
                status = "cancelled"
            await execution.cleanup(status, error)
            code = (
                504
                if error == "deadline_exceeded"
                else (503 if isinstance(exc, (RedisError, SQLAlchemyError)) else 502)
            )
            return JSONResponse(
                {"error": {"code": error}}, status_code=code, headers={"X-Request-ID": execution.id}
            )
        except Exception:
            await execution.cleanup(status, error)
            raise

        async def stream():
            status, error = "cancelled", "client_disconnected"
            try:
                data = first
                while True:
                    value = parse(data)
                    if value is None:
                        status, error = "succeeded", ""
                        execution.bytes_out += len(encode(data))
                        yield encode(data)
                        return
                    execution.usage = usage_fields(value.get("usage")) or execution.usage
                    execution.bytes_out += len(encode(data))
                    yield encode(data)
                    data = await execution.wait(anext(iterator), config.idle_seconds)
            except asyncio.CancelledError:
                raise
            except (StreamError, httpx.HTTPError) as exc:
                status = "failed"
                error = exc.code if isinstance(exc, StreamError) else "upstream_connection_error"
                # 已发响应头后，用SSE错误结束；不发送伪造成功的[DONE]。
                yield encode(json.dumps({"error": {"code": error, "message": error}}))
            finally:
                await execution.cleanup(status, error)

        return DeadlineResponse(
            stream(),
            execution=execution,
            media_type="text/event-stream",
            headers={
                "X-Request-ID": execution.id,
                "Cache-Control": "no-store",
                "X-Accel-Buffering": "no",
            },
        )

    return app


def usage_fields(value):
    if not isinstance(value, dict):
        return {}
    return {
        key: value[key]
        for key in ("prompt_tokens", "completion_tokens", "total_tokens")
        if isinstance(value.get(key), int) and not isinstance(value[key], bool) and value[key] >= 0
    }
