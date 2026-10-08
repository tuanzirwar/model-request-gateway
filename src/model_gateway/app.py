"""实时模型网关：失败关闭，流式输出后不重试，不恢复取消的生成。"""

import asyncio
import hashlib
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
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from .admission import AdmissionMiddleware
from .batching import BatchQueue
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
        headers_sent = False
        completed = False

        async def tracked_send(message):
            nonlocal headers_sent, completed
            await send(message)
            if message["type"] == "http.response.start":
                headers_sent = True
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                completed = True

        response_task = asyncio.create_task(super().__call__(scope, receive, tracked_send))
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
                reason = self.execution.stop_reason if lost in done else "deadline_exceeded"
                self.execution.terminal_override = (
                    "cancelled" if reason == "client_disconnected" else "failed",
                    reason,
                )
                response_task.cancel()
                await asyncio.gather(response_task, return_exceptions=True)
                if headers_sent and not completed and reason != "client_disconnected":
                    # 取消生成后仍完成 HTTP 帧边界；慢/断开的消费者只给极短关闭预算。
                    try:
                        body = self.execution.account_stream_frame(
                            json.dumps({"error": {"code": reason}})
                        )
                    except StreamError:
                        body = b""
                    try:
                        await asyncio.wait_for(
                            send({"type": "http.response.body", "body": body, "more_body": False}),
                            0.25,
                        )
                    except (TimeoutError, OSError):
                        pass
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
        self.redis = Redis.from_url(
            settings.redis_url,
            socket_timeout=2,
            socket_connect_timeout=2,
            max_connections=settings.redis_connections,
        )
        self.quota = Quota(
            self.redis,
            settings.namespace,
            settings.lease_seconds,
            self.metrics,
            batch_size=settings.redis_batch_size,
        )
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.idle_seconds, connect=5),
            limits=httpx.Limits(
                max_connections=settings.upstream_connections,
                max_keepalive_connections=settings.upstream_connections,
                keepalive_expiry=settings.upstream_keepalive_seconds,
            ),
            trust_env=False,
        )
        self.cleanups = set()
        self.coordination_lock = asyncio.Lock()
        self.coordination_registered = False
        self.coordination_initial = False
        self.groups = {model.capacity_key: model.concurrency for model in settings.models.values()}
        self.policy_hash = hashlib.sha256(
            json.dumps(
                {
                    "groups": self.groups,
                    "routes": sorted(
                        (m.alias, m.capacity_key, m.upstream_model, m.endpoint)
                        for m in settings.models.values()
                    ),
                    "lease": settings.lease_seconds,
                    "total": settings.total_seconds,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        self.db_batches = {}
        for name in ("authenticate", "create_record", "update_record"):
            function = getattr(self.database, "batch_" + name)

            async def consume(messages, function=function):
                return await self.db_direct(function, messages)

            self.db_batches[name] = BatchQueue(
                consume,
                size=settings.db_batch_size,
                delay=settings.db_batch_seconds,
                capacity=settings.metadata_queue_size,
                timeout=settings.db_queue_seconds,
            )

    async def db_call(self, function, *args, **kwargs):
        name = function.__name__
        if getattr(function, "__self__", None) is self.database and name in self.db_batches:
            if self.settings.db_batch_size > 1:
                return await self.db_batches[name].submit((args, kwargs))
        return await self.db_direct(function, *args, **kwargs)

    async def ensure_coordination(self, *, refresh=False):
        async with self.coordination_lock:
            if (
                self.coordination_registered
                and self.groups.keys() <= self.quota.epochs.keys()
                and not refresh
            ):
                return
            if not self.coordination_registered:
                try:
                    self.coordination_initial = await self.db_direct(
                        self.database.register_coordination,
                        self.settings.namespace,
                        self.policy_hash,
                    )
                except ValueError as exc:
                    raise RedisError("coordination_policy_mismatch") from exc
                self.coordination_registered = True
            recovery_seconds = 0 if self.coordination_initial else self.settings.total_seconds + 5
            # 仅真正的新命名空间可以立即建立初始状态；之后丢失状态必须隔离等待。
            self.coordination_initial = False
            await self.quota.configure(self.groups, self.policy_hash, recovery_seconds)

    async def db_direct(self, function, *args, **kwargs):
        queued = time.monotonic()
        try:
            await asyncio.wait_for(self.db_slots.acquire(), self.settings.db_queue_seconds)
        except TimeoutError as exc:
            raise HTTPException(503, "database_busy", headers={"Retry-After": "1"}) from exc

        def execute():
            started = time.monotonic()
            self.metrics.stage.labels("db_" + function.__name__, "queue").observe(started - queued)
            try:
                for attempt in range(3):
                    try:
                        return function(*args, **kwargs)
                    except OperationalError as exc:
                        # 仅重试MySQL已明确回滚的死锁；连接断开/提交不确定不得重放。
                        code = getattr(exc.orig, "args", (None,))[0]
                        if code != 1213 or attempt == 2:
                            raise
                        self.metrics.db_retries.inc()
                        time.sleep((0.01 * (2**attempt)) + secrets.randbelow(10) / 1000)
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
                await self.ensure_coordination(refresh=True)
                await self.db_call(self.database.reconcile)
            except (SQLAlchemyError, HTTPException, RedisError):
                LOG.warning("请求对账失败")
            await asyncio.sleep(2)

    async def close(self):
        if self.cleanups:
            await asyncio.gather(*self.cleanups, return_exceptions=True)
        for queue in self.db_batches.values():
            await queue.close()
        await self.client.aclose()
        await self.quota.close()
        await self.redis.aclose()
        if self.db_pending:
            await asyncio.gather(*self.db_pending, return_exceptions=True)
        self.db_executor.shutdown(wait=True)
        self.database.engine.dispose()
        self.database.auth_engine.dispose()


class Execution:
    def __init__(self, runtime, app, model):
        self.runtime, self.app, self.model = runtime, app, model
        self.id = str(uuid.uuid4())
        self.started = time.monotonic()
        self.started_at = time.time()
        self.deadline_at = self.started_at + runtime.settings.total_seconds
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

    def account_stream_frame(self, data):
        encoded = encode(data)
        if self.bytes_out + len(encoded) > self.runtime.settings.max_stream_bytes:
            raise StreamError("response_too_large")
        self.bytes_out += len(encoded)
        return encoded

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
                renewed = await self.runtime.quota.renew(
                    self.app["id"], self.model.capacity_key, self.id
                )
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
                    await self.runtime.quota.release(
                        self.app["id"], self.model.capacity_key, self.id
                    )
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
            except (SQLAlchemyError, HTTPException, RedisError):
                LOG.warning("终态持久化失败 request=%s", self.id)
                if status == "succeeded":
                    # 非流式响应还未发出，提交失败不能伪装成功；流式响应已发送的
                    # 字节无法收回，失败记录交由对账收敛。
                    raise
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
    app.add_middleware(AdmissionMiddleware, settings=settings)

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
        if not auth.startswith("Bearer ") or not 1 <= len(auth[7:]) <= 512:
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
            await runtime.ensure_coordination()
            if not await runtime.quota.ready():
                return JSONResponse({"ready": False}, status_code=503)
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
        record_created = False
        try:
            await runtime.ensure_coordination()
            execution.leased = await runtime.quota.acquire(
                subject["id"],
                model.capacity_key,
                execution.id,
                model.concurrency,
                subject["concurrency"],
            )
            if execution.leased:
                runtime.metrics.active.inc()
                execution.heartbeat = asyncio.create_task(execution.renew_loop())
            await runtime.db_call(
                runtime.database.create_record,
                execution.id,
                subject["id"],
                model.alias,
                config.total_seconds,
                status="running" if execution.leased else "accepted",
                started_at=execution.started_at,
                deadline_at=execution.deadline_at,
            )
            record_created = True
            if not execution.leased:
                await execution.cleanup("rejected", "concurrency_limit")
                return JSONResponse(
                    {"error": {"code": "concurrency_limit"}},
                    status_code=429,
                    headers={"Retry-After": "1", "X-Request-ID": execution.id},
                )
            execution.disconnect_task = asyncio.create_task(execution.monitor_disconnect(request))
            if execution.lost.is_set():
                raise StreamError("lease_lost")
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
            if isinstance(exc, httpx.HTTPError):
                LOG.warning("上游传输异常 type=%s request=%s", type(exc).__name__, execution.id)
            if isinstance(exc, RedisError) and not record_created:
                # Redis接入失败也保存审计，但不重放提交结果不确定的数据库写入。
                try:
                    await runtime.db_call(
                        runtime.database.create_record,
                        execution.id,
                        subject["id"],
                        model.alias,
                        config.total_seconds,
                    )
                except (SQLAlchemyError, HTTPException, RedisError):
                    LOG.warning("接入失败记录持久化失败 request=%s", execution.id)
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
                    encoded = execution.account_stream_frame(data)
                    if value is None:
                        status, error = "succeeded", ""
                        # 提交终态后才发送成功结束标记，正文仍逐帧实时转发。
                        await execution.cleanup(status, error)
                        yield encoded
                        return
                    execution.usage = usage_fields(value.get("usage")) or execution.usage
                    yield encoded
                    data = await execution.wait(anext(iterator), config.idle_seconds)
            except asyncio.CancelledError:
                raise
            except (
                StreamError,
                httpx.HTTPError,
                SQLAlchemyError,
                HTTPException,
                RedisError,
            ) as exc:
                status = "failed"
                error = (
                    exc.code
                    if isinstance(exc, StreamError)
                    else "metadata_unavailable"
                    if isinstance(exc, (SQLAlchemyError, HTTPException, RedisError))
                    else "upstream_connection_error"
                )
                # 已发响应头后，用SSE错误结束；不发送伪造成功的[DONE]。
                try:
                    encoded_error = execution.account_stream_frame(
                        json.dumps({"error": {"code": error, "message": error}})
                    )
                except StreamError:
                    # 错误控制帧也计入预算；剩余容量不足时直接结束连接。
                    return
                yield encoded_error
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
        if isinstance(value.get(key), int)
        and not isinstance(value[key], bool)
        and 0 <= value[key] <= 9223372036854775807
    }
