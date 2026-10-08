"""使用完整ASGI应用验证权限、筛选、统计及监控凭证隔离。"""

import time

import httpx
import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from model_gateway.app import create_app
from model_gateway.config import Model, Settings
from model_gateway.db import Application, Base, RequestRecord, key_hash

KEY = "app-key-" + "a" * 32
MONITOR = "monitor-" + "b" * 32


@pytest.fixture
async def client(tmp_path):
    settings = Settings(
        f"sqlite:///{tmp_path / 'api.db'}",
        "redis://127.0.0.1:1/0",
        "api-test",
        {"coding": Model("coding", "upstream", "http://127.0.0.1:1", "")},
        metrics_key=MONITOR,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        db = app.state.runtime.database
        Base.metadata.create_all(db.engine)
        now = time.time()
        with Session(db.engine) as session, session.begin():
            session.add_all(
                [
                    Application(id="a", key_hash=key_hash(KEY), model_allowlist=["coding"]),
                    Application(id="b", key_hash=key_hash("foreign"), model_allowlist=["coding"]),
                ]
            )
            session.flush()
            session.add_all(
                [
                    RequestRecord(
                        id="1",
                        app_id="a",
                        model="coding",
                        status="succeeded",
                        started_at=now,
                        deadline_at=now + 10,
                        finished_at=now + 1,
                        first_ms=10,
                        bytes_out=100,
                        usage={"total_tokens": 12},
                    ),
                    RequestRecord(
                        id="2",
                        app_id="a",
                        model="coding",
                        status="failed",
                        started_at=now,
                        deadline_at=now + 10,
                        finished_at=now + 2,
                    ),
                    RequestRecord(
                        id="3",
                        app_id="b",
                        model="coding",
                        status="succeeded",
                        started_at=now,
                        deadline_at=now + 10,
                        finished_at=now + 1,
                        usage={"total_tokens": 999},
                    ),
                    RequestRecord(
                        id="old",
                        app_id="a",
                        model="coding",
                        status="succeeded",
                        started_at=now - 90000,
                        deadline_at=now - 89900,
                        finished_at=now - 89990,
                    ),
                ]
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c


async def test_statistics_and_filtered_cursor_are_app_scoped(client):
    headers = {"Authorization": "Bearer " + KEY}
    response = await client.get("/stats", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["total_requests"] == 2
    assert sum(row["observed_total_tokens"] for row in data["groups"]) == 12
    page = (await client.get("/requests?status=succeeded&limit=1", headers=headers)).json()
    assert page["items"][0]["id"] == "1" and page["next_cursor"] == "1"
    next_page = (
        await client.get("/requests?status=succeeded&limit=1&before=1", headers=headers)
    ).json()
    assert next_page["items"][0]["id"] == "old"
    assert (
        await client.get("/requests?status=succeeded&before=2", headers=headers)
    ).status_code == 400
    assert (await client.get("/requests?before=3", headers=headers)).status_code == 400
    assert (await client.get("/requests/3", headers=headers)).status_code == 404
    assert (await client.get("/stats")).status_code == 401


@pytest.mark.parametrize("stream", [False, True])
async def test_commit_failure_does_not_return_success(client, monkeypatch, stream):
    runtime = client._transport.app.state.runtime

    async def leased(*args):
        return True

    def failed_commit(messages):
        raise OperationalError("", {}, Exception("uncertain commit"))

    monkeypatch.setattr(runtime.quota, "acquire", leased)
    monkeypatch.setattr(runtime.quota, "release", leased)
    # 此测试隔离终态 COMMIT 故障；真实协调状态另由 Redis 集成用例覆盖。
    monkeypatch.setattr(runtime, "ensure_coordination", leased)

    # 队列消费者在初始化时捕获方法，需要替换该批的消费函数。
    async def consume(messages):
        return await runtime.db_direct(failed_commit, messages)

    runtime.db_batches["update_record"].consume = consume
    await runtime.client.aclose()

    def upstream(request):
        if stream:
            return httpx.Response(
                200,
                content='data: {"choices":[]}\n\ndata: [DONE]\n\n',
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(200, json={"choices": [], "usage": {"total_tokens": 1}})

    runtime.client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    response = await client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Bearer " + KEY},
        json={"model": "coding", "messages": [{"role": "user", "content": "x"}], "stream": stream},
    )
    if stream:
        assert response.status_code == 200
        assert "metadata_unavailable" in response.text and "[DONE]" not in response.text
    else:
        assert response.status_code == 503
    assert "uncertain commit" not in response.text
    row = runtime.database.get_record("a", response.headers["X-Request-ID"])
    assert row["status"] == "running"


@pytest.mark.parametrize(
    "query",
    [
        "/stats?hours=169",
        "/requests?status=unknown",
        "/requests?since=nan",
        "/requests?since=-1",
        "/requests?limit=0",
    ],
)
async def test_invalid_query_is_rejected(client, query):
    assert (await client.get(query, headers={"Authorization": "Bearer " + KEY})).status_code == 400


async def test_monitor_key_not_application_key_and_assets_are_bounded(client):
    assert (
        await client.get("/metrics", headers={"Authorization": "Bearer " + KEY})
    ).status_code == 401
    response = await client.get("/metrics", headers={"Authorization": "Bearer " + MONITOR})
    assert response.status_code == 200 and "gateway_active_requests" in response.text
    assert (
        await client.get("/stats", headers={"Authorization": "Bearer " + MONITOR})
    ).status_code == 401
    page = await client.get("/")
    assert (
        page.status_code == 200 and "unsafe-inline" not in page.headers["content-security-policy"]
    )
    assert KEY not in page.text and MONITOR not in page.text
    assert (await client.get("/static/console.js")).status_code == 200
    assert (await client.get("/static/config.py")).status_code == 404
    assert (await client.get("/live")).status_code == 200


async def test_oversized_api_key_rejected_before_database(client, monkeypatch):
    runtime = client._transport.app.state.runtime

    def forbidden(key):
        raise AssertionError("超长密钥不应进入数据库")

    monkeypatch.setattr(runtime.database, "authenticate", forbidden)
    response = await client.get("/v1/models", headers={"Authorization": "Bearer " + "x" * 513})
    assert response.status_code == 401
