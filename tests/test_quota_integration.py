"""显式环境变量开启的真实Redis并发与过期测试。"""

import asyncio
import os
import uuid

import pytest
from redis.asyncio import Redis

from model_gateway.quota import Quota

pytestmark = pytest.mark.skipif(not os.environ.get("GATEWAY_TEST_REDIS"), reason="需要真实Redis")


async def test_atomic_capacity_across_two_clients_and_idempotent_release():
    namespace = "quota-test-" + uuid.uuid4().hex
    clients = [Redis.from_url(os.environ["GATEWAY_TEST_REDIS"]) for _ in range(2)]
    quotas = [Quota(client, namespace, 2) for client in clients]
    try:
        results = await asyncio.gather(
            *[quotas[index % 2].acquire("a", "m", str(index), 2, 2) for index in range(20)]
        )
        assert sum(results) == 2
        first = str(results.index(True))
        assert await quotas[0].release("a", "m", first) == 2
        assert await quotas[1].release("a", "m", first) == 0
        assert await quotas[1].acquire("a", "m", "next", 2, 2)
    finally:
        await clients[0].delete(*quotas[0].keys("a", "m"))
        await asyncio.gather(*[client.aclose() for client in clients])


async def test_expired_lease_cannot_be_renewed_or_release_new_request():
    client = Redis.from_url(os.environ["GATEWAY_TEST_REDIS"])
    quota = Quota(client, "quota-test-" + uuid.uuid4().hex, 0.1)
    try:
        assert await quota.acquire("a", "m", "old", 1, 1)
        await asyncio.sleep(0.15)
        assert not await quota.renew("a", "m", "old")
        assert await quota.acquire("a", "m", "new", 1, 1)
        await quota.release("a", "m", "old")
        assert not await quota.acquire("a", "m", "extra", 1, 1)
    finally:
        await client.delete(*quota.keys("a", "m"))
        await client.aclose()


async def test_per_application_limit_and_shared_model_limit():
    client = Redis.from_url(os.environ["GATEWAY_TEST_REDIS"])
    quota = Quota(client, "quota-test-" + uuid.uuid4().hex, 2)
    try:
        assert await quota.acquire("a", "m", "a1", 2, 1)
        assert not await quota.acquire("a", "m", "a2", 2, 1)
        assert await quota.acquire("b", "m", "b1", 2, 1)
        assert not await quota.acquire("c", "m", "c1", 2, 1)
    finally:
        await client.delete(*quota.keys("a", "m"), *quota.keys("b", "m"), *quota.keys("c", "m"))
        await client.aclose()


async def test_pipeline_keeps_global_capacity_and_recovers_only_missing_script():
    clients = [Redis.from_url(os.environ["GATEWAY_TEST_REDIS"]) for _ in range(2)]
    namespace = "pipeline-test-" + uuid.uuid4().hex
    quotas = [Quota(client, namespace, 2, batch_size=64) for client in clients]
    try:
        results = await asyncio.gather(
            *[quotas[index % 2].acquire("a", "m", str(index), 2, 2) for index in range(32)]
        )
        assert sum(results) == 2
        first = str(results.index(True))
        # 局部SHA模拟脚本缓存丢失，不对共享Redis执行SCRIPT FLUSH。
        quotas[0].acquire_script.sha = "0" * 40
        released, acquired = await asyncio.gather(
            quotas[0].release("a", "m", first),
            quotas[0].acquire("a", "m", "next", 2, 2),
        )
        assert released == 2  # 整批重放会返回0，因此也检验成功项未被重放。
        assert acquired
        assert not await quotas[1].acquire("a", "m", "extra", 2, 2)
    finally:
        await asyncio.gather(*[quota.close() for quota in quotas])
        await clients[0].delete(*quotas[0].keys("a", "m"))
        await asyncio.gather(*[client.aclose() for client in clients])
