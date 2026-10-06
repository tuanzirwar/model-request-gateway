"""Redis接入租约，不等同于上游GPU实际计算配额。"""

import hashlib
import time

ACQUIRE = """
local now = redis.call('TIME')
local t = tonumber(now[1]) * 1000 + tonumber(now[2]) / 1000
for _,key in ipairs(KEYS) do redis.call('ZREMRANGEBYSCORE',key,'-inf',t) end
if redis.call('ZCARD',KEYS[1]) >= tonumber(ARGV[2]) or
 redis.call('ZCARD',KEYS[2]) >= tonumber(ARGV[3]) then return 0 end
for _,key in ipairs(KEYS) do
 redis.call('ZADD',key,t+tonumber(ARGV[4]),ARGV[1])
 redis.call('PEXPIRE',key,tonumber(ARGV[4])*2)
end
return 1
"""
RENEW = """
local now = redis.call('TIME')
local t = tonumber(now[1]) * 1000 + tonumber(now[2]) / 1000
for _,key in ipairs(KEYS) do
 local score = redis.call('ZSCORE',key,ARGV[1])
 if not score or tonumber(score) <= t then return 0 end
end
for _,key in ipairs(KEYS) do
 redis.call('ZADD',key,t+tonumber(ARGV[2]),ARGV[1])
 redis.call('PEXPIRE',key,tonumber(ARGV[2])*2)
end
return 1
"""
RELEASE = """
local removed=0
for _,key in ipairs(KEYS) do removed=removed+redis.call('ZREM',key,ARGV[1]) end
return removed
"""


class Quota:
    def __init__(self, redis, namespace, lease_seconds, metrics=None):
        self.redis = redis
        self.namespace = namespace
        self.lease_ms = int(lease_seconds * 1000)
        self.metrics = metrics
        self.acquire_script = redis.register_script(ACQUIRE)
        self.renew_script = redis.register_script(RENEW)
        self.release_script = redis.register_script(RELEASE)

    def keys(self, app_id, model):
        # 两个键使用同一hash tag；当前只验证单Redis，未宣称集群故障一致性。
        tag = hashlib.sha256(model.encode()).hexdigest()[:24]
        return [f"{self.namespace}:{{{tag}}}:model", f"{self.namespace}:{{{tag}}}:app:{app_id}"]

    async def acquire(self, app_id, model, request_id, model_limit, app_limit):
        return bool(
            await self.call(
                "acquire",
                self.acquire_script,
                keys=self.keys(app_id, model),
                args=[request_id, model_limit, app_limit, self.lease_ms],
            )
        )

    async def renew(self, app_id, model, request_id):
        return bool(
            await self.call(
                "renew",
                self.renew_script,
                keys=self.keys(app_id, model),
                args=[request_id, self.lease_ms],
            )
        )

    async def release(self, app_id, model, request_id):
        return await self.call(
            "release", self.release_script, keys=self.keys(app_id, model), args=[request_id]
        )

    async def call(self, operation, script, **kwargs):
        started = time.monotonic()
        try:
            return await script(**kwargs)
        finally:
            if self.metrics:
                self.metrics.stage.labels("redis_" + operation, "execute").observe(
                    time.monotonic() - started
                )
