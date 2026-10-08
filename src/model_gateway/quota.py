"""Redis接入租约，不等同于上游GPU实际计算配额。"""

import hashlib
import time
import uuid

from redis.exceptions import NoScriptError, RedisError

from .batching import BatchQueue

ACQUIRE = """
local now = redis.call('TIME')
local t = tonumber(now[1]) * 1000 + tonumber(now[2]) / 1000
if KEYS[3] then
 if redis.call('HGET',KEYS[3],'epoch') ~= ARGV[5] or
    redis.call('HGET',KEYS[3],'policy') ~= ARGV[6] then return -1 end
 if tonumber(redis.call('HGET',KEYS[3],'ready_at')) > t then return -2 end
end
for i=1,2 do redis.call('ZREMRANGEBYSCORE',KEYS[i],'-inf',t) end
if redis.call('ZCARD',KEYS[1]) >= tonumber(ARGV[2]) or
 redis.call('ZCARD',KEYS[2]) >= tonumber(ARGV[3]) then return 0 end
for i=1,2 do
 local key=KEYS[i]
 redis.call('ZADD',key,t+tonumber(ARGV[4]),ARGV[1])
 redis.call('PEXPIRE',key,tonumber(ARGV[4])*2)
end
return 1
"""
RENEW = """
local now = redis.call('TIME')
local t = tonumber(now[1]) * 1000 + tonumber(now[2]) / 1000
if KEYS[3] and (redis.call('HGET',KEYS[3],'epoch') ~= ARGV[3] or
 redis.call('HGET',KEYS[3],'policy') ~= ARGV[4]) then return 0 end
for i=1,2 do
 local key=KEYS[i]
 local score = redis.call('ZSCORE',key,ARGV[1])
 if not score or tonumber(score) <= t then return 0 end
end
for i=1,2 do
 local key=KEYS[i]
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

PREPARE = """
local epoch=redis.call('HGET',KEYS[1],'epoch')
if epoch then
 if redis.call('HGET',KEYS[1],'policy') ~= ARGV[1] then return false end
 -- 首次 SQL 注册者可完成初始化；其他进程先建立的保守等待不阻塞首次部署。
 if tonumber(ARGV[3]) == 0 then redis.call('HSET',KEYS[1],'ready_at',0) end
 return epoch
end
local now=redis.call('TIME')
local t=tonumber(now[1])*1000+tonumber(now[2])/1000
redis.call('HSET',KEYS[1],'epoch',ARGV[2],'policy',ARGV[1],
 'ready_at',t+tonumber(ARGV[3]))
return ARGV[2]
"""


class Quota:
    def __init__(self, redis, namespace, lease_seconds, metrics=None, *, batch_size=1):
        self.redis = redis
        self.namespace = namespace
        self.lease_ms = int(lease_seconds * 1000)
        self.metrics = metrics
        self.acquire_script = redis.register_script(ACQUIRE)
        self.renew_script = redis.register_script(RENEW)
        self.release_script = redis.register_script(RELEASE)
        self.prepare_script = redis.register_script(PREPARE)
        self.epochs = {}
        self.owners = {}
        self.policy_hash = ""
        self.loaded = False
        self.batch = BatchQueue(self.pipeline, size=batch_size, delay=0)
        self.batch_size = batch_size

    async def pipeline(self, messages):
        if not self.loaded:
            for script in (self.acquire_script, self.renew_script, self.release_script):
                await self.redis.script_load(script.script)
            self.loaded = True
        async with self.redis.pipeline(transaction=False) as pipe:
            for script, kwargs in messages:
                pipe.evalsha(script.sha, len(kwargs["keys"]), *kwargs["keys"], *kwargs["args"])
            results = await pipe.execute(raise_on_error=False)
        for index, result in enumerate(results):
            if isinstance(result, NoScriptError):
                # SCRIPT FLUSH之后只补执行明确没有执行的NOSCRIPT项，绝不重放整个批次。
                script, kwargs = messages[index]
                try:
                    results[index] = await script(**kwargs)
                except Exception as exc:
                    results[index] = exc
        return results

    async def close(self):
        await self.batch.close()

    def keys(self, app_id, model):
        # 两个键使用同一hash tag；当前只验证单Redis，未宣称集群故障一致性。
        tag = hashlib.sha256(model.encode()).hexdigest()[:24]
        return [f"{self.namespace}:{{{tag}}}:model", f"{self.namespace}:{{{tag}}}:app:{app_id}"]

    def guard_key(self, model):
        return self.keys("", model)[0].removesuffix(":model") + ":coordination"

    async def configure(self, groups, policy_hash, recovery_seconds):
        self.policy_hash = policy_hash
        for group in groups:
            epoch = await self.prepare_script(
                keys=[self.guard_key(group)],
                args=[policy_hash, uuid.uuid4().hex, int(recovery_seconds * 1000)],
            )
            if not epoch:
                raise RedisError("coordination_policy_mismatch")
            self.epochs[group] = epoch.decode() if isinstance(epoch, bytes) else epoch

    async def ready(self):
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.time()
            for group in self.epochs:
                pipe.hmget(self.guard_key(group), "epoch", "policy", "ready_at")
            values = await pipe.execute()
        now = values[0][0] * 1000 + values[0][1] / 1000
        return all(
            state[0] == self.epochs[group].encode()
            and state[1] == self.policy_hash.encode()
            and state[2] is not None
            and float(state[2]) <= now
            for group, state in zip(self.epochs, values[1:], strict=True)
        )

    async def acquire(self, app_id, model, request_id, model_limit, app_limit):
        keys = self.keys(app_id, model)
        args = [request_id, model_limit, app_limit, self.lease_ms]
        epoch = self.epochs.get(model)
        if self.policy_hash:
            keys.append(self.guard_key(model))
            args.extend([epoch or "", self.policy_hash])
        result = await self.call(
            "acquire",
            self.acquire_script,
            keys=keys,
            args=args,
        )
        if result < 0:
            raise RedisError("coordination_recovering" if result == -2 else "coordination_lost")
        if result:
            self.owners[request_id] = epoch
        return bool(result)

    async def renew(self, app_id, model, request_id):
        keys = self.keys(app_id, model)
        args = [request_id, self.lease_ms]
        if self.policy_hash:
            keys.append(self.guard_key(model))
            args.extend([self.owners.get(request_id) or "", self.policy_hash])
        return bool(
            await self.call(
                "renew",
                self.renew_script,
                keys=keys,
                args=args,
            )
        )

    async def release(self, app_id, model, request_id):
        self.owners.pop(request_id, None)
        return await self.call(
            "release", self.release_script, keys=self.keys(app_id, model), args=[request_id]
        )

    async def call(self, operation, script, **kwargs):
        started = time.monotonic()
        try:
            if self.batch_size > 1:
                return await self.batch.submit((script, kwargs))
            return await script(**kwargs)
        finally:
            if self.metrics:
                self.metrics.stage.labels("redis_" + operation, "execute").observe(
                    time.monotonic() - started
                )
