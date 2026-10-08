"""进程内有界消息队列：消费完成才确认，不把入队当作持久化成功。"""

import asyncio

from fastapi import HTTPException


class BatchQueue:
    def __init__(self, consume, *, size=64, delay=0.002, capacity=256, timeout=1):
        self.consume = consume
        self.size, self.delay, self.timeout = size, delay, timeout
        self.slots = asyncio.Semaphore(capacity)
        self.queue = asyncio.Queue()
        self.task = None
        self.closing = False

    async def submit(self, message):
        if self.closing:
            raise HTTPException(503, "queue_closed")
        try:
            await asyncio.wait_for(self.slots.acquire(), self.timeout)
        except TimeoutError as exc:
            raise HTTPException(503, "queue_busy", headers={"Retry-After": "1"}) from exc
        if self.closing:
            self.slots.release()
            raise HTTPException(503, "queue_closed")
        future = asyncio.get_running_loop().create_future()
        # 调用方取消不取消已入队的SQL/租约操作，且容量到实际执行结束才归还。
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        self.queue.put_nowait((message, future))
        if self.task is None:
            self.task = asyncio.create_task(self.run())
        return await asyncio.shield(future)

    async def run(self):
        while True:
            first = await self.queue.get()
            if first is None:
                return
            batch = [first]
            if self.size > 1 and self.queue.qsize() < self.size - 1:
                await asyncio.sleep(self.delay)
            while len(batch) < self.size and not self.queue.empty():
                item = self.queue.get_nowait()
                if item is None:
                    self.queue.put_nowait(None)
                    break
                batch.append(item)
            try:
                results = await self.consume([item[0] for item in batch])
                if len(results) != len(batch):
                    raise RuntimeError("批处理确认数量不一致")
            except Exception as exc:
                results = [exc] * len(batch)
            for (_, future), result in zip(batch, results, strict=True):
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)
                self.slots.release()

    async def close(self):
        self.closing = True
        if self.task is not None:
            self.queue.put_nowait(None)
            await self.task
