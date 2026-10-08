"""只重试数据库明确回滚的死锁，拒绝重放提交结果不确定的操作。"""

from dataclasses import replace

import pymysql
import pytest
from sqlalchemy.exc import OperationalError

from model_gateway.app import Runtime
from model_gateway.config import Settings


@pytest.mark.parametrize("code,expected_calls", [(1213, 3), (2006, 1), (1205, 1)])
async def test_deadlock_retry_is_bounded_and_keeps_uncertain_failures(code, expected_calls):
    runtime = Runtime(replace(Settings("sqlite://", "redis://localhost:1", "t", {}), db_workers=1))
    calls = 0

    def fail():
        nonlocal calls
        calls += 1
        raise OperationalError("private sql", {}, pymysql.err.OperationalError(code, "private"))

    try:
        with pytest.raises(OperationalError):
            await runtime.db_call(fail)
        assert calls == expected_calls
        assert not runtime.db_pending
    finally:
        await runtime.close()


async def test_rolled_back_deadlock_can_succeed_on_retry():
    runtime = Runtime(Settings("sqlite://", "redis://localhost:1", "t", {}))
    calls = 0

    def operation():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("", {}, pymysql.err.OperationalError(1213, "deadlock"))
        return "committed"

    try:
        assert await runtime.db_call(operation) == "committed"
        assert calls == 2
    finally:
        await runtime.close()
