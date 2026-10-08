"""真实TUI会话运行时经网关调用本地模型，不替换真实模型输出。"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT.parent / "src"))

from tuicodingagent.config import ConfigLoader  # noqa: E402
from tuicodingagent.models import RuntimeEventType  # noqa: E402
from tuicodingagent.providers.openai import OpenAIProvider  # noqa: E402
from tuicodingagent.runtime import ConversationRuntime  # noqa: E402


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=".local/tui-demo/config.yaml")
    parser.add_argument("--output", default="reports/real-tui-model.json")
    args = parser.parse_args()
    config_file = ROOT / args.config
    output = (ROOT / args.output).resolve()
    if not output.is_relative_to(ROOT / "reports"):
        parser.error("报告必须位于项目reports目录")
    config = ConfigLoader.load(config_file).providers[0]
    # 注入客户端时显式保留TUI原有120秒预算，HTTPX默认5秒会在冷加载时提前取消。
    provider = OpenAIProvider(
        config, httpx.AsyncClient(timeout=httpx.Timeout(120, connect=20), trust_env=False)
    )
    runtime = ConversationRuntime(
        provider,
        system_prompt="请用中文回答，按用户指定的长度输出。只回答用户问题，不执行任何操作。",
        workspace=config_file.parent,
    )
    result = {
        "passed": False,
        "path": "real ConversationRuntime -> OpenAIProvider -> gateway -> real local model",
        "data": "synthetic dialogue",
        "semantic_quality_score": None,
        "turns": [],
        "client_read_timeout_seconds": 120,
    }
    task = None

    async def turn(prompt):
        answer = ""
        async for event in runtime.chat(prompt):
            if event.type == RuntimeEventType.COMPLETED:
                answer = event.text
            elif event.type == RuntimeEventType.FAILED:
                raise RuntimeError(event.text)
        # 完整消费会话事件，确保运行时finally完成后再开始下一轮。
        return answer

    try:
        for prompt in (
            "用一句话解释HTTP请求超时。",
            "接着用一句话说明用户取消与服务端超时的区别。",
        ):
            started = time.monotonic()
            answer = await asyncio.wait_for(turn(prompt), 150)
            result["turns"].append(
                {
                    "prompt": prompt,
                    "answer": answer,
                    "seconds": round(time.monotonic() - started, 3),
                }
            )
            assert answer.strip(), "实际模型返回空内容"
        # 实际模型流首个文本片段后取消，随后再次生成。
        saw_text = asyncio.Event()

        async def long_turn():
            async for event in runtime.chat("请逐行输出数字1到100，每行一个数字，不添加解释。"):
                if event.type == RuntimeEventType.TEXT_DELTA and event.text:
                    saw_text.set()
                elif event.type == RuntimeEventType.FAILED:
                    raise RuntimeError(event.text)

        task = asyncio.create_task(long_turn())
        observer = asyncio.create_task(saw_text.wait())
        try:
            done, _ = await asyncio.wait(
                (task, observer), timeout=150, return_when=asyncio.FIRST_COMPLETED
            )
            if observer not in done:
                if task in done:
                    await task
                raise RuntimeError("取消验收未观察到实际文本输出")
        finally:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        cancelled = task.cancelled()
        assert cancelled
        answer = await asyncio.wait_for(turn("现在只回复：可以继续。"), 150)
        result["cancel_then_new_turn"] = {"cancelled_after_text": cancelled, "next_answer": answer}
        data = yaml.safe_load(config_file.read_text("utf-8"))
        base = data["providers"][0]["base_url"].split("/v1/")[0]
        async with httpx.AsyncClient(trust_env=False) as client:
            for _ in range(30):
                records = (
                    await client.get(
                        base + "/requests", headers={"Authorization": "Bearer " + config.api_key}
                    )
                ).json()["items"]
                if all(row["status"] not in ("accepted", "running") for row in records[:4]):
                    break
                await asyncio.sleep(0.1)
        result["request_records"] = records[:4]
        assert any(row["status"] == "cancelled" for row in records[:4]), records[:4]
        assert records[0]["status"] == "succeeded" and answer.strip()
        result["passed"] = True
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await runtime.close()
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), "utf-8")
        print(
            json.dumps(
                {"passed": result["passed"], "completed_turns": len(result["turns"])},
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
