# 验证方法与实际结果

实验日期：2026-10-06。数据为合成对话和受控故障，不包含用户业务工单。原始报告保留失败及修正后的结果，`final-validation.json`汇总最终验收。

发布补充：[GitHub CI](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml)覆盖Linux/Python 3.12、MySQL 8.4/Redis 7.4服务、迁移、pytest、Ruff及wheel/sdist构建。本机最终报告中的`github_ci_verified: false`保留生成时的状态，云端结果以工作流运行记录为准。公开日志中的本机路径使用占位符，不改变测试结果。

## 环境和结果

Windows、Python 3.14、MySQL 8.4.11；真实Redis 8.8.0运行在已有Alpine WSL中，通过本机端口转发连接。HTTP网关使用独立进程，真实模型为本机已有qwen3.5:4b。环境版本及构建日志另见最终报告。

| 验证层 | 实际结果 | 证据和边界 |
|---|---|---|
| 自动化测试 | 59项通过 | 55项局部/ASGI测试、3项真实Redis测试、1项真实MySQL测试；局部数据访问使用SQLite，不替代MySQL验收 |
| TCP故障验收 | 34项检查通过 | 双独立网关进程、真实MySQL/Redis和受控HTTP SSE上游；包含两次客户端取消检查 |
| 浏览器工作台 | 12项检查通过 | Playwright启动已有Edge的独立无头上下文；真实TCP及测试MySQL/Redis，受控上游，未使用用户浏览器会话 |
| 空库迁移 | 4项检查通过 | 新建专用MySQL库，迁移两次、双精度字段、6秒期限精度及复合索引 |
| 实际TUI与模型 | 多轮、取消和再次生成通过 | 使用实际ConversationRuntime及OpenAIProvider，经网关调用真实模型；未替换模型答案 |
| 慢消费者 | ASGI发送阻塞受总时限限制 | 通过模拟ASGI send验证，未声称完成网络级慢客户端压测 |
| 打包与配置 | 见最终报告 | wheel/sdist构建、Ruff检查、Compose配置解析；未运行Docker容器 |

故障验收覆盖鉴权、模型授权、请求体上限、应用记录隔离、双进程共享容量、取消后额度复用、租约丢失、断流、超大帧、上游HTTP错误、首段及空闲超时、文本/工具分片/usage、进程终止后租约到期与记录对账、游标分页、上游连接释放、Redis不可用时503。具体每项断言在`reports/http-e2e.json`。

真实模型多轮的逐次耗时见`reports/real-tui-model.json`，包括本机模型生成；后续在部分输出后取消，并再次发起新一轮。这里没有模型质量评分、节省人力比例或企业使用量。`first_ms`衡量首个完整协议帧，不是首个可见答案字符。

## 负载实验

1个Uvicorn进程，真实MySQL/Redis，16个模型接入名额，默认4线程专用DB池及匹配连接池，受控非流式HTTP上游立即返回固定JSON。并发1/4/8，每组两轮，每轮约5秒；所有客户端完成预热后同时开始计时，统计成功请求QPS和成功请求p95。采用闭环负载，结果不能外推为长期最大吞吐。

| 并发 | 两轮成功QPS | 两轮p95（ms） |
|---|---|---|
| 1 | 31.71 / 28.58 | 37.52 / 40.06 |
| 4 | 76.59 / 75.00 | 67.22 / 69.83 |
| 8 | 91.53 / 87.62 | 114.08 / 117.78 |

最终交付代码六轮共1,974次计时请求，均为200；在真实模型验收完成后独立执行，模型无并行生成任务。数据库记录在轮次间累计。负载报告同时附步骤均值：DB排队、完整DB操作、Redis往返、上游HTTP头/体等待。没有逐条SQL与连接池checkout的单独跟踪，因此DB完整操作耗时仍不能直接归因于SQL执行或GIL。

旧版本及分阶段基线保存于`benchmark-0.1.json`、`benchmark-unbounded-profile.json`。后者8并发23.63/23.91 QPS，DB鉴权操作均值约41—44ms、记录创建50—63ms，而队列等待约1ms。加入专用有界执行池、匹配连接池及驱动超时后，此轮退化缓解。组合改动不能证明某个参数的单独收益；不写精确提升比例。

相同新代码另做容量对照：8线程97.84—104.00 QPS，4线程90.53—96.46 QPS。两者均无原先明显退化，不能说4线程唯一最优。对照首轮附近曾运行短时回归，记录累计和本机负载也不同，因此只作为容量选择的补充证据。详细结果分别在`benchmark-db8.json`、`benchmark-db4.json`。

以上QPS排除模型推理，不能写成真实模型吞吐。前一版预热屏障不足及与真实模型同时运行的负载报告也保留，最终采用`reports/benchmark.json`。

## 可复现命令

在项目根目录运行，先安装依赖、准备专用测试MySQL/Redis；不要对业务库运行测试。下列端口和开发口令只对应本任务机器。

```powershell
$env:GATEWAY_TEST_REDIS = 'redis://127.0.0.1:16379/0'
$env:GATEWAY_TEST_MYSQL = 'mysql+pymysql://gateway:local-gateway-only@127.0.0.1:13316/model_gateway_test'
.\.venv\Scripts\python.exe -m pytest -q --basetemp=.local/pytest-final
.\.venv\Scripts\python.exe scripts/verify_migrations.py
.\.venv\Scripts\python.exe scripts/verify_e2e.py
.\.venv\Scripts\python.exe scripts/benchmark.py
pwsh -File scripts/start_demo.ps1
.\.venv\Scripts\python.exe scripts/verify_real_tui.py
.\.venv\Scripts\python.exe -m pip install -e '.[browser]'
$env:GATEWAY_BROWSER_CHANNEL = 'msedge'
.\.venv\Scripts\python.exe scripts/verify_browser.py
.\.venv\Scripts\python.exe scripts/verify_all.py --browser --real-model
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m build
```

不设置两个测试环境变量时，4项真实依赖测试会跳过。端到端脚本和负载脚本使用本机测试配置及独立Redis命名空间；迁移脚本每次新建固定前缀的测试数据库。TUI集成脚本要求上一级存在TUICodingAgent源码，且其依赖已安装（本机已执行`pip install -e ..`）。迁移、端到端脚本不是无外部依赖的通用单元测试。

## 实验中实际发现的问题

- 空上游密钥曾被生成`Bearer `头：改为无密钥时不发送该头。
- SSE小片段曾被固定大小读取缓冲：改为增量读取，验证首片段提前交付。
- 重复取消曾中断HTTPX关闭，导致上游仍有活动连接：将I/O收尾与资源清理隔离为独立任务，真实HTTP取消后活动连接归零。
- MySQL单精度Unix时间无法准确表示短期限：增加固定迁移改为Double，实测6秒差值。
- TUI事件只消费到COMPLETED时，生成器finally尚未完成：验收脚本完整消费事件，再开始下一轮。
- 注入HTTPX测试客户端曾使用默认5秒超时，冷模型加载时提前取消，覆盖了TUI原有120秒设置：保留失败报告后显式恢复120秒客户端预算，网关自身首段/空闲/总时限仍生效。
- 浏览器分页首轮断言没有等待下一页完成：改为观察首行请求ID变化后再比较两页ID集合，未以修改后端数据掩盖问题。
- 复杂长回答的真实模型实验持续推理至120秒预算，记录为failed/deadline_exceeded；取消脚本曾忽略FAILED事件继续等待文本，现改为任务/文本事件联合等待并立即报告失败。取消实验采用简单序列输出，仍要求实际收到文本后再取消，不把推理等待包装成已完成回答。
- 进一步实验中短问题也可能持续推理至预算；TUI的thinking=False不发送显式关闭值。增加按模型别名的request_defaults，本机配置reasoning_effort=none、max_tokens=512，调用方可覆盖；记录上游兼容参数依据，不把仅完成协议链路当成模型质量验证。

Redis进程崩溃、集群故障转移、远端GPU实际停止、公开网络部署、完整终端视觉交互及真实团队收益均未验证。项目的可靠性声明限定在已测试的接入、HTTP资源和记录路径。
