# 运行与演示手册

## 五分钟演示

1. 在项目根目录运行`pwsh -File scripts/start_demo.ps1`，确认输出的服务地址。本机数据库、Redis和已有模型必须可达。重复启动会提示已有进程，避免遗留重复服务。
2. 打开该地址根路径的工作台。从`.local/tui-demo/config.yaml`复制应用密钥到登录框；不要截图或提交此配置。密钥仅在页面内存中保存。
3. 输入短问题并开始生成，查看增量输出和请求ID，再在右侧点击请求记录核对状态。
4. 输入较长问题，等待部分输出后停止；刷新记录确认`cancelled`，随后再生成，验证名额可复用。
5. 按失败或取消筛选记录，展示状态、首帧、尝试输出字节和供应商返回usage。明确首帧不是首个可见token。
6. `pwsh -File scripts/stop_demo.ps1`停止本项目进程树。不会停止已有模型、MySQL或其他服务。

真实模型可能先输出推理或等待较久，演示不要许诺固定响应时间。`verify_browser.py`中的快速正常/断流测试用受控上游；不得作为模型效果演示。

本机模型别名默认`reasoning_effort: none`、`max_tokens: 512`，显式请求可覆盖。迁移到其他模型时先核对支持的参数；纯转发时可删除request_defaults。不能把这种默认值写成强制费用限制。

## 应用维护

```powershell
.\.venv\Scripts\python.exe scripts/admin.py list
$env:GATEWAY_APP_KEY = '<至少32字符的随机新密钥>'
.\.venv\Scripts\python.exe scripts/admin.py grant --app tui --models coding --concurrency 2
.\.venv\Scripts\python.exe scripts/admin.py rotate --app tui
Remove-Item Env:GATEWAY_APP_KEY
.\.venv\Scripts\python.exe scripts/admin.py disable --app tui
.\.venv\Scripts\python.exe scripts/admin.py reconcile
.\.venv\Scripts\python.exe scripts/admin.py purge --batch-size 256 --batches 1
```

管理命令只有持有数据库凭据的本机管理员执行，没有公开管理API。`list`不返回密钥或摘要。轮换只改变密钥，不重新启用已禁用应用；禁用和轮换不主动终止已接入流。`grant`会更新授权并启用应用。记录清理使用配置的保留天数，应由部署方安排维护，不声称已有调度服务。

## 服务健康和指标

- `/live`只判断HTTP服务仍能应答。
- `/health`检查MySQL、Redis代际、指纹与恢复窗口，失败返回503；不保证模型已加载、上游正常或表迁移完整。
- `/metrics`为独立监控密钥保护的Prometheus文本接口；设置`GATEWAY_METRICS_KEY`（至少32字符）后重启启用。为空时404。应用密钥不能抓取监控，监控密钥也不能查询应用记录。
- 指标包括本进程活动请求、终态计数、请求耗时分布、DB等待及执行、Redis往返、上游头/体等待、事件循环延迟。标签不含应用ID、请求ID、提示词或密钥。

可关注`gateway_stage_seconds_sum / gateway_stage_seconds_count`的步骤平均耗时。p95应由直方图桶或负载原始样本计算，不能把各步骤平均数相加当作端到端p95。指标是**单进程**的；部署多个Uvicorn worker时随机命中某个worker的抓取不代表全局。当前配方为单worker；多个实例应独立抓取、再在Prometheus端聚合。本项目未部署Prometheus或Grafana。

## 完整Compose配方

```powershell
Copy-Item .env.example .env
# 填写两组不同的随机URL安全数据库口令；可另填监控及上游密钥。
# 修改deploy/gateway.docker.yaml中的真实模型、地址和容量。
docker compose -f compose.full.yaml config --quiet
docker compose -f compose.full.yaml up --build -d
docker compose -f compose.full.yaml exec -e GATEWAY_APP_KEY gateway python scripts/admin.py grant --app tui --models coding
```

执行授权命令前先在宿主设置`GATEWAY_APP_KEY`。配方包含MySQL/Redis健康检查、独立迁移步骤、非root网关、容器健康检查和本机18080端口。模型不随镜像打包；`host.docker.internal`指宿主服务，宿主模型还需允许来自容器的连接。不要通过开启任意公网访问解决接入问题。

**本机只验证配置解析，Docker引擎不可用，尚未完成容器构建与运行。** 配方不是生产上线声明。部署到公网还需TLS、入口访问控制、凭证轮换、独立迁移账号、MySQL运行账号最小权限及Redis专网认证；不直接使用演示数据库授权。

## 故障判断

| 现象 | 检查 | 行为 |
|---|---|---|
| 401 | 应用是否启用、密钥是否轮换 | 不创建生成请求 |
| 403 | 模型别名及应用授权 | 不调用上游 |
| 400/413/408 | 参数、体积、读取时限 | 不静默丢弃不支持的参数 |
| 429 | 模型或应用-模型并发已满 | 即时拒绝，参考Retry-After；流不排队 |
| 503 database_busy | 专用DB执行池及队列耗时 | 避免无界后台SQL；检查连接池和SQL |
| 503依赖故障 | /health、MySQL、Redis | 新请求失败关闭 |
| 502或流内error | 上游HTTP、协议或中断 | 保留部分输出，不假装成功，不自动重试 |
| 504或deadline_exceeded | 首段、空闲及总预算 | 关闭流并清理本地资源 |
| abandoned | 进程终止或终态写失败 | 保守对账，不恢复生成或推断账单 |

先检查应用、模型别名与请求ID，再看指标及服务日志。默认日志不记录模型正文。数据库与Redis没有分布式事务，终态落库失败可能只有对账结果；不能保证取消一定拿到usage。

## 验收复现

```powershell
$env:GATEWAY_TEST_MYSQL = 'mysql+pymysql://gateway:local-gateway-only@127.0.0.1:13316/model_gateway_test'
$env:GATEWAY_TEST_REDIS = 'redis://127.0.0.1:16379/0'
$env:GATEWAY_BROWSER_CHANNEL = 'msedge'
.\.venv\Scripts\python.exe -m pip install -e '.[dev,browser]'
# 本机已有Edge可使用独立无头会话；没有时安装项目自己的Chromium。
.\.venv\Scripts\python.exe scripts/verify_all.py --browser --real-model
.\.venv\Scripts\python.exe scripts/benchmark.py --db-workers 4
```

真实TUI验证要求已启动演示服务且上一级有TUICodingAgent及依赖。该综合入口以本任务机器的测试端口为基础；CI提供MySQL/Redis服务，执行数据库迁移、独立后端测试、静态检查和打包。运行结果见[GitHub CI](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml)；CI不执行依赖本机模型和TUI宿主的验证。


## 0.5 部署与保留期维护

旧的 metadata_transport / metadata_shards 配置已移除，仍带字段会报未知配置；删除两项后运行最新迁移。多个实例的 namespace、容量组、路由、lease/total必须一致，policy冲突不能通过删Redis live key解决。改变策略要先排空旧进程、使用新命名空间。恢复期间health503表示等待旧执行截止。

保留期CLI每次仅一批，默认256行；--batches可限定执行轮数，上限100。此维护不是后台队列或定时任务服务。对账是运行中的网关约每2秒执行；停机期间不会有人对账。最新验收入口scripts/verify_pressure.py，历史verify_all报告不替代0.5结果。
