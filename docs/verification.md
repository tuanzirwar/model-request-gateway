# 最新验收（0.5.0，2026-10-07）

| 层级 | 结果 | 证据 |
|---|---|---|
| 自动化 | 122通过、零跳过，真实MySQL/Redis启用 | reports/pressure-tests.txt |
| 双进程HTTP | 40检查通过 | reports/http-e2e.json |
| 浏览器 | 12检查通过 | reports/browser-e2e.json |
| 空库升级/降级 | 7检查通过 | reports/migrations.json |
| 隔离Redis强杀重启 | 10检查通过 | reports/coordination-restart.json |
| 长流并发/取消 | 12成功、4取消、上游0连接 | reports/v05-streams.json |
| 真实TUI/模型 | 多轮、实际文本后取消、再生成通过 | reports/pressure-real-tui.json |
| 全量回归/构建 | 全部exit0 | reports/pressure-validation.json |
| 冻结源码十万请求 | 全200、416.15QPS、p95/p99=139.72/193.38ms | reports/v05-final-100000.json |
| 索引与统计 | 顺序/分组结果一致，执行计划与写成本已测 | reports/v05-retention-index.json、v05-query-diagnosis.json、v05-index-write-cost.json |

QPS环境：4网关进程、32闭环并发，MySQL缓冲池128MiB，198字节无推理上游，本机端口轮询。每进程写4/读2，总24 SQL连接。带宽实验仅响应正文回环流量，详见优化日志。压测脚本拒绝业务库，创建随机测试应用和Redis命名空间；数据在轮次间积累。

## 复现

```powershell
# gateway.yaml 配置本机依赖，脚本使用 model_gateway_test 专用库。
.venv/Scripts/python.exe scripts/verify_pressure.py --browser --real-model
.venv/Scripts/python.exe scripts/benchmark.py --concurrency 32 --requests 100000 --repeats 1 --gateway-workers 4 --db-workers 4 --concurrency-limit 256 --client raw --output reports/repeat-100000.json
.venv/Scripts/python.exe scripts/benchmark_streams.py
.venv/Scripts/python.exe scripts/benchmark_retention.py
.venv/Scripts/python.exe scripts/benchmark_index_writes.py
.venv/Scripts/python.exe scripts/diagnose_queries.py --benchmark reports/v05-final-100000.json --output reports/repeat-query-diagnosis.json
# Redis强杀脚本要求已准备Alpine WSL及私有本机Redis二进制，只终止自己启动的随机端口实例。
.venv/Scripts/python.exe scripts/verify_coordination_restart.py
```

不并行运行压测、其他SQL实验或真实模型生成，避免资源污染。真实TUI脚本需要父目录存在TUICodingAgent代码与依赖，纯技术源代码包不包含该另一个仓库。浏览器需项目Playwright浏览器或本机Edge。没有这些依赖不要将跳过算成完整验收。

## 能力边界

最终证据为本机本版本；旧报告保留但不能拼接成更好成绩。云端CI状态见仓库Actions，本地验收不代替云端结果；Dockerdaemon不可用，只验证Compose解析。没有tmux，已通过实际会话运行时和TCP，而非tmux视觉演示。HA、公网TLS、真实公网网卡带宽与GPU立即停止未验证，不属于已交付声明。学习资料在仓库外。
