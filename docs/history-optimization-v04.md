# Redis、MQ、数据库优化记录（0.4，本机实验）

## 最终测量与口径

2026-10-07，冻结网关源码后执行100,000次非流式HTTP请求：全部200，95.6233秒，**1,045.77成功QPS**，p95为194.27ms。最终SQL记录100,384条均为succeeded（包含384次预热），MQ stream、pending、dead全部为0；96次存活/就绪/模型检查通过，运行期间六个核心文件SHA256一致。证据：`reports/mq-final-100000.json`。已达到目标区间下限，**没有达到3,000或10,000成功QPS**。

环境为Windows、本机MySQL 8.4.11、Redis 8.8、Python 3.14.2，24核/32GiB；32个网关进程，128客户端并发，每进程2个写连接及2个鉴权连接，总预算128，MQ两分片。客户端轮询32个本机端口，没有部署或测量生产负载均衡器。上游立即返回198字节JSON、不做模型推理；不能换算真实模型吞吐。原始压测报告分别保留每轮累计数据量与软件版本。

此次压测将InnoDB缓冲池临时从128MiB增到1GiB，保留`innodb_flush_log_at_trx_commit=1`和`sync_binlog=1`。测量结束恢复128MiB、原慢日志开关及10秒阈值；见`reports/mysql-settings-restored.json`。**不能声称默认128MiB环境也达到此持续结果。** Redis测试实例未启用AOF，不能声称MQ已实现断电持久化或生产故障转移。

## 为什么采用Redis和MySQL

二者不是不可替换的要求。当前需求是多进程共享接入额度、取消后回收，以及应用隔离的请求状态和历史查询。

Redis用来共享短期租约，Lua在一次原子操作中完成计数、清理过期项及准入判断；本机或进程内计数不能跨网关共享。此次复用Redis Streams做请求元数据MQ，减少引入独立消息系统的运维成本。Streams只排队元数据，没有排队或重放模型调用。需要独立消息存储隔离、长时间堆积、更复杂路由或故障转移时，应评估RabbitMQ/Kafka等；此次没有完成这些替代系统的对照测试。

MySQL保存应用、授权和请求状态，使用事务、唯一主键、外键、复合索引和游标分页。授权直接查库，不缓存禁用状态。PostgreSQL也可以承载这一关系模型；SQLite适合单机开发，但本次多进程写入路径按MySQL验证。没有证据证明MySQL是所有负载下最快的选择，也没有把Redis缓存当作审计事实来源。

## 问题、措施与证据

| 实际问题 | 已实施措施 | 测量或验证 | 判断 |
|---|---|---|---|
| 模型筛选先扫大量应用历史记录 | 0003增加`(app_id,model,started_at,id)` | 十万行稀疏分布，均值41.303→0.620ms，实际扫描22,600→26行 | 此查询需要索引；不是写入QPS的主要优化 |
| 状态筛选需要额外排序 | 0003增加`(app_id,status,started_at,id)` | 2.176→0.579ms，879行及排序→26行；状态+时间游标2.117→2.116ms没有改善 | 保留无收益案例，不宣称所有组合查询都更快 |
| 正常请求有创建、running更新、终态更新三次事务 | 获取租约后直接插入running，再提交终态；有界批量INSERT/CASE UPDATE | 两次持久化仍在模型调用前及成功确认前提交；失败与取消测试覆盖 | 减少事务和往返，不放松成功语义 |
| 每次鉴权SELECT后出现无意义ROLLBACK往返 | 独立AUTOCOMMIT只读鉴权池，Core批量SELECT、去掉全量ORM对象装载 | 禁用和密钥轮换后下一批查询立即可见；写引擎保留事务 | 没有引入失效授权缓存 |
| Redis多次命令往返 | 有界pipeline批量EVALSHA，每个Lua仍独立原子 | NOSCRIPT只补执行未执行单项；连接故障不重放整个pipeline | 降低往返，避免重复占用/释放额度 |
| 多进程各自小事务，SQL提交成本高 | Redis Streams集中分片消费，批量SQL事务，COMMIT后确认 | 最终200,000次元数据操作对应6,809次持久化批调用；提交后崩溃重放、缺少父记录、SQL失败和毒消息实测 | 是元数据合并机制，不是模型调用重试 |
| 消费失败可能丢记录或误报成功 | SQL失败不XACK/XDEL；保留PEL可XAUTOCLAIM；毒消息先写无TTL死信，再原子移出；成功SSE在终态提交后发DONE | 故障注入要求非流式503、流式错误且无DONE；队列满503；失败消息保留、重复ack回复列表有界 | 消费至少一次、主键及终态条件幂等，不声称端到端exactly-once |
| 十万记录的`GROUP BY status,error`验收查询曾超5秒客户端预算 | 状态统计走已有覆盖索引，错误码只对少量失败状态回表 | 最新100,384行应用：273.205→27.360+0.544ms；计数一致 | SQL改写足够，无需为成功行error额外加索引 |
| 数据增长后128MiB缓冲池不足以容纳索引工作集 | 本机临时1GiB缓冲池实验 | 24进程十万次874.29（含502及统计超时）→946.17全200；32进程最终1,045.77全200 | 分别改变缓存、进程及数据量，不能归因成独立固定倍数收益 |
| 历史压力中出现一个上游502 | 记录HTTP异常类型，空闲上游连接2秒到期；模型POST仍不重试 | 前轮1,037.62 QPS有1次502，最终十万次无502 | 零错误复测通过，尚未确认之前502的网络根因 |

32进程情况下鉴权、写池合计128连接，不超过测试服务器151上限。Streams全局活跃与死信合计容量4096，每分片均分；生产者及DB排队有界，提交确认超时返回503，不能靠无界排队制造成功吞吐。各网关必须配置相同namespace、分片数和数据库；分片数变化需排空旧分片后再切换。

## 慢查询日志如何定位

本次仅在专用本机数据库启用FILE慢日志，`long_query_time=0.005`，新建连接继承阈值。采样覆盖最终十万次压力及随后查询诊断；诊断没有与计时压力重叠。原始日志留在忽略目录`.local`，可能含授权哈希、应用ID和参数；公开`reports/mysql-slow-summary.json`只包含指纹、语句类型和数值。分析器不会导出SQL文本，未完成尾部被忽略，脱敏和COMMIT分类有自动化测试。

捕获6,803条慢语句。其中6,566条COMMIT累计56,259.925ms，均值8.568ms、p95 21.601ms；SELECT 22条累计2,319.377ms，INSERT 114条累计2,151.290ms，UPDATE 97条累计1,947.295ms。COMMIT约占已捕获耗时89%，Rows_examined为0。它更支持优先合并事务和检查存储/日志提交成本，而不是给所有写入加索引。该累计值包含并发执行，**不等于墙钟时间**；低于5ms的SQL未被捕获，不能据此计算全体SQL平均耗时。5ms是短期诊断阈值，不建议原样长期全量记录；实际部署需根据SLO、日志容量、轮转及采样设置阈值。

`scripts/diagnose_queries.py`在同一应用运行5次查询并采集EXPLAIN与EXPLAIN ANALYZE。报告`reports/mysql-query-diagnosis.json`证明：

- 旧状态/错误聚合使用`ix_app_model_started_id`且`Using temporary`，要回表读取不在索引内的error。
- 改写的状态聚合使用`ix_app_status_started_id`且`Using index`，仍检查约十万条索引记录，主要收益是避免成功记录回表，**不是扫描行数降到个位数**。
- 失败明细使用同一索引的range扫描；此次应用无失败记录，仅耗时0.544ms。失败比例上升时要重新量测，不保证常数耗时。
- 30天保留期候选SELECT目前无可用finished_at索引，`ALL`及`Using filesort`；平均114.612ms、每次实际检查1,096,849行，返回0行。建议下一轮在测试库测`(finished_at,id)`不可见候选索引和分批清理，核对删除锁时间、写入成本及索引体积。**尚未新增此索引，也没有在本次实验删除历史数据。** 清理不是此次请求热路径，不能冒称已经提升聊天QPS。

后续流程：按总耗时/频次/扫描行数挑选指纹→在专用库复现→核对选择性、回表、排序和锁等待→先改写SQL，再测试最小候选索引→返回内容/顺序一致性→压测写入与清理→迁移上线及慢日志复核。EXPLAIN估计行数与真实扫描量必须分开，EXPLAIN ANALYZE会执行SQL；只对安全SELECT使用。

## 数据规模与是否分库分表

本次查询诊断精确COUNT为**1,096,849条请求元数据**；统计估计TABLE_ROWS为1,170,960，并不是准确计数。数据336,936,960字节（321.33MiB），索引701,186,048字节（668.70MiB），合计约990.03MiB。读取information_schema前设置会话`information_schema_stats_expiry=0`，避免用24小时旧缓存描述规模。全部是合成/压测元数据，不含模型全文，也不是生产用户规模。

现阶段**暂不分库分表**：单MySQL在上述配置完成目标下限；已有明确可改写查询，主要慢日志成本是COMMIT；没有证明单机磁盘、CPU、锁、日志、容量或可用性已达到无法扩展的上限。索引已经约为数据的2.08倍，每个新索引增加存储与写入成本，不能凭“超过百万行”决定加索引或分片。

需要关注持续增长：如果真实长期维持1,000请求/秒，一天新增8,640万条，30天25.92亿条。当前百万行短测不能证明这种保留规模可用；必须测稳定业务负载、保留清理、存储增长、备份恢复窗口、p99和失败率。先确认保留需求并归档/分批删除，调整缓冲池和实例资源，再决定是否引入物理分片。

当优化查询和扩容后仍持续超过约定p99/失败率，提交/IO/锁压力饱和，或容量、备份恢复时间、租户隔离要求超单库预算，才开展分片验证。可优先按app_id路由，匹配现有应用隔离查询；需同时解决热点租户、按UUID查询的路由、跨应用聚合、外键、跨片事务、扩容迁移及重平衡。这些能力尚未实现。

Redis Streams两个消费分片**不等于数据库分库分表**。MySQL原生时间分区也不是可直接打开的优化：当前requests有applications外键，InnoDB用户分区不支持外键；分区键还涉及唯一键约束。保留期分区要独立设计并验证，不能为删除便利破坏当前主键和应用关系。

## 无收益与失败记录

5ms聚批窗口的16进程实验620.82 QPS，低于零窗口方案；默认保留零窗口，只在事件循环调度内聚批。直接关闭鉴权/Redis批处理仅738.89–780.15 QPS；16个MQ分片仅830.84–847.69 QPS，分片过多导致小事务增加。选两分片是本机结果，不是生产通用最优值。安装httptools/hiredis支持可选原生解析，没有独立对照证明它们的固定收益。

最早批量SQL将保留字usage漏掉引用，导致SQL失败；原报告`batch-first-ladder.json`不能当作正确成功吞吐。后修正MySQL标识符引用，并要求提交失败不能200。另有Windows配置启动失败、1次502、统计SELECT超时以及Ollama停机造成的真实对话失败，均保留证据。自动审核曾拒绝失败后ACK、合并发布/阻塞等待及未经验证的JSON_TABLE方案；分别改为保留PEL、独立BLPOP、现有CASE SQL，未绕过审核实施。

历史持续结果：0.3为593成功QPS、99.999%成功率；0.4在24进程/1GiB下946.17 QPS、十万次全200；32进程前轮1,037.62 QPS、1次502；最终32进程1,045.77 QPS、十万次全200。各轮参数和行数不同，不能据此编造某一项优化的精确加速比例。

## 带宽、安全与复现

此次198字节响应仅1.657Mbps正文回环流量，不能证明公网带宽容量。1,000–3,000 QPS、每次64KiB响应仅出口正文约524Mbps–1.573Gbps；每次1MiB约8.39–25.17Gbps，还需上游流量及TCP/TLS开销。真实模型长流需按并发≈QPS×平均持续时间估算连接与内存。

现有密钥长度、请求/响应/SSE累计大小、在途数量、队列、连接池和时限有界；SQL参数绑定、应用隔离及监控独立凭据保持。公网仍需TLS、可信代理限速、网络级带宽与连接防护，MySQL/Redis私有访问。生产MQ应明确AOF/复制、持久化策略、故障恢复和死信处理；本机无AOF结果不支持已实现断电可靠性的声明，也未验证集群模式。

```powershell
.\.venv\Scripts\python.exe -m pip install -e '.[performance]'
# 准备本机专用测试库及Redis；缓冲池实验需单独记录并在结束后恢复。
.\.venv\Scripts\python.exe scripts/benchmark.py --client raw --concurrency 128 --requests 100000 --repeats 1 --concurrency-limit 1024 --db-workers 2 --connections 64 --gateway-workers 32 --max-inflight 128 --metadata-transport redis-streams --metadata-shards 2 --output reports/mq-repeat-100000.json
.\.venv\Scripts\python.exe scripts/diagnose_queries.py --benchmark reports/mq-repeat-100000.json --output reports/mysql-query-repeat.json
.\.venv\Scripts\python.exe scripts/analyze_slow_log.py .local/mysql-optimization-slow.log --output reports/mysql-slow-repeat.json
.\.venv\Scripts\python.exe scripts/verify_pressure.py --metadata-transport redis-streams --browser --real-model
```

MQ为可选模式，默认仍是local。多进程启用时配置`metadata_transport: redis-streams`、一致的`metadata_shards: 2`；依赖支持XAUTOCLAIM的Redis（至少6.2）。README和样例包含批量/队列参数。完整回归结果以`reports/pressure-validation.json`为准，历史报告不代表最新源码状态。

最终完整回归：115项自动化测试零跳过，38项HTTP、12项浏览器、6项迁移、实际TUICodingAgent经网关访问真实模型的多轮/取消/再生成、Ruff及0.4.0构建全部通过。MySQL已恢复128MiB后运行该回归。本机及已检查WSL没有可用tmux，未完成终端视觉验收；未运行云端CI或完整生产部署。

技术依据：[MySQL慢日志](https://dev.mysql.com/doc/refman/8.4/en/slow-query-log.html)、[索引用途](https://dev.mysql.com/doc/refman/8.4/en/mysql-indexes.html)、[InnoDB分区与外键限制](https://dev.mysql.com/doc/refman/8.4/en/create-table-foreign-keys.html)、[Redis pipeline](https://redis.io/docs/latest/develop/using-commands/pipelining/)、[XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/)、[SQLAlchemy AUTOCOMMIT](https://docs.sqlalchemy.org/en/20/core/connections.html)。收益判断来自本项目实测，不由文档推定。
