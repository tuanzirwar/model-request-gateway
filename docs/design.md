# Architecture and decisions (0.5.0)

## Scope and evidence

TUICodingAgent sessions and sub-agents share a model endpoint. The gateway controls application access and concurrent admission, forwards Chat Completions, closes streams safely, and records application-scoped outcomes. Actual TUI multi-turn/cancel/regenerate tests verify this integration. Controlled HTTP fixtures isolate failures. No enterprise adoption or inference-speed improvement is claimed.

## Component responsibilities

| Component | Required responsibility | Alternative / boundary |
|---|---|---|
| FastAPI / Uvicorn | Async HTTP admission and streamed responses | Django could also implement this; the scope is model I/O |
| HTTPX | Shared upstream connection pool and explicit response close | No client per request; no transparent partial-stream retry |
| MySQL / SQLAlchemy / PyMySQL | Current authorization, request history, relational constraints and indexes | PostgreSQL is also viable; synchronous SQL runs in a dedicated bounded executor |
| Redis / Lua / ZSET | Atomic cross-process capacity-group and application-group leases | MySQL admission is possible; no measured claim that it is slower |
| Local batch queues | Amortize authentication, insert, update and Redis round trips | Volatile queues; each caller waits for confirmed SQL commit |
| Alembic | Explicit schema evolution | Web startup does not implicitly create or mutate tables |
| Prometheus client | Fixed-label, per-process stage metrics | No additional metrics database, hosted Grafana or high-cardinality identity labels |

Redis avoids per-heartbeat SQL transactions and represents individual expiring owners directly. This is an operational/semantic choice, not proof that a MySQL quota implementation would fail. Redis adds an availability dependency. A small single-process deployment could use a semaphore instead. Concurrency control is not requests-per-second or token-budget limiting.

The formal runtime has **no message broker**. Four matched local/Streams rounds did not show benefit. The synchronous model request has no durable offline execution requirement. A mature workflow platform may legitimately use a broker and stream output through Redis; this gateway does not implement that workflow architecture.

## Actual request order

1. Per-process admission and bounded authentication header; query MySQL for current enabled application.
2. Bounded body read (1 MiB, 10 seconds), parameter validation, authorized model alias lookup.
3. Create Execution UUID and immutable execution start/deadline. Verify namespace policy; Redis Lua atomically obtains both leases.
4. Immediately start heartbeat. Persist `running` with original timestamps and wait for SQL commit. Full capacity produces a `rejected` audit and 429. SQL failure cleans the lease without calling the model.
5. Use server-side URL/model/credentials in the shared HTTPX client. No SQL transaction remains open during generation.
6. Validate the first complete SSE frame before sending response headers. Forward each frame and tool fragment; save valid provider-reported usage. Non-streaming JSON is size-bounded.
7. Only explicit upstream `[DONE]` establishes successful stream completion. Commit the successful outcome before forwarding DONE. An error after headers produces an SSE error or EOF, not a changed HTTP status.
8. A single independent cleanup task reaps I/O, closes the upstream (3-second budget), releases the UUID lease, then saves outcome. Repeated cancellation does not interrupt ownership cleanup. Final HTTP termination has a 0.25-second best-effort send budget.
9. Periodic reconciliation (about 2 seconds, 256 records) marks expired unknown executions `abandoned`, not succeeded. It never replays generation.

## Capacity groups and configuration

`capacity_group` joins aliases sharing actual capacity; absent value defaults to alias. All aliases in a group require equal concurrency limits. Application limits apply per capacity group. This must be configured explicitly; identical URLs do not necessarily imply identical capacity.

MySQL `coordination_states` persists namespace/policy fingerprints. Redis guards hold policy, epoch and recovery-ready time. Conflicting process configurations fail closed. New namespaces can initialize immediately. Missing Redis guards for an existing namespace trigger `total_seconds + 5` recovery quarantine; old owners cannot renew in a new epoch. Redis kill/restart with loss has been exercised on an isolated instance.

Policy changes are deployment changes: stop/drain old instances, choose a new namespace, deploy consistently. Do not clear live quota keys or silently mutate active capacity limits. The guard is not a Redis HA/failover or remote GPU fencing solution. Use dedicated Redis with `noeviction`; partial key deletion/eviction can invalidate occupancy knowledge.

## Consistency and clocks

Redis lease expiration is renewable admission ownership. MySQL `deadline_at` is a fixed execution cutoff, and `finished_at` is an observed persisted outcome time. They are not duplicate fields that must be kept equal. No distributed transaction joins Redis and MySQL. Compensation, idempotent release, expiry, conditional SQL updates and reconciliation handle known failure windows; some failures remain unknown.

Known terminal outcomes cannot overwrite one another. A late explicitly observed terminal may replace provisional `abandoned`; late `running` cannot resurrect it. UTC seconds are stored as DOUBLE; monotonic time controls local duration, Redis TIME controls lease calculations. SQL reconciliation assumes synchronized host clocks.

## SQL and bounds

Applications use unique high-entropy key digests. Request UUIDs are primary keys and rows reference applications. Query methods always restrict app_id. Requests store only metadata and provider usage, not messages, tool arguments or answers. Cursor ordering is `(started_at DESC, id DESC)`, never UUID chronological order.

Indexes: `(app_id,started_at,id)`, `(app_id,status,started_at,id)`, `(app_id,model,started_at,id)`, `(status,deadline_at)`, `(finished_at,id)`. Filters combining model/status may still require residual filtering. Retention/reconciliation select bounded primary keys with READ COMMITTED and SKIP LOCKED, then delete/update short transactions.

Default resources per instance: four SQL threads / four write connections, two separate autocommit authentication connections, 32 SQL waiting slots, one-second queue timeout; local batch size 64 and queue capacity 256, zero deliberate collection delay. HTTPX max connections 100, Redis max connections 128, admission 128; request/non-streaming body 1 MiB, SSE frame 64 KiB, cumulative stream 16 MiB. SQL connect/read/write budgets are 3/5/5 seconds. Cancellation cannot stop a running SQL thread; shielded futures hold capacity until actual completion.

## Mature implementations consulted

- [LiteLLM limiter source](https://github.com/BerriAI/litellm/blob/2c67ae90bd1e920a71c32bb4b863363dd9c949c4/litellm/proxy/hooks/parallel_request_limiter_v3.py): atomic multiple-limit Redis admission using request owners. LiteLLM's primary SQL choice is PostgreSQL, not MySQL.
- [Dify Celery integration](https://github.com/langgenius/dify/blob/e7d9c8897a4ddf104396e4efc8c321e0f8075bac/api/extensions/ext_celery.py): background indexing/workflow jobs; Redis output channels and SQL history have different duties.
- [BiSheng workflow tasks](https://github.com/dataelement/bisheng/blob/cb9b77a89a914864d0f5c5e1051322d3e13bc4f0/src/backend/bisheng/worker/workflow/tasks.py): background workflow execution, not the same as this direct forwarding scope.

## Related merged contributions

[Xinference #5626](https://github.com/xorbitsai/inference/pull/5626) informs quota ownership and streaming handoff; [Dynamiq #971](https://github.com/dynamiq-ai/dynamiq/pull/971) informs close/aclose ownership; [txtai #1311](https://github.com/neuml/txtai/pull/1311) informs incremental-output tests; [RAG-Anything #376](https://github.com/HKUDS/RAG-Anything/pull/376) informs why single-consumption streaming objects are not complete-answer caches. These projects are not gateway dependencies, and their merged PRs do not certify this gateway.

## Intentionally outside scope

Billing/exactly-once charge records, QPS token buckets, durable generation queues, provider failover, HA, SQL sharding, vector retrieval, full-answer caching, administrator web UI, public ingress and GPU hard cancellation. No benchmark justifies adding these features now. Container recipes remain configuration-checked references, not verified deployment. Local tests and learning readiness are not production operations evidence.


## Verified statistics path

A real application with 100,096 requests exposed a five-second timeout in `/stats`. Migration 0006 adds a virtual BIGINT `usage_total_tokens`, computed from the provider usage JSON by the database, and covering index `(app_id,started_at,model,status,first_ms,finished_at,bytes_out,usage_total_tokens)`. The JSON remains authoritative; code does not dual-write a second token value. The derived field is internal, not returned in request details. Provider counters outside signed 64-bit range are omitted.

The covering scan avoids random primary-row/JSON reads; it still scans the relevant application/time window and uses a small temporary aggregate. It is not O(1), preaggregation, or a billing ledger. Group results were compared with the old expression, then the actual HTTP API was tested with the unchanged five-second driver timeout. The index's separate ABBA write-cost experiment and latest full-path benchmark include its maintenance cost.
