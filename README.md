# Model Request Gateway

[![Backend checks](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml/badge.svg)](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml)

A Python gateway for coding agents and other clients sharing model endpoints. It provides application authentication, model access control, distributed concurrency limits, incremental SSE forwarding, and application-scoped request metadata.

Built with **FastAPI, HTTPX, MySQL, and Redis**. Supports a documented subset of the OpenAI-compatible Chat Completions API, including streaming tool calls and usage fields.

## Features

- **Shared admission control:** Redis Lua atomically acquires capacity-group and application/group leases across gateway processes. Leases renew during execution and expire after process termination.
- **Streaming lifecycle management:** bounded SSE parsing, first-frame and idle timeouts, a total deadline, and cancellation-safe HTTP connection cleanup.
- **Request tracking:** MySQL stores request status, timing, forwarded byte counts, and reported usage. Queries use application isolation, filters, and cursor pagination. Prompts and answers are not stored by default.
- **Bounded database work:** synchronous operations run in a dedicated executor with bounded admission and a queue timeout.
- **Overload protection:** per-process admission before authentication, bounded connection pools, API-key length checks, and a cumulative SSE byte budget.
- **Observability:** readiness and liveness endpoints, plus per-process Prometheus metrics protected by a separate monitoring key.
- **Browser console:** streamed generation, cancellation, request details, filters, pagination, and statistics. Credentials stay in page memory.
- **Administration:** CLI commands for application access, key rotation, disabling new requests, record reconciliation, and retention cleanup.

## Architecture

```mermaid
flowchart LR
  C[Agent sessions and clients] --> G[FastAPI gateway instances]
  G -->|Authorization and request metadata| M[MySQL]
  G -->|Atomic admission leases| R[Redis]
  G -->|HTTPX and incremental SSE| U[Model endpoints]
```

Database transactions are short and do not remain open during model generation. Admission leases remain owned by the request until streaming completes or cleanup runs. Failed or truncated streams are not recorded as successful answers.

## Quick start

Requires **Python 3.11+**, MySQL, Redis, and a reachable Chat Completions endpoint.

```bash
git clone https://github.com/tuanzirwar/model-request-gateway.git
cd model-request-gateway
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp gateway.example.yaml gateway.yaml
```

On Windows, activate the environment with `.venv\Scripts\Activate.ps1` and copy the configuration with `Copy-Item gateway.example.yaml gateway.yaml`.

Edit `gateway.yaml` to configure the database, Redis, model aliases, upstream endpoints, and concurrency limits. Set upstream credentials through the environment variable named by each model's `key_env`. Adapt or remove `request_defaults` when the upstream model does not support those parameters.

```bash
python -m alembic upgrade head
export GATEWAY_APP_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
python scripts/admin.py grant --app agent --models coding --concurrency 2
python -m uvicorn model_gateway.app:create_app --factory --host 127.0.0.1 --port 8788 --workers 2
```

Keep the generated application key in a password manager or secret store. Use it as the Bearer token for API requests or console login. Configuration and credentials are excluded from Git.

Open `http://127.0.0.1:8788/` for the console or `/docs` for the API explorer.

```bash
curl -N http://127.0.0.1:8788/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_APP_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"coding","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

Client model names refer to configured gateway aliases. For TUICodingAgent, set `base_url` to the complete `/v1/chat/completions` URL and `wire_api` to `chat_completions`.

## API

| Endpoint | Purpose |
|---|---|
| `GET /live` | Process liveness without database or Redis access |
| `GET /health` | MySQL and Redis coordination readiness; does not probe model availability |
| `GET /metrics` | Per-process Prometheus metrics; disabled without a monitoring key |
| `GET /v1/models` | Models authorized for the application |
| `POST /v1/chat/completions` | Streaming or non-streaming generation with an `X-Request-ID` header |
| `GET /requests` | Application-scoped cursor pagination with status/model/since filters |
| `GET /requests/{UUID}` | Request status, timing, bytes, and reported usage |
| `GET /stats?hours=24` | Application-scoped aggregates over a 1-168 hour window |

Admission rejection returns `429`; unavailable dependencies return `503`; first-frame or total deadline expiry returns `504`; upstream failures return `502`. Once streaming headers have been sent, errors end the stream through an SSE error or connection closure. The gateway does not emit a fake successful `[DONE]` or automatically retry partially delivered output.

## Deployment

`compose.yaml` provides development MySQL and Redis services. `compose.full.yaml` includes the database, Redis, a migration job, and a non-root gateway image. Copy `.env.example` to `.env`, replace the password placeholders, and configure `deploy/gateway.docker.yaml` before use. Database passwords in this Compose configuration must use URL-safe characters.

The complete Compose deployment has been configuration-checked but has not been run end to end. GitHub CI runs MySQL and Redis service containers and verifies migrations, tests, linting, and package builds.

For monitoring, set `GATEWAY_METRICS_KEY` to a separate random key of at least 32 characters. Application keys cannot access metrics.

## Testing

```bash
export GATEWAY_TEST_MYSQL='mysql+pymysql://USER:PASSWORD@127.0.0.1:3306/model_gateway_test'
export GATEWAY_TEST_REDIS='redis://127.0.0.1:6379/0'
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m build
```

Use a dedicated test database and Redis instance. Dependency integration tests are skipped when their test environment variables are absent.

Current **v0.5.0** local acceptance passes **122 automated tests** with real MySQL/Redis and no skips, **40 HTTP checks**, **12 browser checks**, **7 migration checks**, an isolated Redis kill/restart test, long-stream cancellation, real TUICodingAgent/model integration, lint, and package builds. Cloud CI has not been rerun for this local revision.

A frozen-source 100,000-request run recorded **416.15 successful QPS**, **100% HTTP 200**, **p95 139.72 ms / p99 193.38 ms**: four gateway processes, 32 closed-loop clients, a 128 MiB MySQL buffer pool, and a 198-byte controlled JSON upstream with **no inference**. All persisted benchmark requests reached terminal success. This is not model throughput, production maximum capacity, or public-network bandwidth.

Retention lookup on approximately 1.14 million synthetic rows changed from **2797.93 ms** to **0.83 ms** mean after adding `(finished_at,id)`, with matching ordered IDs and automatic index selection. Bounded deletion, concurrent locked-row skipping and separate index-write-cost measurements are recorded.

## Coordination and failure boundaries

Aliases sharing physical capacity can set the same `capacity_group`; group limits must match. Redis Lua checks both group and application/group occupancy atomically. MySQL persists namespace policy fingerprints; Redis epochs fence old lease renewal. Losing coordination state for an existing namespace blocks admission for the total request budget plus five seconds. Deploy policy changes by draining old instances and choosing a new namespace; do not delete live keys. Use a dedicated Redis instance with `noeviction`.

The runtime uses bounded **in-process batching**, not a message broker. Matched Streams experiments showed no benefit and the experimental runtime was removed. Redis lease expiration and SQL execution/final timestamps have different meanings; no distributed transaction or remote GPU cancellation guarantee is claimed.

## Technical documentation

- [Architecture and component decisions](docs/design.md)
- [Latest verification and reproduction](docs/verification.md)
- [Optimization journal, including no-benefit results](docs/optimization-journal.md)
- [Operations and demo](docs/runbook.md)
- [Acceptance checklist](checklist.md)

Reports preserve earlier failures and historical versions. Historical 0.4 Streams and temporary larger-buffer measurements are not current runtime results. Study slides and resume/interview material are kept outside this repository.

### Statistics query optimization

On the same 100,096 application records, a virtual BIGINT usage column and an application/time covering index reduced the three-round mean statistics SQL time from 10,388.40 ms to 97.40 ms with identical results. The real HTTP statistics endpoint returned 200 in 171.34 ms. Index insert costs were measured separately; see `docs/optimization-journal.md` and `reports/v05-statistics-index.json`.

### Statistics query optimization

On the same 100,096 application records, a virtual BIGINT usage column and an application/time covering index reduced the three-round mean statistics SQL time from 10,388.40 ms to 97.40 ms with identical results. The real HTTP statistics endpoint returned 200 in 171.34 ms. Index insert costs were measured separately; see `docs/optimization-journal.md` and `reports/v05-statistics-index.json`.
