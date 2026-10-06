# Model Request Gateway

[![Backend checks](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml/badge.svg)](https://github.com/tuanzirwar/model-request-gateway/actions/workflows/ci.yaml)

A Python gateway for coding agents and other clients sharing model endpoints. It provides application authentication, model access control, distributed concurrency limits, incremental SSE forwarding, and application-scoped request metadata.

Built with **FastAPI, HTTPX, MySQL, and Redis**. Supports a documented subset of the OpenAI-compatible Chat Completions API, including streaming tool calls and usage fields.

## Features

- **Shared admission control:** Redis Lua atomically acquires model-wide and application/model leases across gateway processes. Leases renew during execution and expire after process termination.
- **Streaming lifecycle management:** bounded SSE parsing, first-frame and idle timeouts, a total deadline, and cancellation-safe HTTP connection cleanup.
- **Request tracking:** MySQL stores request status, timing, forwarded byte counts, and reported usage. Queries use application isolation, filters, and cursor pagination. Prompts and answers are not stored by default.
- **Bounded database work:** synchronous operations run in a dedicated executor with bounded admission and a queue timeout.
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
| `GET /health` | MySQL and Redis readiness; does not probe model availability |
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

Use a dedicated test database and Redis instance. Four dependency integration tests are skipped when the test environment variables are absent.

Recorded validation includes **59 automated tests**, **34 real HTTP acceptance checks**, **12 browser checks**, and **4 fresh-database migration checks**. Additional tests exercise TUICodingAgent with a real local model, including multiple turns, cancellation, and subsequent generation.

Load tests use real HTTP, MySQL, and Redis with a controlled upstream that does not perform inference. QPS results measure gateway request handling, not model generation throughput. Reports preserve initial failures and subsequent successful runs.

## Technical documentation

- [Architecture and design decisions](docs/design.md)
- [Operations, monitoring, and deployment](docs/runbook.md)
- [Validation methods and results](docs/verification.md)
- [Acceptance checklist](checklist.md)

Detailed technical documents are currently in Chinese. Console screenshots are available in [desktop](reports/console-desktop.png) and [mobile](reports/console-mobile.png) layouts.

## Scope

Concurrency leases control admission to the gateway; they do not guarantee that a remote model stops computing immediately after disconnect. Redis restart and failover consistency have not been validated. Prometheus metrics are per process.

The gateway does not implement GPU scheduling, billing, stream replay, generation queues, automatic provider failover, or Responses/Anthropic protocols. Model defaults are caller-overridable defaults, not enforced spending limits.
