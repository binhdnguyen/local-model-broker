# Local Model Broker

A dependency-free, localhost OpenAI-compatible broker that exposes one stable model ID while routing requests to prioritized local model servers.

```text
Clients ──► http://127.0.0.1:8879/v1 ──► :8888/v1 (preferred)
                     model: local-auto └─► :8880/v1 (fallback)
```

The broker lets Pi CLI, Hermes, CLIProxyAPI, and other OpenAI-compatible clients use `local-auto` without knowing which checkpoint or local server is currently active.

## Features

- Stable public model ID: `local-auto`
- Prioritized upstream discovery and automatic return to the preferred server
- Safe cross-port failover only when both servers advertise the exact same model and sufficient capacity
- Hot model rediscovery for model-specific 404 responses
- Live context-window and output-token metadata
- Request token-limit clamping to the active model's capacity
- JSON and SSE model-field rewriting without changing generated content
- Streaming without buffering the full response
- Downstream cancellation propagated to the upstream request
- Conservative metadata when an upstream omits nonstandard context fields
- No Python runtime dependencies outside the standard library
- Optional Pi provider extension and hardened systemd user-service template

## Requirements

- Python 3.11 or newer
- OpenAI-compatible local model server on one or both upstream ports
- Node.js 22 or newer only for Pi extension tests
- systemd user services only if using the included unit template

Default addresses:

| Role | Address |
|---|---|
| Public broker | `http://127.0.0.1:8879/v1` |
| Preferred upstream | `http://127.0.0.1:8888/v1` |
| Fallback upstream | `http://127.0.0.1:8880/v1` |
| Public model | `local-auto` |

## Quick Start

Clone the private repository into the path expected by the included systemd unit:

```bash
git clone https://github.com/binhdnguyen/local-model-broker.git \
  ~/.local/share/local-model-broker
cd ~/.local/share/local-model-broker
```

Start the broker directly:

```bash
python3 broker.py \
  --host 127.0.0.1 \
  --port 8879 \
  --alias local-auto \
  --upstream http://127.0.0.1:8888/v1 \
  --upstream http://127.0.0.1:8880/v1
```

Check the catalog:

```bash
curl -sS http://127.0.0.1:8879/v1/models
```

Send a completion:

```bash
curl -sS http://127.0.0.1:8879/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "local-auto",
    "messages": [{"role": "user", "content": "Reply only OK"}],
    "max_tokens": 16
  }'
```

The response's top-level `model` field remains `local-auto`, regardless of the upstream checkpoint name.

## Install as a systemd User Service

The template assumes the repository is located at:

```text
~/.local/share/local-model-broker
```

Install and enable it:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/local-model-broker.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now local-model-broker.service
```

Inspect the service:

```bash
systemctl --user status local-model-broker.service --no-pager
journalctl --user -u local-model-broker.service -n 100 --no-pager
```

After changing broker code:

```bash
systemctl --user restart local-model-broker.service
```

## Pi CLI Integration

Install the included dynamic provider extension:

```bash
mkdir -p ~/.pi/agent/extensions
cp extensions/local-auto-provider.ts ~/.pi/agent/extensions/
```

The extension:

- registers provider and model ID `local-auto`;
- reads live model metadata from the broker;
- refreshes metadata before each active `local-auto` run;
- rebinds the Pi model only when metadata changes;
- maps DeepSeek and Qwen reasoning formats;
- uses conservative `16,384` context and `4,096` output-token limits if metadata is unavailable.

Smoke-test Pi:

```bash
pi --provider local-auto --model local-auto \
  --thinking off --no-tools --no-session \
  -p 'Reply only OK'
```

## Other Clients

Any OpenAI-compatible client can use:

```text
Base URL: http://127.0.0.1:8879/v1
API key:  any non-empty local value
Model:    local-auto
```

For CLIProxyAPI, register `local-auto` as a text-only OpenAI-compatible model whose base URL is the broker endpoint. Hermes custom providers can point directly to the same base URL.

## Routing and Safety Contracts

### Upstream selection

1. Prefer the first healthy upstream in configured order.
2. Use the fallback when the preferred server is unavailable.
3. Re-check priority after the discovery TTL and return to the preferred server when it recovers.
4. Preserve a sticky selection only within the discovery TTL.

### Failover

Transparent retry is restricted to inference routes:

- `/v1/chat/completions`
- `/v1/completions`
- `/v1/responses`
- `/v1/embeddings`

The broker does not replay arbitrary non-idempotent API operations such as file or batch creation after an ambiguous failure.

Cross-port failover is allowed only when the replacement advertises:

- the exact same model ID;
- at least the original context capacity;
- at least the original output-token capacity.

Otherwise, the broker returns HTTP `409` with error code `local_model_changed`.

### Model rediscovery

A 404 triggers model rediscovery only when the upstream explicitly reports a missing model. Unrelated resource 404 responses are returned unchanged.

### Response rewriting

The broker rewrites only top-level OpenAI protocol `model` fields. It does not alter:

- generated assistant content;
- nested application metadata;
- tool output;
- arbitrary user-defined JSON.

Body validators such as `Digest`, `Content-MD5`, and `ETag` are removed when the response body is rewritten.

### Cancellation

A downstream EOF or TCP FIN is treated as cancellation. The broker cancels the active upstream task and closes its connection, including:

- before upstream response headers arrive;
- during a silent SSE interval.

Clients must not half-close their write side while continuing to wait for a response.

## Configuration

```text
usage: broker.py [-h] [--host HOST] [--port PORT] [--alias ALIAS]
                 [--upstream UPSTREAM] [--discovery-ttl SECONDS]
                 [--discovery-timeout SECONDS] [--request-timeout SECONDS]
                 [--log-level LEVEL]
```

Environment-variable defaults:

| Variable | Default |
|---|---|
| `LOCAL_MODEL_BROKER_HOST` | `127.0.0.1` |
| `LOCAL_MODEL_BROKER_PORT` | `8879` |
| `LOCAL_MODEL_BROKER_ALIAS` | `local-auto` |

Pass `--upstream` multiple times to define priority order.

## Testing

Run the complete Python unit and integration suite:

```bash
python3 -m unittest -v test_broker.py test_integration.py
```

Run the Pi provider tests:

```bash
node --experimental-strip-types --test test_local_auto_provider.mjs
```

Run syntax and service-template checks:

```bash
python3 -m py_compile broker.py test_broker.py test_integration.py
node --experimental-strip-types --check extensions/local-auto-provider.ts
systemd-analyze --user verify systemd/local-model-broker.service
```

The integration suite covers real HTTP sockets, SSE streaming, failover, preferred-server recovery, response framing, cancellation before headers, and cancellation during silent SSE periods.

## Repository Layout

```text
.
├── broker.py                         Broker and asyncio HTTP transport/server
├── extensions/
│   └── local-auto-provider.ts        Pi dynamic provider extension
├── systemd/
│   └── local-model-broker.service    Portable user-service template
├── test_broker.py                    Routing and protocol unit tests
├── test_integration.py               Real-socket integration tests
├── test_local_auto_provider.mjs      Pi extension tests
├── instrutions.md                    Maintainer and local deployment notes
├── pyproject.toml                    Python project metadata
└── uv.lock                           Reproducible empty dependency set
```

## Security

The default service binds only to `127.0.0.1`. Keep it local unless you add authentication, TLS, and network-level access controls. Client authorization headers are stripped before forwarding because the default upstreams are local model servers.
