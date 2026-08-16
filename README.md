# Local Model Broker

**Tired of editing config files every time you swap a local model?**

Pi, OpenCode, Claude Code, Cline, Roocode, Continue, Aider — how many local AI agents do you run?

Every time you change a checkpoint, you have to re-configure all of them. That's 5–6 agents × N configs each × 1 model swap = a lot of wasted keystrokes.

Stop hardcoding model IDs into dozens of agent configs. Declare your local model servers once, and point everything at `local-auto`.

```text
Clients ──► http://127.0.0.1:8879/v1 ──► :8888/v1 (preferred)
                     model: local-auto └─► :8880/v1 (fallback)
```

The broker is a dependency-free, localhost OpenAI-compatible proxy that exposes **one stable model ID** (`local-auto`) while routing requests to your prioritized local model servers. Use it from Pi CLI, Hermes, OpenCode, Claude Code, Cline, Roocode, Continue, Aider, CLIProxyAPI, llama.cpp, vLLM, Ollama — anything that speaks OpenAI protocol on the wire.

---

## What makes this different?

| Problem | Broker fix |
|---|---|
| You swap a checkpoint and now every agent config needs updating | **Your agents never know the model changed** — just restart the broker |
| Your primary model server crashes mid-session | **Automatic failover** to the fallback — but only if it's running the *same* model with *enough* context/output capacity |
| You juggle 5–6 agents and they all need different model IDs | **One alias for all of them** — point every agent at the same broker URL |
| An agent sends `deepseek-ai/DeepSeek-V4-Flash-0731` in the response but your Hermes profile expects `Local` | **Response rewriting** — every top-level `model` field is replaced with `local-auto` without touching generated text |
| Your client token-limits are fixed at install time | **Live metadata clamping** — the broker reads the active model's context window and output-token limit fresh on every discovery, and clamps your request so it never exceeds what the server can actually handle |
| You maintain a private fork of a model server with minor changes | **No maintenance** — the broker is <500 lines of stdlib Python, zero pip dependencies |
| The upstream goes silent for 30 seconds | **Cancellation propagates** — close your client and the upstream connection drops immediately |

---

## Quick Start

```bash
git clone https://github.com/binhdnguyen/local-model-broker.git \
  ~/.local/share/local-model-broker
cd ~/.local/share/local-model-broker

python3 broker.py \
  --host 127.0.0.1 \
  --port 8879 \
  --alias local-auto \
  --upstream http://127.0.0.1:8888/v1 \
  --upstream http://127.0.0.1:8880/v1
```

Verify it works:

```bash
curl -sS http://127.0.0.1:8879/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "local-auto",
    "messages": [{"role": "user", "content": "Reply only OK"}],
    "max_tokens": 16
  }'
```

The response's `model` field says `local-auto` — not whatever unwieldy checkpoint name sits upstream.  
From now on, `local-auto` is the only model ID your clients ever see.

---

## One alias for every client

```
http://127.0.0.1:8879/v1  ─  model: local-auto
```

| Client | Notes |
|---|---|
| Pi | Included extension auto-refreshes metadata |
| Hermes, OpenCode, Claude Code, Cline, Roo Code, Continue, Aider | Point to the broker URL with model `local-auto` |
| CLIProxyAPI | Register as OpenAI-compatible text model |
| llama.cpp / vLLM / Ollama | Also work as upstreams, not just clients |
| curl / any HTTP client | `"model": "local-auto"` in the request body |

Any non-empty API key works — the broker doesn't authenticate by default (binds to `127.0.0.1`).

---

## Features

- **One stable model ID forever** — never touch a client config again
- **Prioritized upstream discovery** — always prefers `:8888`, returns when it recovers
- **Safe cross-port failover** — checks model identity + capacity before redirecting; otherwise returns HTTP 409 `local_model_changed`
- **Hot model rediscovery** — a 404 triggers re-read of the catalog before giving up
- **Live metadata clamping** — context window and output-token limits fetched fresh from the active model, your request is clamped to match
- **Response rewriting** — JSON and SSE `model` fields rewritten to `local-auto` without touching generated content
- **Streaming without buffering** — no full-response buffering means low latency even for long generations
- **Cancellation propagation** — downstream disconnect kills the upstream request immediately
- **Zero pip dependencies** — pure Python 3.11+ stdlib, no `requirements.txt` needed
- **systemd user-service template** included — runs as `--user`, restarts after crash
- **Pi CLI extension** included — dynamic provider that refreshes metadata before each run

---

## Routing Safety Contracts

The broker doesn't blindly retry. It understands the difference between a transient network blip and a fundamental model change.

| Scenario | Behaviour |
|---|---|
| Preferred server down | Fall through to fallback |
| Both servers running different models | HTTP 409 — no silent model swap |
| Upstream returns 404 for model ID | Rediscover catalog and retry once |
| Upstream 404 for unrelated resource | Pass through unchanged |
| Client drops connection | Cancel upstream immediately (before headers or during SSE silence) |
| All upstreams fail | Return 502 with error code `no_backend_available` |

Transparent retry is restricted to inference routes (`/v1/chat/completions`, `/v1/completions`, `/v1/responses`, `/v1/embeddings`). Non-idempotent operations like file or batch creation are never replayed.

---

## Install as a systemd User Service

```bash
mkdir -p ~/.config/systemd/user
cp systemd/local-model-broker.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now local-model-broker.service
```

After changing broker code:

```bash
systemctl --user restart local-model-broker.service
```

---

## Pi CLI Integration

```bash
cp extensions/local-auto-provider.ts ~/.pi/agent/extensions/

pi --provider local-auto --model local-auto \
  --thinking off --no-tools --no-session \
  -p 'Reply only OK'
```

The extension: registers the provider, reads live metadata from the broker, refreshes before each run, and rebinds Pi only when metadata changes. It maps DeepSeek and Qwen reasoning formats. Falls back to 16K context / 4K output if metadata is missing.

---

## Configuration

```
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

Pass `--upstream` multiple times for priority order.

---

## Testing

```bash
python3 -m unittest -v test_broker.py test_integration.py
node --experimental-strip-types --test test_local_auto_provider.mjs
```

Covers real HTTP sockets, SSE streaming, failover, recovery, response framing, and cancellation at various stages.

---

## Repository Layout

```
.
├── broker.py                         Broker and asyncio HTTP transport/server
├── extensions/
│   └── local-auto-provider.ts        Pi dynamic provider extension
├── systemd/
│   └── local-model-broker.service    Portable user-service template
├── test_broker.py                    Routing and protocol unit tests
├── test_integration.py               Real-socket integration tests
├── test_local_auto_provider.mjs      Pi extension tests
├── pyproject.toml                    Python project metadata
└── uv.lock                           Reproducible empty dependency set
```

---

## Security

Binds to `127.0.0.1` by default. Keep it there unless you add authentication, TLS, and network-level access controls. Authorization headers are stripped before forwarding to local upstreams.

---

## License

MIT
