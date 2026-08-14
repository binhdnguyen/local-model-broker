# Local Model Broker Instructions

## Location

```text
/home/binhdnguyen/.local/share/local-model-broker
```

This directory is the canonical source repository for the broker and its integration assets.

## Purpose

The broker exposes one stable OpenAI-compatible model:

- Endpoint: `http://127.0.0.1:8879/v1`
- Public model ID: `local-auto`
- Preferred upstream: `http://127.0.0.1:8888/v1`
- Fallback upstream: `http://127.0.0.1:8880/v1`

Hermes, Pi CLI, and CLIProxyAPI use `local-auto` instead of a checkpoint-specific model ID.

## Important Files

```text
broker.py                          Broker implementation
test_broker.py                    Unit tests
test_integration.py               HTTP and cancellation integration tests
test_local_auto_provider.mjs      Pi provider tests
extensions/local-auto-provider.ts Installable Pi provider extension
systemd/local-model-broker.service Portable user-service template
```

Runtime installations and client configuration live outside the repository:

```text
/home/binhdnguyen/.pi/agent/extensions/local-auto-provider.ts
/home/binhdnguyen/CLIProxyAPI/config.yaml
/home/binhdnguyen/.config/systemd/user/local-model-broker.service
/home/binhdnguyen/.hermes/config.yaml
/home/binhdnguyen/.hermes/profiles/tieu-ho/config.yaml
/home/binhdnguyen/.hermes/profiles/translator/config.yaml
```

## Development Rules

1. Use test-driven development for behavioral changes.
2. Preserve the public model ID `local-auto`.
3. Rewrite only top-level OpenAI protocol `model` fields, never generated content or arbitrary metadata.
4. Cross-port failover is allowed only when both upstreams advertise the exact same model ID and sufficient capacity.
5. Retry only inference routes; never replay arbitrary non-idempotent OpenAI API operations.
6. Rediscover on 404 only for a model-specific missing-model error.
7. Downstream EOF/FIN is treated as cancellation and promptly closes the upstream request. Clients must not half-close their write side while expecting a response.
8. Do not replace cancellation with a shorter request timeout.
9. Restart the service only after all tests and syntax checks pass.

## Run Tests

```bash
cd /home/binhdnguyen/.local/share/local-model-broker

/home/binhdnguyen/.local/bin/python3.11 -m unittest -v \
  test_broker.py test_integration.py

node --experimental-strip-types --test \
  test_local_auto_provider.mjs
```

## Syntax and Configuration Checks

```bash
cd /home/binhdnguyen/.local/share/local-model-broker

/home/binhdnguyen/.local/bin/python3.11 -m py_compile \
  broker.py test_broker.py test_integration.py

node --experimental-strip-types --check \
  extensions/local-auto-provider.ts

systemd-analyze --user verify \
  systemd/local-model-broker.service
```

## Service Commands

```bash
systemctl --user restart local-model-broker.service
systemctl --user status local-model-broker.service --no-pager
journalctl --user -u local-model-broker.service -n 100 --no-pager
```

## Live Checks

Catalog:

```bash
curl -sS http://127.0.0.1:8879/v1/models
```

Broker completion:

```bash
curl -sS http://127.0.0.1:8879/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"local-auto","messages":[{"role":"user","content":"Reply only OK"}]}'
```

Pi CLI:

```bash
pi --provider local-auto --model local-auto \
  --thinking off --no-tools --no-session \
  -p 'Reply only OK'
```

Hermes:

```bash
hermes -p tieu-ho --reasoning none -z 'Reply only OK'
```

## Completion Requirements

Before declaring the broker complete:

- All Python unit and integration tests pass.
- All Pi provider tests pass.
- Python and TypeScript syntax checks pass.
- YAML and JSON configurations parse successfully.
- `local-model-broker.service` is active after restart.
- `/v1/models` returns `id: local-auto` with live context and output limits.
- Broker, CLIProxyAPI, Hermes, and Pi smoke tests return a valid response.
