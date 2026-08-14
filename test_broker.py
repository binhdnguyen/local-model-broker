from __future__ import annotations

import io
import json
import unittest
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError

from broker import Broker, Upstream, _response_chunks


@dataclass
class FakeResponse:
    status: int
    body: Any
    headers: dict[str, str]

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _size: int = -1) -> bytes:
        return self.body

    def getcode(self) -> int:
        return self.status


class FakeTransport:
    def __init__(self) -> None:
        self.catalogs: dict[str, list[dict[str, Any]] | Exception] = {}
        self.responses: dict[tuple[str, str], list[FakeResponse | HTTPError | URLError]] = {}
        self.requests: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def set_catalog(self, base_url: str, models: list[dict[str, Any]] | Exception) -> None:
        self.catalogs[base_url] = models

    def queue(
        self,
        base_url: str,
        path: str,
        response: FakeResponse | HTTPError | URLError,
    ) -> None:
        self.responses.setdefault((base_url, path), []).append(response)

    def fetch_catalog(self, upstream: Upstream, _timeout: float) -> list[dict[str, Any]]:
        result = self.catalogs.get(upstream.base_url, URLError("down"))
        if isinstance(result, Exception):
            raise result
        return result

    def request(
        self,
        upstream: Upstream,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
        _timeout: float,
    ) -> FakeResponse:
        self.requests.append((upstream.base_url, path, headers, body))
        queued = self.responses[(upstream.base_url, path)].pop(0)
        if isinstance(queued, Exception):
            raise queued
        return queued


class BrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = FakeTransport()
        self.broker = Broker(
            upstreams=(
                Upstream("primary", "http://127.0.0.1:8888/v1"),
                Upstream("secondary", "http://127.0.0.1:8880/v1"),
            ),
            transport=self.transport,
            alias="local-auto",
            discovery_ttl=0,
        )

    @staticmethod
    def model(model_id: str, max_model_len: int = 262144) -> dict[str, Any]:
        return {"id": model_id, "object": "model", "max_model_len": max_model_len}

    def test_models_exposes_stable_alias_with_live_context(self) -> None:
        self.transport.set_catalog(
            "http://127.0.0.1:8888/v1",
            [self.model("vendor/new-checkpoint", 524288)],
        )

        status, headers, body = self.broker.handle("GET", "/v1/models", {}, None)

        payload = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(payload["data"][0]["id"], "local-auto")
        self.assertEqual(payload["data"][0]["root"], "vendor/new-checkpoint")
        self.assertEqual(payload["data"][0]["max_model_len"], 524288)

    def test_model_detail_exposes_stable_alias_with_live_context(self) -> None:
        self.transport.set_catalog(
            "http://127.0.0.1:8888/v1",
            [self.model("vendor/new-checkpoint", 524288)],
        )

        status, _, body = self.broker.handle("GET", "/v1/models/local-auto", {}, None)

        payload = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(payload["id"], "local-auto")
        self.assertEqual(payload["root"], "vendor/new-checkpoint")
        self.assertEqual(payload["max_model_len"], 524288)

    def test_prefers_8888_and_keeps_sticky_upstream(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, [self.model("primary-model")])
        self.transport.set_catalog(secondary, [self.model("secondary-model")])
        self.transport.queue(primary, "/v1/chat/completions", FakeResponse(200, b'{"ok":1}', {}))
        self.transport.queue(primary, "/v1/chat/completions", FakeResponse(200, b'{"ok":2}', {}))

        first = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )
        second = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(first[0], 200)
        self.assertEqual(second[0], 200)
        self.assertEqual([request[0] for request in self.transport.requests], [primary, primary])

    def test_fails_over_to_8880_when_8888_is_down(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, URLError("down"))
        self.transport.set_catalog(secondary, [self.model("secondary-model")])
        self.transport.queue(secondary, "/v1/chat/completions", FakeResponse(200, b'{"ok":true}', {}))

        status, _, _ = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual(self.transport.requests[0][0], secondary)

    def test_returns_to_preferred_upstream_after_recovery(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, URLError("down"))
        self.transport.set_catalog(secondary, [self.model("shared-model")])
        self.transport.queue(
            secondary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"shared-model","choices":[]}', {}),
        )
        self.assertEqual(
            self.broker.handle(
                "POST",
                "/v1/chat/completions",
                {"content-type": "application/json"},
                b'{"model":"local-auto","messages":[]}',
            )[0],
            200,
        )

        self.transport.set_catalog(primary, [self.model("shared-model")])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"shared-model","choices":[]}', {}),
        )

        status, _, _ = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual([request[0] for request in self.transport.requests], [secondary, primary])

    def test_request_connection_failure_fails_over_to_other_port(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, [self.model("primary-model")])
        self.transport.set_catalog(secondary, [self.model("primary-model")])
        self.transport.queue(primary, "/v1/chat/completions", URLError("connection reset"))
        self.transport.queue(
            secondary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"primary-model","choices":[]}', {}),
        )

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual([request[0] for request in self.transport.requests], [primary, secondary])
        self.assertEqual(json.loads(body)["model"], "local-auto")

    def test_transient_http_failure_fails_over_to_other_port(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, [self.model("primary-model")])
        self.transport.set_catalog(secondary, [self.model("primary-model")])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(503, b'{"error":"loading"}', {}),
        )
        self.transport.queue(
            secondary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"primary-model","choices":[]}', {}),
        )

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual([request[0] for request in self.transport.requests], [primary, secondary])
        self.assertEqual(json.loads(body)["model"], "local-auto")

    def test_non_inference_post_is_not_replayed_after_connection_failure(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        model = self.model("shared-model")
        self.transport.set_catalog(primary, [model])
        self.transport.set_catalog(secondary, [model])
        self.transport.queue(primary, "/v1/files", URLError("connection reset"))

        status, _, body = self.broker.handle(
            "POST",
            "/v1/files",
            {"content-type": "application/json"},
            b'{"purpose":"assistants"}',
        )

        self.assertEqual(status, 502)
        self.assertEqual(json.loads(body)["error"]["code"], "upstream_error")
        self.assertEqual([request[0] for request in self.transport.requests], [primary])

    def test_rejects_incompatible_model_during_in_flight_failover(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, [self.model("deepseek-ai/model-a", 262144)])
        self.transport.set_catalog(secondary, [self.model("unsloth/Qwen-model-b", 262144)])
        self.transport.queue(primary, "/v1/chat/completions", URLError("connection reset"))

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "local_model_changed")
        self.assertEqual([request[0] for request in self.transport.requests], [primary])

    def test_rejects_same_family_different_model_during_cross_port_failover(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        self.transport.set_catalog(primary, [self.model("deepseek-ai/model-a")])
        self.transport.set_catalog(secondary, [self.model("deepseek-ai/model-b")])
        self.transport.queue(primary, "/v1/chat/completions", URLError("connection reset"))

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body)["error"]["code"], "local_model_changed")
        self.assertEqual([request[0] for request in self.transport.requests], [primary])

    def test_rewrites_request_and_response_model(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        self.transport.set_catalog(primary, [self.model("vendor/new-checkpoint")])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(
                200,
                b'{"model":"served-canonical-name","metadata":{"model":"user-defined"},"choices":[{"message":{"content":"vendor/new-checkpoint"}}]}',
                {
                    "content-type": "application/json",
                    "content-md5": "stale-md5",
                    "digest": "sha-256=stale",
                    "etag": "stale-etag",
                },
            ),
        )

        status, response_headers, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {
                "content-type": "application/json",
                "authorization": "Bearer client-key",
                "accept-encoding": "gzip, deflate, br",
            },
            b'{"model":"local-auto","messages":[]}',
        )

        sent = json.loads(self.transport.requests[0][3] or b"{}")
        received = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(sent["model"], "vendor/new-checkpoint")
        self.assertEqual(received["model"], "local-auto")
        self.assertEqual(received["choices"][0]["message"]["content"], "vendor/new-checkpoint")
        self.assertEqual(received["metadata"]["model"], "user-defined")
        self.assertNotIn("content-md5", response_headers)
        self.assertNotIn("digest", response_headers)
        self.assertNotIn("etag", response_headers)
        self.assertNotIn("authorization", self.transport.requests[0][2])
        self.assertNotIn("accept-encoding", self.transport.requests[0][2])

    def test_connection_named_headers_are_not_forwarded(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        self.transport.set_catalog(primary, [self.model("shared-model")])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(
                200,
                b'{"model":"shared-model","choices":[]}',
                {
                    "content-type": "application/json",
                    "connection": "x-response-internal",
                    "x-response-internal": "secret",
                },
            ),
        )

        status, response_headers, _ = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {
                "content-type": "application/json",
                "connection": "x-request-internal",
                "x-request-internal": "secret",
            },
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertNotIn("x-request-internal", self.transport.requests[0][2])
        self.assertNotIn("connection", self.transport.requests[0][2])
        self.assertNotIn("x-response-internal", response_headers)
        self.assertNotIn("connection", response_headers)

    def test_clamps_client_output_limit_to_live_model_capacity(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        self.transport.set_catalog(primary, [self.model("vendor/smaller-model", 65536)])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"vendor/smaller-model","choices":[]}', {}),
        )

        status, _, _ = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","max_tokens":131072,"max_completion_tokens":32768,"max_output_tokens":50000,"messages":[]}',
        )

        sent = json.loads(self.transport.requests[0][3] or b"{}")
        self.assertEqual(status, 200)
        self.assertEqual(sent["max_tokens"], 16384)
        self.assertEqual(sent["max_completion_tokens"], 16384)
        self.assertEqual(sent["max_output_tokens"], 16384)

    def test_catalog_advertises_derived_output_limit(self) -> None:
        self.transport.set_catalog(
            "http://127.0.0.1:8888/v1",
            [self.model("vendor/smaller-model", 65536)],
        )

        status, _, body = self.broker.handle("GET", "/v1/models", {}, None)

        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"][0]["max_output_tokens"], 16384)

    def test_catalog_uses_conservative_limits_without_context_metadata(self) -> None:
        self.transport.set_catalog(
            "http://127.0.0.1:8888/v1",
            [{"id": "vendor/minimal-model", "object": "model"}],
        )

        status, _, body = self.broker.handle("GET", "/v1/models", {}, None)

        model = json.loads(body)["data"][0]
        self.assertEqual(status, 200)
        self.assertEqual(model["max_model_len"], 16384)
        self.assertEqual(model["max_output_tokens"], 4096)

    def test_404_invalidates_catalog_and_retries_once(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        catalogs = iter(
            [
                [self.model("deepseek-ai/old-model")],
                [self.model("deepseek-ai/new-model")],
            ]
        )

        def fetch_catalog(_upstream: Upstream, _timeout: float) -> list[dict[str, Any]]:
            return next(catalogs)

        self.transport.fetch_catalog = fetch_catalog  # type: ignore[method-assign]
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            HTTPError(primary, 404, "missing", {}, io.BytesIO(b'{"error":{"message":"The model deepseek-ai/old-model does not exist"}}')),
        )
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"deepseek-ai/new-model","choices":[]}', {"content-type": "application/json"}),
        )

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        sent_models = [json.loads(request[3] or b"{}")["model"] for request in self.transport.requests]
        self.assertEqual(status, 200)
        self.assertEqual(sent_models, ["deepseek-ai/old-model", "deepseek-ai/new-model"])
        self.assertEqual(json.loads(body)["model"], "local-auto")

    def test_404_with_unchanged_catalog_falls_back_to_other_port(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        model = self.model("deepseek-ai/shared-model")
        self.transport.set_catalog(primary, [model])
        self.transport.set_catalog(secondary, [model])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            HTTPError(primary, 404, "missing", {}, io.BytesIO(b'{"error":{"message":"The model deepseek-ai/shared-model does not exist"}}')),
        )
        self.transport.queue(
            secondary,
            "/v1/chat/completions",
            FakeResponse(200, b'{"model":"deepseek-ai/shared-model","choices":[]}', {}),
        )

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual([request[0] for request in self.transport.requests], [primary, secondary])
        self.assertEqual(json.loads(body)["model"], "local-auto")

    def test_non_model_404_is_returned_without_failover(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        model = self.model("deepseek-ai/shared-model")
        self.transport.set_catalog(primary, [model])
        self.transport.set_catalog(secondary, [model])
        self.transport.queue(
            primary,
            "/v1/files/missing",
            FakeResponse(
                404,
                b'{"error":{"message":"File not found"}}',
                {"content-type": "application/json"},
            ),
        )

        status, _, body = self.broker.handle(
            "GET",
            "/v1/files/missing",
            {"accept": "application/json"},
            None,
        )

        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["message"], "File not found")
        self.assertEqual([request[0] for request in self.transport.requests], [primary])

    def test_resource_404_mentioning_model_is_not_retried(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        secondary = "http://127.0.0.1:8880/v1"
        model = self.model("deepseek-ai/shared-model")
        self.transport.set_catalog(primary, [model])
        self.transport.set_catalog(secondary, [model])
        self.transport.queue(
            primary,
            "/v1/files/missing",
            FakeResponse(
                404,
                b'{"error":{"message":"File not found for model deepseek-ai/shared-model"}}',
                {"content-type": "application/json"},
            ),
        )

        status, _, body = self.broker.handle(
            "GET",
            "/v1/files/missing",
            {"accept": "application/json"},
            None,
        )

        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["error"]["message"], "File not found for model deepseek-ai/shared-model")
        self.assertEqual([request[0] for request in self.transport.requests], [primary])

    def test_streaming_chunks_rewrite_model_without_buffering_protocol(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        self.transport.set_catalog(primary, [self.model("vendor/live")])
        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(
                200,
                b'data: {"model":"vendor/live","choices":[{"delta":{"content":"vendor/live"}}]}\n\ndata: [DONE]\n\n',
                {"content-type": "text/event-stream"},
            ),
        )

        status, headers, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","stream":true,"messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "text/event-stream")
        self.assertIn(b'"model":"local-auto"', body)
        self.assertIn(b'"content":"vendor/live"', body)
        self.assertTrue(body.endswith(b"data: [DONE]\n\n"))

    def test_streaming_response_is_lazy_and_rewrites_split_model_id(self) -> None:
        primary = "http://127.0.0.1:8888/v1"
        self.transport.set_catalog(primary, [self.model("vendor/live")])
        consumed: list[str] = []

        def chunks():
            consumed.append("started")
            yield b'data: {"model":"vendor/'
            consumed.append("second")
            yield b'live","choices":[]}\n\n'

        self.transport.queue(
            primary,
            "/v1/chat/completions",
            FakeResponse(200, chunks(), {"content-type": "text/event-stream"}),
        )

        status, _, body = self.broker.handle(
            "POST",
            "/v1/chat/completions",
            {"content-type": "application/json"},
            b'{"model":"local-auto","stream":true,"messages":[]}',
        )

        self.assertEqual(status, 200)
        self.assertEqual(consumed, [])
        self.assertEqual(b"".join(body), b'data: {"model":"local-auto","choices":[]}\n\n')
        self.assertEqual(consumed, ["started", "second"])

    def test_response_chunks_prefers_read1_for_incremental_delivery(self) -> None:
        class IncrementalResponse:
            def __init__(self) -> None:
                self.chunks = iter((b"first", b"second", b""))
                self.closed = False

            def read(self, _size: int) -> bytes:
                raise AssertionError("read() may buffer an SSE response")

            def read1(self, _size: int) -> bytes:
                return next(self.chunks)

            def close(self) -> None:
                self.closed = True

        response = IncrementalResponse()

        self.assertEqual(list(_response_chunks(response)), [b"first", b"second"])
        self.assertTrue(response.closed)

    def test_returns_503_when_no_upstream_is_ready(self) -> None:
        self.transport.set_catalog("http://127.0.0.1:8888/v1", URLError("down"))
        self.transport.set_catalog("http://127.0.0.1:8880/v1", URLError("down"))

        status, _, body = self.broker.handle("GET", "/v1/models", {}, None)

        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body)["error"]["code"], "local_model_unavailable")


if __name__ == "__main__":
    unittest.main()
