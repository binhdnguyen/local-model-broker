from __future__ import annotations

import asyncio
from collections.abc import Iterator
import json
import socket
import struct
import threading
import unittest
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

import broker as broker_module
from broker import Broker, BrokerServer, Upstream, UrlLibTransport


class MockModelHandler(BaseHTTPRequestHandler):
    model_id = "vendor/secondary-model"
    context_window = 196608

    def do_GET(self) -> None:
        if self.path == "/v1/models":
            self._json(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": self.model_id,
                            "object": "model",
                            "max_model_len": self.context_window,
                        }
                    ],
                },
            )
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length))
        if self.path != "/v1/chat/completions":
            self._json(404, {"error": "not found"})
            return
        self._json(
            200,
            {
                "model": payload["model"],
                "choices": [{"message": {"role": "assistant", "content": "OK"}}],
            },
        )

    def _json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ControlledSSEHandler(BaseHTTPRequestHandler):
    model_id = "deepseek-ai/cancel-model"

    def do_GET(self) -> None:
        if self.path != "/v1/models":
            self._json(404, {"error": "not found"})
            return
        self._json(
            200,
            {
                "object": "list",
                "data": [
                    {
                        "id": self.model_id,
                        "object": "model",
                        "max_model_len": 65536,
                    }
                ],
            },
        )

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        self.server.request_received.set()  # type: ignore[attr-defined]
        try:
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("connection", "close")
            self.end_headers()
            self.wfile.write(
                b'data: {"model":"deepseek-ai/cancel-model","choices":[{"delta":{"content":"chunk-1"}}]}\n\n'
            )
            self.wfile.flush()
            self.server.first_chunk_sent.set()  # type: ignore[attr-defined]

            if not self.server.allow_terminal.wait(timeout=5):
                return

            self.wfile.write(
                b'data: {"model":"deepseek-ai/cancel-model","choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
                b"data: [DONE]\n\n"
            )
            self.wfile.flush()
            self.server.stream_completed.set()  # type: ignore[attr-defined]
        except (ConnectionError, OSError):
            self.server.upstream_broken.set()  # type: ignore[attr-defined]
        finally:
            self.server.upstream_closed.set()  # type: ignore[attr-defined]

    def _json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ControlledSSEServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), ControlledSSEHandler)
        self.request_received = threading.Event()
        self.first_chunk_sent = threading.Event()
        self.allow_terminal = threading.Event()
        self.stream_completed = threading.Event()
        self.upstream_broken = threading.Event()
        self.upstream_closed = threading.Event()


class CancellationModelHandler(BaseHTTPRequestHandler):
    model_id = "deepseek-ai/cancel-model"

    def do_GET(self) -> None:
        if self.path != "/v1/models":
            self._json(404, {"error": "not found"})
            return
        self._json(
            200,
            {
                "object": "list",
                "data": [
                    {
                        "id": self.model_id,
                        "object": "model",
                        "max_model_len": 65536,
                    }
                ],
            },
        )

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        self.server.request_received.set()  # type: ignore[attr-defined]
        if self.server.send_sse:  # type: ignore[attr-defined]
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("connection", "close")
            self.end_headers()
            self.wfile.write(
                b'data: {"model":"deepseek-ai/cancel-model","choices":[]}\n\n'
            )
            self.wfile.flush()
        try:
            while self.connection.recv(1):
                pass
        except (ConnectionError, OSError):
            pass
        finally:
            self.server.upstream_closed.set()  # type: ignore[attr-defined]

    def _json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class CancellationServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, send_sse: bool) -> None:
        super().__init__(("127.0.0.1", 0), CancellationModelHandler)
        self.send_sse = send_sse
        self.request_received = threading.Event()
        self.upstream_closed = threading.Event()

class BrokerIntegrationTests(unittest.TestCase):
    def test_http_broker_uses_secondary_and_rewrites_live_model(self) -> None:
        upstream_server = ThreadingHTTPServer(("127.0.0.1", 0), MockModelHandler)
        upstream_thread = threading.Thread(target=upstream_server.serve_forever, daemon=True)
        upstream_thread.start()
        secondary_port = upstream_server.server_address[1]

        broker = Broker(
            upstreams=(
                Upstream("down-primary", "http://127.0.0.1:1/v1"),
                Upstream("secondary", f"http://127.0.0.1:{secondary_port}/v1"),
            ),
            transport=UrlLibTransport(),
            alias="local-auto",
            discovery_ttl=0,
        )
        broker_server = BrokerServer(("127.0.0.1", 0), broker)
        broker_thread = threading.Thread(target=broker_server.serve_forever, daemon=True)
        broker_thread.start()
        broker_port = broker_server.server_address[1]

        try:
            with urlopen(f"http://127.0.0.1:{broker_port}/v1/models", timeout=5) as response:
                catalog = json.loads(response.read())
            request = Request(
                f"http://127.0.0.1:{broker_port}/v1/chat/completions",
                data=json.dumps(
                    {
                        "model": "local-auto",
                        "messages": [{"role": "user", "content": "Reply only OK"}],
                    }
                ).encode(),
                headers={"content-type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=5) as response:
                completion = json.loads(response.read())

            self.assertEqual(catalog["data"][0]["id"], "local-auto")
            self.assertEqual(catalog["data"][0]["root"], "vendor/secondary-model")
            self.assertEqual(catalog["data"][0]["max_model_len"], 196608)
            self.assertEqual(completion["model"], "local-auto")
            self.assertEqual(completion["choices"][0]["message"]["content"], "OK")
        finally:
            broker_server.shutdown()
            broker_server.server_close()
            upstream_server.shutdown()
            upstream_server.server_close()



class BrokerCancellationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.upstream: CancellationServer | None = None
        self.broker_server: BrokerServer | None = None

    def tearDown(self) -> None:
        if self.broker_server is not None:
            self.broker_server.shutdown()
            self.broker_server.server_close()
        if self.upstream is not None:
            self.upstream.shutdown()
            self.upstream.server_close()

    def _start_servers(self, *, send_sse: bool) -> tuple[CancellationServer, int]:
        upstream = CancellationServer(send_sse)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        upstream_port = upstream.server_address[1]
        broker = Broker(
            upstreams=(
                Upstream("upstream", f"http://127.0.0.1:{upstream_port}/v1"),
            ),
            transport=UrlLibTransport(),
            alias="local-auto",
            discovery_ttl=0,
            request_timeout=30,
        )
        broker_server = BrokerServer(("127.0.0.1", 0), broker)
        threading.Thread(target=broker_server.serve_forever, daemon=True).start()
        self.upstream = upstream
        self.broker_server = broker_server
        return upstream, broker_server.server_address[1]

    @staticmethod
    def _request(broker_port: int) -> socket.socket:
        client = socket.create_connection(("127.0.0.1", broker_port), timeout=5)
        body = b'{"model":"local-auto","stream":true,"messages":[]}'
        client.sendall(
            b"POST /v1/chat/completions HTTP/1.1\r\n"
            b"host: 127.0.0.1\r\n"
            b"content-type: application/json\r\n"
            + f"content-length: {len(body)}\r\n".encode()
            + b"connection: close\r\n\r\n"
            + body
        )
        return client

    def test_client_write_half_close_does_not_cancel_sse_response(self) -> None:
        upstream = ControlledSSEServer()
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        upstream_port = upstream.server_address[1]
        broker = Broker(
            upstreams=(
                Upstream("upstream", f"http://127.0.0.1:{upstream_port}/v1"),
            ),
            transport=UrlLibTransport(),
            alias="local-auto",
            discovery_ttl=0,
            request_timeout=30,
        )
        broker_server = BrokerServer(("127.0.0.1", 0), broker)
        threading.Thread(target=broker_server.serve_forever, daemon=True).start()
        self.broker_server = broker_server
        self.upstream = upstream

        broker_port = broker_server.server_address[1]
        client = self._request(broker_port)
        client.settimeout(5)

        self.assertTrue(upstream.first_chunk_sent.wait(5), "upstream did not send first chunk")
        received = b""
        while b"chunk-1" not in received:
            chunk = client.recv(4096)
            if not chunk:
                break
            received += chunk
        self.assertIn(b"chunk-1", received)

        # Client half-closes the write direction
        client.shutdown(socket.SHUT_WR)

        # Allow upstream to send terminal event
        upstream.allow_terminal.set()

        while True:
            try:
                chunk = client.recv(4096)
                if not chunk:
                    break
                received += chunk
            except (ConnectionError, OSError):
                break
        client.close()

        self.assertTrue(upstream.stream_completed.is_set(), "upstream did not complete stream")
        self.assertFalse(upstream.upstream_broken.is_set(), "upstream connection was broken prematurely")
        self.assertIn(b'"finish_reason":"stop"', received)
        self.assertIn(b"data: [DONE]", received)

    @staticmethod
    def _reset_close(sock: socket.socket) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        sock.close()

    def test_client_disconnect_cancels_upstream_before_headers(self) -> None:
        upstream, broker_port = self._start_servers(send_sse=False)
        client = self._request(broker_port)
        self.assertTrue(upstream.request_received.wait(2), "upstream did not receive request")

        self._reset_close(client)

        self.assertTrue(
            upstream.upstream_closed.wait(2),
            "upstream remained connected after downstream canceled before headers",
        )

    def test_client_disconnect_cancels_silent_sse_upstream(self) -> None:
        upstream, broker_port = self._start_servers(send_sse=True)
        client = self._request(broker_port)
        client.settimeout(2)
        received = b""
        while b'data: {"model":"local-auto"' not in received:
            received += client.recv(4096)
        self.assertTrue(upstream.request_received.is_set())

        self._reset_close(client)

        self.assertTrue(
            upstream.upstream_closed.wait(2),
            "upstream remained connected after downstream canceled silent SSE",
        )


class BodylessResponseIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_204_response_omits_content_length(self) -> None:
        class BodylessBroker:
            async def handle_async(self, *_args: object) -> tuple[int, dict[str, str], bytes]:
                return 204, {"content-type": "application/json"}, b""

        server = await asyncio.start_server(
            lambda reader, writer: asyncio.create_task(
                broker_module._handle_connection(BodylessBroker(), reader, writer)
            ),
            "127.0.0.1",
            0,
        )
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(
                b"DELETE /v1/files/example HTTP/1.1\r\n"
                b"host: 127.0.0.1\r\n"
                b"connection: close\r\n\r\n"
            )
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), 2)
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

        head, _, body = response.partition(b"\r\n\r\n")
        self.assertTrue(head.startswith(b"HTTP/1.1 204"))
        self.assertNotIn(b"content-length:", head.lower())
        self.assertEqual(body, b"")

class ResponseWriteFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_response_write_failure_closes_upstream_body(self) -> None:
        class ClosableAsyncBody:
            def __init__(self) -> None:
                self.closed = False

            def __aiter__(self) -> ClosableAsyncBody:
                return self

            async def __anext__(self) -> bytes:
                return b"data: test\r\n\r\n"

            async def aclose(self) -> None:
                self.closed = True

        class FailingWriter:
            def write(self, _data: bytes) -> None:
                pass

            async def drain(self) -> None:
                raise BrokenPipeError("client write failed")

        body = ClosableAsyncBody()

        class StubBroker:
            async def handle_async(
                self, *_args: object
            ) -> tuple[int, dict[str, str], ClosableAsyncBody]:
                return 200, {"content-type": "text/event-stream"}, body

        await broker_module._serve_response(
            StubBroker(),  # type: ignore[arg-type]
            "POST",
            "/v1/chat/completions",
            {},
            None,
            FailingWriter(),  # type: ignore[arg-type]
        )

        self.assertTrue(body.closed, "upstream async body was not closed on write failure")

    async def test_response_write_failure_closes_sync_upstream_body(self) -> None:
        class ClosableSyncBody:
            def __init__(self) -> None:
                self.closed = False

            def __iter__(self) -> Iterator[bytes]:
                yield b"data: test\r\n\r\n"

            def close(self) -> None:
                self.closed = True

        class FailingWriter:
            def write(self, _data: bytes) -> None:
                pass

            async def drain(self) -> None:
                raise ConnectionResetError("connection reset by peer")

        body = ClosableSyncBody()

        class StubBroker:
            async def handle_async(
                self, *_args: object
            ) -> tuple[int, dict[str, str], ClosableSyncBody]:
                return 200, {"content-type": "text/event-stream"}, body

        await broker_module._serve_response(
            StubBroker(),  # type: ignore[arg-type]
            "POST",
            "/v1/chat/completions",
            {},
            None,
            FailingWriter(),  # type: ignore[arg-type]
        )

        self.assertTrue(body.closed, "upstream sync body was not closed on write failure")


if __name__ == "__main__":
    unittest.main()
