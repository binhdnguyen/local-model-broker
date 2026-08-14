from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
import socket
import ssl
import threading
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Protocol, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit

LOGGER = logging.getLogger("local-model-broker")
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
STRIPPED_REQUEST_HEADERS = HOP_BY_HOP_HEADERS | {
    "accept-encoding",
    "authorization",
    "content-length",
    "expect",
    "host",
}
STRIPPED_RESPONSE_HEADERS = HOP_BY_HOP_HEADERS | {"content-length", "server", "date"}
BODY_VALIDATOR_HEADERS = {
    "content-md5",
    "digest",
    "etag",
    "content-digest",
    "repr-digest",
}

ResponseBody = bytes | Iterable[bytes] | AsyncIterable[bytes]
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Upstream:
    name: str
    base_url: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))


@dataclass(frozen=True, slots=True)
class Selection:
    upstream: Upstream
    model: dict[str, Any]
    selected_at: float

    @property
    def model_id(self) -> str:
        return str(self.model["id"])


@dataclass(slots=True)
class TransportResponse:
    status: int
    body: ResponseBody
    headers: dict[str, str]

    async def aclose(self) -> None:
        await _close_body(self.body)


class Transport(Protocol):
    def fetch_catalog(
        self, upstream: Upstream, timeout: float
    ) -> list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]: ...

    def request(
        self,
        upstream: Upstream,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> TransportResponse | Awaitable[TransportResponse]: ...


class _AsyncHTTPBody:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
        timeout: float,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._headers = headers
        self._timeout = timeout
        self._started = False
        self._closed = False

    def __aiter__(self) -> AsyncIterator[bytes]:
        if self._started:
            raise RuntimeError("upstream response body can only be consumed once")
        self._started = True
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in _iter_http_body(self._reader, self._headers, self._timeout):
                yield chunk
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._writer.close()
        with suppress(ConnectionError, OSError):
            await self._writer.wait_closed()


class UrlLibTransport:
    """Dependency-free asynchronous HTTP transport.

    The legacy class name is retained because existing callers import it. All
    network operations are asyncio-native so downstream cancellation can cancel
    the upstream read in the same event loop.
    """

    async def fetch_catalog(self, upstream: Upstream, timeout: float) -> list[dict[str, Any]]:
        response = await self.request(
            upstream,
            "GET",
            "/v1/models",
            {"accept": "application/json"},
            None,
            timeout,
        )
        try:
            if response.status < 200 or response.status >= 300:
                raise ValueError(f"{upstream.name} model catalog returned HTTP {response.status}")
            body = await _body_bytes(response.body)
            payload = json.loads(body)
        finally:
            await _close_response(response)
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError(f"{upstream.name} returned no model list")
        return [model for model in data if isinstance(model, dict) and model.get("id")]

    async def request(
        self,
        upstream: Upstream,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> TransportResponse:
        parsed = urlsplit(upstream.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"Unsupported upstream URL: {upstream.base_url}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        tls: ssl.SSLContext | bool | None = None
        server_hostname: str | None = None
        if parsed.scheme == "https":
            tls = ssl.create_default_context()
            server_hostname = parsed.hostname

        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection(
                    parsed.hostname,
                    port,
                    ssl=tls,
                    server_hostname=server_hostname,
                )
                target = _upstream_target(parsed.path, path)
                request_headers = dict(headers)
                request_headers["host"] = parsed.netloc
                request_headers["connection"] = "close"
                if body is not None:
                    request_headers["content-length"] = str(len(body))
                writer.write(f"{method} {target} HTTP/1.1\r\n".encode("ascii"))
                for name, value in request_headers.items():
                    writer.write(f"{name}: {value}\r\n".encode("latin-1"))
                writer.write(b"\r\n")
                if body:
                    writer.write(body)
                await writer.drain()
                status, response_headers = await _read_response_head(reader)

                if method == "HEAD" or status in {204, 304} or 100 <= status < 200:
                    writer.close()
                    with suppress(ConnectionError, OSError):
                        await writer.wait_closed()
                    return TransportResponse(status, b"", response_headers)

                content_type = response_headers.get("content-type", "").lower()
                if content_type.startswith("text/event-stream"):
                    return TransportResponse(
                        status,
                        _AsyncHTTPBody(reader, writer, response_headers, timeout),
                        response_headers,
                    )

                response_body = await _collect_http_body(reader, response_headers, timeout)
                writer.close()
                with suppress(ConnectionError, OSError):
                    await writer.wait_closed()
                return TransportResponse(status, response_body, response_headers)
        except BaseException:
            if writer is not None:
                writer.close()
            raise


class Broker:
    def __init__(
        self,
        *,
        upstreams: tuple[Upstream, ...],
        transport: Transport,
        alias: str,
        discovery_ttl: float = 2.0,
        discovery_timeout: float = 1.5,
        request_timeout: float = 3600.0,
    ) -> None:
        if not upstreams:
            raise ValueError("at least one upstream is required")
        self._upstreams = upstreams
        self._transport = transport
        self._alias = alias
        self._discovery_ttl = discovery_ttl
        self._discovery_timeout = discovery_timeout
        self._request_timeout = request_timeout
        self._selection: Selection | None = None
        self._lock = threading.Lock()

    @property
    def request_timeout(self) -> float:
        return self._request_timeout

    def handle(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> tuple[int, dict[str, str], ResponseBody]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.handle_async(method, path, headers, body))
        raise RuntimeError("Broker.handle() cannot run inside an event loop; await handle_async()")

    async def handle_async(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> tuple[int, dict[str, str], ResponseBody]:
        route = urlsplit(path).path.rstrip("/") or "/"
        if method == "GET" and route == "/healthz":
            return await self._health()
        if method == "GET" and route == "/v1/models":
            return await self._models()
        if method == "GET" and route == f"/v1/models/{self._alias}":
            return await self._model_detail()
        if not route.startswith("/v1/"):
            return self._json_error(404, "not_found", f"Unsupported path: {route}")
        if method not in {"POST", "GET", "DELETE"}:
            return self._json_error(405, "method_not_allowed", f"Unsupported method: {method}")
        return await self._proxy(method, path, headers, body)

    async def _health(self) -> tuple[int, dict[str, str], bytes]:
        try:
            selection = await self._select()
        except LocalModelUnavailable as error:
            return self._json_error(503, "local_model_unavailable", str(error))
        return self._json(
            200,
            {
                "status": "ok",
                "alias": self._alias,
                "upstream": selection.upstream.base_url,
                "model": selection.model_id,
            },
        )

    async def _models(self) -> tuple[int, dict[str, str], bytes]:
        try:
            selection = await self._select()
        except LocalModelUnavailable as error:
            return self._json_error(503, "local_model_unavailable", str(error))
        return self._json(200, {"object": "list", "data": [self._public_model(selection)]})

    async def _model_detail(self) -> tuple[int, dict[str, str], bytes]:
        try:
            selection = await self._select()
        except LocalModelUnavailable as error:
            return self._json_error(503, "local_model_unavailable", str(error))
        return self._json(200, self._public_model(selection))

    async def _proxy(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes | None,
    ) -> tuple[int, dict[str, str], ResponseBody]:
        failed_upstreams: set[Upstream] = set()
        initial: Selection | None = None
        not_found: tuple[Upstream, str] | None = None
        retryable = _is_retryable_inference(method, path)

        for attempt in range(3):
            try:
                selection = await self._select(
                    force=attempt > 0,
                    exclude=frozenset(failed_upstreams),
                )
            except LocalModelUnavailable as error:
                return self._json_error(503, "local_model_unavailable", str(error))

            if not_found is not None:
                failed_upstream, failed_model = not_found
                if selection.upstream == failed_upstream and selection.model_id == failed_model:
                    failed_upstreams.add(selection.upstream)
                    self._invalidate(selection)
                    try:
                        selection = await self._select(
                            force=True,
                            exclude=frozenset(failed_upstreams),
                        )
                    except LocalModelUnavailable as error:
                        return self._json_error(503, "local_model_unavailable", str(error))
                not_found = None

            if initial is None:
                initial = selection
            elif selection.upstream != initial.upstream and not _models_compatible(
                initial, selection
            ):
                return self._json_error(
                    409,
                    "local_model_changed",
                    f"Local model changed from {initial.model_id} to {selection.model_id}; "
                    "refusing to fail over to an incompatible model",
                )

            request_connection_tokens = _connection_tokens(headers)
            upstream_headers = {
                key: value
                for key, value in ((name.lower(), value) for name, value in headers.items())
                if key not in STRIPPED_REQUEST_HEADERS
                and key not in request_connection_tokens
            }
            upstream_body = _rewrite_request_model(
                body,
                selection.model_id,
                _max_output_tokens(selection.model),
            )

            try:
                response = await _resolve(
                    self._transport.request(
                        selection.upstream,
                        method,
                        path,
                        upstream_headers,
                        upstream_body,
                        self._request_timeout,
                    )
                )
            except HTTPError as error:
                response = TransportResponse(
                    status=error.code,
                    body=error.read(),
                    headers={
                        key.lower(): value
                        for key, value in (error.headers.items() if error.headers else ())
                    },
                )
            except (OSError, URLError, TimeoutError, socket.timeout) as error:
                LOGGER.warning("Upstream request failed: %s", error)
                if not retryable or attempt >= 2:
                    return self._json_error(502, "upstream_error", str(error))
                failed_upstreams.add(selection.upstream)
                self._invalidate(selection)
                continue

            if response.status in {502, 503, 504} and retryable and attempt < 2:
                LOGGER.warning(
                    "Upstream %s returned transient HTTP %d",
                    selection.upstream.base_url,
                    response.status,
                )
                failed_upstreams.add(selection.upstream)
                self._invalidate(selection)
                await _close_response(response)
                continue

            if (
                response.status == 404
                and retryable
                and attempt < 2
                and _is_model_not_found(response.body, selection.model_id)
            ):
                self._invalidate(selection)
                not_found = (selection.upstream, selection.model_id)
                await _close_response(response)
                continue

            raw_response_headers = {
                key.lower(): value for key, value in response.headers.items()
            }
            response_connection_tokens = _connection_tokens(raw_response_headers)
            content_type = raw_response_headers.get("content-type", "application/json")
            rewritten_content = _rewrites_response_body(content_type)
            response_headers = {
                key: value
                for key, value in raw_response_headers.items()
                if key not in STRIPPED_RESPONSE_HEADERS
                and key not in response_connection_tokens
                and (not rewritten_content or key not in BODY_VALIDATOR_HEADERS)
            }
            response_headers.setdefault("content-type", content_type)
            response_body = _rewrite_response_model(response.body, self._alias, content_type)
            return response.status, response_headers, response_body

        return self._json_error(502, "upstream_error", "Upstream retry exhausted")

    async def _select(
        self,
        *,
        force: bool = False,
        exclude: frozenset[Upstream] = frozenset(),
    ) -> Selection:
        now = time.monotonic()
        with self._lock:
            current = self._selection
            if (
                not force
                and current is not None
                and current.upstream not in exclude
                and now - current.selected_at < self._discovery_ttl
            ):
                return current
            candidates = [upstream for upstream in self._upstreams if upstream not in exclude]

        errors: list[str] = []
        for upstream in candidates:
            try:
                models = await _resolve(
                    self._transport.fetch_catalog(upstream, self._discovery_timeout)
                )
            except (OSError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as error:
                errors.append(f"{upstream.name}: {error}")
                continue
            if not models:
                errors.append(f"{upstream.name}: empty model catalog")
                continue
            selection = Selection(upstream=upstream, model=models[0], selected_at=now)
            with self._lock:
                previous = self._selection
                if previous is None or (
                    previous.upstream != selection.upstream
                    or previous.model_id != selection.model_id
                ):
                    LOGGER.info(
                        "Selected %s model=%s",
                        selection.upstream.base_url,
                        selection.model_id,
                    )
                self._selection = selection
            return selection

        with self._lock:
            if self._selection is current:
                self._selection = None
        raise LocalModelUnavailable("; ".join(errors) or "No local model endpoint is ready")

    def _invalidate(self, selection: Selection) -> None:
        with self._lock:
            if self._selection == selection:
                self._selection = None

    def _public_model(self, selection: Selection) -> dict[str, Any]:
        model = dict(selection.model)
        context = _context_capacity(selection.model) or 16_384
        model["id"] = self._alias
        model["object"] = "model"
        model["owned_by"] = "local-model-broker"
        model["root"] = selection.model_id
        model["upstream"] = selection.upstream.base_url
        model["max_model_len"] = context
        model["max_output_tokens"] = _max_output_tokens(selection.model)
        return model

    @staticmethod
    def _json(status: int, payload: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
        return status, {"content-type": "application/json"}, json.dumps(payload).encode()

    @classmethod
    def _json_error(
        cls,
        status: int,
        code: str,
        message: str,
    ) -> tuple[int, dict[str, str], bytes]:
        return cls._json(
            status,
            {"error": {"message": message, "type": "broker_error", "code": code}},
        )


class LocalModelUnavailable(RuntimeError):
    pass


async def _resolve(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value


def _strip_version_prefix(path: str) -> str:
    return path[3:] if path.startswith("/v1/") else path


def _upstream_target(base_path: str, requested_path: str) -> str:
    requested = urlsplit(requested_path)
    relative = _strip_version_prefix(requested.path)
    prefix = base_path.rstrip("/")
    target = f"{prefix}{relative}" or "/"
    if not target.startswith("/"):
        target = f"/{target}"
    if requested.query:
        target = f"{target}?{requested.query}"
    return target


def _context_capacity(model: dict[str, Any]) -> int | None:
    for key in (
        "max_model_len",
        "context_window",
        "context_length",
        "max_context_length",
    ):
        value = model.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    return None


def _max_output_tokens(model: dict[str, Any]) -> int:
    for key in ("max_output_tokens", "max_tokens"):
        value = model.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return min(65_536, value)
    context = _context_capacity(model)
    if context is not None:
        return min(65_536, max(1_024, context // 4))
    return 4_096


def _models_compatible(original: Selection, replacement: Selection) -> bool:
    if replacement.model_id != original.model_id:
        return False
    original_context = _context_capacity(original.model) or 16_384
    replacement_context = _context_capacity(replacement.model) or 16_384
    if replacement_context < original_context:
        return False
    return _max_output_tokens(replacement.model) >= _max_output_tokens(original.model)


def _is_model_not_found(body: ResponseBody, model_id: str) -> bool:
    if not isinstance(body, bytes):
        return False
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return False
    code = str(error.get("code", "")).lower().replace("-", "_")
    if code in {"model_not_found", "invalid_model"}:
        return True
    message = str(error.get("message", "")).strip().lower()
    normalized_model = model_id.lower().strip("`'\"")
    model_mentioned = normalized_model in message
    model_subject = message.startswith("the model ") or message.startswith("model ")
    missing = "does not exist" in message or "not found" in message
    return model_mentioned and model_subject and missing


def _is_retryable_inference(method: str, path: str) -> bool:
    route = urlsplit(path).path.rstrip("/")
    return method == "POST" and route in {
        "/v1/chat/completions",
        "/v1/completions",
        "/v1/responses",
        "/v1/embeddings",
    }


def _connection_tokens(headers: dict[str, str]) -> frozenset[str]:
    value = next(
        (header_value for name, header_value in headers.items() if name.lower() == "connection"),
        "",
    )
    return frozenset(token.strip().lower() for token in value.split(",") if token.strip())


def _rewrite_request_model(
    body: bytes | None,
    model_id: str,
    max_output_tokens: int,
) -> bytes | None:
    if not body:
        return body
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body
    if isinstance(payload, dict):
        if "model" in payload:
            payload["model"] = model_id
        for key in ("max_tokens", "max_completion_tokens", "max_output_tokens"):
            value = payload.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value > max_output_tokens:
                payload[key] = max_output_tokens
        return json.dumps(payload, separators=(",", ":")).encode()
    return body


def _rewrite_response_model(
    body: ResponseBody,
    alias: str,
    content_type: str,
) -> ResponseBody:
    if content_type.lower().startswith("text/event-stream"):
        if isinstance(body, bytes):
            return b"".join(_rewrite_sse_stream((body,), alias))
        if isinstance(body, AsyncIterable):
            return _RewrittenSSEBody(body, alias)
        return _rewrite_sse_stream(body, alias)
    if isinstance(body, bytes):
        return _rewrite_json_model(body, alias)
    return body


def _rewrites_response_body(content_type: str) -> bool:
    lower = content_type.lower()
    return "json" in lower or lower.startswith("text/event-stream")


def _rewrite_json_model(body: bytes, alias: str) -> bytes:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body
    if not isinstance(payload, dict):
        return body
    model = payload.get("model")
    if not isinstance(model, str) or model == alias:
        return body
    payload["model"] = alias
    return json.dumps(payload, separators=(",", ":")).encode()


def _rewrite_sse_stream(chunks: Iterable[bytes], alias: str) -> Iterator[bytes]:
    buffer = b""
    try:
        for chunk in chunks:
            buffer += chunk
            while (newline := buffer.find(b"\n")) >= 0:
                line = buffer[: newline + 1]
                buffer = buffer[newline + 1 :]
                yield _rewrite_sse_line(line, alias)
        if buffer:
            yield _rewrite_sse_line(buffer, alias)
    finally:
        close = getattr(chunks, "close", None)
        if callable(close):
            close()

async def _close_response(response: Any) -> None:
    aclose = getattr(response, "aclose", None)
    if callable(aclose):
        result = aclose()
        if inspect.isawaitable(result):
            await result
        return
    await _close_body(response.body)



class _RewrittenSSEBody:
    def __init__(self, chunks: AsyncIterable[bytes], alias: str) -> None:
        self._chunks = chunks
        self._alias = alias
        self._started = False
        self._closed = False

    def __aiter__(self) -> AsyncIterator[bytes]:
        if self._started:
            raise RuntimeError("rewritten SSE body can only be consumed once")
        self._started = True
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[bytes]:
        buffer = b""
        try:
            async for chunk in self._chunks:
                buffer += chunk
                while (newline := buffer.find(b"\n")) >= 0:
                    line = buffer[: newline + 1]
                    buffer = buffer[newline + 1 :]
                    yield _rewrite_sse_line(line, self._alias)
            if buffer:
                yield _rewrite_sse_line(buffer, self._alias)
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await _close_body(self._chunks)


def _rewrite_sse_line(line: bytes, alias: str) -> bytes:
    if line.endswith(b"\r\n"):
        content, ending = line[:-2], b"\r\n"
    elif line.endswith(b"\n"):
        content, ending = line[:-1], b"\n"
    else:
        content, ending = line, b""
    if not content.startswith(b"data:"):
        return line
    raw_payload = content[5:]
    whitespace = len(raw_payload) - len(raw_payload.lstrip(b" \t"))
    payload = raw_payload[whitespace:]
    rewritten = _rewrite_json_model(payload, alias)
    if rewritten == payload:
        return line
    return content[: 5 + whitespace] + rewritten + ending


def _response_chunks(response: Any, chunk_size: int = 65_536) -> Iterator[bytes]:
    read_chunk = getattr(response, "read1", None)
    if not callable(read_chunk):
        read_chunk = response.read
    try:
        while chunk := read_chunk(chunk_size):
            yield chunk
    finally:
        response.close()


async def _read_response_head(
    reader: asyncio.StreamReader,
) -> tuple[int, dict[str, str]]:
    while True:
        status_line = await reader.readline()
        if not status_line:
            raise ConnectionError("upstream closed before response headers")
        parts = status_line.rstrip(b"\r\n").split(b" ", 2)
        if len(parts) < 2:
            raise ConnectionError(f"malformed upstream status line: {status_line!r}")
        try:
            status = int(parts[1])
        except ValueError as error:
            raise ConnectionError(f"malformed upstream status: {status_line!r}") from error
        headers = await _read_headers(reader)
        if not (100 <= status < 200 and status != 101):
            return status, headers


async def _read_headers(reader: asyncio.StreamReader) -> dict[str, str]:
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in {b"", b"\r\n", b"\n"}:
            return headers
        if b":" not in line:
            raise ConnectionError(f"malformed HTTP header: {line!r}")
        name, value = line.split(b":", 1)
        headers[name.strip().decode("latin-1").lower()] = value.strip().decode("latin-1")


async def _iter_http_body(
    reader: asyncio.StreamReader,
    headers: dict[str, str],
    timeout: float,
) -> AsyncIterator[bytes]:
    transfer_encoding = headers.get("transfer-encoding", "").lower()
    if "chunked" in transfer_encoding:
        while True:
            size_line = await asyncio.wait_for(reader.readline(), timeout)
            if not size_line:
                raise ConnectionError("upstream closed during chunked response")
            try:
                size = int(size_line.split(b";", 1)[0].strip(), 16)
            except ValueError as error:
                raise ConnectionError(f"invalid upstream chunk size: {size_line!r}") from error
            if size == 0:
                await _read_headers(reader)
                return
            chunk = await asyncio.wait_for(reader.readexactly(size), timeout)
            ending = await asyncio.wait_for(reader.readexactly(2), timeout)
            if ending != b"\r\n":
                raise ConnectionError("invalid upstream chunk terminator")
            yield chunk
        return

    length_header = headers.get("content-length")
    if length_header is not None:
        try:
            remaining = int(length_header)
        except ValueError as error:
            raise ConnectionError(f"invalid upstream content-length: {length_header}") from error
        while remaining > 0:
            chunk = await asyncio.wait_for(reader.read(min(65_536, remaining)), timeout)
            if not chunk:
                raise ConnectionError("upstream closed before content-length was satisfied")
            remaining -= len(chunk)
            yield chunk
        return

    while True:
        chunk = await asyncio.wait_for(reader.read(65_536), timeout)
        if not chunk:
            return
        yield chunk


async def _collect_http_body(
    reader: asyncio.StreamReader,
    headers: dict[str, str],
    timeout: float,
) -> bytes:
    return b"".join([chunk async for chunk in _iter_http_body(reader, headers, timeout)])


async def _body_bytes(body: ResponseBody) -> bytes:
    if isinstance(body, bytes):
        return body
    if isinstance(body, AsyncIterable):
        return b"".join([chunk async for chunk in body])
    return b"".join(body)


async def _close_body(body: ResponseBody) -> None:
    aclose = getattr(body, "aclose", None)
    if callable(aclose):
        result = aclose()
        if inspect.isawaitable(result):
            await result
        return
    close = getattr(body, "close", None)
    if callable(close):
        close()


async def _read_client_request(
    reader: asyncio.StreamReader,
) -> tuple[str, str, dict[str, str], bytes | None]:
    request_line = await reader.readline()
    if not request_line:
        raise EOFError
    parts = request_line.rstrip(b"\r\n").split(b" ")
    if len(parts) != 3:
        raise ValueError(f"malformed request line: {request_line!r}")
    method = parts[0].decode("ascii").upper()
    path = parts[1].decode("latin-1")
    headers = await _read_headers(reader)
    transfer_encoding = headers.get("transfer-encoding", "").lower()
    if "chunked" in transfer_encoding:
        body = await _collect_http_body(reader, headers, 3600.0)
    else:
        length_header = headers.get("content-length")
        length = int(length_header) if length_header else 0
        body = await reader.readexactly(length) if length else None
    return method, path, headers, body


async def _watch_downstream(reader: asyncio.StreamReader) -> None:
    try:
        while await reader.read(1):
            pass
    except (ConnectionError, asyncio.IncompleteReadError):
        return


async def _serve_response(
    broker: Broker,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes | None,
    writer: asyncio.StreamWriter,
) -> None:
    response_body: ResponseBody = b""
    try:
        try:
            status, response_headers, response_body = await broker.handle_async(
                method, path, headers, body
            )
        except Exception as error:
            LOGGER.exception("Unhandled broker error")
            status, response_headers, response_body = broker._json_error(
                502, "broker_error", str(error)
            )
        reason = _reason_phrase(status)
        writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode("latin-1"))
        for name, value in response_headers.items():
            writer.write(f"{name}: {value}\r\n".encode("latin-1"))
        has_body = method != "HEAD" and status not in {204, 304} and not 100 <= status < 200
        if isinstance(response_body, bytes) and has_body:
            writer.write(f"content-length: {len(response_body)}\r\n".encode("ascii"))
        writer.write(b"connection: close\r\n\r\n")
        await writer.drain()
        if isinstance(response_body, bytes) and has_body:
            writer.write(response_body)
            await writer.drain()
        elif isinstance(response_body, AsyncIterable):
            async for chunk in response_body:
                writer.write(chunk)
                await writer.drain()
        else:
            for chunk in response_body:
                writer.write(chunk)
                await writer.drain()
    except (BrokenPipeError, ConnectionResetError):
        LOGGER.info("Client disconnected during response stream")
    finally:
        await _close_body(response_body)


async def _handle_connection(
    broker: Broker,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    serve_task: asyncio.Task[None] | None = None
    watch_task: asyncio.Task[None] | None = None
    try:
        method, path, headers, body = await _read_client_request(reader)
        serve_task = asyncio.create_task(
            _serve_response(broker, method, path, headers, body, writer)
        )
        watch_task = asyncio.create_task(_watch_downstream(reader))
        done, _ = await asyncio.wait(
            {serve_task, watch_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if watch_task in done and not serve_task.done():
            serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)
    except EOFError:
        pass
    except (ConnectionError, ValueError, asyncio.IncompleteReadError) as error:
        LOGGER.warning("Invalid downstream request: %s", error)
    finally:
        for task in (serve_task, watch_task):
            if task is not None and not task.done():
                task.cancel()
        pending = [task for task in (serve_task, watch_task) if task is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        writer.close()
        with suppress(ConnectionError, OSError):
            await writer.wait_closed()


def _reason_phrase(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "Response"


class BrokerServer:
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], broker: Broker) -> None:
        self.broker = broker
        family = socket.AF_INET6 if ":" in address[0] else socket.AF_INET
        self._socket = socket.socket(family, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(address)
        self._socket.listen(socket.SOMAXCONN)
        self._socket.setblocking(False)
        self.server_address = self._socket.getsockname()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._shutdown_event: asyncio.Event | None = None
        self._shutdown_requested = False
        self._tasks: set[asyncio.Task[None]] = set()
        self._closed = threading.Event()

    def serve_forever(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._serve())
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
            self._closed.set()

    async def _serve(self) -> None:
        self._shutdown_event = asyncio.Event()
        if self._shutdown_requested:
            self._shutdown_event.set()

        def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            task = asyncio.create_task(_handle_connection(self.broker, reader, writer))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        server = await asyncio.start_server(connected, sock=self._socket)
        try:
            await self._shutdown_event.wait()
        finally:
            server.close()
            await server.wait_closed()
            for task in tuple(self._tasks):
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)

    def shutdown(self) -> None:
        self._shutdown_requested = True
        if self._loop is not None and self._shutdown_event is not None:
            self._loop.call_soon_threadsafe(self._shutdown_event.set)

    def server_close(self) -> None:
        self.shutdown()
        if self._loop is None:
            self._socket.close()
        else:
            self._closed.wait(timeout=5)


def _parse_upstream(value: str, index: int) -> Upstream:
    return Upstream(name=f"upstream-{index + 1}", base_url=value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stable OpenAI-compatible alias for local model servers")
    parser.add_argument("--host", default=os.getenv("LOCAL_MODEL_BROKER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LOCAL_MODEL_BROKER_PORT", "8879")))
    parser.add_argument("--alias", default=os.getenv("LOCAL_MODEL_BROKER_ALIAS", "local-auto"))
    parser.add_argument(
        "--upstream",
        action="append",
        dest="upstreams",
        help="OpenAI-compatible base URL in priority order; repeatable",
    )
    parser.add_argument("--discovery-ttl", type=float, default=2.0)
    parser.add_argument("--discovery-timeout", type=float, default=1.5)
    parser.add_argument("--request-timeout", type=float, default=3600.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    upstream_urls = args.upstreams or [
        "http://127.0.0.1:8888/v1",
        "http://127.0.0.1:8880/v1",
    ]
    broker = Broker(
        upstreams=tuple(_parse_upstream(url, index) for index, url in enumerate(upstream_urls)),
        transport=UrlLibTransport(),
        alias=args.alias,
        discovery_ttl=args.discovery_ttl,
        discovery_timeout=args.discovery_timeout,
        request_timeout=args.request_timeout,
    )
    server = BrokerServer((args.host, args.port), broker)
    LOGGER.info(
        "Listening on http://%s:%d/v1 alias=%s upstreams=%s",
        args.host,
        args.port,
        args.alias,
        ",".join(upstream_urls),
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
