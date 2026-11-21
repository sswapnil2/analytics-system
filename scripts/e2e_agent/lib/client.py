"""Pure-stdlib HTTP, REST, OTLP/JSON+Protobuf, PLST, and MCP JSON-RPC 2.0 client."""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import re
import ssl
import struct
import urllib.parse
from dataclasses import dataclass
from typing import Any

from lib.plst import encode_plst_frame


class HttpStatusError(RuntimeError):
    """Raised by HttpResponse.raise_for_status() on non-2xx responses."""

    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        preview = response.text(errors="replace")[:512]
        super().__init__(f"HTTP {response.status}: {preview}")


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Structured HTTP response that never throws on 3xx/4xx/5xx status codes."""

    status: int
    headers: dict[str, str]
    body: bytes

    def text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self.body.decode(encoding, errors=errors)

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    def is_success(self) -> bool:
        return 200 <= self.status < 300

    def raise_for_status(self) -> HttpResponse:
        if not self.is_success():
            raise HttpStatusError(self)
        return self


# ---------------------------------------------------------------------------
# Pure-stdlib OTLP/Protobuf Encoder (ExportTraceServiceRequest)
# ---------------------------------------------------------------------------


def _pb_varint(value: int) -> bytes:
    val = int(value) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        byte = val & 0x7F
        val >>= 7
        if val:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _pb_tag(field_num: int, wire_type: int) -> bytes:
    return _pb_varint((field_num << 3) | wire_type)


def _pb_ld(field_num: int, payload: bytes) -> bytes:
    return _pb_tag(field_num, 2) + _pb_varint(len(payload)) + payload


def _pb_str(field_num: int, text: str) -> bytes:
    return _pb_ld(field_num, text.encode("utf-8"))


def _pb_fixed64(field_num: int, value: int) -> bytes:
    return _pb_tag(field_num, 1) + struct.pack("<Q", int(value) & 0xFFFFFFFFFFFFFFFF)


def _pb_double(field_num: int, value: float) -> bytes:
    return _pb_tag(field_num, 1) + struct.pack("<d", float(value))


def _pb_id_bytes(raw_id: Any, nbytes: int) -> bytes:
    if isinstance(raw_id, (bytes, bytearray, memoryview)):
        b = bytes(raw_id)
        if len(b) == nbytes:
            return b
        return hashlib.sha256(b).digest()[:nbytes]
    s = str(raw_id or "").strip()
    if len(s) == nbytes * 2:
        try:
            return bytes.fromhex(s)
        except ValueError:
            pass
    if not s:
        return b""
    return hashlib.sha256(s.encode("utf-8")).digest()[:nbytes]


def _pb_any_value(val: dict[str, Any]) -> bytes:
    if "stringValue" in val:
        return _pb_str(1, str(val["stringValue"]))
    if "boolValue" in val:
        return _pb_tag(2, 0) + _pb_varint(1 if val["boolValue"] else 0)
    if "intValue" in val:
        return _pb_tag(3, 0) + _pb_varint(int(val["intValue"]))
    if "doubleValue" in val:
        return _pb_double(4, float(val["doubleValue"]))
    if "arrayValue" in val:
        arr = val["arrayValue"] or {}
        inner = b"".join(_pb_ld(1, _pb_any_value(v)) for v in arr.get("values", []))
        return _pb_ld(5, inner)
    if "kvlistValue" in val:
        kvlist = val["kvlistValue"] or {}
        inner = b"".join(_pb_ld(1, _pb_key_value(kv)) for kv in kvlist.get("values", []))
        return _pb_ld(6, inner)
    if "bytesValue" in val:
        raw_b = val["bytesValue"]
        if isinstance(raw_b, str):
            raw_b = base64.b64decode(raw_b)
        return _pb_ld(7, bytes(raw_b))
    return b""


def _pb_key_value(kv: dict[str, Any]) -> bytes:
    out = bytearray()
    key = str(kv.get("key", ""))
    out.extend(_pb_str(1, key))
    val = kv.get("value")
    if isinstance(val, dict):
        out.extend(_pb_ld(2, _pb_any_value(val)))
    return bytes(out)


def _pb_status(status: dict[str, Any]) -> bytes:
    out = bytearray()
    msg = status.get("message")
    if msg:
        out.extend(_pb_str(2, str(msg)))
    raw_code = status.get("code", 0)
    if isinstance(raw_code, str):
        norm = raw_code.strip().upper()
        code = 2 if norm in ("2", "ERROR", "STATUS_CODE_ERROR") else (1 if norm in ("1", "OK", "STATUS_CODE_OK") else 0)
    else:
        code = int(raw_code or 0)
    if code != 0:
        out.extend(_pb_tag(3, 0) + _pb_varint(code))
    return bytes(out)


def _pb_event(ev: dict[str, Any]) -> bytes:
    out = bytearray()
    ts = int(ev.get("timeUnixNano", 0) or 0)
    if ts:
        out.extend(_pb_fixed64(1, ts))
    if ev.get("name"):
        out.extend(_pb_str(2, str(ev["name"])))
    for kv in ev.get("attributes", []) or []:
        out.extend(_pb_ld(3, _pb_key_value(kv)))
    return bytes(out)


def _pb_link(link: dict[str, Any]) -> bytes:
    out = bytearray()
    tr_b = _pb_id_bytes(link.get("traceId", ""), 16)
    if tr_b:
        out.extend(_pb_ld(1, tr_b))
    sp_b = _pb_id_bytes(link.get("spanId", ""), 8)
    if sp_b:
        out.extend(_pb_ld(2, sp_b))
    if link.get("traceState"):
        out.extend(_pb_str(3, str(link["traceState"])))
    for kv in link.get("attributes", []) or []:
        out.extend(_pb_ld(4, _pb_key_value(kv)))
    return bytes(out)


def _pb_span(sp: dict[str, Any]) -> bytes:
    out = bytearray()
    tr_b = _pb_id_bytes(sp.get("traceId", ""), 16)
    if tr_b:
        out.extend(_pb_ld(1, tr_b))
    sp_b = _pb_id_bytes(sp.get("spanId", ""), 8)
    if sp_b:
        out.extend(_pb_ld(2, sp_b))
    if sp.get("traceState"):
        out.extend(_pb_str(3, str(sp["traceState"])))
    if sp.get("parentSpanId"):
        p_b = _pb_id_bytes(sp["parentSpanId"], 8)
        if p_b:
            out.extend(_pb_ld(4, p_b))
    if sp.get("name"):
        out.extend(_pb_str(5, str(sp["name"])))
    kind = sp.get("kind")
    if isinstance(kind, int) and kind != 0:
        out.extend(_pb_tag(6, 0) + _pb_varint(kind))
    start_ns = int(sp.get("startTimeUnixNano", 0) or 0)
    if start_ns:
        out.extend(_pb_fixed64(7, start_ns))
    end_ns = int(sp.get("endTimeUnixNano", 0) or 0)
    if end_ns:
        out.extend(_pb_fixed64(8, end_ns))
    for kv in sp.get("attributes", []) or []:
        out.extend(_pb_ld(9, _pb_key_value(kv)))
    for ev in sp.get("events", []) or []:
        out.extend(_pb_ld(11, _pb_event(ev)))
    for lk in sp.get("links", []) or []:
        out.extend(_pb_ld(13, _pb_link(lk)))
    if sp.get("status"):
        st_bytes = _pb_status(sp["status"])
        if st_bytes:
            out.extend(_pb_ld(15, st_bytes))
    return bytes(out)


def encode_otlp_trace_protobuf(req: dict[str, Any]) -> bytes:
    """Encode an OTLP ExportTraceServiceRequest JSON dict into OTLP/Protobuf wire bytes."""
    out = bytearray()
    for rs in req.get("resourceSpans", []) or []:
        rs_buf = bytearray()
        res = rs.get("resource")
        if isinstance(res, dict):
            res_buf = bytearray()
            for kv in res.get("attributes", []) or []:
                res_buf.extend(_pb_ld(1, _pb_key_value(kv)))
            dropped = int(res.get("droppedAttributesCount", 0) or 0)
            if dropped:
                res_buf.extend(_pb_tag(2, 0) + _pb_varint(dropped))
            if res_buf:
                rs_buf.extend(_pb_ld(1, bytes(res_buf)))

        for ss in rs.get("scopeSpans", []) or []:
            ss_buf = bytearray()
            scope = ss.get("scope")
            if isinstance(scope, dict):
                sc_buf = bytearray()
                if scope.get("name"):
                    sc_buf.extend(_pb_str(1, str(scope["name"])))
                if scope.get("version"):
                    sc_buf.extend(_pb_str(2, str(scope["version"])))
                for kv in scope.get("attributes", []) or []:
                    sc_buf.extend(_pb_ld(3, _pb_key_value(kv)))
                if sc_buf:
                    ss_buf.extend(_pb_ld(1, bytes(sc_buf)))
            for sp in ss.get("spans", []) or []:
                ss_buf.extend(_pb_ld(2, _pb_span(sp)))
            if ss.get("schemaUrl"):
                ss_buf.extend(_pb_str(3, str(ss["schemaUrl"])))
            rs_buf.extend(_pb_ld(2, bytes(ss_buf)))

        if rs.get("schemaUrl"):
            rs_buf.extend(_pb_str(3, str(rs["schemaUrl"])))
        out.extend(_pb_ld(1, bytes(rs_buf)))
    return bytes(out)


# ---------------------------------------------------------------------------
# PulseClient
# ---------------------------------------------------------------------------


class PulseClient:
    """Pure-stdlib HTTP/REST, PLST, OTLP, and MCP JSON-RPC 2.0 client."""

    def __init__(self, base_url: str, *, timeout: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"Invalid Pulse base_url: {base_url!r}")
        self._scheme = parsed.scheme
        self._host = parsed.hostname or "127.0.0.1"
        self._port = parsed.port or (443 if self._scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self.timeout = timeout

    def _connect(self, timeout: float) -> http.client.HTTPConnection:
        if self._scheme == "https":
            return http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=timeout,
                context=ssl._create_unverified_context(),
            )
        return http.client.HTTPConnection(self._host, self._port, timeout=timeout)

    def _build_path(self, path: str, query: dict[str, Any] | None) -> str:
        clean = path if path.startswith("/") else f"/{path}"
        full = f"{self._base_path}{clean}" if self._base_path else clean
        if query:
            filtered = [
                (k, str(v).lower() if isinstance(v, bool) else str(v))
                for k, v in query.items()
                if v is not None
            ]
            if filtered:
                qs = urllib.parse.urlencode(filtered)
                sep = "&" if "?" in full else "?"
                full = f"{full}{sep}{qs}"
        return full

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | bytearray | str | None = None,
        json_body: Any = None,
        token: str | None = None,
        api_key: str | None = None,
        project_id: str | None = None,
        query: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """Send an HTTP request and return HttpResponse without raising on 3xx/4xx/5xx."""
        req_headers: dict[str, str] = {}
        if headers:
            for k, v in headers.items():
                req_headers[k] = v

        has_lower = {k.lower() for k in req_headers}
        if token is not None and "authorization" not in has_lower:
            req_headers["Authorization"] = f"Bearer {token}"
        if api_key is not None and "x-pulse-key" not in has_lower:
            req_headers["x-pulse-key"] = api_key
        if project_id is not None and "x-pulse-project" not in has_lower:
            req_headers["x-pulse-project"] = project_id

        payload: bytes | None
        if json_body is not None:
            payload = json.dumps(json_body).encode("utf-8")
            if "content-type" not in {k.lower() for k in req_headers}:
                req_headers["Content-Type"] = "application/json"
        elif isinstance(body, str):
            payload = body.encode("utf-8")
        elif isinstance(body, (bytes, bytearray)):
            payload = bytes(body)
        else:
            payload = None

        target = self._build_path(path, query)
        effective_timeout = self.timeout if timeout is None else timeout
        conn = self._connect(effective_timeout)
        try:
            conn.request(method.upper(), target, body=payload, headers=req_headers)
            resp = conn.getresponse()
            raw = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return HttpResponse(status=resp.status, headers=resp_headers, body=raw)
        finally:
            conn.close()

    def ingest_plst(
        self,
        frame_bytes: bytes,
        *,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
        content_type: str = "application/x-pulse-trace",
    ) -> HttpResponse:
        """Send a raw PLST v1 binary frame to `POST /v1/agent/traces`."""
        req_headers = dict(headers) if headers else {}
        if "content-type" not in {k.lower() for k in req_headers}:
            req_headers["Content-Type"] = content_type
        return self.request(
            "POST",
            "/v1/agent/traces",
            headers=req_headers,
            body=frame_bytes,
            api_key=api_key,
        )

    def ingest_plst_batch(
        self,
        batch: dict[str, Any],
        *,
        api_key: str | None = None,
        use_dict: bool = False,
        dict_bytes: bytes | None = None,
        flags: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Encode `batch` as a PLST v1 frame and ingest it via `POST /v1/agent/traces`."""
        frame = encode_plst_frame(batch, use_dict=use_dict, dict_bytes=dict_bytes, flags=flags)
        return self.ingest_plst(frame, api_key=api_key, headers=headers)

    def ingest_json(
        self,
        batch: dict[str, Any],
        *,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Send a native TraceIngestBatch JSON payload to `POST /v1/agent/traces`."""
        return self.request(
            "POST",
            "/v1/agent/traces",
            headers=headers,
            json_body=batch,
            api_key=api_key,
        )

    def ingest_otlp_json(
        self,
        payload: dict[str, Any],
        *,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Send an OTLP/JSON ExportTraceServiceRequest to `POST /v1/otel/v1/traces`."""
        return self.request(
            "POST",
            "/v1/otel/v1/traces",
            headers=headers,
            json_body=payload,
            api_key=api_key,
        )

    def ingest_otlp_pb(
        self,
        payload: dict[str, Any] | bytes | bytearray,
        *,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Send an OTLP/Protobuf ExportTraceServiceRequest to `POST /v1/otel/v1/traces`."""
        raw = (
            bytes(payload)
            if isinstance(payload, (bytes, bytearray))
            else encode_otlp_trace_protobuf(payload)
        )
        req_headers = dict(headers) if headers else {}
        if "content-type" not in {k.lower() for k in req_headers}:
            req_headers["Content-Type"] = "application/x-protobuf"
        return self.request(
            "POST",
            "/v1/otel/v1/traces",
            headers=req_headers,
            body=raw,
            api_key=api_key,
        )

    def mcp_call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        req_id: int | str | None = 1,
        token: str | None = None,
        project_id: str | None = None,
    ) -> HttpResponse:
        """Invoke a JSON-RPC 2.0 request (or notification when `req_id is None`) on `POST /v1/mcp`."""
        rpc: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            rpc["params"] = params
        if req_id is not None:
            rpc["id"] = req_id
        return self.request(
            "POST",
            "/v1/mcp",
            json_body=rpc,
            token=token,
            project_id=project_id,
        )

    def mcp_tool_call(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        req_id: int | str | None = 1,
        token: str | None = None,
        project_id: str | None = None,
    ) -> HttpResponse:
        """Invoke `tools/call` for an MCP tool on `POST /v1/mcp`."""
        return self.mcp_call(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
            req_id=req_id,
            token=token,
            project_id=project_id,
        )

    def get_metrics(self, *, token: str | None = None) -> HttpResponse:
        """Fetch Prometheus text metrics from `GET /metrics`."""
        return self.request("GET", "/metrics", token=token)

    def get_healthz(self) -> HttpResponse:
        """Fetch `GET /healthz`."""
        return self.request("GET", "/healthz")

    def get_readyz(self) -> HttpResponse:
        """Fetch `GET /readyz`."""
        return self.request("GET", "/readyz")

    def check_studio_static(self, *, require_agents_route: bool = False) -> dict[str, Any]:
        """Verify Studio SPA root HTML, `/assets/*.js` bundle, and optional `#/agents` route presence."""
        root_resp = self.request("GET", "/")
        if root_resp.status != 200:
            raise AssertionError(f"GET / returned {root_resp.status}: {root_resp.text(errors='replace')[:200]}")
        html = root_resp.text()
        if '<div id="root"></div>' not in html:
            raise AssertionError("Studio index.html missing '<div id=\"root\"></div>'")

        asset_paths = re.findall(r'(?:src|href)="(/assets/[^"]+\.js)"', html)
        if not asset_paths:
            raise AssertionError("Studio index.html missing /assets/*.js script tag")

        assets_checked: list[str] = []
        agents_found = False
        for asset in asset_paths:
            asset_resp = self.request("GET", asset)
            if asset_resp.status != 200:
                raise AssertionError(f"GET {asset} returned {asset_resp.status}")
            assets_checked.append(asset)
            body_text = asset_resp.text(errors="replace")
            # Minifiers may emit the route id with any quote style, including template literals.
            if "#/agents" in body_text or any(f"{q}agents{q}" in body_text for q in "\"'`"):
                agents_found = True

        if require_agents_route and not agents_found:
            raise AssertionError("Studio JS bundle does not contain #/agents route markers")

        return {
            "status": root_resp.status,
            "assets": assets_checked,
            "agents_route_detected": agents_found,
        }
