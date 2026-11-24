"""Pure-stdlib path-style S3 HTTP server with prefix call counters, CAS, and fault injection."""

from __future__ import annotations

import hashlib
import http.server
import socket
import threading
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import formatdate
from typing import Any
from xml.sax.saxutils import escape as xml_escape


@dataclass(frozen=True, slots=True)
class S3Object:
    """In-memory object stored by MockS3Server."""

    key: str
    data: bytes
    etag: str
    last_modified_epoch: float
    content_type: str = "application/octet-stream"

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def last_modified_iso(self) -> str:
        dt = datetime.fromtimestamp(self.last_modified_epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    @property
    def last_modified_http(self) -> str:
        return formatdate(self.last_modified_epoch, usegmt=True)


@dataclass(slots=True)
class S3FaultConfig:
    """Fault injection rules for MockS3Server."""

    unreachable: bool = False
    fail_prefixes: tuple[str, ...] = ()
    status: int = 503
    ops: frozenset[str] | None = None
    latency_ms: int = 0


@dataclass(slots=True)
class _MultipartUploadState:
    bucket: str
    key: str
    content_type: str
    parts: dict[int, tuple[bytes, str]] = field(default_factory=dict)


def _top_prefix(key_or_prefix: str) -> str:
    clean = key_or_prefix.lstrip("/")
    if not clean:
        return "(root)"
    return clean.split("/", 1)[0]


def _prefix_matches(candidate: str, prefix: str) -> bool:
    if not prefix:
        return True
    if candidate.startswith(prefix):
        return True
    norm_p = prefix.rstrip("/")
    return _top_prefix(candidate) == norm_p and "/" not in prefix.rstrip("/")


def _normalize_etag(val: str) -> str:
    return val.strip().strip('"')


def _parse_range_header(header_val: str, total_len: int) -> tuple[int, int] | None:
    """Parse HTTP `Range: bytes=...` into inclusive `(start, end)` or None if invalid/unsatisfiable."""
    spec = header_val.strip()
    if not spec.lower().startswith("bytes="):
        return None
    range_part = spec[6:].strip()
    if "," in range_part or "-" not in range_part:
        return None
    start_s, end_s = range_part.split("-", 1)
    start_s = start_s.strip()
    end_s = end_s.strip()
    if total_len <= 0:
        return None
    try:
        if not start_s:
            # Suffix range: bytes=-N
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(0, total_len - suffix)
            return start, total_len - 1
        start = int(start_s)
        if start < 0 or start >= total_len:
            return None
        if not end_s:
            return start, total_len - 1
        end = int(end_s)
        if end < start:
            return None
        return start, min(end, total_len - 1)
    except ValueError:
        return None


def _s3_error_xml(code: str, message: str, resource: str = "") -> bytes:
    res_xml = f"<Resource>{xml_escape(resource)}</Resource>" if resource else ""
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<Error><Code>{xml_escape(code)}</Code>"
        f"<Message>{xml_escape(message)}</Message>{res_xml}</Error>"
    )
    return body.encode("utf-8")


class _QuietThreadingHTTPServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[Any], mock_s3: MockS3Server) -> None:
        self.mock_s3 = mock_s3
        super().__init__(server_address, handler_cls)

    def handle_error(self, request: Any, client_address: Any) -> None:
        # Suppress noisy stack traces on intentional connection drops / client shutdowns.
        pass


class _MockS3Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: _QuietThreadingHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _mock(self) -> MockS3Server:
        return self.server.mock_s3

    def _read_request_body(self) -> bytes:
        te = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in te:
            chunks: list[bytes] = []
            while True:
                line = self.rfile.readline()
                if not line:
                    break
                size_str = line.split(b";", 1)[0].strip()
                if not size_str:
                    continue
                chunk_size = int(size_str, 16)
                if chunk_size == 0:
                    # Consume trailing headers / blank line
                    while True:
                        trailer = self.rfile.readline()
                        if not trailer or trailer in (b"\r\n", b"\n"):
                            break
                    break
                data = self.rfile.read(chunk_size)
                chunks.append(data)
                self.rfile.read(2)  # CRLF after chunk
            return b"".join(chunks)

        cl = self.headers.get("Content-Length")
        if cl is not None:
            n = int(cl)
            return self.rfile.read(n) if n > 0 else b""
        return b""

    def _send_bytes(
        self,
        status: int,
        body: bytes = b"",
        *,
        content_type: str = "application/xml",
        headers: dict[str, str] | None = None,
        head_only: bool = False,
    ) -> None:
        self.send_response(status)
        if status != 204:
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        if not head_only and status != 204 and body:
            self.wfile.write(body)

    def _drop_connection(self) -> None:
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass
        self.close_connection = True

    def _split_target(self) -> tuple[str, str, dict[str, list[str]]]:
        parsed = urllib.parse.urlsplit(self.path)
        raw_path = parsed.path.lstrip("/")
        if "/" in raw_path:
            raw_bucket, raw_key = raw_path.split("/", 1)
        else:
            raw_bucket, raw_key = raw_path, ""
        bucket = urllib.parse.unquote(raw_bucket) or self._mock().default_bucket
        key = urllib.parse.unquote(raw_key)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        return bucket, key, query

    def _apply_fault_and_record(self, op: str, key_or_prefix: str) -> bool:
        """Record call, apply latency/fault injection. Returns True if request was aborted/faulted."""
        mock = self._mock()
        mock._record_call(op, key_or_prefix)
        fault = mock._get_fault_snapshot()
        if fault.latency_ms > 0:
            time.sleep(fault.latency_ms / 1000.0)
        if fault.unreachable:
            self._drop_connection()
            return True
        if fault.fail_prefixes:
            op_match = fault.ops is None or op in fault.ops
            if op_match and any(_prefix_matches(key_or_prefix, p) for p in fault.fail_prefixes):
                self._send_bytes(
                    fault.status,
                    _s3_error_xml(
                        "ServiceUnavailable" if fault.status == 503 else "InjectedFault",
                        f"MockS3 injected HTTP {fault.status} for {op} {key_or_prefix}",
                        resource=key_or_prefix,
                    ),
                )
                return True
        return False

    def do_GET(self) -> None:
        bucket, key, query = self._split_target()
        if not key:
            # ListObjectsV2 or ListObjects v1
            list_prefix = (query.get("prefix") or [""])[0]
            if self._apply_fault_and_record("LIST", list_prefix):
                return
            self._handle_list(bucket, query)
            return

        if self._apply_fault_and_record("GET", key):
            return

        mock = self._mock()
        obj = mock._get_obj(bucket, key)
        if obj is None:
            self._send_bytes(
                404,
                _s3_error_xml("NoSuchKey", "The specified key does not exist.", resource=key),
            )
            mock._fire_callbacks("GET", key)
            return

        if_match = self.headers.get("If-Match")
        if if_match and _normalize_etag(if_match) not in ("*", obj.etag):
            self._send_bytes(
                412,
                _s3_error_xml("PreconditionFailed", "If-Match precondition failed", resource=key),
            )
            return

        if_none_match = self.headers.get("If-None-Match")
        if if_none_match and _normalize_etag(if_none_match) in ("*", obj.etag):
            self._send_bytes(304, b"", headers={"ETag": f'"{obj.etag}"'})
            return

        range_hdr = self.headers.get("Range")
        base_headers = {
            "ETag": f'"{obj.etag}"',
            "Last-Modified": obj.last_modified_http,
            "Accept-Ranges": "bytes",
        }
        if range_hdr:
            parsed_range = _parse_range_header(range_hdr, obj.size)
            if parsed_range is None:
                self._send_bytes(
                    416,
                    _s3_error_xml("InvalidRange", "Requested range not satisfiable", resource=key),
                    headers={"Content-Range": f"bytes */{obj.size}"},
                )
                return
            start, end = parsed_range
            slice_bytes = obj.data[start : end + 1]
            base_headers["Content-Range"] = f"bytes {start}-{end}/{obj.size}"
            self._send_bytes(
                206,
                slice_bytes,
                content_type=obj.content_type,
                headers=base_headers,
            )
        else:
            self._send_bytes(
                200,
                obj.data,
                content_type=obj.content_type,
                headers=base_headers,
            )
        mock._fire_callbacks("GET", key)

    def do_HEAD(self) -> None:
        bucket, key, _ = self._split_target()
        if not key:
            if self._apply_fault_and_record("HEAD", ""):
                return
            self._send_bytes(200, b"", head_only=True)
            return

        if self._apply_fault_and_record("HEAD", key):
            return

        mock = self._mock()
        obj = mock._get_obj(bucket, key)
        if obj is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            mock._fire_callbacks("HEAD", key)
            return

        self.send_response(200)
        self.send_header("Content-Type", obj.content_type)
        self.send_header("Content-Length", str(obj.size))
        self.send_header("ETag", f'"{obj.etag}"')
        self.send_header("Last-Modified", obj.last_modified_http)
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        mock._fire_callbacks("HEAD", key)

    def do_PUT(self) -> None:
        bucket, key, query = self._split_target()
        mock = self._mock()

        # Multipart UploadPart: PUT /<bucket>/<key>?partNumber=N&uploadId=U
        if "partNumber" in query and "uploadId" in query:
            body = self._read_request_body()
            if self._apply_fault_and_record("MULTIPART", key):
                return
            part_num = int(query["partNumber"][0])
            upload_id = query["uploadId"][0]
            etag = mock._upload_part(upload_id, part_num, body)
            if etag is None:
                self._send_bytes(
                    404,
                    _s3_error_xml("NoSuchUpload", "The specified multipart upload does not exist.", resource=key),
                )
                return
            self._send_bytes(200, b"", headers={"ETag": f'"{etag}"'})
            mock._fire_callbacks("MULTIPART", key)
            return

        body = self._read_request_body()
        copy_source = self.headers.get("x-amz-copy-source")
        if copy_source is not None:
            if self._apply_fault_and_record("COPY", key):
                return
            src_unquoted = urllib.parse.unquote(copy_source).lstrip("/")
            if "/" in src_unquoted:
                src_bucket, src_key = src_unquoted.split("/", 1)
            else:
                src_bucket, src_key = bucket, src_unquoted
            if_none_match = self.headers.get("If-None-Match")
            status, obj, err_code = mock._copy_obj(
                src_bucket,
                src_key,
                bucket,
                key,
                if_none_match=if_none_match,
            )
            if status == 404:
                self._send_bytes(
                    404,
                    _s3_error_xml(err_code or "NoSuchKey", "Copy source not found", resource=src_key),
                )
                return
            if status == 412:
                self._send_bytes(
                    412,
                    _s3_error_xml(err_code or "PreconditionFailed", "Copy destination already exists", resource=key),
                )
                return
            assert obj is not None
            xml_body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<CopyObjectResult>"
                f"<LastModified>{obj.last_modified_iso}</LastModified>"
                f'<ETag>"{obj.etag}"</ETag>'
                "</CopyObjectResult>"
            ).encode("utf-8")
            self._send_bytes(200, xml_body, headers={"ETag": f'"{obj.etag}"'})
            mock._fire_callbacks("COPY", key)
            return

        if self._apply_fault_and_record("PUT", key):
            return

        if_none_match = self.headers.get("If-None-Match")
        if_match = self.headers.get("If-Match")
        content_type = self.headers.get("Content-Type") or "application/octet-stream"
        status, obj = mock._put_obj(
            bucket,
            key,
            body,
            content_type=content_type,
            if_none_match=if_none_match,
            if_match=if_match,
        )
        if status == 412:
            self._send_bytes(
                412,
                _s3_error_xml(
                    "PreconditionFailed",
                    "At least one of the pre-conditions you specified did not hold",
                    resource=key,
                ),
            )
            mock._fire_callbacks("PUT", key)
            return

        assert obj is not None
        self._send_bytes(200, b"", headers={"ETag": f'"{obj.etag}"'})
        mock._fire_callbacks("PUT", key)

    def do_POST(self) -> None:
        bucket, key, query = self._split_target()
        body = self._read_request_body()
        mock = self._mock()

        # Batch DeleteObjects: POST /<bucket>?delete
        if not key and "delete" in query:
            fault = mock._get_fault_snapshot()
            if fault.latency_ms > 0:
                time.sleep(fault.latency_ms / 1000.0)
            if fault.unreachable:
                self._drop_connection()
                return
            keys_to_delete: list[str] = []
            if body:
                root = ET.fromstring(body)
                for elem in root.iter():
                    if elem.tag.endswith("Key") and elem.text:
                        keys_to_delete.append(elem.text)
            for dk in keys_to_delete:
                mock._record_call("DELETE", dk)
            if fault.fail_prefixes and (fault.ops is None or "DELETE" in fault.ops):
                if any(
                    any(_prefix_matches(dk, p) for p in fault.fail_prefixes)
                    for dk in keys_to_delete
                ):
                    self._send_bytes(
                        fault.status,
                        _s3_error_xml("ServiceUnavailable", "Injected fault on batch DeleteObjects"),
                    )
                    return
            deleted_xml_parts: list[str] = []
            for dk in keys_to_delete:
                mock._delete_obj(bucket, dk)
                deleted_xml_parts.append(f"<Deleted><Key>{xml_escape(dk)}</Key></Deleted>")
                mock._fire_callbacks("DELETE", dk)
            resp_xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                + "".join(deleted_xml_parts)
                + "</DeleteResult>"
            ).encode("utf-8")
            self._send_bytes(200, resp_xml)
            return

        # InitiateMultipartUpload: POST /<bucket>/<key>?uploads
        if "uploads" in query:
            if self._apply_fault_and_record("MULTIPART", key):
                return
            content_type = self.headers.get("Content-Type") or "application/octet-stream"
            upload_id = mock._initiate_multipart(bucket, key, content_type=content_type)
            resp_xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Bucket>{xml_escape(bucket)}</Bucket>"
                f"<Key>{xml_escape(key)}</Key>"
                f"<UploadId>{xml_escape(upload_id)}</UploadId>"
                "</InitiateMultipartUploadResult>"
            ).encode("utf-8")
            self._send_bytes(200, resp_xml)
            mock._fire_callbacks("MULTIPART", key)
            return

        # CompleteMultipartUpload: POST /<bucket>/<key>?uploadId=U
        if "uploadId" in query:
            if self._apply_fault_and_record("MULTIPART", key):
                return
            upload_id = query["uploadId"][0]
            part_numbers: list[int] = []
            if body:
                root = ET.fromstring(body)
                for elem in root.iter():
                    if elem.tag.endswith("PartNumber") and elem.text:
                        part_numbers.append(int(elem.text.strip()))
            obj = mock._complete_multipart(upload_id, part_numbers=part_numbers or None)
            if obj is None:
                self._send_bytes(
                    404,
                    _s3_error_xml("NoSuchUpload", "The specified multipart upload does not exist.", resource=key),
                )
                return
            resp_xml = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Location>{xml_escape(mock.endpoint)}/{xml_escape(bucket)}/{xml_escape(key)}</Location>"
                f"<Bucket>{xml_escape(bucket)}</Bucket>"
                f"<Key>{xml_escape(key)}</Key>"
                f'<ETag>"{obj.etag}"</ETag>'
                "</CompleteMultipartUploadResult>"
            ).encode("utf-8")
            self._send_bytes(200, resp_xml, headers={"ETag": f'"{obj.etag}"'})
            mock._fire_callbacks("MULTIPART", key)
            return

        self._send_bytes(400, _s3_error_xml("InvalidRequest", "Unsupported POST request"))

    def do_DELETE(self) -> None:
        bucket, key, query = self._split_target()
        mock = self._mock()

        if "uploadId" in query:
            if self._apply_fault_and_record("MULTIPART", key):
                return
            mock._abort_multipart(query["uploadId"][0])
            self._send_bytes(204, b"")
            mock._fire_callbacks("MULTIPART", key)
            return

        if self._apply_fault_and_record("DELETE", key):
            return
        mock._delete_obj(bucket, key)
        self._send_bytes(204, b"")
        mock._fire_callbacks("DELETE", key)

    def _handle_list(self, bucket: str, query: dict[str, list[str]]) -> None:
        mock = self._mock()
        is_v2 = (query.get("list-type") or [""])[0] == "2"
        prefix = (query.get("prefix") or [""])[0]
        delimiter = (query.get("delimiter") or [""])[0]
        max_keys_raw = (query.get("max-keys") or ["1000"])[0]
        max_keys = max(1, min(1000, int(max_keys_raw)))
        token = (query.get("continuation-token") or query.get("marker") or [""])[0]
        start_after = (query.get("start-after") or [""])[0]

        objs = mock._list_bucket_objects(bucket, prefix=prefix)
        contents: list[S3Object] = []
        common_prefixes_set: set[str] = set()

        for obj in objs:
            if delimiter:
                rel = obj.key[len(prefix) :]
                d_idx = rel.find(delimiter)
                if d_idx >= 0:
                    common_prefixes_set.add(prefix + rel[: d_idx + len(delimiter)])
                    continue
            contents.append(obj)

        # Combine contents and common prefixes into a unified sorted entry list for pagination
        entries: list[tuple[str, str, S3Object | None]] = []
        for obj in contents:
            entries.append((obj.key, "obj", obj))
        for cp in sorted(common_prefixes_set):
            entries.append((cp, "cp", None))
        entries.sort(key=lambda item: item[0])

        cursor = token or start_after
        if cursor:
            entries = [e for e in entries if e[0] > cursor]

        is_truncated = len(entries) > max_keys
        page = entries[:max_keys]
        next_token = page[-1][0] if is_truncated and page else ""

        parts: list[str] = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f"<Name>{xml_escape(bucket)}</Name>",
            f"<Prefix>{xml_escape(prefix)}</Prefix>",
        ]
        if delimiter:
            parts.append(f"<Delimiter>{xml_escape(delimiter)}</Delimiter>")
        if is_v2:
            parts.append(f"<KeyCount>{len(page)}</KeyCount>")
        parts.append(f"<MaxKeys>{max_keys}</MaxKeys>")
        parts.append(f"<IsTruncated>{'true' if is_truncated else 'false'}</IsTruncated>")
        if is_v2 and token:
            parts.append(f"<ContinuationToken>{xml_escape(token)}</ContinuationToken>")
        if is_v2 and next_token:
            parts.append(f"<NextContinuationToken>{xml_escape(next_token)}</NextContinuationToken>")
        elif not is_v2 and next_token:
            parts.append(f"<NextMarker>{xml_escape(next_token)}</NextMarker>")

        for entry_key, kind, obj in page:
            if kind == "obj" and obj is not None:
                parts.append(
                    "<Contents>"
                    f"<Key>{xml_escape(obj.key)}</Key>"
                    f"<LastModified>{obj.last_modified_iso}</LastModified>"
                    f'<ETag>"{obj.etag}"</ETag>'
                    f"<Size>{obj.size}</Size>"
                    "<StorageClass>STANDARD</StorageClass>"
                    "</Contents>"
                )
            else:
                parts.append(
                    "<CommonPrefixes>"
                    f"<Prefix>{xml_escape(entry_key)}</Prefix>"
                    "</CommonPrefixes>"
                )
        parts.append("</ListBucketResult>")
        self._send_bytes(200, "".join(parts).encode("utf-8"))
        mock._fire_callbacks("LIST", prefix)


class MockS3Server:
    """Thread-safe pure-stdlib path-style S3 HTTP server with prefix counters & fault injection."""

    def __init__(self, *, bucket: str = "pulse-e2e", host: str = "127.0.0.1", port: int = 0) -> None:
        self.default_bucket = bucket
        self._host = host
        self._lock = threading.RLock()
        self._buckets: dict[str, dict[str, S3Object]] = {bucket: {}}
        self._uploads: dict[str, _MultipartUploadState] = {}
        self._calls: list[tuple[str, str]] = []  # (key_or_prefix, op)
        self._counts: Counter[tuple[str, str]] = Counter()
        self._fault = S3FaultConfig()
        self._callbacks: list[Callable[..., Any]] = []

        self._httpd = _QuietThreadingHTTPServer((host, port), _MockS3Handler, self)
        self.port: int = int(self._httpd.server_address[1])
        self.endpoint: str = f"http://{host}:{self.port}"
        self.s3_url: str = f"s3://{bucket}"
        self._thread: threading.Thread | None = None

    def start(self) -> MockS3Server:
        if self._thread is not None:
            return self
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name=f"MockS3Server-{self.port}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._thread is None:
            return
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None

    def __enter__(self) -> MockS3Server:
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    def env_vars(self) -> dict[str, str]:
        """Return environment variables pointing pulse-node to this MockS3Server."""
        return {
            "PULSE_OBJECT_STORE_URL": self.s3_url,
            "AWS_ENDPOINT": self.endpoint,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": "us-east-1",
        }

    # --- Internal thread-safe state operations ---

    def _record_call(self, op: str, key_or_prefix: str) -> None:
        top = _top_prefix(key_or_prefix)
        with self._lock:
            self._calls.append((key_or_prefix, op))
            self._counts[(top, op)] += 1

    def _get_fault_snapshot(self) -> S3FaultConfig:
        with self._lock:
            return S3FaultConfig(
                unreachable=self._fault.unreachable,
                fail_prefixes=self._fault.fail_prefixes,
                status=self._fault.status,
                ops=self._fault.ops,
                latency_ms=self._fault.latency_ms,
            )

    def _fire_callbacks(self, op: str, key: str) -> None:
        with self._lock:
            cbs = list(self._callbacks)
        for cb in cbs:
            cb(op, key)

    def _get_obj(self, bucket: str, key: str) -> S3Object | None:
        with self._lock:
            return self._buckets.get(bucket, {}).get(key)

    def _put_obj(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        if_none_match: str | None = None,
        if_match: str | None = None,
    ) -> tuple[int, S3Object | None]:
        etag = hashlib.md5(data).hexdigest()
        obj = S3Object(
            key=key,
            data=bytes(data),
            etag=etag,
            last_modified_epoch=time.time(),
            content_type=content_type,
        )
        with self._lock:
            bmap = self._buckets.setdefault(bucket, {})
            existing = bmap.get(key)
            if if_none_match is not None:
                norm = _normalize_etag(if_none_match)
                if existing is not None and (norm == "*" or norm == existing.etag):
                    return 412, None
            if if_match is not None:
                norm = _normalize_etag(if_match)
                if existing is None or (norm != "*" and norm != existing.etag):
                    return 412, None
            bmap[key] = obj
            return 200, obj

    def _copy_obj(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        *,
        if_none_match: str | None = None,
    ) -> tuple[int, S3Object | None, str | None]:
        with self._lock:
            src_obj = self._buckets.get(src_bucket, {}).get(src_key)
            if src_obj is None:
                return 404, None, "NoSuchKey"
            dst_map = self._buckets.setdefault(dst_bucket, {})
            existing = dst_map.get(dst_key)
            if if_none_match is not None:
                norm = _normalize_etag(if_none_match)
                if existing is not None and (norm == "*" or norm == existing.etag):
                    return 412, None, "PreconditionFailed"
            copied = S3Object(
                key=dst_key,
                data=src_obj.data,
                etag=src_obj.etag,
                last_modified_epoch=time.time(),
                content_type=src_obj.content_type,
            )
            dst_map[dst_key] = copied
            return 200, copied, None

    def _delete_obj(self, bucket: str, key: str) -> bool:
        with self._lock:
            bmap = self._buckets.get(bucket)
            if bmap is None or key not in bmap:
                return False
            del bmap[key]
            return True

    def _list_bucket_objects(self, bucket: str, prefix: str = "") -> list[S3Object]:
        with self._lock:
            bmap = self._buckets.get(bucket, {})
            objs = [obj for k, obj in bmap.items() if k.startswith(prefix)]
        objs.sort(key=lambda o: o.key)
        return objs

    def _initiate_multipart(self, bucket: str, key: str, *, content_type: str = "application/octet-stream") -> str:
        upload_id = uuid.uuid4().hex
        with self._lock:
            self._uploads[upload_id] = _MultipartUploadState(
                bucket=bucket,
                key=key,
                content_type=content_type,
            )
        return upload_id

    def _upload_part(self, upload_id: str, part_number: int, data: bytes) -> str | None:
        etag = hashlib.md5(data).hexdigest()
        with self._lock:
            st = self._uploads.get(upload_id)
            if st is None:
                return None
            st.parts[part_number] = (bytes(data), etag)
        return etag

    def _complete_multipart(
        self, upload_id: str, *, part_numbers: list[int] | None = None
    ) -> S3Object | None:
        with self._lock:
            st = self._uploads.pop(upload_id, None)
            if st is None:
                return None
            ordered = part_numbers if part_numbers else sorted(st.parts.keys())
            combined = b"".join(st.parts[n][0] for n in ordered if n in st.parts)
            etag = hashlib.md5(combined).hexdigest()
            obj = S3Object(
                key=st.key,
                data=combined,
                etag=etag,
                last_modified_epoch=time.time(),
                content_type=st.content_type,
            )
            self._buckets.setdefault(st.bucket, {})[st.key] = obj
            return obj

    def _abort_multipart(self, upload_id: str) -> None:
        with self._lock:
            self._uploads.pop(upload_id, None)

    # --- Public Counter, Inspection & Fault Injection APIs ---

    def reset_counts(self) -> None:
        """Clear all recorded request counts."""
        with self._lock:
            self._calls.clear()
            self._counts.clear()

    def get_counts(self, prefix: str | None = None) -> dict[tuple[str, str], int]:
        """Return `{(top_prefix, op): count}`, optionally filtered by key/prefix."""
        with self._lock:
            if prefix is None or prefix == "":
                return dict(self._counts)
            filtered: Counter[tuple[str, str]] = Counter()
            for key_or_p, op in self._calls:
                if _prefix_matches(key_or_p, prefix):
                    filtered[(_top_prefix(key_or_p), op)] += 1
            return dict(filtered)

    def put_count(self, prefix: str = "") -> int:
        """Return number of PUT requests matching `prefix` (empty string counts all PUTs)."""
        with self._lock:
            if not prefix:
                return sum(1 for _, op in self._calls if op == "PUT")
            return sum(
                1 for key_or_p, op in self._calls if op == "PUT" and _prefix_matches(key_or_p, prefix)
            )

    def total_calls(self) -> int:
        """Return total number of S3 HTTP requests recorded since last reset."""
        with self._lock:
            return len(self._calls)

    def list_objects(self, prefix: str = "", *, bucket: str | None = None) -> list[str]:
        """Return sorted list of stored object keys matching `prefix` (without incrementing counters)."""
        b = bucket or self.default_bucket
        return [obj.key for obj in self._list_bucket_objects(b, prefix=prefix)]

    def get_object(self, key: str, *, bucket: str | None = None) -> bytes | None:
        """Return raw object bytes for `key` (or None if missing), without incrementing counters."""
        obj = self._get_obj(bucket or self.default_bucket, key)
        return obj.data if obj is not None else None

    def put_object_raw(
        self,
        key: str,
        data: bytes,
        *,
        bucket: str | None = None,
        content_type: str = "application/octet-stream",
    ) -> S3Object:
        """Directly seed/overwrite an object in MockS3Server without incrementing HTTP counters."""
        _, obj = self._put_obj(
            bucket or self.default_bucket,
            key,
            data,
            content_type=content_type,
        )
        assert obj is not None
        return obj

    def delete_object_raw(self, key: str, *, bucket: str | None = None) -> bool:
        """Directly remove an object from MockS3Server without incrementing HTTP counters."""
        return self._delete_obj(bucket or self.default_bucket, key)

    def head_object(self, key: str, *, bucket: str | None = None) -> S3Object | None:
        """Return S3Object metadata (`size`, `etag`, `last_modified_epoch`) or None if absent."""
        return self._get_obj(bucket or self.default_bucket, key)

    def set_fault(
        self,
        *,
        unreachable: bool = False,
        fail_prefixes: Iterable[str] | None = None,
        status: int = 503,
        ops: Iterable[str] | None = None,
        latency_ms: int = 0,
    ) -> None:
        """Configure S3 fault injection (503s, unreachable drop, op/prefix filtering, latency)."""
        with self._lock:
            self._fault = S3FaultConfig(
                unreachable=bool(unreachable),
                fail_prefixes=tuple(fail_prefixes) if fail_prefixes else (),
                status=int(status),
                ops=frozenset(op.upper() for op in ops) if ops is not None else None,
                latency_ms=int(latency_ms),
            )

    def clear_faults(self) -> None:
        """Disable all injected S3 faults and latency."""
        with self._lock:
            self._fault = S3FaultConfig()

    def on_operation(self, callback: Callable[[str, str], Any]) -> None:
        """Register a callback `callback(op, key)` invoked when an S3 operation completes."""
        with self._lock:
            self._callbacks.append(callback)

    def clear_callbacks(self) -> None:
        """Remove all registered `on_operation` callbacks."""
        with self._lock:
            self._callbacks.clear()
