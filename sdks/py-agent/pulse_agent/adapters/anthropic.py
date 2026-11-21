"""Zero-dependency duck-typed Anthropic adapter (messages.create, streaming)."""

from __future__ import annotations

import functools
import inspect
from typing import Any, AsyncIterator, Callable, Iterator, Optional

from ..client import PulseAgentClient, SpanContext, TraceContext
from ..model import SpanType
from ._helpers import _get_field, _open_adapter_span, _summarize_value
from .openai import _ProxyNamespace


def _extract_anthropic_usage(resp: Any) -> tuple[int, int, int, Optional[str], Optional[str]]:
    model = _get_field(resp, "model")
    usage = _get_field(resp, "usage")
    in_tok = int(_get_field(usage, "input_tokens", default=0) or 0)
    out_tok = int(_get_field(usage, "output_tokens", default=0) or 0)
    cached_tok = int(_get_field(usage, "cache_read_input_tokens", "cached_tokens", default=0) or 0)
    stop_reason = _get_field(resp, "stop_reason")
    return in_tok, out_tok, cached_tok, model, stop_reason


def _finalize_anthropic_span(sp: SpanContext, resp: Any) -> None:
    in_tok, out_tok, cached_tok, resp_model, stop_reason = _extract_anthropic_usage(resp)
    if resp_model:
        sp.set_model(resp_model, provider="anthropic", finish_reason=stop_reason)
    elif stop_reason:
        sp.finish_reason = stop_reason
    sp.set_usage(
        input_tokens=in_tok,
        output_tokens=out_tok,
        cached_tokens=cached_tok,
        total_tokens=in_tok + out_tok,
    )
    sp.set_output(_summarize_value(resp))


def _apply_anthropic_event(
    span: SpanContext,
    event: Any,
    chunks: list[str],
) -> None:
    if isinstance(event, str):
        if event:
            chunks.append(event)
            span.update(output={"text": "".join(chunks)}, emit=True)
        return
    delta = _get_field(event, "delta")
    text = _get_field(delta, "text")
    if isinstance(text, str) and text:
        chunks.append(text)
    msg = _get_field(event, "message") or event
    in_tok, out_tok, cached_tok, resp_model, stop_reason = _extract_anthropic_usage(msg)
    if stop_reason is None:
        stop_reason = _get_field(delta, "stop_reason")
    if resp_model:
        span.set_model(resp_model, provider="anthropic")
    span.update(
        output={"text": "".join(chunks)} if chunks else None,
        input_tokens=in_tok or None,
        output_tokens=out_tok or None,
        cached_tokens=cached_tok or None,
        finish_reason=stop_reason,
        emit=True,
    )


class _SyncAnthropicTextStream:
    def __init__(self, inner: Any, wrapper: "_SyncAnthropicStreamWrapper") -> None:
        self._inner = inner
        self._wrapper = wrapper
        self._iter: Optional[Iterator[Any]] = None

    def _ensure_iter(self) -> Iterator[Any]:
        if self._iter is None:
            self._iter = iter(self._inner)
        return self._iter

    def __iter__(self) -> "_SyncAnthropicTextStream":
        self._ensure_iter()
        return self

    def __next__(self) -> Any:
        try:
            chunk = next(self._ensure_iter())
        except StopIteration:
            if not self._wrapper._in_cm:
                self._wrapper._finish()
            raise
        except BaseException as exc:
            self._wrapper._span.set_error(exc)
            if not self._wrapper._in_cm:
                self._wrapper._finish()
            raise
        if isinstance(chunk, str):
            if chunk:
                self._wrapper._chunks.append(chunk)
                self._wrapper._span.update(
                    output={"text": "".join(self._wrapper._chunks)}, emit=True
                )
        else:
            _apply_anthropic_event(self._wrapper._span, chunk, self._wrapper._chunks)
        return chunk

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


class _AsyncAnthropicTextStream:
    def __init__(self, inner: Any, wrapper: "_AsyncAnthropicStreamWrapper") -> None:
        self._inner = inner
        self._wrapper = wrapper
        self._aiter: Optional[AsyncIterator[Any]] = None

    def _ensure_aiter(self) -> AsyncIterator[Any]:
        if self._aiter is None:
            self._aiter = self._inner.__aiter__()
        return self._aiter

    def __aiter__(self) -> "_AsyncAnthropicTextStream":
        self._ensure_aiter()
        return self

    async def __anext__(self) -> Any:
        try:
            chunk = await self._ensure_aiter().__anext__()
        except StopAsyncIteration:
            if not self._wrapper._in_cm:
                self._wrapper._finish()
            raise
        except BaseException as exc:
            self._wrapper._span.set_error(exc)
            if not self._wrapper._in_cm:
                self._wrapper._finish()
            raise
        if isinstance(chunk, str):
            if chunk:
                self._wrapper._chunks.append(chunk)
                self._wrapper._span.update(
                    output={"text": "".join(self._wrapper._chunks)}, emit=True
                )
        else:
            _apply_anthropic_event(self._wrapper._span, chunk, self._wrapper._chunks)
        return chunk

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


class _SyncAnthropicStreamWrapper:
    def __init__(self, inner: Any, span: SpanContext) -> None:
        self._inner = inner
        self._active = inner
        self._span = span
        self._chunks: list[str] = []
        self._closed = False
        self._in_cm = False
        self._final_msg_seen = False
        self._iter: Optional[Iterator[Any]] = None
        self._aiter: Optional[AsyncIterator[Any]] = None

    def _ensure_iter(self) -> Iterator[Any]:
        if self._iter is None:
            self._iter = iter(self._active)
        return self._iter

    def __iter__(self) -> "_SyncAnthropicStreamWrapper":
        self._ensure_iter()
        return self

    def __next__(self) -> Any:
        try:
            event = next(self._ensure_iter())
        except StopIteration:
            if not self._in_cm:
                self._finish()
            raise
        except BaseException as exc:
            self._span.set_error(exc)
            if not self._in_cm:
                self._finish()
            raise
        _apply_anthropic_event(self._span, event, self._chunks)
        return event

    def _ensure_aiter(self) -> AsyncIterator[Any]:
        if self._aiter is None:
            self._aiter = self._active.__aiter__()
        return self._aiter

    def __aiter__(self) -> "_SyncAnthropicStreamWrapper":
        self._ensure_aiter()
        return self

    async def __anext__(self) -> Any:
        try:
            event = await self._ensure_aiter().__anext__()
        except StopAsyncIteration:
            if not self._in_cm:
                self._finish()
            raise
        except BaseException as exc:
            self._span.set_error(exc)
            if not self._in_cm:
                self._finish()
            raise
        _apply_anthropic_event(self._span, event, self._chunks)
        return event

    @property
    def text_stream(self) -> Any:
        inner_ts = getattr(self._active, "text_stream")
        if hasattr(inner_ts, "__aiter__"):
            return _AsyncAnthropicTextStream(inner_ts, self)  # type: ignore[arg-type]
        return _SyncAnthropicTextStream(inner_ts, self)

    def get_final_message(self, *args: Any, **kwargs: Any) -> Any:
        res = self._active.get_final_message(*args, **kwargs)
        if inspect.isawaitable(res):

            async def _await_final() -> Any:
                msg = await res
                _finalize_anthropic_span(self._span, msg)
                self._final_msg_seen = True
                return msg

            return _await_final()
        _finalize_anthropic_span(self._span, res)
        self._final_msg_seen = True
        return res

    def _finish(self) -> None:
        if not self._closed:
            self._closed = True
            if self._chunks and not self._final_msg_seen:
                self._span.set_output({"text": "".join(self._chunks)})
            self._span.end()

    def close(self) -> None:
        try:
            if hasattr(self._active, "close"):
                self._active.close()
            elif hasattr(self._inner, "close"):
                self._inner.close()
        finally:
            self._finish()

    async def aclose(self) -> None:
        try:
            if hasattr(self._active, "aclose"):
                res = self._active.aclose()
                if inspect.isawaitable(res):
                    await res
            elif hasattr(self._inner, "aclose"):
                res = self._inner.aclose()
                if inspect.isawaitable(res):
                    await res
            elif hasattr(self._active, "close"):
                self._active.close()
        finally:
            self._finish()

    def __enter__(self) -> "_SyncAnthropicStreamWrapper":
        self._in_cm = True
        if hasattr(self._inner, "__enter__"):
            entered = self._inner.__enter__()
            if entered is not None:
                self._active = entered
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        if exc_val is not None:
            self._span.set_error(exc_val, exc_tb=exc_tb)
        try:
            if hasattr(self._inner, "__exit__"):
                return self._inner.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._finish()
        return None

    async def __aenter__(self) -> "_SyncAnthropicStreamWrapper":
        self._in_cm = True
        if hasattr(self._inner, "__aenter__"):
            entered = await self._inner.__aenter__()
            if entered is not None:
                self._active = entered
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        if exc_val is not None:
            self._span.set_error(exc_val, exc_tb=exc_tb)
        try:
            if hasattr(self._inner, "__aexit__"):
                return await self._inner.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._finish()
        return None

    def __getattr__(self, item: str) -> Any:
        return getattr(self._active, item)


class _AsyncAnthropicStreamWrapper:
    def __init__(self, inner: Any, span: SpanContext) -> None:
        self._inner = inner
        self._active = inner
        self._span = span
        self._chunks: list[str] = []
        self._closed = False
        self._in_cm = False
        self._final_msg_seen = False
        self._aiter: Optional[AsyncIterator[Any]] = None

    def _ensure_aiter(self) -> AsyncIterator[Any]:
        if self._aiter is None:
            self._aiter = self._active.__aiter__()
        return self._aiter

    def __aiter__(self) -> "_AsyncAnthropicStreamWrapper":
        self._ensure_aiter()
        return self

    async def __anext__(self) -> Any:
        try:
            event = await self._ensure_aiter().__anext__()
        except StopAsyncIteration:
            if not self._in_cm:
                self._finish()
            raise
        except BaseException as exc:
            self._span.set_error(exc)
            if not self._in_cm:
                self._finish()
            raise
        _apply_anthropic_event(self._span, event, self._chunks)
        return event

    @property
    def text_stream(self) -> Any:
        inner_ts = getattr(self._active, "text_stream")
        if hasattr(inner_ts, "__aiter__"):
            return _AsyncAnthropicTextStream(inner_ts, self)
        return _SyncAnthropicTextStream(inner_ts, self)  # type: ignore[arg-type]

    async def get_final_message(self, *args: Any, **kwargs: Any) -> Any:
        res = self._active.get_final_message(*args, **kwargs)
        msg = await res if inspect.isawaitable(res) else res
        _finalize_anthropic_span(self._span, msg)
        self._final_msg_seen = True
        return msg

    def _finish(self) -> None:
        if not self._closed:
            self._closed = True
            if self._chunks and not self._final_msg_seen:
                self._span.set_output({"text": "".join(self._chunks)})
            self._span.end()

    async def aclose(self) -> None:
        try:
            if hasattr(self._active, "aclose"):
                res = self._active.aclose()
                if inspect.isawaitable(res):
                    await res
            elif hasattr(self._inner, "aclose"):
                res = self._inner.aclose()
                if inspect.isawaitable(res):
                    await res
            elif hasattr(self._active, "close"):
                self._active.close()
        finally:
            self._finish()

    async def __aenter__(self) -> "_AsyncAnthropicStreamWrapper":
        self._in_cm = True
        if hasattr(self._inner, "__aenter__"):
            entered = await self._inner.__aenter__()
            if entered is not None:
                self._active = entered
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        if exc_val is not None:
            self._span.set_error(exc_val, exc_tb=exc_tb)
        try:
            if hasattr(self._inner, "__aexit__"):
                return await self._inner.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._finish()
        return None

    def __getattr__(self, item: str) -> Any:
        return getattr(self._active, item)


def _wrap_anthropic_callable(
    fn: Callable[..., Any],
    trace_or_client: Optional[TraceContext | PulseAgentClient],
    *,
    name: str,
    default_model: Optional[str],
    capture_stream: bool,
    force_stream: bool = False,
) -> Callable[..., Any]:
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
            model_name = kwargs.get("model") or default_model
            is_stream = (force_stream or bool(kwargs.get("stream", False))) and capture_stream
            in_payload = _summarize_value(kwargs.get("messages") or kwargs or args)
            sp = _open_adapter_span(
                trace_or_client,
                name=name,
                span_type=SpanType.LLM,
                provider="anthropic",
                model=model_name,
                input_data=in_payload,
                emit_on_start=is_stream,
            )
            if sp is None:
                return await fn(*args, **kwargs)
            if is_stream:
                sp.emit_start()
                try:
                    with sp.scoped_context():
                        resp = await fn(*args, **kwargs)
                except BaseException as exc:
                    sp.set_error(exc)
                    sp.end()
                    raise
                if hasattr(resp, "__aiter__") or hasattr(resp, "__aenter__"):
                    return _AsyncAnthropicStreamWrapper(resp, sp)
                _finalize_anthropic_span(sp, resp)
                sp.end()
                return resp

            async with sp:
                resp = await fn(*args, **kwargs)
                _finalize_anthropic_span(sp, resp)
                return resp

        return async_wrapped

    @functools.wraps(fn)
    def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
        model_name = kwargs.get("model") or default_model
        is_stream = (force_stream or bool(kwargs.get("stream", False))) and capture_stream
        in_payload = _summarize_value(kwargs.get("messages") or kwargs or args)
        sp = _open_adapter_span(
            trace_or_client,
            name=name,
            span_type=SpanType.LLM,
            provider="anthropic",
            model=model_name,
            input_data=in_payload,
            emit_on_start=is_stream,
        )
        if sp is None:
            return fn(*args, **kwargs)
        if is_stream:
            sp.emit_start()
            try:
                with sp.scoped_context():
                    resp = fn(*args, **kwargs)
            except BaseException as exc:
                sp.set_error(exc)
                sp.end()
                raise
            if (hasattr(resp, "__aenter__") and not hasattr(resp, "__enter__")) or (
                hasattr(resp, "__aiter__") and not hasattr(resp, "__iter__")
            ):
                return _AsyncAnthropicStreamWrapper(resp, sp)
            if hasattr(resp, "__enter__") or (
                hasattr(resp, "__iter__") and not isinstance(resp, (dict, list, str, bytes))
            ):
                return _SyncAnthropicStreamWrapper(resp, sp)
            _finalize_anthropic_span(sp, resp)
            sp.end()
            return resp
        with sp:
            resp = fn(*args, **kwargs)
            _finalize_anthropic_span(sp, resp)
            return resp

    return sync_wrapped


def wrap_anthropic(
    target: Any,
    trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    capture_stream: bool = True,
) -> Any:
    """Wraps an Anthropic callable or client object (messages.create, messages.stream)."""
    if callable(target):
        return _wrap_anthropic_callable(
            target,
            trace_or_client,
            name=name or getattr(target, "__name__", "anthropic.messages"),
            default_model=model,
            capture_stream=capture_stream,
        )

    messages = getattr(target, "messages", None)
    if messages is None:
        return target
    msg_overrides: dict[str, Any] = {}
    if callable(getattr(messages, "create", None)):
        msg_overrides["create"] = _wrap_anthropic_callable(
            messages.create,
            trace_or_client,
            name=name or "anthropic.messages.create",
            default_model=model,
            capture_stream=capture_stream,
        )
    if callable(getattr(messages, "stream", None)):
        msg_overrides["stream"] = _wrap_anthropic_callable(
            messages.stream,
            trace_or_client,
            name=name or "anthropic.messages.stream",
            default_model=model,
            capture_stream=capture_stream,
            force_stream=True,
        )
    if not msg_overrides:
        return target
    return _ProxyNamespace(target, {"messages": _ProxyNamespace(messages, msg_overrides)})


anthropic_adapter = wrap_anthropic
