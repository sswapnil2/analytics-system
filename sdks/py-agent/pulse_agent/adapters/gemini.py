"""Zero-dependency duck-typed Google Gemini adapter (google-genai & google-generativeai)."""

from __future__ import annotations

import functools
import inspect
from typing import Any, AsyncIterator, Callable, Iterator, Optional

from ..client import PulseAgentClient, SpanContext, TraceContext
from ..model import SpanType
from ._helpers import _get_field, _open_adapter_span, _summarize_value
from .openai import _ProxyNamespace


def _extract_gemini_usage(resp: Any) -> tuple[int, int, int, int, Optional[str]]:
    model = _get_field(resp, "model_version", "model")
    usage = _get_field(resp, "usage_metadata", "usageMetadata")
    in_tok = int(
        _get_field(usage, "prompt_token_count", "promptTokenCount", "input_tokens", default=0)
        or 0
    )
    out_tok = int(
        _get_field(
            usage, "candidates_token_count", "candidatesTokenCount", "output_tokens", default=0
        )
        or 0
    )
    cached_tok = int(
        _get_field(usage, "cached_content_token_count", "cachedContentTokenCount", default=0) or 0
    )
    total_tok = int(
        _get_field(usage, "total_token_count", "totalTokenCount", default=0) or 0
    )
    if total_tok == 0 and (in_tok or out_tok):
        total_tok = in_tok + out_tok
    return in_tok, out_tok, cached_tok, total_tok, model


def _safe_gemini_text(resp: Any) -> Optional[str]:
    if isinstance(resp, dict):
        t = resp.get("text")
        if isinstance(t, str):
            return t
    else:
        try:
            t = getattr(resp, "text", None)
            if isinstance(t, str):
                return t
        except Exception:
            pass
    candidates = _get_field(resp, "candidates")
    if isinstance(candidates, (list, tuple)) and candidates:
        content = _get_field(candidates[0], "content")
        parts = _get_field(content, "parts")
        if isinstance(parts, (list, tuple)):
            texts = [_get_field(p, "text") for p in parts]
            joined = "".join(t for t in texts if isinstance(t, str))
            if joined:
                return joined
    return None


def _finalize_gemini_span(sp: SpanContext, resp: Any) -> None:
    in_tok, out_tok, cached_tok, total_tok, resp_model = _extract_gemini_usage(resp)
    if resp_model:
        sp.set_model(resp_model, provider="google")
    sp.set_usage(
        input_tokens=in_tok,
        output_tokens=out_tok,
        cached_tokens=cached_tok,
        total_tokens=total_tok,
    )
    text = _safe_gemini_text(resp)
    if isinstance(text, str) and not isinstance(resp, dict):
        sp.set_output({"text": text})
    elif isinstance(text, str) and "text" in resp:
        sp.set_output({"text": text})
    else:
        sp.set_output(_summarize_value(resp))


def _apply_gemini_chunk(span: SpanContext, chunk: Any, chunks: list[str]) -> None:
    text = _safe_gemini_text(chunk)
    if isinstance(text, str) and text:
        chunks.append(text)
    in_tok, out_tok, cached_tok, total_tok, resp_model = _extract_gemini_usage(chunk)
    if resp_model:
        span.set_model(resp_model, provider="google")
    span.update(
        output={"text": "".join(chunks)} if chunks else None,
        input_tokens=in_tok or None,
        output_tokens=out_tok or None,
        cached_tokens=cached_tok or None,
        total_tokens=total_tok or None,
        emit=True,
    )


class _SyncGeminiStreamWrapper:
    def __init__(self, inner: Any, span: SpanContext) -> None:
        self._inner = inner
        self._span = span
        self._chunks: list[str] = []
        self._closed = False
        self._iter: Optional[Iterator[Any]] = None

    def _ensure_iter(self) -> Iterator[Any]:
        if self._iter is None:
            self._iter = iter(self._inner)
        return self._iter

    def __iter__(self) -> "_SyncGeminiStreamWrapper":
        self._ensure_iter()
        return self

    def __next__(self) -> Any:
        try:
            chunk = next(self._ensure_iter())
        except StopIteration:
            self._finish()
            raise
        except BaseException as exc:
            self._span.set_error(exc)
            self._finish()
            raise
        _apply_gemini_chunk(self._span, chunk, self._chunks)
        return chunk

    def _finish(self) -> None:
        if not self._closed:
            self._closed = True
            if self._chunks:
                self._span.set_output({"text": "".join(self._chunks)})
            self._span.end()

    def close(self) -> None:
        try:
            if hasattr(self._inner, "close"):
                self._inner.close()
        finally:
            self._finish()

    def __enter__(self) -> "_SyncGeminiStreamWrapper":
        if hasattr(self._inner, "__enter__"):
            self._inner.__enter__()
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

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


class _AsyncGeminiStreamWrapper:
    def __init__(self, inner: Any, span: SpanContext) -> None:
        self._inner = inner
        self._span = span
        self._chunks: list[str] = []
        self._closed = False
        self._aiter: Optional[AsyncIterator[Any]] = None

    def _ensure_aiter(self) -> AsyncIterator[Any]:
        if self._aiter is None:
            self._aiter = self._inner.__aiter__()
        return self._aiter

    def __aiter__(self) -> "_AsyncGeminiStreamWrapper":
        self._ensure_aiter()
        return self

    async def __anext__(self) -> Any:
        try:
            chunk = await self._ensure_aiter().__anext__()
        except StopAsyncIteration:
            self._finish()
            raise
        except BaseException as exc:
            self._span.set_error(exc)
            self._finish()
            raise
        _apply_gemini_chunk(self._span, chunk, self._chunks)
        return chunk

    def _finish(self) -> None:
        if not self._closed:
            self._closed = True
            if self._chunks:
                self._span.set_output({"text": "".join(self._chunks)})
            self._span.end()

    async def aclose(self) -> None:
        try:
            if hasattr(self._inner, "aclose"):
                res = self._inner.aclose()
                if inspect.isawaitable(res):
                    await res
            elif hasattr(self._inner, "close"):
                self._inner.close()
        finally:
            self._finish()

    async def __aenter__(self) -> "_AsyncGeminiStreamWrapper":
        if hasattr(self._inner, "__aenter__"):
            await self._inner.__aenter__()
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
        return getattr(self._inner, item)


def _wrap_gemini_callable(
    fn: Callable[..., Any],
    trace_or_client: Optional[TraceContext | PulseAgentClient],
    *,
    name: str,
    default_model: Optional[str],
    capture_stream: bool = True,
    force_stream: bool = False,
) -> Callable[..., Any]:
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
            model_name = kwargs.get("model") or default_model
            is_stream = (force_stream or bool(kwargs.get("stream", False))) and capture_stream
            in_payload = _summarize_value(
                kwargs.get("contents") or kwargs.get("prompt") or kwargs or args
            )
            sp = _open_adapter_span(
                trace_or_client,
                name=name,
                span_type=SpanType.LLM,
                provider="google",
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
                if hasattr(resp, "__aiter__"):
                    return _AsyncGeminiStreamWrapper(resp, sp)
                _finalize_gemini_span(sp, resp)
                sp.end()
                return resp

            async with sp:
                resp = await fn(*args, **kwargs)
                _finalize_gemini_span(sp, resp)
                return resp

        return async_wrapped

    @functools.wraps(fn)
    def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
        model_name = kwargs.get("model") or default_model
        is_stream = (force_stream or bool(kwargs.get("stream", False))) and capture_stream
        in_payload = _summarize_value(
            kwargs.get("contents") or kwargs.get("prompt") or kwargs or args
        )
        sp = _open_adapter_span(
            trace_or_client,
            name=name,
            span_type=SpanType.LLM,
            provider="google",
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
            if hasattr(resp, "__aiter__") and not hasattr(resp, "__iter__"):
                return _AsyncGeminiStreamWrapper(resp, sp)
            if hasattr(resp, "__iter__") and not isinstance(resp, (dict, list, str, bytes)):
                return _SyncGeminiStreamWrapper(resp, sp)
            _finalize_gemini_span(sp, resp)
            sp.end()
            return resp
        with sp:
            resp = fn(*args, **kwargs)
            _finalize_gemini_span(sp, resp)
            return resp

    return sync_wrapped


def wrap_gemini(
    target: Any,
    trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    capture_stream: bool = True,
) -> Any:
    """Wraps a Gemini callable, google-genai client, or GenerativeModel."""
    if callable(target):
        return _wrap_gemini_callable(
            target,
            trace_or_client,
            name=name or getattr(target, "__name__", "gemini.generate_content"),
            default_model=model,
            capture_stream=capture_stream,
        )

    models = getattr(target, "models", None)
    if models is not None:
        m_overrides: dict[str, Any] = {}
        if callable(getattr(models, "generate_content", None)):
            m_overrides["generate_content"] = _wrap_gemini_callable(
                models.generate_content,
                trace_or_client,
                name=name or "gemini.models.generate_content",
                default_model=model,
                capture_stream=capture_stream,
            )
        if callable(getattr(models, "generate_content_stream", None)):
            m_overrides["generate_content_stream"] = _wrap_gemini_callable(
                models.generate_content_stream,
                trace_or_client,
                name=name or "gemini.models.generate_content_stream",
                default_model=model,
                capture_stream=capture_stream,
                force_stream=True,
            )
        if m_overrides:
            return _ProxyNamespace(target, {"models": _ProxyNamespace(models, m_overrides)})

    # google-generativeai GenerativeModel
    gm_overrides: dict[str, Any] = {}
    inferred_model = model or getattr(target, "model_name", None)
    if callable(getattr(target, "generate_content", None)):
        gm_overrides["generate_content"] = _wrap_gemini_callable(
            target.generate_content,
            trace_or_client,
            name=name or "gemini.generate_content",
            default_model=inferred_model,
            capture_stream=capture_stream,
        )
    if callable(getattr(target, "generate_content_async", None)):
        gm_overrides["generate_content_async"] = _wrap_gemini_callable(
            target.generate_content_async,
            trace_or_client,
            name=name or "gemini.generate_content_async",
            default_model=inferred_model,
            capture_stream=capture_stream,
        )
    return _ProxyNamespace(target, gm_overrides) if gm_overrides else target


gemini_adapter = wrap_gemini
