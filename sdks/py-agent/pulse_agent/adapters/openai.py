"""Zero-dependency duck-typed OpenAI adapter (chat.completions, responses, embeddings, streaming)."""

from __future__ import annotations

import functools
import inspect
from typing import Any, AsyncIterator, Callable, Iterator, Optional

from ..client import PulseAgentClient, SpanContext, TraceContext
from ..model import SpanType
from ._helpers import _get_field, _open_adapter_span, _summarize_value


def _extract_openai_usage(resp: Any) -> tuple[int, int, int, int, Optional[str], Optional[str]]:
    model = _get_field(resp, "model")
    usage = _get_field(resp, "usage")
    in_tok = int(_get_field(usage, "prompt_tokens", "input_tokens", default=0) or 0)
    out_tok = int(_get_field(usage, "completion_tokens", "output_tokens", default=0) or 0)
    details = _get_field(usage, "prompt_tokens_details", "input_tokens_details")
    cached_tok = int(_get_field(details, "cached_tokens", default=0) or 0)
    if cached_tok == 0:
        cached_tok = int(_get_field(usage, "cached_tokens", default=0) or 0)
    total_tok = int(_get_field(usage, "total_tokens", default=0) or 0)
    if total_tok == 0 and (in_tok or out_tok):
        total_tok = in_tok + out_tok
    finish_reason: Optional[str] = None
    choices = _get_field(resp, "choices")
    if isinstance(choices, (list, tuple)) and choices:
        finish_reason = _get_field(choices[0], "finish_reason")
    return in_tok, out_tok, cached_tok, total_tok, model, finish_reason


def _finalize_openai_span(sp: SpanContext, resp: Any, *, is_embedding: bool = False) -> None:
    in_tok, out_tok, cached_tok, total_tok, resp_model, finish_reason = _extract_openai_usage(resp)
    if resp_model:
        sp.set_model(resp_model, provider="openai", finish_reason=finish_reason)
    elif finish_reason:
        sp.finish_reason = finish_reason
    sp.set_usage(
        input_tokens=in_tok,
        output_tokens=out_tok,
        cached_tokens=cached_tok,
        total_tokens=total_tok,
    )
    if is_embedding:
        data = _get_field(resp, "data")
        sp.set_output({"embedding_count": len(data) if isinstance(data, list) else 1})
    else:
        sp.set_output(_summarize_value(resp))


class _SyncOpenAIStreamWrapper:
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

    def __iter__(self) -> "_SyncOpenAIStreamWrapper":
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
        self._on_chunk(chunk)
        return chunk

    def _on_chunk(self, chunk: Any) -> None:
        in_tok, out_tok, cached_tok, total_tok, resp_model, finish_reason = _extract_openai_usage(
            chunk
        )
        if resp_model:
            self._span.set_model(resp_model, provider="openai")
        choices = _get_field(chunk, "choices")
        delta_text = None
        if isinstance(choices, (list, tuple)) and choices:
            delta = _get_field(choices[0], "delta")
            delta_text = _get_field(delta, "content")
            if isinstance(delta_text, str) and delta_text:
                self._chunks.append(delta_text)
        self._span.update(
            output={"text": "".join(self._chunks)} if self._chunks else None,
            input_tokens=in_tok or None,
            output_tokens=out_tok or None,
            cached_tokens=cached_tok or None,
            total_tokens=total_tok or None,
            finish_reason=finish_reason,
            emit=True,
        )

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

    def __enter__(self) -> "_SyncOpenAIStreamWrapper":
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


class _AsyncOpenAIStreamWrapper:
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

    def __aiter__(self) -> "_AsyncOpenAIStreamWrapper":
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
        in_tok, out_tok, cached_tok, total_tok, resp_model, finish_reason = _extract_openai_usage(
            chunk
        )
        if resp_model:
            self._span.set_model(resp_model, provider="openai")
        choices = _get_field(chunk, "choices")
        if isinstance(choices, (list, tuple)) and choices:
            delta = _get_field(choices[0], "delta")
            delta_text = _get_field(delta, "content")
            if isinstance(delta_text, str) and delta_text:
                self._chunks.append(delta_text)
        self._span.update(
            output={"text": "".join(self._chunks)} if self._chunks else None,
            input_tokens=in_tok or None,
            output_tokens=out_tok or None,
            cached_tokens=cached_tok or None,
            total_tokens=total_tok or None,
            finish_reason=finish_reason,
            emit=True,
        )
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

    async def __aenter__(self) -> "_AsyncOpenAIStreamWrapper":
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


def _wrap_openai_callable(
    fn: Callable[..., Any],
    trace_or_client: Optional[TraceContext | PulseAgentClient],
    *,
    name: str,
    span_type: SpanType,
    default_model: Optional[str],
    capture_stream: bool,
) -> Callable[..., Any]:
    is_embedding = span_type == SpanType.EMBEDDING

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
            model_name = kwargs.get("model") or default_model
            is_stream = bool(kwargs.get("stream", False)) and capture_stream
            in_payload = _summarize_value(
                kwargs.get("messages") or kwargs.get("input") or kwargs or args
            )
            sp = _open_adapter_span(
                trace_or_client,
                name=name,
                span_type=span_type,
                provider="openai",
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
                    return _AsyncOpenAIStreamWrapper(resp, sp)
                _finalize_openai_span(sp, resp, is_embedding=is_embedding)
                sp.end()
                return resp

            async with sp:
                resp = await fn(*args, **kwargs)
                _finalize_openai_span(sp, resp, is_embedding=is_embedding)
                return resp

        return async_wrapped

    @functools.wraps(fn)
    def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
        model_name = kwargs.get("model") or default_model
        is_stream = bool(kwargs.get("stream", False)) and capture_stream
        in_payload = _summarize_value(
            kwargs.get("messages") or kwargs.get("input") or kwargs or args
        )
        sp = _open_adapter_span(
            trace_or_client,
            name=name,
            span_type=span_type,
            provider="openai",
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
            if hasattr(resp, "__iter__") and not isinstance(resp, (dict, list, str, bytes)):
                return _SyncOpenAIStreamWrapper(resp, sp)
            _finalize_openai_span(sp, resp, is_embedding=is_embedding)
            sp.end()
            return resp

        with sp:
            resp = fn(*args, **kwargs)
            _finalize_openai_span(sp, resp, is_embedding=is_embedding)
            return resp

    return sync_wrapped


class _ProxyNamespace:
    def __init__(self, inner: Any, overrides: dict[str, Any]) -> None:
        self._inner = inner
        self._overrides = overrides

    def __getattr__(self, item: str) -> Any:
        if item in self._overrides:
            return self._overrides[item]
        return getattr(self._inner, item)


def wrap_openai(
    target: Any,
    trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    capture_stream: bool = True,
) -> Any:
    """Wraps an OpenAI callable or client object (chat.completions, responses, embeddings)."""
    if callable(target):
        return _wrap_openai_callable(
            target,
            trace_or_client,
            name=name or getattr(target, "__name__", "openai.call"),
            span_type=SpanType.LLM,
            default_model=model,
            capture_stream=capture_stream,
        )

    overrides: dict[str, Any] = {}
    chat = getattr(target, "chat", None)
    completions = getattr(chat, "completions", None) if chat is not None else None
    if completions is not None and callable(getattr(completions, "create", None)):
        wrapped_create = _wrap_openai_callable(
            completions.create,
            trace_or_client,
            name=name or "openai.chat.completions",
            span_type=SpanType.LLM,
            default_model=model,
            capture_stream=capture_stream,
        )
        wrapped_comp = _ProxyNamespace(completions, {"create": wrapped_create})
        overrides["chat"] = _ProxyNamespace(chat, {"completions": wrapped_comp})

    responses = getattr(target, "responses", None)
    if responses is not None and callable(getattr(responses, "create", None)):
        wrapped_resp_create = _wrap_openai_callable(
            responses.create,
            trace_or_client,
            name=name or "openai.responses",
            span_type=SpanType.LLM,
            default_model=model,
            capture_stream=capture_stream,
        )
        overrides["responses"] = _ProxyNamespace(responses, {"create": wrapped_resp_create})

    embeddings = getattr(target, "embeddings", None)
    if embeddings is not None and callable(getattr(embeddings, "create", None)):
        wrapped_emb_create = _wrap_openai_callable(
            embeddings.create,
            trace_or_client,
            name=name or "openai.embeddings",
            span_type=SpanType.EMBEDDING,
            default_model=model,
            capture_stream=False,
        )
        overrides["embeddings"] = _ProxyNamespace(embeddings, {"create": wrapped_emb_create})

    return _ProxyNamespace(target, overrides) if overrides else target


openai_adapter = wrap_openai
