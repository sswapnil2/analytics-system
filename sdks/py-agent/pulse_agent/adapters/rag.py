"""Zero-dependency duck-typed RAG retrieval helpers with linked_span_ids propagation."""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, Sequence

from .. import context as _ctx
from ..client import PulseAgentClient, TraceContext
from ..model import SpanType
from ._helpers import _get_field, _open_adapter_span, _summarize_value
from .openai import _ProxyNamespace


def _normalize_docs_result(res: Any) -> tuple[Any, list[Any]]:
    if res is None:
        return [], []
    if isinstance(res, (list, tuple)):
        return res, list(res)
    if isinstance(res, (str, bytes, dict)):
        return res, [res]
    if hasattr(res, "__iter__"):
        mat = list(res)
        return mat, mat
    return res, [res]


def _summarize_docs(docs: Sequence[Any]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for idx, doc in enumerate(docs):
        doc_id = _get_field(doc, "id", "doc_id", "chunk_id")
        score = _get_field(doc, "score", "similarity")
        source = _get_field(doc, "source", "uri", "url")
        if source is None:
            meta = _get_field(doc, "metadata")
            source = _get_field(meta, "source", "uri", "url")
        entry: dict[str, Any] = {"id": str(doc_id) if doc_id is not None else f"doc_{idx}"}
        if score is not None:
            entry["score"] = score
        if source is not None:
            entry["source"] = str(source)
        content = _get_field(doc, "page_content", "text", "content")
        if isinstance(content, str):
            entry["chars"] = len(content)
        summaries.append(entry)
    return summaries


def trace_rag_retrieval(
    trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    query: Any = None,
    docs: Optional[Sequence[Any]] = None,
    *,
    retriever_fn: Optional[Callable[..., Any]] = None,
    name: str = "rag.retrieval",
    top_k: Optional[int] = None,
    metadata: Any = None,
) -> Any:
    """Records a retrieval span (either over pre-fetched docs or by invoking retriever_fn) and links downstream LLM spans."""
    in_payload: dict[str, Any] = {"query": _summarize_value(query)}
    if top_k is not None:
        in_payload["top_k"] = int(top_k)

    sp = _open_adapter_span(
        trace_or_client,
        name=name,
        span_type=SpanType.RETRIEVAL,
        input_data=in_payload,
    )
    if sp is None:
        if retriever_fn is not None:
            return retriever_fn(query)
        return list(docs) if docs is not None else []

    if metadata is not None:
        sp.set_metadata(metadata)

    with sp:
        if retriever_fn is not None:
            raw_res = retriever_fn(query)
            result_docs, doc_list = _normalize_docs_result(raw_res)
        else:
            raw_docs = list(docs) if docs is not None else []
            result_docs, doc_list = raw_docs, raw_docs
        doc_summaries = _summarize_docs(doc_list)
        sp.set_attributes({"retrieval.doc_count": len(doc_list)})
        sp.set_output({"count": len(doc_list), "docs": doc_summaries})

    cur_tr = trace_or_client if isinstance(trace_or_client, TraceContext) else _ctx.get_current_trace()
    if cur_tr is not None:
        cur_tr.last_retrieval_span_id = sp.span_id
    return result_docs


rag_adapter = trace_rag_retrieval


def wrap_retriever(
    retriever: Any,
    trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    *,
    name: str = "rag.retrieval",
) -> Any:
    """Wraps a callable retriever or object with .retrieve() / .get_relevant_documents()."""
    if callable(retriever):
        if inspect.iscoroutinefunction(retriever):

            @functools.wraps(retriever)
            async def async_wrapped(query: Any, *args: Any, **kwargs: Any) -> Any:
                sp = _open_adapter_span(
                    trace_or_client,
                    name=name,
                    span_type=SpanType.RETRIEVAL,
                    input_data={"query": _summarize_value(query)},
                )
                if sp is None:
                    return await retriever(query, *args, **kwargs)
                async with sp:
                    docs = await retriever(query, *args, **kwargs)
                    docs, doc_list = _normalize_docs_result(docs)
                    sp.set_attributes({"retrieval.doc_count": len(doc_list)})
                    sp.set_output({"count": len(doc_list), "docs": _summarize_docs(doc_list)})
                cur_tr = (
                    trace_or_client
                    if isinstance(trace_or_client, TraceContext)
                    else _ctx.get_current_trace()
                )
                if cur_tr is not None:
                    cur_tr.last_retrieval_span_id = sp.span_id
                return docs

            return async_wrapped

        @functools.wraps(retriever)
        def sync_wrapped(query: Any, *args: Any, **kwargs: Any) -> Any:
            return trace_rag_retrieval(
                trace_or_client,
                query=query,
                retriever_fn=lambda q: retriever(q, *args, **kwargs),
                name=name,
            )

        return sync_wrapped

    overrides: dict[str, Any] = {}
    for method_name in (
        "retrieve",
        "get_relevant_documents",
        "aretrieve",
        "aget_relevant_documents",
        "invoke",
        "ainvoke",
    ):
        method = getattr(retriever, method_name, None)
        if callable(method):
            overrides[method_name] = wrap_retriever(method, trace_or_client, name=name)
    if overrides:
        return _ProxyNamespace(retriever, overrides)
    return retriever
