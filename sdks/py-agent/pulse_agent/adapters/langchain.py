"""Zero-dependency duck-typed LangChain callback handler."""

from __future__ import annotations

import threading
from typing import Any, Optional, Sequence
import uuid

from .. import context as _ctx
from ..client import PulseAgentClient, SpanContext, TraceContext
from ..model import SpanType
from ..privacy import MAX_SPAN_DEPTH
from ._helpers import _get_field, _open_adapter_span, _summarize_value


class LangChainCallbackHandler:
    """Duck-typed LangChain callback handler without importing langchain_core."""

    def __init__(
        self,
        trace_or_client: Optional[TraceContext | PulseAgentClient] = None,
    ) -> None:
        self.trace_or_client = trace_or_client
        self._runs: dict[str, SpanContext] = {}
        self._anon_stack: list[str] = []
        self._lock = threading.Lock()

    def _push_run(
        self,
        run_id: Any,
        *,
        name: str,
        span_type: SpanType,
        input_data: Any = None,
        parent_run_id: Any = None,
        model: Optional[str] = None,
        tool_name: Optional[str] = None,
    ) -> Optional[SpanContext]:
        key = str(run_id) if run_id is not None else uuid.uuid4().hex
        parent_span_id = None
        parent_depth: Optional[int] = None
        if parent_run_id is not None:
            with self._lock:
                parent_sp = self._runs.get(str(parent_run_id))
                if parent_sp is not None:
                    parent_span_id = parent_sp.span_id
                    parent_depth = min(MAX_SPAN_DEPTH, parent_sp.depth + 1)

        sp = _open_adapter_span(
            self.trace_or_client,
            name=name,
            span_type=span_type,
            model=model,
            input_data=_summarize_value(input_data),
        )
        if sp is None:
            return None
        if parent_span_id is not None:
            sp.parent_span_id = parent_span_id
            if parent_depth is not None:
                sp.depth = parent_depth
        if tool_name is not None:
            sp.tool_name = tool_name
        with self._lock:
            self._runs[key] = sp
            if run_id is None:
                self._anon_stack.append(key)
        return sp

    def _pop_run(self, run_id: Any) -> Optional[SpanContext]:
        with self._lock:
            if run_id is not None:
                return self._runs.pop(str(run_id), None)
            if self._anon_stack:
                key = self._anon_stack.pop()
                return self._runs.pop(key, None)
            return None

    def _peek_run(self, run_id: Any) -> Optional[SpanContext]:
        with self._lock:
            if run_id is not None:
                return self._runs.get(str(run_id))
            if self._anon_stack:
                return self._runs.get(self._anon_stack[-1])
            return None

    # LLM callbacks
    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: Sequence[str],
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        model_name = _get_field(kwargs.get("invocation_params"), "model", "model_name") or _get_field(
            serialized, "name"
        )
        self._push_run(
            run_id,
            name=str(_get_field(serialized, "name", default="langchain.llm")),
            span_type=SpanType.LLM,
            input_data={"prompts": list(prompts)},
            parent_run_id=parent_run_id,
            model=model_name,
        )

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: Sequence[Sequence[Any]],
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        model_name = _get_field(kwargs.get("invocation_params"), "model", "model_name") or _get_field(
            serialized, "name"
        )
        self._push_run(
            run_id,
            name=str(_get_field(serialized, "name", default="langchain.chat_model")),
            span_type=SpanType.LLM,
            input_data={"messages": _summarize_value(messages)},
            parent_run_id=parent_run_id,
            model=model_name,
        )

    def on_llm_new_token(self, token: str, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._peek_run(run_id)
        if sp is not None:
            sp.add_event("llm.token", {"token": token})

    def on_llm_end(self, response: Any, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is None:
            return
        llm_output = _get_field(response, "llm_output") or {}
        token_usage = _get_field(llm_output, "token_usage", "usage") or {}
        in_tok = int(_get_field(token_usage, "prompt_tokens", "input_tokens", default=0) or 0)
        out_tok = int(_get_field(token_usage, "completion_tokens", "output_tokens", default=0) or 0)
        tot_tok = int(_get_field(token_usage, "total_tokens", default=0) or 0)
        model_name = _get_field(llm_output, "model_name", "model")
        if model_name:
            sp.set_model(str(model_name))
        if in_tok or out_tok or tot_tok:
            sp.set_usage(input_tokens=in_tok, output_tokens=out_tok, total_tokens=tot_tok or None)
        sp.set_output(_summarize_value(response))
        sp.end()

    def on_llm_error(self, error: BaseException, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_error(error)
            sp.end()

    # Chain callbacks
    def on_chain_start(
        self,
        serialized: dict[str, Any],
        inputs: Any,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        del kwargs
        self._push_run(
            run_id,
            name=str(_get_field(serialized, "name", default="langchain.chain")),
            span_type=SpanType.AGENT,
            input_data=inputs,
            parent_run_id=parent_run_id,
        )

    def on_chain_end(self, outputs: Any, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_output(_summarize_value(outputs))
            sp.end()

    def on_chain_error(self, error: BaseException, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_error(error)
            sp.end()

    # Tool callbacks
    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        del kwargs
        tname = str(_get_field(serialized, "name", default="langchain.tool"))
        sp = self._push_run(
            run_id,
            name=tname,
            span_type=SpanType.TOOL,
            input_data={"input": input_str},
            parent_run_id=parent_run_id,
            tool_name=tname,
        )
        if sp is not None:
            sp.tool_name = tname

    def on_tool_end(self, output: Any, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_output(_summarize_value(output))
            sp.end()

    def on_tool_error(self, error: BaseException, *, run_id: Any = None, **kwargs: Any) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_error(error)
            sp.end()

    # Retriever callbacks
    def on_retriever_start(
        self,
        serialized: dict[str, Any],
        query: str,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        del kwargs
        self._push_run(
            run_id,
            name=str(_get_field(serialized, "name", default="langchain.retriever")),
            span_type=SpanType.RETRIEVAL,
            input_data={"query": query},
            parent_run_id=parent_run_id,
        )

    def on_retriever_end(
        self, documents: Sequence[Any], *, run_id: Any = None, **kwargs: Any
    ) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_output({"documents": _summarize_value(list(documents))})
            sp.end()
            cur_tr = (
                self.trace_or_client
                if isinstance(self.trace_or_client, TraceContext)
                else _ctx.get_current_trace()
            )
            if cur_tr is not None:
                cur_tr.last_retrieval_span_id = sp.span_id

    def on_retriever_error(
        self, error: BaseException, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        del kwargs
        sp = self._pop_run(run_id)
        if sp is not None:
            sp.set_error(error)
            sp.end()


PulseLangChainCallbackHandler = LangChainCallbackHandler
