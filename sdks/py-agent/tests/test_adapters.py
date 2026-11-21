import asyncio
import json
from pathlib import Path
import sys
import unittest

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

import _bootstrap  # noqa: F401
from pulse_agent import (
    LangChainCallbackHandler,
    PulseAgentClient,
    SpanPhase,
    SpanType,
    TraceStatus,
    get_current_span,
    trace_rag_retrieval,
    wrap_anthropic,
    wrap_gemini,
    wrap_openai,
    wrap_retriever,
)
from pulse_agent.context import get_current_span_stack


class _DummyRecordingClient(PulseAgentClient):
    def __init__(self) -> None:
        super().__init__(api_key="pk_test", endpoint="http://127.0.0.1:9", flush_interval_ms=0)

    def drain_all(self):
        return self._drain_chunk(10_000)


class TestAdapters(unittest.TestCase):
    def test_openai_callable_client_and_streaming(self) -> None:
        client = _DummyRecordingClient()
        with client.trace(trace_id="tr_oa", agent_name="oa") as tr:
            fn = wrap_openai(
                lambda **kw: {
                    "model": kw.get("model", "gpt-4o-mini"),
                    "usage": {"prompt_tokens": 12, "completion_tokens": 6, "total_tokens": 18},
                    "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                },
                tr,
            )
            out = fn(model="gpt-4o-mini")
            self.assertEqual(out["choices"][0]["message"]["content"], "ok")

            class _FakeCompletions:
                def create(self, **kw):
                    if kw.get("stream"):
                        return iter(
                            [
                                {"model": "gpt-4o-mini", "choices": [{"delta": {"content": "hi "}}]},
                                {
                                    "model": "gpt-4o-mini",
                                    "choices": [{"delta": {"content": "there"}, "finish_reason": "stop"}],
                                    "usage": {"prompt_tokens": 5, "completion_tokens": 2},
                                },
                            ]
                        )
                    return {"model": "gpt-4o-mini", "usage": {"prompt_tokens": 3, "completion_tokens": 1}}

            class _FakeEmbeddings:
                def create(self, **kw):
                    return {"model": "text-embedding-3-small", "data": [[0.1, 0.2]], "usage": {"prompt_tokens": 8}}

            class _FakeOpenAI:
                chat = type("Chat", (), {"completions": _FakeCompletions()})()
                embeddings = _FakeEmbeddings()

            wrapped_oa = wrap_openai(_FakeOpenAI(), tr)
            chunks = list(wrapped_oa.chat.completions.create(model="gpt-4o-mini", stream=True))
            self.assertEqual(len(chunks), 2)
            emb = wrapped_oa.embeddings.create(model="text-embedding-3-small", input="hello")
            self.assertEqual(len(emb["data"]), 1)

        batch = client.drain_all()
        self.assertEqual(len(batch.traces), 1)
        phases = [s.span_phase for s in batch.spans]
        self.assertIn(SpanPhase.START, phases)
        self.assertIn(SpanPhase.UPDATE, phases)
        self.assertIn(SpanPhase.FINAL, phases)
        types = {s.span_type for s in batch.spans}
        self.assertEqual(types, {SpanType.LLM, SpanType.EMBEDDING})

    def test_anthropic_and_gemini_adapters(self) -> None:
        client = _DummyRecordingClient()
        with client.trace(trace_id="tr_ag", agent_name="ag") as tr:
            an_fn = wrap_anthropic(
                lambda **kw: {
                    "model": "claude-3-5-sonnet",
                    "usage": {"input_tokens": 20, "output_tokens": 10, "cache_read_input_tokens": 5},
                    "content": [{"text": "ok"}],
                    "stop_reason": "end_turn",
                },
                tr,
            )
            an_fn(model="claude-3-5-sonnet")

            class _FakeGeminiModels:
                def generate_content(self, **kw):
                    return {
                        "text": "ok",
                        "usage_metadata": {"prompt_token_count": 10, "candidates_token_count": 4},
                    }

            class _FakeGeminiClient:
                models = _FakeGeminiModels()

            gm = wrap_gemini(_FakeGeminiClient(), tr)
            gm.models.generate_content(model="gemini-2.0-flash", contents="hello")

            async def async_gm(**kw):
                return {
                    "text": "async-ok",
                    "usageMetadata": {"promptTokenCount": 6, "candidatesTokenCount": 3},
                }

            wrapped_async_gm = wrap_gemini(async_gm, tr)
            asyncio.run(wrapped_async_gm(model="gemini-2.5-flash"))

        batch = client.drain_all()
        self.assertEqual(len(batch.spans), 3)
        self.assertEqual(batch.spans[0].provider, "anthropic")
        self.assertEqual(batch.spans[0].cached_tokens, 5)
        self.assertEqual(batch.spans[1].provider, "google")
        self.assertEqual(batch.spans[1].input_tokens, 10)
        self.assertEqual(batch.spans[2].output_tokens, 3)

    def test_langchain_callbacks_and_rag_linking(self) -> None:
        client = _DummyRecordingClient()
        with client.trace(trace_id="tr_lc", agent_name="lc") as tr:
            cb = LangChainCallbackHandler(tr)
            # Without run_id (E2E pattern)
            cb.on_llm_start({"name": "ChatOpenAI"}, ["hello"])
            cb.on_llm_end({"generations": [[{"text": "world"}]]})

            # With explicit run_id + parent_run_id + error
            cb.on_chain_start({"name": "AgentChain"}, {"q": "hi"}, run_id="r1")
            cb.on_tool_start({"name": "search"}, "query", run_id="r2", parent_run_id="r1")
            cb.on_tool_end("result", run_id="r2")
            cb.on_chain_error(RuntimeError("boom"), run_id="r1")

            # RAG retrieval + linked LLM span
            docs = trace_rag_retrieval(tr, query="refund", docs=[{"id": "d1", "score": 0.9}])
            self.assertEqual(len(docs), 1)
            ret = wrap_retriever(lambda q: [{"id": "d2", "score": 0.8}], tr)
            docs2 = ret("policy")
            self.assertEqual(docs2[0]["id"], "d2")

            with tr.generation(name="grounded.llm", model="gpt-4o-mini", provider="openai"):
                pass

        batch = client.drain_all()
        self.assertEqual(len(batch.spans), 6)
        tool_sp = [s for s in batch.spans if s.span_type == SpanType.TOOL][0]
        chain_sp = [s for s in batch.spans if s.span_type == SpanType.AGENT][0]
        self.assertEqual(tool_sp.parent_span_id, chain_sp.span_id)
        self.assertEqual(chain_sp.status, TraceStatus.ERROR)

        rag_spans = [s for s in batch.spans if s.span_type == SpanType.RETRIEVAL]
        llm_grounded = [s for s in batch.spans if s.name == "grounded.llm"][0]
        self.assertEqual(llm_grounded.linked_span_ids, [rag_spans[-1].span_id])

    def test_streaming_contextvar_cleanup_and_custom_iterable(self) -> None:
        client = _DummyRecordingClient()

        class _CustomIterable:
            def __init__(self, items):
                self._items = items

            def __iter__(self):
                return iter(self._items)

        with client.trace(trace_id="tr_oai_clean", agent_name="oa") as tr:
            oa_stream = wrap_openai(
                lambda **kw: _CustomIterable(
                    [
                        {"model": "gpt-4o-mini", "choices": [{"delta": {"content": "hello "}}]},
                        {
                            "model": "gpt-4o-mini",
                            "choices": [{"delta": {"content": "world"}, "finish_reason": "stop"}],
                            "usage": {"prompt_tokens": 3, "completion_tokens": 2},
                        },
                    ]
                ),
                tr,
            )
            chunks = list(oa_stream(model="gpt-4o-mini", stream=True))
            self.assertEqual(len(chunks), 2)
            self.assertIsNone(get_current_span())
            self.assertEqual(get_current_span_stack(), ())

            with tr.tool("sibling_after_stream") as sib:
                self.assertIsNone(sib.parent_span_id)
                self.assertEqual(sib.depth, 0)

            # Stream call raising before returning iterator must also not leak
            def _boom(**kw):
                raise RuntimeError("oai invoke boom")

            with self.assertRaises(RuntimeError):
                wrap_openai(_boom, tr)(model="gpt-4o-mini", stream=True)
            self.assertIsNone(get_current_span())

            # Async OpenAI stream + aclose
            async def _async_oa(**kw):
                async def _gen():
                    yield {"model": "gpt-4o-mini", "choices": [{"delta": {"content": "a"}}]}
                    yield {"model": "gpt-4o-mini", "choices": [{"delta": {"content": "b"}}]}

                return _gen()

            async def _run_async_oa():
                s = await wrap_openai(_async_oa, tr)(model="gpt-4o-mini", stream=True)
                self.assertIsNone(get_current_span())
                first = await s.__anext__()
                self.assertEqual(first["choices"][0]["delta"]["content"], "a")
                await s.aclose()
                self.assertIsNone(get_current_span())

            asyncio.run(_run_async_oa())

        batch = client.drain_all()
        final_spans = [s for s in batch.spans if s.span_phase == SpanPhase.FINAL]
        self.assertEqual(len(final_spans), 4)
        err_spans = [s for s in final_spans if s.status == TraceStatus.ERROR]
        self.assertEqual(len(err_spans), 1)
        self.assertEqual(err_spans[0].error_type, "RuntimeError")

    def test_anthropic_async_and_messages_stream_context_manager(self) -> None:
        client = _DummyRecordingClient()

        class _SyncMsgStreamCM:
            def __init__(self, fail: bool = False):
                self._fail = fail
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.closed = True
                return False

            @property
            def text_stream(self):
                yield "Hel"
                if self._fail:
                    raise ValueError("mid-stream fail")
                yield "lo"

            def get_final_message(self):
                return {
                    "model": "claude-3-5-sonnet",
                    "usage": {"input_tokens": 11, "output_tokens": 7},
                    "stop_reason": "end_turn",
                    "content": [{"text": "Hello"}],
                }

        class _AsyncMsgStreamCM:
            def __enter__(self):
                raise AssertionError("not sync")

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                return False

            @property
            def text_stream(self):
                async def _agen():
                    yield "Async"
                    yield "Hi"

                return _agen()

            async def get_final_message(self):
                return {
                    "model": "claude-3-5-sonnet",
                    "usage": {"input_tokens": 5, "output_tokens": 4},
                    "stop_reason": "end_turn",
                }

        class _FakeMessages:
            def create(self, **kw):
                return iter(
                    [
                        {"delta": {"text": "chunk1"}},
                        {"delta": {"text": "chunk2"}, "message": {"usage": {"input_tokens": 2, "output_tokens": 3}}},
                    ]
                )

            def stream(self, **kw):
                return _SyncMsgStreamCM(fail=kw.get("fail", False))

        class _FakeAsyncMessages:
            async def create(self, **kw):
                async def _agen():
                    yield {"delta": {"text": "hi"}}
                    raise RuntimeError("async mid fail")

                return _agen()

            def stream(self, **kw):
                return _AsyncMsgStreamCM()

        with client.trace(trace_id="tr_an", agent_name="an") as tr:
            an = wrap_anthropic(type("C", (), {"messages": _FakeMessages()})(), tr)
            evs = list(an.messages.create(model="claude-3-5-sonnet", stream=True))
            self.assertEqual(len(evs), 2)
            self.assertIsNone(get_current_span())

            # Sync messages.stream context manager: span must NOT close until __exit__
            with an.messages.stream(model="claude-3-5-sonnet") as s:
                self.assertEqual(list(s.text_stream), ["Hel", "lo"])
                # Inspect queue non-destructively before __exit__
                self.assertFalse(
                    any(
                        kind == "span"
                        and item.span_phase == SpanPhase.FINAL
                        and item.name == "anthropic.messages.stream"
                        for kind, item in client._queue
                    )
                )
                final_msg = s.get_final_message()
                self.assertEqual(final_msg["usage"]["output_tokens"], 7)

            # Sync messages.stream error inside context
            with self.assertRaises(ValueError):
                with an.messages.stream(model="claude-3-5-sonnet", fail=True) as s_err:
                    list(s_err.text_stream)

            # Async Anthropic create(stream=True) & messages.stream
            an_async = wrap_anthropic(type("AC", (), {"messages": _FakeAsyncMessages()})(), tr)

            async def _run_async():
                st = await an_async.messages.create(model="claude-3-5-sonnet", stream=True)
                with self.assertRaises(RuntimeError):
                    async for _ in st:
                        pass
                self.assertIsNone(get_current_span())

                async with an_async.messages.stream(model="claude-3-5-sonnet") as acm:
                    parts = [p async for p in acm.text_stream]
                    self.assertEqual(parts, ["Async", "Hi"])
                    fm = await acm.get_final_message()
                    self.assertEqual(fm["usage"]["output_tokens"], 4)

            asyncio.run(_run_async())

        batch = client.drain_all()
        finals = [s for s in batch.spans if s.span_phase == SpanPhase.FINAL]
        self.assertEqual(len(finals), 5)
        self.assertEqual(finals[1].output_tokens, 7)
        self.assertEqual(finals[2].status, TraceStatus.ERROR)
        self.assertEqual(finals[2].error_type, "ValueError")
        self.assertEqual(finals[3].status, TraceStatus.ERROR)
        self.assertEqual(finals[3].error_type, "RuntimeError")
        self.assertEqual(finals[4].output_tokens, 4)

    def test_gemini_streaming_sync_and_async(self) -> None:
        client = _DummyRecordingClient()

        class _FakeGemModels:
            def generate_content_stream(self, **kw):
                def _gen():
                    yield {"text": "Part1 ", "usage_metadata": {"prompt_token_count": 4, "candidates_token_count": 1}}
                    if kw.get("fail"):
                        raise RuntimeError("gem stream boom")
                    yield {"text": "Part2", "usage_metadata": {"prompt_token_count": 4, "candidates_token_count": 3}}

                return _gen()

        with client.trace(trace_id="tr_gem_s", agent_name="gem") as tr:
            gm = wrap_gemini(type("G", (), {"models": _FakeGemModels()})(), tr)
            stream = gm.models.generate_content_stream(model="gemini-2.5-flash", contents="hi")
            self.assertIsNone(get_current_span())
            # Must not emit FINAL before iteration
            pre = client.drain_all()
            self.assertEqual([s.span_phase for s in pre.spans], [SpanPhase.START])

            chunks = list(stream)
            self.assertEqual(len(chunks), 2)
            self.assertIsNone(get_current_span())

            # Failing stream captures error on FINAL
            err_stream = gm.models.generate_content_stream(
                model="gemini-2.5-flash", contents="fail", fail=True
            )
            with self.assertRaises(RuntimeError):
                list(err_stream)

            # Async stream callable
            async def _async_gem(**kw):
                async def _agen():
                    yield {"text": "A1", "usageMetadata": {"promptTokenCount": 2, "candidatesTokenCount": 1}}
                    yield {"text": "A2", "usageMetadata": {"promptTokenCount": 2, "candidatesTokenCount": 2}}

                return _agen()

            async def _run_async():
                wrapped = wrap_gemini(_async_gem, tr)
                s = await wrapped(model="gemini-2.5-flash", stream=True)
                items = [c async for c in s]
                self.assertEqual(len(items), 2)
                self.assertIsNone(get_current_span())

            asyncio.run(_run_async())

        batch = client.drain_all()
        finals = [s for s in batch.spans if s.span_phase == SpanPhase.FINAL]
        self.assertEqual(len(finals), 3)
        self.assertEqual(finals[0].output, {"text": "Part1 Part2"})
        self.assertEqual(finals[0].output_tokens, 3)
        self.assertEqual(finals[1].status, TraceStatus.ERROR)
        self.assertEqual(finals[1].error_type, "RuntimeError")
        self.assertEqual(finals[2].output, {"text": "A1A2"})
        self.assertEqual(finals[2].output_tokens, 2)

    def test_langchain_child_depth_and_rag_edge_cases(self) -> None:
        client = _DummyRecordingClient()
        with client.trace(trace_id="tr_lc_d", agent_name="lc") as tr:
            cb = LangChainCallbackHandler(tr)
            cb.on_chain_start({"name": "RootChain"}, {"q": "hi"}, run_id="c1")
            cb.on_llm_start({"name": "LLM"}, ["prompt"], run_id="l1", parent_run_id="c1")
            cb.on_tool_start({"name": "Tool"}, "arg", run_id="t1", parent_run_id="l1")
            cb.on_tool_end("out", run_id="t1")
            cb.on_llm_end({"generations": [[{"text": "ans"}]]}, run_id="l1")
            cb.on_chain_end({"ok": True}, run_id="c1")

            # Single-dict return in trace_rag_retrieval
            single = trace_rag_retrieval(
                tr,
                query="q",
                retriever_fn=lambda q: {"id": "single_doc", "text": "hello"},
            )
            self.assertEqual(single["id"], "single_doc")

            # Multi-method retriever object wraps all present methods
            class _MultiRetriever:
                def retrieve(self, q):
                    return [{"id": "sync_ret"}]

                async def aretrieve(self, q):
                    return [{"id": "async_ret"}]

                def invoke(self, q):
                    return {"id": "inv_single"}

            wrapped_ret = wrap_retriever(_MultiRetriever(), tr)
            self.assertEqual(wrapped_ret.retrieve("q")[0]["id"], "sync_ret")
            self.assertEqual(wrapped_ret.invoke("q")["id"], "inv_single")
            self.assertEqual(asyncio.run(wrapped_ret.aretrieve("q"))[0]["id"], "async_ret")

        batch = client.drain_all()
        by_name = {s.name: s for s in batch.spans if s.span_type != SpanType.RETRIEVAL}
        self.assertEqual(by_name["RootChain"].depth, 0)
        self.assertEqual(by_name["LLM"].depth, 1)
        self.assertEqual(by_name["Tool"].depth, 2)
        rag_spans = [s for s in batch.spans if s.span_type == SpanType.RETRIEVAL]
        self.assertEqual(len(rag_spans), 4)
        for rs in rag_spans:
            self.assertEqual(rs.attributes["retrieval.doc_count"], 1)


if __name__ == "__main__":
    unittest.main()
