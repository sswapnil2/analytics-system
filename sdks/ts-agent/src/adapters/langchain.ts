import type { PulseAgentClient, SpanHandle } from "../client.js";
import type { AdapterOptions } from "./shared.js";

export interface LangChainCallbackHandler {
  readonly name: string;
  handleChainStart?(
    chain: Record<string, unknown>,
    inputs: unknown,
    runId: string,
    parentRunId?: string,
    tags?: string[],
    metadata?: Record<string, unknown>,
  ): void;
  handleChainEnd?(outputs: unknown, runId: string, parentRunId?: string): void;
  handleChainError?(err: unknown, runId: string, parentRunId?: string): void;
  handleLLMStart?(
    llm: Record<string, unknown>,
    prompts: string[],
    runId: string,
    parentRunId?: string,
    extraParams?: Record<string, unknown>,
  ): void;
  handleChatModelStart?(
    llm: Record<string, unknown>,
    messages: unknown,
    runId: string,
    parentRunId?: string,
    extraParams?: Record<string, unknown>,
  ): void;
  handleLLMNewToken?(token: string, idx: unknown, runId: string): void;
  handleLLMEnd?(output: Record<string, unknown>, runId: string): void;
  handleLLMError?(err: unknown, runId: string): void;
  handleToolStart?(
    tool: Record<string, unknown>,
    input: string,
    runId: string,
    parentRunId?: string,
  ): void;
  handleToolEnd?(output: unknown, runId: string): void;
  handleToolError?(err: unknown, runId: string): void;
  handleRetrieverStart?(
    retriever: Record<string, unknown>,
    query: string,
    runId: string,
    parentRunId?: string,
  ): void;
  handleRetrieverEnd?(documents: unknown[], runId: string): void;
  handleRetrieverError?(err: unknown, runId: string): void;
  handleAgentAction?(action: Record<string, unknown>, runId: string): void;
  handleAgentEnd?(action: Record<string, unknown>, runId: string): void;
}

interface TrackedRun {
  span: SpanHandle;
  startTs: number;
  sawFirstToken: boolean;
}

function extractName(obj: Record<string, unknown> | undefined, fallback: string): string {
  if (!obj) return fallback;
  if (typeof obj.name === "string" && obj.name.length > 0) return obj.name;
  if (Array.isArray(obj.id) && obj.id.length > 0) {
    const last = obj.id[obj.id.length - 1];
    if (typeof last === "string" && last.length > 0) return last;
  }
  return fallback;
}

export function createLangChainCallbackHandler(
  pulseClient: PulseAgentClient,
  options: AdapterOptions = {},
): LangChainCallbackHandler {
  const runs = new Map<string, TrackedRun>();

  const resolveParentSpanId = (parentRunId?: string): string | null | undefined => {
    if (!parentRunId) return undefined;
    return runs.get(parentRunId)?.span.spanId;
  };

  const startRun = (
    runId: string,
    parentRunId: string | undefined,
    name: string,
    spanType: "agent" | "llm" | "tool" | "retrieval",
    extra: {
      input?: unknown;
      model?: string;
      toolName?: string;
      metadata?: Record<string, unknown>;
    } = {},
  ): void => {
    const startTs = Date.now();
    const span = pulseClient.span({
      spanId: runId,
      parentSpanId: resolveParentSpanId(parentRunId),
      name,
      spanType,
      startTsMs: startTs,
      model: extra.model,
      toolName: extra.toolName,
      input: extra.input,
      attributes: options.attributes,
      metadata: extra.metadata ?? options.metadata,
      emitStart: options.emitStart,
    });
    runs.set(runId, { span, startTs, sawFirstToken: false });
  };

  return {
    name: "pulse_agent_callback_handler",

    handleChainStart(chain, inputs, runId, parentRunId, _tags, metadata) {
      startRun(runId, parentRunId, extractName(chain, "langchain.chain"), "agent", {
        input: inputs,
        metadata,
      });
    },

    handleChainEnd(outputs, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ output: outputs });
    },

    handleChainError(err, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ error: err });
    },

    handleLLMStart(llm, prompts, runId, parentRunId, extraParams) {
      const inv = extraParams?.invocation_params as Record<string, unknown> | undefined;
      const model =
        (typeof inv?.model_name === "string" ? inv.model_name : undefined) ??
        (typeof inv?.model === "string" ? inv.model : undefined);
      startRun(runId, parentRunId, extractName(llm, "langchain.llm"), "llm", {
        input: prompts,
        model,
      });
    },

    handleChatModelStart(llm, messages, runId, parentRunId, extraParams) {
      const inv = extraParams?.invocation_params as Record<string, unknown> | undefined;
      const model =
        (typeof inv?.model_name === "string" ? inv.model_name : undefined) ??
        (typeof inv?.model === "string" ? inv.model : undefined);
      startRun(runId, parentRunId, extractName(llm, "langchain.chat_model"), "llm", {
        input: messages,
        model,
      });
    },

    handleLLMNewToken(_token, _idx, runId) {
      const tracked = runs.get(runId);
      if (!tracked || tracked.sawFirstToken) return;
      tracked.sawFirstToken = true;
      const now = Date.now();
      tracked.span.addEvent(
        "first_token",
        { ttft_ms: Math.max(0, now - tracked.startTs) },
        now,
      );
    },

    handleLLMEnd(output, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      const llmOut = output?.llmOutput as Record<string, unknown> | undefined;
      const tokUsage = (llmOut?.tokenUsage ?? llmOut?.usage) as Record<string, unknown> | undefined;
      const inputTokens =
        typeof tokUsage?.promptTokens === "number"
          ? tokUsage.promptTokens
          : typeof tokUsage?.input_tokens === "number"
            ? tokUsage.input_tokens
            : undefined;
      const outputTokens =
        typeof tokUsage?.completionTokens === "number"
          ? tokUsage.completionTokens
          : typeof tokUsage?.output_tokens === "number"
            ? tokUsage.output_tokens
            : undefined;
      const totalTokens =
        typeof tokUsage?.totalTokens === "number"
          ? tokUsage.totalTokens
          : typeof tokUsage?.total_tokens === "number"
            ? tokUsage.total_tokens
            : undefined;

      tracked.span.end({
        output,
        inputTokens,
        outputTokens,
        totalTokens,
      });
    },

    handleLLMError(err, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ error: err });
    },

    handleToolStart(tool, input, runId, parentRunId) {
      const toolName = extractName(tool, "langchain.tool");
      startRun(runId, parentRunId, toolName, "tool", {
        input,
        toolName,
      });
    },

    handleToolEnd(output, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ output });
    },

    handleToolError(err, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ error: err });
    },

    handleRetrieverStart(retriever, query, runId, parentRunId) {
      startRun(
        runId,
        parentRunId,
        extractName(retriever, "langchain.retriever"),
        "retrieval",
        { input: { query } },
      );
    },

    handleRetrieverEnd(documents, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({
        output: {
          documentsCount: documents.length,
          documents,
        },
        attributes: {
          "retrieval.documents_count": documents.length,
        },
      });
    },

    handleRetrieverError(err, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ error: err });
    },

    handleAgentAction(action, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      tracked.span.addEvent("agent_action", action);
    },

    handleAgentEnd(action, runId) {
      const tracked = runs.get(runId);
      if (!tracked) return;
      runs.delete(runId);
      tracked.span.end({ output: action });
    },
  };
}
