export { wrapOpenAI, type OpenAIClientLike, type OpenAICompletionsLike } from "./openai.js";
export {
  wrapAnthropic,
  type AnthropicClientLike,
  type AnthropicMessagesLike,
} from "./anthropic.js";
export { wrapGemini, type GeminiClientLike } from "./gemini.js";
export {
  createLangChainCallbackHandler,
  type LangChainCallbackHandler,
} from "./langchain.js";
export {
  traceEmbedding,
  traceGuardrail,
  traceRag,
  wrapRetriever,
  type EmbeddingTraceOptions,
  type GuardrailEvaluationResult,
  type GuardrailTraceOptions,
  type RagDocument,
  type RagTraceOptions,
  type RetrieverLike,
} from "./rag.js";
export type { AdapterOptions } from "./shared.js";
