/**
 * Pre-shared raw-content AI Agent Zstd dictionary (`AGENT_DICT_ID = 2`).
 * Must match `crates/pulse-agent/src/protocol.rs::agent_zstd_dict()` byte-for-byte
 * (1,014 bytes, SHA-256: 7d9a2067671e1e0d8396caedf72b8189b755ca1c6a5a1d47a833e66895ba1385).
 */

export const AGENT_DICT_ID = 2;

const AGENT_ZSTD_DICT_STR =
  '{"messages":[{"role":"system","content":\0' +
  '{"role":"user","content":\0' +
  '{"role":"assistant","content":\0' +
  '{"role":"tool","tool_call_id":\0' +
  '"tool_calls":[{"id":\0' +
  '"type":"function","function":{"name":\0' +
  '"arguments":\0' +
  '"choices":[{"index":0,"message":\0' +
  '"finish_reason":"stop"\0' +
  '"finish_reason":"tool_calls"\0' +
  '"usage":{"prompt_tokens":\0' +
  '"completion_tokens":\0' +
  '"total_tokens":\0' +
  "gen_ai.system\0gen_ai.request.model\0gen_ai.response.model\0" +
  "gen_ai.usage.input_tokens\0gen_ai.usage.output_tokens\0" +
  "gen_ai.operation.name\0gen_ai.tool.name\0gen_ai.response.finish_reasons\0" +
  "llm.model_name\0llm.token_count.prompt\0llm.token_count.completion\0" +
  "input.value\0output.value\0service.name\0deployment.environment\0" +
  "openai\0anthropic\0google\0bedrock\0azure\0" +
  "gpt-4o\0gpt-4o-mini\0gpt-4.1\0gpt-4.1-mini\0o3-mini\0" +
  "claude-sonnet-4\0claude-3-5-sonnet\0claude-3-5-haiku\0" +
  "gemini-2.5-pro\0gemini-2.5-flash\0" +
  "production\0staging\0development\0unversioned\0" +
  "agent\0llm\0tool\0retrieval\0embedding\0guardrail\0workflow\0" +
  "start\0update\0final\0ok\0error\0timeout\0cancelled\0" +
  "[REDACTED]\0_truncated\0_preview\0_original_bytes\0";

export const AGENT_ZSTD_DICT: Uint8Array = new TextEncoder().encode(AGENT_ZSTD_DICT_STR);
