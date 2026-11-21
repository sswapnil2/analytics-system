/** Minimal span shape the trace view needs; the API returns more. */
export interface SpanNode {
  span_id: string;
  parent_span_id: string | null;
  start_ts_ms: number;
}

/**
 * Orders spans depth-first (parents before children, siblings by start time) and tags
 * each with its depth. Spans whose parent is missing, or that sit in a parent cycle,
 * are shown as roots so nothing disappears from the trace view.
 */
export function orderSpanTree<T extends SpanNode>(spans: T[]): { span: T; depth: number }[] {
  const ids = new Set(spans.map((s) => s.span_id));
  const children = new Map<string, T[]>();
  const roots: T[] = [];
  for (const s of spans) {
    const p = s.parent_span_id;
    if (p && p !== s.span_id && ids.has(p)) {
      const list = children.get(p) ?? [];
      list.push(s);
      children.set(p, list);
    } else {
      roots.push(s);
    }
  }
  const byStart = (a: T, b: T) => a.start_ts_ms - b.start_ts_ms;
  const out: { span: T; depth: number }[] = [];
  const seen = new Set<string>();
  const walk = (s: T, depth: number) => {
    if (seen.has(s.span_id)) return;
    seen.add(s.span_id);
    out.push({ span: s, depth });
    for (const c of (children.get(s.span_id) ?? []).sort(byStart)) walk(c, depth + 1);
  };
  for (const r of roots.sort(byStart)) walk(r, 0);
  // Spans only reachable through a cycle never hit a root; show them flat at the end.
  for (const s of [...spans].sort(byStart)) if (!seen.has(s.span_id)) walk(s, 0);
  return out;
}

/** "$0.0042" for small amounts, "$12.30" otherwise; "–" when unknown. */
export function formatUsd(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "–";
  if (v === 0) return "$0";
  if (Math.abs(v) < 0.01) return `$${v.toFixed(4)}`;
  return `$${v.toFixed(2)}`;
}

/** Eval score 0..1 as "87%"; "–" when there is no score yet. */
export function formatScore(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "–";
  return `${Math.round(v * 100)}%`;
}

export interface ChatMessage {
  role: string;
  text: string;
}

const textOf = (content: unknown): string | null => {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    // Anthropic / OpenAI content blocks: keep text, name the rest.
    const parts = content.map((b) =>
      typeof b === "string"
        ? b
        : b && typeof b === "object"
          ? typeof (b as { text?: unknown }).text === "string"
            ? (b as { text: string }).text
            : `[${String((b as { type?: unknown }).type ?? "block")}]`
          : "",
    );
    return parts.filter(Boolean).join("\n");
  }
  return null;
};

const asMessage = (m: unknown): ChatMessage | null => {
  if (!m || typeof m !== "object") return null;
  const o = m as { role?: unknown; content?: unknown; tool_calls?: unknown };
  if (typeof o.role !== "string") return null;
  let text = textOf(o.content);
  if (Array.isArray(o.tool_calls) && o.tool_calls.length) {
    const calls = o.tool_calls
      .map((c) => (c as { function?: { name?: string } })?.function?.name ?? "tool")
      .join(", ");
    text = `${text ? `${text}\n` : ""}→ calls ${calls}`;
  }
  return text === null ? null : { role: o.role, text };
};

/**
 * Pulls chat messages out of an LLM step's input or output so it can be read as a
 * conversation: a message list, `{messages}`, OpenAI `{choices:[{message}]}`, a single
 * `{role, content}`, or Anthropic `{role, content:[blocks]}`. Null when it isn't chat.
 */
export function toChatMessages(value: unknown): ChatMessage[] | null {
  if (!value || typeof value !== "object") return null;
  const o = value as { messages?: unknown; choices?: unknown };
  const list = Array.isArray(value)
    ? value
    : Array.isArray(o.messages)
      ? o.messages
      : Array.isArray(o.choices)
        ? o.choices.map((c) => (c as { message?: unknown })?.message)
        : [value];
  const out = list.map(asMessage);
  return out.length > 0 && out.every((m): m is ChatMessage => m !== null) ? out : null;
}
