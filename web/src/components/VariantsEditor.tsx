import React from "react";
import { Plus, X } from "lucide-react";
import { btn, field, fieldLabel } from "./ui";

/** One variant in the new-flag form: weight in percent, payload as JSON text. */
export interface VariantDraft {
  key: string;
  pct: number;
  payload: string;
}

export const defaultVariants = (): VariantDraft[] => [
  { key: "control", pct: 50, payload: "" },
  { key: "treatment", pct: 50, payload: "" },
];

const KEY_RE = /^[a-z0-9][a-z0-9_-]{0,47}$/;

/** First problem with the variants, or null when they can be saved. */
export function variantsProblem(vs: VariantDraft[]): string | null {
  if (vs.length === 0) return "Add at least one variant.";
  const keys = vs.map((v) => v.key.trim());
  const bad = keys.find((k) => !KEY_RE.test(k));
  if (bad !== undefined) return `Variant key "${bad}" must be lowercase letters, digits, _ or -.`;
  if (new Set(keys).size !== keys.length) return "Variant keys must be different.";
  const total = vs.reduce((a, v) => a + v.pct, 0);
  if (Math.abs(total - 100) > 0.001) return `Variant splits add up to ${total}%, not 100%.`;
  for (const v of vs) {
    if (!v.payload.trim()) continue;
    try {
      JSON.parse(v.payload);
    } catch {
      return `The payload for "${v.key}" isn't valid JSON.`;
    }
  }
  return null;
}

/** Variants as the API wants them: weights in basis points summing to exactly 10,000. */
export function toAllocations(vs: VariantDraft[]) {
  const out = vs.map((v) => ({
    key: v.key.trim(),
    weight_bps: Math.round(v.pct * 100),
    payload: v.payload.trim() ? JSON.parse(v.payload) : null,
  }));
  out[out.length - 1].weight_bps += 10_000 - out.reduce((a, v) => a + v.weight_bps, 0);
  return out;
}

export const VariantsEditor: React.FC<{ variants: VariantDraft[]; onChange: (v: VariantDraft[]) => void }> = ({
  variants,
  onChange,
}) => {
  const set = (i: number, patch: Partial<VariantDraft>) => onChange(variants.map((v, j) => (j === i ? { ...v, ...patch } : v)));
  const even = () => {
    const base = Math.floor(10_000 / variants.length) / 100;
    onChange(variants.map((v, i) => ({ ...v, pct: i === 0 ? +(100 - base * (variants.length - 1)).toFixed(2) : base })));
  };
  const total = variants.reduce((a, v) => a + v.pct, 0);
  return (
    <fieldset className="space-y-2 sm:col-span-3">
      <legend className={fieldLabel}>Variants</legend>
      <p className="text-xs text-muted">
        The first is the control. Name them for what users see, such as <code className="font-mono">short_form</code>, and
        add an optional JSON payload your app reads instead of hard-coding the change.
      </p>
      {variants.map((v, i) => (
        <div key={i} className="grid grid-cols-[minmax(0,1fr)_5.5rem_auto] items-center gap-2 sm:grid-cols-[minmax(0,10rem)_5.5rem_minmax(0,1fr)_auto]">
          <input
            aria-label={`Variant ${i + 1} key`}
            value={v.key}
            onChange={(e) => set(i, { key: e.target.value })}
            className={`${field} w-full font-mono`}
          />
          <label className="flex items-center gap-1 text-sm text-ink-2">
            <input
              aria-label={`Variant ${i + 1} share of users, percent`}
              type="number"
              min={0}
              max={100}
              value={v.pct}
              onChange={(e) => set(i, { pct: Number(e.target.value) })}
              className={`${field} w-full`}
            />
            %
          </label>
          <input
            aria-label={`Variant ${i + 1} payload (JSON, optional)`}
            value={v.payload}
            onChange={(e) => set(i, { payload: e.target.value })}
            placeholder='Payload, e.g. {"steps": 3}'
            className={`${field} order-last col-span-3 w-full font-mono sm:order-none sm:col-span-1`}
          />
          <button
            type="button"
            disabled={variants.length <= 1}
            onClick={() => onChange(variants.filter((_, j) => j !== i))}
            className={btn.ghost}
            aria-label={`Remove variant ${v.key || i + 1}`}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      ))}
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => onChange([...variants, { key: `variant_${variants.length}`, pct: 0, payload: "" }])}
          className={btn.ghost}
        >
          <Plus className="h-3.5 w-3.5" />
          Add variant
        </button>
        <button type="button" onClick={even} className={btn.ghost}>
          Split evenly
        </button>
        <span className={`text-xs ${Math.abs(total - 100) > 0.001 ? "text-danger" : "text-muted"}`}>Total {total}%</span>
      </div>
    </fieldset>
  );
};
