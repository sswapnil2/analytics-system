import React, { useEffect, useId, useRef, useState } from "react";
import { Check, ChevronDown, Search } from "lucide-react";
import { field, fieldLabel } from "./ui";

export { propertyOptions } from "../lib/format";

export interface PickerOption {
  value: string;
  label: string;
  /** Heading the option is listed under. */
  group: string;
}

const RECENT_MAX = 5;


function readRecents(key: string): string[] {
  try {
    const v = JSON.parse(window.localStorage.getItem(key) ?? "[]");
    return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

function writeRecents(key: string, list: string[]) {
  try {
    window.localStorage.setItem(key, JSON.stringify(list));
  } catch {
    // Private mode or full storage: recents are a convenience, skip them.
  }
}

/**
 * Searchable single-choice picker for properties (and anything listed beside them, like cohorts).
 * A button opens a search box over a listbox; recent picks come first. Only lists what it's given.
 */
export const PropertyPicker: React.FC<{
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: PickerOption[];
  /** The "nothing chosen" choice, listed first, e.g. "No breakdown". Omit when a choice is required. */
  noneLabel?: string;
  /** localStorage key for recent picks; per project. */
  recentKey: string;
  /** Search box placeholder; also names what is listed in the no-match message. */
  placeholder?: string;
  /** For dense rows: the label is for screen readers only and the button is shorter. */
  dense?: boolean;
}> = ({ label, value, onChange, options, noneLabel, recentKey, placeholder = "Search properties", dense = false }) => {
  const id = useId();
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [active, setActive] = useState(0);
  // Opens upward when there's more room above, like a native select near the bottom of the window.
  const [up, setUp] = useState(false);
  const [recents, setRecents] = useState<string[]>(() => readRecents(recentKey));
  useEffect(() => setRecents(readRecents(recentKey)), [recentKey]);
  const boxRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  const needle = q.trim().toLowerCase();
  const none: PickerOption[] = noneLabel === undefined ? [] : [{ value: "", label: noneLabel, group: "" }];
  const byValue = new Map(options.map((o) => [o.value, o]));
  const recentOpts = needle
    ? []
    : recents.map((v) => byValue.get(v)).filter((o): o is PickerOption => !!o).map((o) => ({ ...o, group: "Recent" }));
  const matches = options.filter(
    (o) => !needle || o.label.toLowerCase().includes(needle) || o.value.toLowerCase().includes(needle),
  );
  const flat = [...(needle ? [] : none), ...recentOpts, ...matches];
  const current = byValue.get(value)?.label ?? (value ? value : noneLabel ?? "Choose…");

  const show = () => {
    // The search is empty whenever the list is closed, so this is the unfiltered list: start on the current choice.
    setActive(Math.max(0, flat.findIndex((o) => o.value === value)));
    const r = triggerRef.current?.getBoundingClientRect();
    const below = r ? window.innerHeight - r.bottom : Infinity;
    setUp(!!r && below < 340 && r.top > below);
    setOpen(true);
  };
  const close = (refocus: boolean) => {
    setOpen(false);
    setQ("");
    if (refocus) triggerRef.current?.focus();
  };
  const pick = (o: PickerOption) => {
    if (o.value) {
      const next = [o.value, ...recents.filter((v) => v !== o.value)].slice(0, RECENT_MAX);
      setRecents(next);
      writeRecents(recentKey, next);
    }
    onChange(o.value);
    close(true);
  };

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => boxRef.current && !boxRef.current.contains(e.target as Node) && close(false);
    window.addEventListener("mousedown", onDown);
    return () => window.removeEventListener("mousedown", onDown);
  }, [open]);
  useEffect(() => {
    if (open) listRef.current?.querySelector(`[data-i="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [open, active]);

  const onKey = (e: React.KeyboardEvent) => {
    const go = (n: number) => {
      e.preventDefault();
      if (flat.length) setActive((n + flat.length) % flat.length);
    };
    if (e.key === "ArrowDown") go(active + 1);
    else if (e.key === "ArrowUp") go(active - 1);
    else if (e.key === "PageDown") go(Math.min(flat.length - 1, active + 8));
    else if (e.key === "PageUp") go(Math.max(0, active - 8));
    else if (e.key === "Enter") {
      e.preventDefault();
      if (flat[active]) pick(flat[active]);
    } else if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      close(true);
    } else if (e.key === "Tab") close(false);
  };

  // Render options in order, with a heading wherever the group changes.
  const groups: { name: string; items: { o: PickerOption; i: number }[] }[] = [];
  flat.forEach((o, i) => {
    const last = groups[groups.length - 1];
    if (last && last.name === o.group) last.items.push({ o, i });
    else groups.push({ name: o.group, items: [{ o, i }] });
  });
  const optId = (i: number) => `${id}-opt-${i}`;

  return (
    <div className={`flex min-w-0 flex-col gap-1 ${dense ? "flex-1 basis-36" : ""}`} ref={boxRef}>
      <span id={`${id}-label`} className={dense ? "sr-only" : fieldLabel}>
        {label}
      </span>
      <div className="relative">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-labelledby={`${id}-label ${id}-value`}
        onClick={() => (open ? close(false) : show())}
        onKeyDown={(e) => {
          if ((e.key === "ArrowDown" || e.key === "ArrowUp") && !open) {
            e.preventDefault();
            show();
          }
        }}
        className={`${field} flex w-full items-center justify-between gap-2 text-left ${dense ? "py-1" : ""}`}
      >
        <span id={`${id}-value`} className={`truncate ${value ? "" : "text-muted"}`}>
          {current}
        </span>
        <ChevronDown className="h-4 w-4 shrink-0 text-muted" aria-hidden="true" />
      </button>
      {open && (
        <div className={`absolute left-0 right-0 z-40 min-w-[min(16rem,calc(100vw-2rem))] rounded-xl ${up ? "bottom-full mb-1 origin-bottom" : "top-full mt-1 origin-top"} border border-line-strong bg-panel p-1 shadow-pop motion-safe:animate-pop`}>
          <div className="relative p-1">
            <Search className="pointer-events-none absolute left-3.5 top-1/2 h-4 w-4 -translate-y-1/2 text-faint" aria-hidden="true" />
            <input
              autoFocus
              role="combobox"
              aria-expanded="true"
              aria-controls={`${id}-list`}
              aria-autocomplete="list"
              aria-activedescendant={flat[active] ? optId(active) : undefined}
              aria-label={`Search ${label.toLowerCase()}`}
              placeholder={placeholder}
              value={q}
              onChange={(e) => {
                setQ(e.target.value);
                setActive(0);
              }}
              onKeyDown={onKey}
              className={`${field} w-full pl-8`}
            />
          </div>
          <div ref={listRef} id={`${id}-list`} role="listbox" aria-label={label} className="max-h-64 overflow-auto">
            {flat.length === 0 && <p className="px-3 py-2 text-sm text-muted">Nothing matches "{q.trim()}".</p>}
            {groups.map((g, gi) => (
              <div key={`${g.name}-${gi}`} role="group" aria-label={g.name || undefined}>
                {g.name && (
                  <p className="px-3 pb-1 pt-2 text-xs font-semibold text-muted" aria-hidden="true">
                    {g.name}
                  </p>
                )}
                {g.items.map(({ o, i }) => {
                  const selected = o.value === value;
                  return (
                    <div
                      key={`${g.name}-${o.value}`}
                      id={optId(i)}
                      data-i={i}
                      role="option"
                      aria-selected={selected}
                      onMouseMove={() => i !== active && setActive(i)}
                      // Keep focus in the search box.
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => pick(o)}
                      className={`flex cursor-pointer items-center gap-2 rounded-md px-3 py-2 text-sm ${
                        i === active ? "bg-hover" : ""
                      } ${selected ? "font-semibold text-brand" : "text-ink"}`}
                    >
                      <span className="min-w-0 flex-1 truncate">{o.label}</span>
                      {selected && <Check className="h-4 w-4 shrink-0" aria-hidden="true" />}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      )}
      </div>
    </div>
  );
};
