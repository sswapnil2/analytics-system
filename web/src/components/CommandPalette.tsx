import React, { useEffect, useMemo, useRef, useState } from "react";
import { Search } from "lucide-react";
import { Command, matchCommands } from "../lib/palette";
import { Kbd } from "./ui";

/**
 * ⌘K / Ctrl+K palette: type to filter, arrows to move, Enter to run, Esc to close.
 * A combobox over a listbox, so screen readers announce the active option.
 */
export const CommandPalette: React.FC<{ commands: Command[]; onClose: () => void }> = ({ commands, onClose }) => {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const listRef = useRef<HTMLUListElement>(null);
  const returnTo = useRef(document.activeElement as HTMLElement | null);
  const shown = useMemo(() => matchCommands(commands, query), [commands, query]);

  useEffect(() => setActive(0), [query]);
  useEffect(() => {
    listRef.current?.querySelector(`[data-i="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [active]);
  // Hand focus back to whatever opened the palette.
  useEffect(() => () => returnTo.current?.focus?.(), []);

  const run = (c?: Command) => {
    if (!c) return;
    onClose();
    c.run();
  };
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((i) => (i + 1) % Math.max(1, shown.length));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => (i - 1 + shown.length) % Math.max(1, shown.length));
    } else if (e.key === "Enter") {
      e.preventDefault();
      run(shown[active]);
    } else if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    } else if (e.key === "Tab") {
      e.preventDefault(); // one focusable thing: keep focus in the dialog
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center bg-ink/30 px-4 pt-[12vh] motion-safe:animate-fade-in" onMouseDown={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        className="w-full max-w-xl overflow-hidden rounded-xl border border-line-strong bg-panel shadow-pop motion-safe:animate-pop"
        onMouseDown={(e) => e.stopPropagation()}
        onKeyDown={onKey}
      >
        <div className="flex items-center gap-2.5 border-b border-line px-4">
          <Search className="h-4 w-4 shrink-0 text-faint" aria-hidden="true" />
          <input
            autoFocus
            role="combobox"
            aria-expanded="true"
            aria-controls="cmdk-list"
            aria-activedescendant={shown[active] ? `cmdk-${shown[active].id}` : undefined}
            aria-label="Search pages and actions"
            placeholder="Go to a page or run an action"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="h-12 w-full bg-transparent text-md text-ink placeholder:text-faint focus:outline-none"
          />
          <Kbd>Esc</Kbd>
        </div>
        <ul id="cmdk-list" ref={listRef} role="listbox" aria-label="Results" className="max-h-[50vh] overflow-y-auto p-1.5">
          {shown.length === 0 && <li className="px-3 py-6 text-center text-sm text-muted">Nothing matches “{query}”.</li>}
          {shown.map((c, i) => (
            <React.Fragment key={c.id}>
              {(i === 0 || shown[i - 1].group !== c.group) && (
                <li role="presentation" className="px-2.5 pb-1 pt-2 text-xs text-muted">
                  {c.group}
                </li>
              )}
              <li
                id={`cmdk-${c.id}`}
                data-i={i}
                role="option"
                aria-selected={i === active}
                onMouseMove={() => setActive(i)}
                onClick={() => run(c)}
                className={`flex h-9 cursor-pointer items-center gap-2.5 rounded-lg px-2.5 text-sm ${i === active ? "bg-brand-soft text-ink" : "text-ink-2"}`}
              >
                {c.icon ? <c.icon className="h-4 w-4 shrink-0 text-muted" /> : <span className="w-4" />}
                <span className="truncate">{c.label}</span>
              </li>
            </React.Fragment>
          ))}
        </ul>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-line px-4 py-2 text-xs text-muted">
          <span><Kbd>↑</Kbd> <Kbd>↓</Kbd> move</span>
          <span><Kbd>Enter</Kbd> open</span>
          <span className="hidden sm:inline">In a run: <Kbd>J</Kbd> <Kbd>K</Kbd> steps, <Kbd>[</Kbd> <Kbd>]</Kbd> runs</span>
        </div>
      </div>
    </div>
  );
};
