import type React from "react";

export interface Command {
  id: string;
  label: string;
  /** Section heading, e.g. "Go to". */
  group: string;
  /** Extra words that should match, never shown. */
  keywords?: string;
  icon?: React.ComponentType<{ className?: string }>;
  run: () => void;
}

/** Every word typed must appear somewhere in the label, group or keywords. */
export function matchCommands(commands: Command[], query: string): Command[] {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (!words.length) return commands;
  return commands.filter((c) => {
    const hay = `${c.label} ${c.group} ${c.keywords ?? ""}`.toLowerCase();
    return words.every((w) => hay.includes(w));
  });
}
