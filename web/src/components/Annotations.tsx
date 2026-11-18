import React, { useCallback, useEffect, useState } from "react";
import { Flag, Trash2 } from "lucide-react";
import { Annotation, api } from "../api/client";
import { useAccess } from "../auth/access";
import { btn, field } from "./ui";
import { errorMessage, formatDate } from "../lib/format";

/** The project's annotations within a range, reloaded when the range changes. */
export function useAnnotations(projectId: string, startMs: number, endMs: number) {
  const [list, setList] = useState<Annotation[]>([]);
  const [error, setError] = useState<string | null>(null);
  // Round to the minute so a range ending at "now" doesn't refetch on every render.
  const s = Math.floor(startMs / 60_000);
  const e = Math.ceil(endMs / 60_000);
  const reload = useCallback(async () => {
    try {
      setList(await api.listAnnotations(projectId, s * 60_000, e * 60_000));
      setError(null);
    } catch (err) {
      setError(errorMessage(err));
    }
  }, [projectId, s, e]);
  useEffect(() => void reload(), [reload]);
  return { list, error, reload };
}

const dateInput = (ms: number) => {
  const d = new Date(ms);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
};

/** Annotations listed under a chart, with a form to add one (members and above). */
export const AnnotationsList: React.FC<{
  projectId: string;
  annotations: Annotation[];
  error: string | null;
  onChanged: () => void;
}> = ({ projectId, annotations, error, onChanged }) => {
  const { canEdit } = useAccess();
  const [date, setDate] = useState(() => dateInput(Date.now()));
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  const add = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!text.trim()) return setFormError("Describe what happened, e.g. \"v2.3 released\".");
    setBusy(true);
    setFormError(null);
    try {
      // Noon local time, so the note lands on the chosen day in day buckets.
      const [y, m, d] = date.split("-").map(Number);
      await api.createAnnotation(projectId, { ts_ms: new Date(y, m - 1, d, 12).getTime(), text: text.trim() });
      setText("");
      onChanged();
    } catch (err) {
      setFormError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="mt-4 border-t border-line pt-3">
      {error && <p className="mb-2 text-sm text-danger">Couldn't load annotations: {error}</p>}
      {annotations.length > 0 && (
        <ul className="mb-3 space-y-1 text-sm">
          {annotations.map((a) => (
            <li key={a.id} className="flex items-center gap-2">
              <Flag className="h-3.5 w-3.5 shrink-0 text-warn" aria-hidden="true" />
              <span className="tabular text-xs text-muted">
                {formatDate(a.ts_ms, { day: "numeric", month: "short" })}
              </span>
              <span className="min-w-0 flex-1 truncate text-ink">{a.text}</span>
              {canEdit && (
                <button
                  type="button"
                  className="rounded p-1 text-faint hover:bg-hover hover:text-ink"
                  aria-label={`Delete annotation "${a.text}"`}
                  onClick={() => void api.deleteAnnotation(projectId, a.id).then(onChanged, (err) => setFormError(errorMessage(err)))}
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
      {canEdit && (
        <form onSubmit={(e) => void add(e)} className="flex flex-wrap items-center gap-2">
          <input type="date" value={date} onChange={(e) => setDate(e.target.value)} className={`${field} w-auto`} aria-label="Annotation date" />
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            maxLength={280}
            placeholder="Mark a release or change, e.g. v2.3 released"
            className={`${field} min-w-[14rem] flex-1`}
            aria-label="Annotation text"
          />
          <button type="submit" disabled={busy} className={btn.secondary}>
            <Flag className="h-3.5 w-3.5" /> {busy ? "Adding…" : "Add annotation"}
          </button>
        </form>
      )}
      {formError && <p className="mt-1 text-sm text-danger">{formError}</p>}
    </div>
  );
};
