import React, { useEffect, useRef, useState } from "react";
import { DecisionInput, DecisionOutcome, FlagAction } from "../api/client";
import { btn, field, fieldLabel } from "./ui";

/** A native modal <dialog>: focus is trapped, Esc closes, the page behind is inert. */
const Modal: React.FC<{ title: string; onClose: () => void; children: React.ReactNode }> = ({ title, onClose, children }) => {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const d = ref.current;
    if (d && !d.open) d.showModal();
    return () => d?.close();
  }, []);
  return (
    <dialog
      ref={ref}
      aria-label={title}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
      className="w-[min(34rem,calc(100vw-2rem))] rounded-xl border border-line-strong bg-panel p-0 text-ink shadow-pop backdrop:bg-black/40"
    >
      <div className="space-y-4 p-5">
        <h2 className="text-[0.9375rem] font-semibold text-ink">{title}</h2>
        {children}
      </div>
    </dialog>
  );
};

/** The last look before an experiment goes live. */
export const StartReview: React.FC<{
  name: string;
  flagKey: string;
  flagWasOff: boolean;
  trafficText: string;
  endsText: string;
  /** What will be measured, on whom: one line each, so a wrong metric or audience shows before launch. */
  recap: string[];
  /** Shown above the buttons when the plan can't detect the target in time. */
  warning: string | null;
  busy: boolean;
  onStart: () => void;
  onClose: () => void;
}> = ({ name, flagKey, flagWasOff, trafficText, endsText, recap, warning, busy, onStart, onClose }) => (
  <Modal title={`Start "${name}"?`} onClose={onClose}>
    <ul className="space-y-1 rounded-lg border border-line bg-sunken px-3 py-2 text-sm text-ink">
      {recap.map((r) => (
        <li key={r}>{r}</li>
      ))}
    </ul>
    <ul className="list-disc space-y-1.5 pl-5 text-sm text-ink-2">
      <li>
        Users start getting variants of <code className="font-mono">{flagKey}</code> now
        {flagWasOff ? ", so the flag turns on." : "."}
      </li>
      <li>{trafficText}</li>
      <li>It ends {endsText}. You can pause or stop it any time.</li>
      <li>
        Results only count users whose app asked for their variant with exposure tracking on. Check that with your engineer
        before starting.
      </li>
    </ul>
    {warning && (
      <p role="alert" className="rounded-lg border border-warn/40 bg-warn-soft px-3 py-2 text-sm text-ink">
        {warning}
      </p>
    )}
    <div className="flex justify-end gap-2">
      <button type="button" onClick={onClose} className={btn.ghost}>
        Back
      </button>
      <button type="button" disabled={busy} onClick={onStart} className={btn.primary}>
        {busy ? "Starting…" : "Start experiment"}
      </button>
    </div>
  </Modal>
);

const DEFAULT_ACTION: Record<DecisionOutcome, FlagAction> = {
  ship: "rollout",
  keep_control: "rollback",
  inconclusive: "rollback",
};

/** Ends an experiment with a recorded outcome, reason and what happens to its flag. */
export const ConcludeDialog: React.FC<{
  experimentName: string;
  flagKey: string;
  treatments: string[];
  initialVariant?: string;
  /** Guardrails with no data among exposed users: shipping needs an explicit check. */
  blindGuardrails: string[];
  /** Guardrails a variant made significantly worse, by variant. */
  harmedBy: Record<string, string[]>;
  /** Why the sample can't back a ship yet (no users, or short of the plan); null when it can. */
  sampleCaveat: string | null;
  busy: boolean;
  onConclude: (d: DecisionInput) => void;
  onClose: () => void;
}> = ({ experimentName, flagKey, treatments, initialVariant, blindGuardrails, harmedBy, sampleCaveat, busy, onConclude, onClose }) => {
  const [outcome, setOutcome] = useState<DecisionOutcome>(initialVariant ? "ship" : "inconclusive");
  const [variant, setVariant] = useState(initialVariant ?? treatments[0] ?? "");
  const [action, setAction] = useState<FlagAction>(DEFAULT_ACTION[initialVariant ? "ship" : "inconclusive"]);
  const [reason, setReason] = useState("");
  const [everyDays, setEveryDays] = useState(2);
  const [checked, setChecked] = useState<Set<string>>(new Set());
  const shipping = outcome === "ship";
  // Each reason the evidence is thin needs its own tick, and is saved with the decision.
  const harmed = shipping ? harmedBy[variant] ?? [] : [];
  const caveats = shipping
    ? [
        ...(harmed.length ? [`${variant} made ${harmed.join(", ")} significantly worse.`] : []),
        ...(sampleCaveat ? [sampleCaveat] : []),
        ...(blindGuardrails.length
          ? [
              `${blindGuardrails.join(", ")} ${blindGuardrails.length > 1 ? "have" : "has"} no data from users in this test, so Pulse can't tell whether ${variant} harmed ${blindGuardrails.length > 1 ? "them" : "it"}.`,
            ]
          : []),
      ]
    : [];
  const ready = reason.trim().length > 0 && caveats.every((c) => checked.has(c)) && (!shipping || !!variant);

  const pickOutcome = (o: DecisionOutcome) => {
    setOutcome(o);
    setAction(DEFAULT_ACTION[o]);
  };
  const actions: { value: FlagAction; label: string }[] = [
    ...(shipping
      ? [
          { value: "rollout" as const, label: `Give everyone ${variant}` },
          { value: "staged" as const, label: `Give ${variant} to 25%, then 50%, then everyone` },
        ]
      : []),
    { value: "rollback", label: "Turn the flag off, so everyone gets the default" },
    { value: "keep", label: "Leave the flag as it is, so the current split keeps serving" },
  ];

  return (
    <Modal title={`Conclude "${experimentName}"`} onClose={onClose}>
      <form
        noValidate
        className="space-y-4"
        onSubmit={(e) => {
          e.preventDefault();
          if (ready)
            onConclude({
              outcome,
              variant: shipping ? variant : null,
              reason: reason.trim(),
              flag_action: action,
              acknowledged: caveats,
              ...(action === "staged" ? { every_days: everyDays } : {}),
            });
        }}
      >
        <fieldset className="space-y-1.5">
          <legend className={fieldLabel}>Decision</legend>
          {treatments.map((t) => (
            <label key={t} className="flex items-center gap-2 text-sm text-ink">
              <input
                type="radio"
                name="outcome"
                checked={shipping && variant === t}
                onChange={() => {
                  setVariant(t);
                  pickOutcome("ship");
                }}
              />
              Ship {t}
            </label>
          ))}
          <label className="flex items-center gap-2 text-sm text-ink">
            <input type="radio" name="outcome" checked={outcome === "keep_control"} onChange={() => pickOutcome("keep_control")} />
            Keep control: the change didn't help or did harm
          </label>
          <label className="flex items-center gap-2 text-sm text-ink">
            <input type="radio" name="outcome" checked={outcome === "inconclusive"} onChange={() => pickOutcome("inconclusive")} />
            Inconclusive: no clear answer
          </label>
        </fieldset>

        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>
            What happens to <code className="font-mono">{flagKey}</code>
          </span>
          <select value={action} onChange={(e) => setAction(e.target.value as FlagAction)} className={field}>
            {actions.map((a) => (
              <option key={a.value} value={a.value}>
                {a.label}
              </option>
            ))}
          </select>
          {action === "staged" && (
            <span className="flex flex-wrap items-center gap-2 text-xs text-muted">
              Next step every
              <select
                aria-label="Days between rollout steps"
                value={everyDays}
                onChange={(e) => setEveryDays(Number(e.target.value))}
                className={`${field} w-auto py-0.5`}
              >
                {[1, 2, 3, 7].map((d) => (
                  <option key={d} value={d}>
                    {d} {d === 1 ? "day" : "days"}
                  </option>
                ))}
              </select>
              . Watch your dashboards between steps; changing the flag by hand stops the schedule.
            </span>
          )}
          {action === "keep" && (
            <span className="text-xs text-warn">Users keep getting both variants until you change the flag.</span>
          )}
        </label>

        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Why</span>
          <textarea
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            rows={3}
            placeholder="e.g. +7.7 pts approval, range +5.5% to +27.5%; guardrails held."
            className={field}
          />
          <span className="text-xs text-muted">Saved with the experiment, so the next person knows what was decided and why.</span>
        </label>

        {caveats.map((c) => (
          <label key={c} className="flex items-start gap-2 rounded-lg border border-warn/40 bg-warn-soft px-3 py-2 text-sm text-ink">
            <input
              type="checkbox"
              checked={checked.has(c)}
              onChange={(e) =>
                setChecked((prev) => {
                  const next = new Set(prev);
                  if (e.target.checked) next.add(c);
                  else next.delete(c);
                  return next;
                })
              }
              className="mt-1"
            />
            <span>
              {c} I accept this, and it's saved with the decision.
            </span>
          </label>
        ))}

        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className={btn.ghost}>
            Cancel
          </button>
          <button type="submit" disabled={busy || !ready} className={btn.primary}>
            {busy ? "Concluding…" : "Conclude"}
          </button>
        </div>
      </form>
    </Modal>
  );
};

/** Pausing serves everyone the app's default until resumed; say so and ask why. */
export const PauseDialog: React.FC<{
  experimentName: string;
  flagKey: string;
  busy: boolean;
  onPause: (reason: string) => void;
  onClose: () => void;
}> = ({ experimentName, flagKey, busy, onPause, onClose }) => {
  const [reason, setReason] = useState("");
  return (
    <Modal title={`Pause "${experimentName}"?`} onClose={onClose}>
      <form
        noValidate
        className="space-y-4"
        onSubmit={(e) => {
          e.preventDefault();
          onPause(reason.trim());
        }}
      >
        <ul className="list-disc space-y-1 pl-5 text-sm text-ink-2">
          <li>
            <code className="font-mono">{flagKey}</code> turns off, so everyone gets the app's default experience within a
            few minutes. No new users join.
          </li>
          <li>Users already in the test who come back meanwhile see the default too, which can shrink the measured difference.</li>
          <li>On resume, each user gets the same variant as before. The end date doesn't move.</li>
        </ul>
        <label className="flex flex-col gap-1">
          <span className={fieldLabel}>Why (optional)</span>
          <input
            value={reason}
            maxLength={500}
            onChange={(e) => setReason(e.target.value)}
            placeholder="e.g. Payment errors spiked; checking whether this test is the cause"
            className={field}
          />
          <span className="text-xs text-muted">Shown on the experiment while it's paused, and kept in the audit log.</span>
        </label>
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className={btn.ghost}>
            Cancel
          </button>
          <button type="submit" disabled={busy} className={btn.primary}>
            {busy ? "Pausing…" : "Pause and serve the default"}
          </button>
        </div>
      </form>
    </Modal>
  );
};
