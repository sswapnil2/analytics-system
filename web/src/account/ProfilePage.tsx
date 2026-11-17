import React, { useCallback, useEffect, useState } from "react";
import { Laptop } from "lucide-react";
import { ActiveSession, api, AuthUser } from "../api/client";
import { btn, EmptyState, ErrorNotice, field, fieldLabel, ListSkeleton, PageHeader, Panel } from "../components/ui";
import { errorMessage, formatTimestamp } from "../lib/format";
import { RoleBadge } from "./SettingsPage";

/** "Chrome on macOS" from a user agent; the raw string is in the row's title. */
function describeAgent(ua: string | null): string {
  if (!ua) return "Unknown device";
  const browser = /Edg\//.test(ua)
    ? "Edge"
    : /Firefox\//.test(ua)
      ? "Firefox"
      : /Chrome\//.test(ua)
        ? "Chrome"
        : /Safari\//.test(ua)
          ? "Safari"
          : /curl|python|node|okhttp/i.test(ua)
            ? "API client"
            : "Browser";
  const os = /iPhone|iPad/.test(ua)
    ? "iOS"
    : /Android/.test(ua)
      ? "Android"
      : /Mac OS X|Macintosh/.test(ua)
        ? "macOS"
        : /Windows/.test(ua)
          ? "Windows"
          : /Linux/.test(ua)
            ? "Linux"
            : "";
  return os ? `${browser} on ${os}` : browser;
}

const Saved: React.FC<{ text: string | null }> = ({ text }) =>
  text ? (
    <span role="status" className="text-sm text-ok">
      {text}
    </span>
  ) : null;

export const ProfilePage: React.FC<{ user: AuthUser | null; onUserChanged: (u: AuthUser) => void }> = ({
  user,
  onUserChanged,
}) => {
  const [name, setName] = useState(user?.name ?? "");
  const [savingName, setSavingName] = useState(false);
  const [nameMsg, setNameMsg] = useState<string | null>(null);
  const [nameErr, setNameErr] = useState<string | null>(null);

  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [pwBusy, setPwBusy] = useState(false);
  const [pwMsg, setPwMsg] = useState<string | null>(null);
  const [pwErr, setPwErr] = useState<string | null>(null);

  const [sessions, setSessions] = useState<ActiveSession[] | null>(null);
  const [sessErr, setSessErr] = useState<string | null>(null);

  useEffect(() => setName(user?.name ?? ""), [user?.name]);

  const loadSessions = useCallback(() => {
    setSessErr(null);
    api.listSessions().then(setSessions).catch((e) => setSessErr(errorMessage(e)));
  }, []);
  useEffect(loadSessions, [loadSessions]);

  if (!user) {
    return (
      <div className="space-y-5">
        <PageHeader title="Profile" description="Your account on this Pulse server." />
        <Panel>
          <EmptyState
            title="You're signed in with an access token"
            body="Tokens aren't a person, so there's no profile. Sign in with an email account to manage your name, password and sessions."
          />
        </Panel>
      </div>
    );
  }

  const saveName = async (e: React.FormEvent) => {
    e.preventDefault();
    setSavingName(true);
    setNameErr(null);
    setNameMsg(null);
    try {
      onUserChanged(await api.updateMe({ name: name.trim() }));
      setNameMsg("Saved");
    } catch (err) {
      setNameErr(errorMessage(err));
    } finally {
      setSavingName(false);
    }
  };

  const mismatch = confirm.length > 0 && confirm !== next;
  const tooShort = next.length > 0 && next.length < 10;
  const changePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    setPwErr(null);
    setPwMsg(null);
    if (!current || next.length < 10 || next !== confirm) return;
    setPwBusy(true);
    try {
      const res = await api.changePassword({ current_password: current, new_password: next });
      setCurrent("");
      setNext("");
      setConfirm("");
      setPwMsg(
        res.other_sessions_signed_out > 0
          ? `Password changed. Signed out ${res.other_sessions_signed_out} other session${res.other_sessions_signed_out === 1 ? "" : "s"}.`
          : "Password changed.",
      );
      loadSessions();
    } catch (err) {
      setPwErr(errorMessage(err));
    } finally {
      setPwBusy(false);
    }
  };

  const signOutSession = async (id: string) => {
    try {
      await api.revokeSession(id);
      loadSessions();
    } catch (err) {
      setSessErr(errorMessage(err));
    }
  };
  const signOutOthers = async () => {
    try {
      await api.revokeOtherSessions();
      loadSessions();
    } catch (err) {
      setSessErr(errorMessage(err));
    }
  };
  const others = sessions?.filter((s) => !s.current).length ?? 0;

  return (
    <div className="space-y-5">
      <PageHeader title="Profile" description="Your name, password and where you're signed in." />

      <Panel title="Account" actions={user.role ? <RoleBadge role={user.role} /> : undefined}>
        <form onSubmit={(e) => void saveName(e)} className="grid max-w-xl gap-4">
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Name</span>
            <input value={name} onChange={(e) => setName(e.target.value)} autoComplete="name" className={field} />
          </label>
          <label className="flex flex-col gap-1">
            <span className={fieldLabel}>Email</span>
            <input value={user.email} readOnly aria-describedby="email-hint" className={`${field} bg-sunken text-ink-2`} />
            <span id="email-hint" className="text-xs text-muted">
              Your sign-in email can't be changed here.
            </span>
          </label>
          {nameErr && <ErrorNotice message={nameErr} />}
          <div className="flex items-center gap-3">
            <button type="submit" disabled={savingName || name.trim() === (user.name ?? "")} className={btn.primary}>
              {savingName ? "Saving…" : "Save name"}
            </button>
            <Saved text={nameMsg} />
          </div>
        </form>
      </Panel>

      <Panel title="Password">
        {!user.has_password ? (
          <p className="max-w-xl text-sm text-ink-2">
            You sign in with Google, so this account has no password. To add one, use "Forgot password" on the
            sign-in page.
          </p>
        ) : (
          <form onSubmit={(e) => void changePassword(e)} className="grid max-w-xl gap-4" noValidate>
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>Current password</span>
              <input
                type="password"
                autoComplete="current-password"
                value={current}
                onChange={(e) => setCurrent(e.target.value)}
                className={field}
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>New password</span>
              <input
                type="password"
                autoComplete="new-password"
                value={next}
                onChange={(e) => setNext(e.target.value)}
                aria-invalid={tooShort}
                aria-describedby="new-pw-hint"
                className={field}
              />
              <span id="new-pw-hint" className={`text-xs ${tooShort ? "text-danger" : "text-muted"}`}>
                Use at least 10 characters. Your other sessions are signed out when you change it.
              </span>
            </label>
            <label className="flex flex-col gap-1">
              <span className={fieldLabel}>Confirm new password</span>
              <input
                type="password"
                autoComplete="new-password"
                value={confirm}
                onChange={(e) => setConfirm(e.target.value)}
                aria-invalid={mismatch}
                className={field}
              />
              {mismatch && <span className="text-xs text-danger">The passwords don't match.</span>}
            </label>
            {pwErr && <ErrorNotice message={pwErr} />}
            <div className="flex items-center gap-3">
              <button
                type="submit"
                disabled={pwBusy || !current || next.length < 10 || next !== confirm}
                className={btn.primary}
              >
                {pwBusy ? "Changing…" : "Change password"}
              </button>
              <Saved text={pwMsg} />
            </div>
          </form>
        )}
      </Panel>

      <Panel
        title="Active sessions"
        meta={sessions ? `${sessions.length} signed in` : undefined}
        actions={
          others > 0 ? (
            <button type="button" onClick={() => void signOutOthers()} className={btn.secondary}>
              Sign out other sessions
            </button>
          ) : undefined
        }
      >
        {sessErr && <ErrorNotice message={sessErr} onRetry={loadSessions} />}
        {!sessions && !sessErr ? (
          <ListSkeleton rows={2} />
        ) : (
          <ul className="divide-y divide-line">
            {(sessions ?? []).map((s) => (
              <li key={s.id} className="flex flex-wrap items-center gap-3 py-3 first:pt-0 last:pb-0">
                <Laptop className="h-5 w-5 shrink-0 text-muted" aria-hidden="true" />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 text-sm font-semibold text-ink" title={s.user_agent ?? undefined}>
                    {describeAgent(s.user_agent)}
                    {s.current && (
                      <span className="rounded-full bg-brand-soft px-2 py-0.5 text-xs font-semibold text-brand">
                        This device
                      </span>
                    )}
                  </div>
                  <div className="text-xs text-muted">
                    Signed in {formatTimestamp(s.created_at_ms)} · last active {formatTimestamp(s.last_seen_ms)}
                  </div>
                </div>
                {!s.current && (
                  <button type="button" onClick={() => void signOutSession(s.id)} className={btn.secondary}>
                    Sign out
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </div>
  );
};
