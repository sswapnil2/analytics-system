import React, { useEffect, useState } from "react";
import { api, ApiAuthError, AuthConfig, setStoredAdminToken } from "../api/client";
import { btn, field } from "../components/ui";
import { errorMessage } from "../lib/format";

export type AuthView = "signin" | "signup" | "callback";

/** Reads `#/signin`, `#/signup` or `#/auth/callback?code=…` from the URL hash. */
export function parseAuthHash(): { view: AuthView; params: URLSearchParams } {
  const raw = window.location.hash.replace(/^#\/?/, "");
  const [path, query = ""] = raw.split("?");
  const params = new URLSearchParams(query);
  if (path === "signup") return { view: "signup", params };
  if (path === "auth/callback") return { view: "callback", params };
  return { view: "signin", params };
}

const GOOGLE_ERRORS: Record<string, string> = {
  google_denied: "Google sign-in was cancelled.",
  google_state: "That Google sign-in link expired. Try again.",
  google_token: "Google didn't confirm your account. Try again.",
  google_unverified: "Your Google email isn't verified yet. Verify it with Google, then try again.",
  signup_closed: "Sign-ups are closed for this workspace. Ask an admin to add your email domain.",
};

export const Logo: React.FC<{ size?: "sm" | "md" }> = ({ size = "md" }) => (
  <span
    className={`flex items-center justify-center rounded-lg bg-brand ${size === "md" ? "h-8 w-8" : "h-7 w-7"}`}
    aria-hidden="true"
  >
    <svg viewBox="0 0 24 24" className={size === "md" ? "h-5 w-5" : "h-[18px] w-[18px]"}>
      <polyline
        points="2 12 6 12 9 5 13 19 16 12 22 12"
        fill="none"
        strokeWidth="2.2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="stroke-on-brand"
      />
    </svg>
  </span>
);

const GoogleMark: React.FC = () => (
  <svg width="18" height="18" viewBox="0 0 48 48" aria-hidden="true">
    <path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z" />
    <path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z" />
    <path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z" />
    <path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z" />
  </svg>
);

/** Illustrative funnel on the sign-in page; labelled as a sample, not live data. */
const SampleFunnel: React.FC = () => {
  const rows: { label: string; kept: number; lost: number; count: string; strong?: boolean }[] = [
    { label: "Opened app", kept: 100, lost: 0, count: "12,480" },
    { label: "Submitted PAN", kept: 72, lost: 28, count: "8,930" },
    { label: "Verified OTP", kept: 42, lost: 30, count: "5,210", strong: true },
    { label: "Approved mandate", kept: 38, lost: 4, count: "4,770" },
  ];
  return (
    <figure className="max-w-[520px] space-y-3.5 rounded-xl border border-line-strong/70 bg-panel p-6">
      <figcaption className="text-[0.8125rem] text-muted">Sample funnel, last 14 days</figcaption>
      <div className="space-y-2.5">
        {rows.map((r) => (
          <div key={r.label} className="grid grid-cols-[8.5rem_minmax(0,1fr)_4rem] items-center gap-3 text-sm">
            <span className={r.strong ? "font-semibold" : undefined}>{r.label}</span>
            <span className="flex h-[22px] overflow-hidden rounded">
              <span className="bg-brand" style={{ width: `${r.kept}%` }} />
              <span className={r.strong ? "hatch-drop-strong" : "hatch-drop"} style={{ width: `${r.lost}%` }} />
            </span>
            <span className="tabular text-right">{r.count}</span>
          </div>
        ))}
      </div>
      <p className="text-sm font-medium text-danger">
        3,720 users dropped before verifying OTP. 31% of them hit a PAN gateway timeout first.
      </p>
    </figure>
  );
};

const AuthLayout: React.FC<{ heading: string; body: string; aside: React.ReactNode; children: React.ReactNode }> = ({
  heading,
  body,
  aside,
  children,
}) => (
  <div className="flex min-h-screen flex-wrap bg-panel text-ink">
    <section className="flex min-w-0 flex-[1_1_520px] flex-col gap-14 bg-sunken px-6 py-10 sm:px-14 sm:py-12">
      <div className="flex items-center gap-2.5">
        <Logo />
        <span className="text-xl font-bold tracking-[-0.01em]">Pulse</span>
      </div>
      <div className="max-w-[520px] space-y-4">
        <h1 className="text-[2.25rem] font-semibold leading-[1.08] tracking-[-0.025em] sm:text-[2.75rem]">{heading}</h1>
        <p className="text-[1.0625rem] leading-relaxed text-ink-2">{body}</p>
      </div>
      {aside}
    </section>
    <section className="flex min-w-0 flex-[1_1_480px] items-center justify-center px-6 py-12">
      <div className="w-full max-w-[380px]">{children}</div>
    </section>
  </div>
);

const inputCls = `${field} h-11 w-full text-[0.9375rem]`;
const labelCls = "flex flex-col gap-1.5 text-sm font-medium text-ink";

function useAuthConfig(): AuthConfig | null {
  const [cfg, setCfg] = useState<AuthConfig | null>(null);
  useEffect(() => {
    api
      .getAuthConfig()
      .then(setCfg)
      // Older servers without account support: fall back to token sign-in only.
      .catch(() => setCfg({ password: false, google: false, signup_open: false }));
  }, []);
  return cfg;
}

const GoogleButton: React.FC<{ label: string }> = ({ label }) => (
  <a
    href="/v1/auth/google/start"
    className="flex h-11 items-center justify-center gap-2.5 rounded-lg border border-line-strong bg-panel text-[0.9375rem] font-medium text-ink hover:bg-hover"
  >
    <GoogleMark />
    {label}
  </a>
);

const OrDivider: React.FC = () => (
  <div className="flex items-center gap-3 text-[0.8125rem] text-muted">
    <span className="h-px flex-1 bg-line" />
    or with email
    <span className="h-px flex-1 bg-line" />
  </div>
);

const FormError: React.FC<{ id: string; message: string | null }> = ({ id, message }) =>
  message ? (
    <p id={id} role="alert" className="rounded-lg bg-danger-soft px-3 py-2.5 text-sm text-danger">
      {message}
    </p>
  ) : null;

// ---------------------------------------------------------------------------

export const SignInScreen: React.FC<{ onSignedIn: () => void; reason?: string; googleError?: string | null }> = ({
  onSignedIn,
  reason,
  googleError,
}) => {
  const cfg = useAuthConfig();
  const [mode, setMode] = useState<"password" | "token">("password");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(
    reason ?? (googleError ? GOOGLE_ERRORS[googleError] ?? "Google sign-in didn't work. Try again." : null),
  );

  const accountsAvailable = cfg?.password !== false;
  const showToken = mode === "token" || (cfg !== null && !accountsAvailable);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    if (showToken) {
      const value = token.trim();
      if (!value) return setError("Paste your admin token to continue.");
      setBusy(true);
      setStoredAdminToken(value);
      try {
        await api.getStatus();
        onSignedIn();
      } catch (err) {
        setStoredAdminToken("");
        setError(
          err instanceof ApiAuthError
            ? "That token wasn't accepted. Check that you copied the whole sk_ token."
            : `Couldn't reach the Pulse server: ${errorMessage(err)}`,
        );
      } finally {
        setBusy(false);
      }
      return;
    }
    if (!email.trim() || !password) return setError("Enter your email and password.");
    setBusy(true);
    try {
      const session = await api.logIn({ email: email.trim(), password });
      setStoredAdminToken(session.token);
      onSignedIn();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <AuthLayout
      heading="Find where users drop off. Then watch why."
      body="Funnels, journeys and retention for your product, with the session replay one click away from every drop."
      aside={<SampleFunnel />}
    >
      <form onSubmit={(e) => void submit(e)} className="flex flex-col gap-6" noValidate>
        <div className="space-y-1.5">
          <h2 className="text-[1.625rem] font-semibold tracking-[-0.015em]">Sign in</h2>
          <p className="text-[0.9375rem] text-muted">
            {showToken ? "Use the admin token from the server log. It starts with sk_." : "Welcome back. Use your work account."}
          </p>
        </div>

        {!showToken && cfg?.google && (
          <>
            <GoogleButton label="Continue with Google" />
            <OrDivider />
          </>
        )}

        <FormError id="signin-error" message={error} />

        {showToken ? (
          <label className={labelCls}>
            Admin token
            <input
              type="password"
              autoComplete="off"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              placeholder="sk_…"
              className={`${inputCls} font-mono`}
              aria-describedby={error ? "signin-error" : undefined}
              autoFocus
            />
          </label>
        ) : (
          <div className="flex flex-col gap-4">
            <label className={labelCls}>
              Work email
              <input
                type="email"
                autoComplete="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className={inputCls}
                aria-describedby={error ? "signin-error" : undefined}
                autoFocus
              />
            </label>
            <label className={labelCls}>
              Password
              <input
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className={inputCls}
              />
            </label>
          </div>
        )}

        <button type="submit" disabled={busy || cfg === null} className={`${btn.primary} h-11 text-[0.9375rem]`}>
          {busy ? "Signing in…" : "Sign in"}
        </button>

        <div className="flex flex-col gap-2 text-sm text-muted">
          {accountsAvailable && (
            <span>
              New to Pulse?{" "}
              <a href="#/signup" className="font-semibold text-brand hover:underline">
                Create an account
              </a>
            </span>
          )}
          {accountsAvailable && (
            <button
              type="button"
              onClick={() => {
                setError(null);
                setMode(showToken ? "password" : "token");
              }}
              className="self-start text-left hover:text-ink hover:underline"
            >
              {showToken ? "Sign in with email instead" : "Sign in with an access token instead"}
            </button>
          )}
        </div>
      </form>
    </AuthLayout>
  );
};

// ---------------------------------------------------------------------------

const SETUP_STEPS = [
  ["Create your account", "The first account becomes the workspace admin."],
  ["Install the SDK", "Web, Android and iOS. One key per project."],
  ["Build your first funnel", "Pick the steps from your onboarding flow."],
] as const;

export const SignUpScreen: React.FC<{ onSignedIn: () => void }> = ({ onSignedIn }) => {
  const cfg = useAuthConfig();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [touchedPw, setTouchedPw] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const pwTooShort = password.length > 0 && password.length < 10;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setTouchedPw(true);
    setError(null);
    if (!email.trim()) return setError("Enter your work email.");
    if (password.length < 10) return;
    setBusy(true);
    try {
      const session = await api.signUp({ email: email.trim(), password, name: name.trim() || undefined });
      setStoredAdminToken(session.token);
      onSignedIn();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <AuthLayout
      heading="Set up your workspace."
      body="Create an account, add the SDK key to your app, and your first funnel fills in as events arrive."
      aside={
        <ol className="max-w-[460px] space-y-[18px]">
          {SETUP_STEPS.map(([title, sub], i) => (
            <li key={title} className="grid grid-cols-[2rem_minmax(0,1fr)] items-start gap-3.5">
              <span
                className={`flex h-7 w-7 items-center justify-center rounded-full text-sm font-semibold ${
                  i === 0 ? "bg-brand text-on-brand" : "border-[1.5px] border-faint text-ink-2"
                }`}
              >
                {i + 1}
              </span>
              <span className="flex flex-col gap-0.5">
                <span className="font-semibold">{title}</span>
                <span className="text-sm text-muted">{sub}</span>
              </span>
            </li>
          ))}
        </ol>
      }
    >
      <form onSubmit={(e) => void submit(e)} className="flex flex-col gap-6" noValidate>
        <div className="space-y-1.5">
          <h2 className="text-[1.625rem] font-semibold tracking-[-0.015em]">Create your account</h2>
          <p className="text-[0.9375rem] text-muted">You'll be looking at your own data in a few minutes.</p>
        </div>

        {cfg && !cfg.signup_open && (
          <p role="status" className="rounded-lg bg-warn-soft px-3 py-2.5 text-sm text-warn">
            Sign-ups are limited on this workspace. Use an email domain your admin allowed.
          </p>
        )}

        {cfg?.google && (
          <>
            <GoogleButton label="Sign up with Google" />
            <OrDivider />
          </>
        )}

        <FormError id="signup-error" message={error} />

        <div className="flex flex-col gap-4">
          <label className={labelCls}>
            Your name
            <input type="text" autoComplete="name" value={name} onChange={(e) => setName(e.target.value)} className={inputCls} autoFocus />
          </label>
          <label className={labelCls}>
            Work email
            <input
              type="email"
              autoComplete="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className={inputCls}
              aria-describedby={error ? "signup-error" : undefined}
            />
          </label>
          <label className={labelCls}>
            Password
            <input
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              onBlur={() => setTouchedPw(true)}
              aria-invalid={touchedPw && (pwTooShort || password.length === 0)}
              aria-describedby="pw-hint"
              className={`${inputCls} ${touchedPw && password.length < 10 ? "border-danger" : ""}`}
            />
            <span
              id="pw-hint"
              className={`text-[0.8125rem] font-normal ${touchedPw && password.length < 10 ? "text-danger" : "text-muted"}`}
            >
              Use at least 10 characters.
            </span>
          </label>
        </div>

        <button type="submit" disabled={busy || cfg === null} className={`${btn.primary} h-11 text-[0.9375rem]`}>
          {busy ? "Creating account…" : "Create account"}
        </button>
        <span className="text-sm text-muted">
          Already have an account?{" "}
          <a href="#/signin" className="font-semibold text-brand hover:underline">
            Sign in
          </a>
        </span>
      </form>
    </AuthLayout>
  );
};

// ---------------------------------------------------------------------------

/** Lands here after Google: swaps the one-time code for a session. */
export const AuthCallback: React.FC<{ code: string | null; onSignedIn: () => void }> = ({ code, onSignedIn }) => {
  const [error, setError] = useState<string | null>(code ? null : "This sign-in link is missing its code.");

  useEffect(() => {
    if (!code) return;
    let cancelled = false;
    api
      .exchangeLoginCode(code)
      .then((session) => {
        if (cancelled) return;
        setStoredAdminToken(session.token);
        // Drop the code from the address bar and history.
        window.history.replaceState(null, "", "#/funnels");
        onSignedIn();
      })
      .catch((err) => !cancelled && setError(errorMessage(err)));
    return () => {
      cancelled = true;
    };
  }, [code, onSignedIn]);

  return (
    <div className="flex min-h-screen items-center justify-center bg-canvas p-6">
      <div className="max-w-sm space-y-4 text-center" role="status">
        <div className="flex justify-center">
          <Logo />
        </div>
        {error ? (
          <>
            <p className="text-sm text-ink">Couldn't finish signing in: {error}</p>
            <a href="#/signin" className={btn.secondary}>
              Back to sign in
            </a>
          </>
        ) : (
          <p className="text-sm text-muted">Signing you in…</p>
        )}
      </div>
    </div>
  );
};
