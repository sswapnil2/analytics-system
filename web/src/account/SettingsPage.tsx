import React, { useCallback, useEffect, useState } from "react";
import { Check, Copy, FolderPlus, KeyRound, Plus, RotateCw, UserPlus } from "lucide-react";
import {
  api,
  ApiKeyRecord,
  AuthUser,
  OrgInfo,
  OrgInvite,
  OrgMember,
  outranks,
  ProjectInfo,
  Role,
  roleAtLeast,
  ROLES,
} from "../api/client";
import { btn, EmptyState, ErrorNotice, field, fieldLabel, ListSkeleton, PageHeader, Panel } from "../components/ui";
import { errorMessage, formatTimestamp } from "../lib/format";

const ROLE_HELP: Record<Role, string> = {
  viewer: "Reads reports and dashboards",
  member: "Also creates and edits reports, dashboards, cohorts and flags",
  admin: "Also manages members, invites and API keys",
  owner: "Also renames the organisation and manages owners",
};

const label = (r: Role) => r[0].toUpperCase() + r.slice(1);

export const RoleBadge: React.FC<{ role: Role }> = ({ role }) => (
  <span
    title={ROLE_HELP[role]}
    className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
      role === "owner" || role === "admin" ? "bg-brand-soft text-brand" : "border border-line-strong text-ink-2"
    }`}
  >
    {label(role)}
  </span>
);

/** A one-time link or key with a Copy button. */
const OneTimeValue: React.FC<{ title: string; value: string }> = ({ title, value }) => {
  const [copied, setCopied] = useState(false);
  return (
    <div
      className="flex items-center justify-between gap-2 rounded-lg border border-ok/30 bg-ok-soft px-3 py-2 text-xs text-ink"
      role="status"
    >
      <div className="min-w-0">
        <div className="font-semibold text-ok">{title}</div>
        <code className="block truncate font-mono text-ink">{value}</code>
      </div>
      <button
        type="button"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(value);
            setCopied(true);
            window.setTimeout(() => setCopied(false), 2000);
          } catch {
            // clipboard blocked: the value is still selectable
          }
        }}
        className={btn.secondary}
      >
        {copied ? <Check className="h-3.5 w-3.5 text-ok" /> : <Copy className="h-3.5 w-3.5" />}
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
};

/** Roles `me` may hand out. */
const grantable = (me: Role) => ROLES.filter((r) => outranks(me, r));

/** IANA zones from the browser, with the browser's own zone first. */
function timeZones(): string[] {
  const own = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const all = typeof Intl.supportedValuesOf === "function" ? Intl.supportedValuesOf("timeZone") : [];
  return Array.from(new Set([own, "UTC", ...all]));
}

export const SettingsPage: React.FC<{
  user: AuthUser | null;
  projectId: string;
  projectName: string;
  projects: ProjectInfo[];
  /** Called with the new project's id after it is created, so the app can switch to it. */
  onProjectCreated: (id: string) => void;
  /** Called after a project is changed, so the app reloads the project list. */
  onProjectsChanged: () => void;
}> = ({ user, projectId, projectName, projects, onProjectCreated, onProjectsChanged }) => {
  const [org, setOrg] = useState<OrgInfo | null>(null);
  const [members, setMembers] = useState<OrgMember[] | null>(null);
  const [invites, setInvites] = useState<OrgInvite[]>([]);
  const [keys, setKeys] = useState<ApiKeyRecord[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ title: string; value: string } | null>(null);

  const [orgName, setOrgName] = useState("");
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<Role>("viewer");
  const [inviting, setInviting] = useState(false);
  const [keyLabel, setKeyLabel] = useState("");
  const [creatingKey, setCreatingKey] = useState(false);
  const [projName, setProjName] = useState("");
  const [projTz, setProjTz] = useState(() => timeZones()[0]);
  const [creatingProject, setCreatingProject] = useState(false);
  const zones = React.useMemo(timeZones, []);

  const me = org?.role;
  const isAdmin = roleAtLeast(me, "admin");

  const load = useCallback(async () => {
    setError(null);
    try {
      const o = await api.getOrg();
      setOrg(o);
      setOrgName(o.name ?? "");
      setMembers(await api.listMembers());
      if (roleAtLeast(o.role, "admin")) {
        const [inv, k] = await Promise.all([api.listInvites(), projectId ? api.listApiKeys(projectId) : []]);
        setInvites(inv);
        setKeys(k.filter((x) => !x.revoked));
      }
    } catch (err) {
      setError(errorMessage(err));
    }
  }, [projectId]);
  useEffect(() => void load(), [load]);

  const act = async (fn: () => Promise<unknown>) => {
    setError(null);
    try {
      await fn();
      await load();
    } catch (err) {
      setError(errorMessage(err));
    }
  };

  const invite = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inviteEmail.trim()) return setError("Enter the email address to invite.");
    setInviting(true);
    await act(async () => {
      const inv = await api.createInvite({ email: inviteEmail.trim(), role: inviteRole });
      // Build the link from where the Studio is open, which is what the invitee will reach.
      const token = inv.invite_url?.split("/").pop() ?? "";
      setNotice({
        title: `Invite link for ${inv.email}. Send it to them; it works once and expires in 7 days.`,
        value: `${window.location.origin}/#/invite/${token}`,
      });
      setInviteEmail("");
    });
    setInviting(false);
  };

  const createProject = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!projName.trim()) return setError("Name the project.");
    setCreatingProject(true);
    await act(async () => {
      const res = await api.createProject({ name: projName.trim(), timezone: projTz });
      setNotice({
        title: `Created "${res.project.name}". Copy its ingest key now; it is shown only once.`,
        value: res.api_key.plaintext_key,
      });
      setProjName("");
      onProjectCreated(res.project.id);
    });
    setCreatingProject(false);
  };

  const setProjectTz = (p: ProjectInfo, timezone: string) => {
    if (!window.confirm(`Move "${p.name}" to ${timezone}? Reports bucket days and hours on this clock from now on.`)) return;
    void act(async () => {
      await api.updateProject(p.id, { timezone });
      onProjectsChanged();
    });
  };

  // A new key with the same label and role, then the old one revoked at once (the confirm says so).
  const rotateKey = (k: ApiKeyRecord) => {
    if (!window.confirm(`Rotate "${k.label}"? A new key is created and this one stops working right away. Update your apps with the new key.`))
      return;
    void act(async () => {
      const created = await api.createApiKey(projectId, { label: k.label, role: k.role });
      await api.revokeApiKey(projectId, k.id);
      setNotice({ title: `New key for "${k.label}". Copy it now; it is shown only once.`, value: created.plaintext_key });
    });
  };

  const createKey = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreatingKey(true);
    await act(async () => {
      const created = await api.createApiKey(projectId, { label: keyLabel.trim() || "Ingest key" });
      setNotice({ title: "Copy your new key now. It is shown only once.", value: created.plaintext_key });
      setKeyLabel("");
    });
    setCreatingKey(false);
  };

  const resetLink = (m: OrgMember) =>
    act(async () => {
      const r = await api.memberResetLink(m.id);
      const token = r.reset_url.split("/").pop() ?? "";
      setNotice({
        title: `Password reset link for ${m.email}. It works once and expires in an hour.`,
        value: `${window.location.origin}/#/reset/${token}`,
      });
    });

  if (!org && !error) {
    return (
      <div className="space-y-5">
        <PageHeader title="Settings" description="Members, invites and API keys." />
        <Panel>
          <ListSkeleton rows={4} />
        </Panel>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <PageHeader
        title="Settings"
        description={
          isAdmin
            ? "Who can see this organisation's projects, what they can change, and the keys your apps send events with."
            : "Who can see this organisation's projects. Ask an admin to change roles or invite people."
        }
        actions={me ? <RoleBadge role={me} /> : undefined}
      />

      {error && <ErrorNotice message={error} onRetry={() => void load()} />}
      {notice && <OneTimeValue title={notice.title} value={notice.value} />}

      {org && (
        <Panel title="Organisation">
          {me === "owner" ? (
            <form
              onSubmit={(e) => {
                e.preventDefault();
                void act(() => api.renameOrg(orgName.trim()));
              }}
              className="flex max-w-xl flex-wrap items-end gap-2"
            >
              <label className="flex min-w-[14rem] flex-1 flex-col gap-1">
                <span className={fieldLabel}>Name</span>
                <input value={orgName} onChange={(e) => setOrgName(e.target.value)} className={field} />
              </label>
              <button type="submit" disabled={!orgName.trim() || orgName.trim() === org.name} className={btn.primary}>
                Rename
              </button>
            </form>
          ) : (
            <p className="text-sm text-ink">{org.name ?? org.id}</p>
          )}
        </Panel>
      )}

      <Panel title="Members" meta={members ? `${members.length} ${members.length === 1 ? "person" : "people"}` : undefined}>
        {!members ? (
          <ListSkeleton rows={3} />
        ) : (
          <div className="relative overflow-x-auto">
            <table className="w-full min-w-[520px] text-left text-sm">
              <thead>
                <tr className="border-b border-line text-xs text-muted">
                  <th className="py-2 pr-3 font-medium">Name</th>
                  <th className="py-2 pr-3 font-medium">Email</th>
                  <th className="py-2 pr-3 font-medium">Role</th>
                  <th className="py-2 pr-3 font-medium">Joined</th>
                  {isAdmin && <th className="py-2 text-right font-medium">
                    <span className="sr-only">Actions</span>
                  </th>}
                </tr>
              </thead>
              <tbody className="divide-y divide-line">
                {members.map((m) => {
                  const self = m.id === user?.id;
                  const canManage = isAdmin && !self && !!me && outranks(me, m.role);
                  return (
                    <tr key={m.id}>
                      <td className="py-2.5 pr-3 font-medium text-ink">
                        {m.name || "—"}
                        {self && <span className="ml-1.5 text-xs font-normal text-muted">(you)</span>}
                      </td>
                      <td className="py-2.5 pr-3 text-ink-2">{m.email}</td>
                      <td className="py-2.5 pr-3">
                        {canManage ? (
                          <select
                            aria-label={`Role for ${m.email}`}
                            value={m.role}
                            onChange={(e) => void act(() => api.setMemberRole(m.id, e.target.value as Role))}
                            className={`${field} py-1`}
                          >
                            {grantable(me!).map((r) => (
                              <option key={r} value={r}>
                                {label(r)}
                              </option>
                            ))}
                          </select>
                        ) : (
                          <RoleBadge role={m.role} />
                        )}
                      </td>
                      <td className="py-2.5 pr-3 text-xs text-muted">{formatTimestamp(m.created_at_ms)}</td>
                      {isAdmin && (
                        <td className="py-2.5 text-right">
                          {canManage && (
                            <span className="inline-flex gap-1">
                              {m.has_password && (
                                <button type="button" onClick={() => void resetLink(m)} className={btn.ghost}>
                                  Reset link
                                </button>
                              )}
                              <button
                                type="button"
                                onClick={() => {
                                  if (window.confirm(`Remove ${m.email}? Their account is deleted and they're signed out.`))
                                    void act(() => api.removeMember(m.id));
                                }}
                                className={`${btn.ghost} text-danger hover:text-danger`}
                              >
                                Remove
                              </button>
                            </span>
                          )}
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      {isAdmin && me && (
        <Panel title="Invites" meta={invites.length > 0 ? `${invites.length} pending` : undefined}>
          <div className="space-y-4">
            <form onSubmit={(e) => void invite(e)} className="flex flex-wrap items-end gap-2">
              <label className="flex min-w-[14rem] flex-1 flex-col gap-1">
                <span className={fieldLabel}>Email</span>
                <input
                  type="email"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                  placeholder="teammate@company.com"
                  className={field}
                />
              </label>
              <label className="flex flex-col gap-1">
                <span className={fieldLabel}>Role</span>
                <select value={inviteRole} onChange={(e) => setInviteRole(e.target.value as Role)} className={field}>
                  {grantable(me).map((r) => (
                    <option key={r} value={r}>
                      {label(r)}
                    </option>
                  ))}
                </select>
              </label>
              <button type="submit" disabled={inviting} className={btn.primary}>
                <UserPlus className="h-3.5 w-3.5" />
                {inviting ? "Inviting…" : "Invite"}
              </button>
            </form>
            <p className="text-xs text-muted">
              {ROLE_HELP[inviteRole]}. Pulse doesn't send email: you get a link to pass on.
            </p>
            {invites.length > 0 && (
              <ul className="divide-y divide-line border-t border-line">
                {invites.map((inv) => (
                  <li key={inv.id} className="flex flex-wrap items-center gap-3 py-2.5 text-sm">
                    <span className="min-w-0 flex-1 truncate text-ink">{inv.email}</span>
                    <RoleBadge role={inv.role} />
                    <span className="text-xs text-muted">expires {formatTimestamp(inv.expires_at_ms)}</span>
                    <button type="button" onClick={() => void act(() => api.deleteInvite(inv.id))} className={btn.ghost}>
                      Revoke
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </Panel>
      )}

      {isAdmin && (
        <Panel title="Projects" meta={`${projects.length}`}>
          <div className="space-y-4">
            {projects.length > 0 && (
              <ul className="divide-y divide-line text-sm">
                {projects.map((p) => (
                  <li key={p.id} className="flex flex-wrap items-baseline justify-between gap-2 py-2">
                    <span className="font-medium text-ink">{p.name}</span>
                    <span className="flex items-center gap-2 text-xs text-muted">
                      <select
                        aria-label={`Time zone for ${p.name}`}
                        value={p.timezone}
                        onChange={(e) => setProjectTz(p, e.target.value)}
                        className="rounded-md border border-line-strong bg-field px-2 py-1 text-xs text-ink focus:border-brand focus:outline-none focus:ring-2 focus:ring-brand/25"
                      >
                        {(zones.includes(p.timezone) ? zones : [p.timezone, ...zones]).map((z) => (
                          <option key={z} value={z}>
                            {z.replace(/_/g, " ")}
                          </option>
                        ))}
                      </select>
                      <code className="font-mono">{p.id}</code>
                    </span>
                  </li>
                ))}
              </ul>
            )}
            <form onSubmit={(e) => void createProject(e)} className="flex flex-wrap items-end gap-2">
              <label className="flex min-w-[12rem] flex-1 flex-col gap-1">
                <span className={fieldLabel}>New project name</span>
                <input value={projName} onChange={(e) => setProjName(e.target.value)} placeholder="Web app (production)" className={field} />
              </label>
              <label className="flex min-w-[12rem] flex-col gap-1">
                <span className={fieldLabel}>Time zone (days and hours in reports)</span>
                <select value={projTz} onChange={(e) => setProjTz(e.target.value)} className={field}>
                  {zones.map((z) => (
                    <option key={z} value={z}>
                      {z.replace(/_/g, " ")}
                    </option>
                  ))}
                </select>
              </label>
              <button type="submit" disabled={creatingProject} className={btn.primary}>
                <FolderPlus className="h-3.5 w-3.5" />
                {creatingProject ? "Creating…" : "Create project"}
              </button>
            </form>
          </div>
        </Panel>
      )}

      {isAdmin && projectId && (
        <Panel title="API keys" meta={projectName}>
          <div className="space-y-4">
            <form onSubmit={(e) => void createKey(e)} className="flex flex-wrap items-end gap-2">
              <label className="flex min-w-[14rem] flex-1 flex-col gap-1">
                <span className={fieldLabel}>Key label</span>
                <input
                  value={keyLabel}
                  onChange={(e) => setKeyLabel(e.target.value)}
                  placeholder="Web SDK production"
                  className={field}
                />
              </label>
              <button type="submit" disabled={creatingKey} className={btn.primary}>
                <Plus className="h-3.5 w-3.5" />
                {creatingKey ? "Creating…" : "Create ingest key"}
              </button>
            </form>
            {keys.length === 0 ? (
              <EmptyState icon={KeyRound} title="No API keys yet" body="Create an ingest key to send events from your app." />
            ) : (
              <div className="relative overflow-x-auto">
                <table className="w-full min-w-[480px] text-left text-sm">
                  <thead>
                    <tr className="border-b border-line text-xs text-muted">
                      <th className="py-2 pr-3 font-medium">Key</th>
                      <th className="py-2 pr-3 font-medium">Label</th>
                      <th className="py-2 pr-3 font-medium">Created</th>
                      <th className="py-2 text-right font-medium">
                        <span className="sr-only">Actions</span>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-line">
                    {keys.map((k) => (
                      <tr key={k.id}>
                        <td className="py-2.5 pr-3 font-mono text-xs text-ink">{k.key_prefix}</td>
                        <td className="py-2.5 pr-3 text-ink-2">{k.label}</td>
                        <td className="py-2.5 pr-3 text-xs text-muted">{formatTimestamp(k.created_at_ms)}</td>
                        <td className="py-2.5 text-right">
                          <button type="button" onClick={() => rotateKey(k)} className={btn.ghost}>
                            <RotateCw className="h-3.5 w-3.5" /> Rotate
                          </button>
                          <button
                            type="button"
                            onClick={() => {
                              if (window.confirm(`Revoke "${k.label}"? Apps using it stop sending events.`))
                                void act(() => api.revokeApiKey(projectId, k.id));
                            }}
                            className={`${btn.ghost} text-danger hover:text-danger`}
                          >
                            Revoke
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </Panel>
      )}
    </div>
  );
};
