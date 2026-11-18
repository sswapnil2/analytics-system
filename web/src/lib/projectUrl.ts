/** The project a Studio link points at: `#/funnels?q=…&p=<id>`. */
export function projectFromHash(hash: string): string {
  return new URLSearchParams(hash.split("?")[1] ?? "").get("p") ?? "";
}

/**
 * The same hash with `p=<pid>`, or null when nothing needs to change (already
 * there, or not a workspace route such as `#/signin`).
 */
export function withProject(hash: string, pid: string, workspaceIds: readonly string[]): string | null {
  const [path, search = ""] = hash.replace(/^#/, "").split("?");
  if (!pid || !workspaceIds.includes(path.replace(/^\//, "").split("/")[0])) return null;
  const params = new URLSearchParams(search);
  if (params.get("p") === pid) return null;
  params.set("p", pid);
  return `#${path}?${params.toString()}`;
}
