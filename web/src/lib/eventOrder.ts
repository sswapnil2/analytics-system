import { api } from "../api/client";

const cache = new Map<string, Promise<string[]>>();

/**
 * Puts `events` in the order users usually first do them, so default reports
 * read like the product ("app opened → product viewed → add to cart") instead
 * of busiest-first. For each pair it compares how many users' first `a` came
 * before their first `b` with the reverse. Ties keep the incoming order.
 * ponytail: n·(n−1) tiny funnel queries; keep n ≤ 5 or add a server endpoint.
 */
export function typicalOrder(projectId: string, events: string[]): Promise<string[]> {
  const key = `${projectId}\u0000${events.join("\u0000")}`;
  const hit = cache.get(key);
  if (hit) return hit;
  const end = Date.now();
  const time_range = { start_ts_ms: end - 30 * 86_400_000, end_ts_ms: end };
  const firstThen = (a: string, b: string) =>
    api
      .queryFunnel(projectId, {
        time_range,
        steps: [a, b].map((event_name) => ({ event_name, filters: [], first_time_ever: true })),
        order_mode: "loose_order",
        conversion_window_ms: 30 * 86_400_000,
        exclusions: [],
        hold_constant_props: [],
        breakdown_by: null,
      })
      .then((r) => r.overall_steps[1]?.entered_users ?? 0)
      .catch(() => 0);
  const pairs: [string, string][] = [];
  for (let i = 0; i < events.length; i++) for (let j = i + 1; j < events.length; j++) pairs.push([events[i], events[j]]);
  const p = Promise.all(pairs.map(async ([a, b]) => [a, b, await firstThen(a, b), await firstThen(b, a)] as const)).then(
    (rows) => {
      const wins = new Map(events.map((e) => [e, 0]));
      for (const [a, b, ab, ba] of rows) {
        if (ab > ba) wins.set(a, (wins.get(a) ?? 0) + 1);
        else if (ba > ab) wins.set(b, (wins.get(b) ?? 0) + 1);
      }
      return [...events].sort((x, y) => (wins.get(y) ?? 0) - (wins.get(x) ?? 0));
    },
  );
  cache.set(key, p);
  p.catch(() => cache.delete(key));
  return p;
}
