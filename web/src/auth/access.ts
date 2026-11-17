import { createContext, useContext } from "react";

/** What the signed-in user may change. Admin tokens (no user) can do everything. */
export interface Access {
  canEdit: boolean; // member and up: dashboards, cohorts, experiments, lexicon
  canAdmin: boolean; // admin and up: keys, imports, deletions, enterprise settings
}

export const AccessContext = createContext<Access>({ canEdit: true, canAdmin: true });
export const useAccess = () => useContext(AccessContext);
