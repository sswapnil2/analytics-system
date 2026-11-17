#!/usr/bin/env bash
# Nightly backup of /var/lib/pulse. Stops the node for a few seconds so SQLite and segment files
# are consistent (SDKs retry ingest). Keeps 7 local copies; uploads off-box if RCLONE_REMOTE is set.
# ponytail: stop-copy-start; switch to an online snapshot if a few seconds of downtime matters.
set -euo pipefail
DEST=/var/backups/pulse
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$DEST" && chmod 700 "$DEST"

systemctl stop pulse
trap 'systemctl start pulse' EXIT
tar -C /var/lib -czf "$DEST/pulse-$STAMP.tar.gz" pulse
systemctl start pulse
trap - EXIT

ls -1t "$DEST"/pulse-*.tar.gz | tail -n +8 | xargs -r rm --
if [[ -n "${RCLONE_REMOTE:-}" ]]; then
  rclone copy "$DEST/pulse-$STAMP.tar.gz" "$RCLONE_REMOTE"
fi
