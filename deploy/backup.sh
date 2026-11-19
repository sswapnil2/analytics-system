#!/usr/bin/env bash
# Nightly backup of /var/lib/pulse. Stops the node for a few seconds so SQLite and segment files
# are consistent (SDKs retry ingest). Keeps 7 local copies; uploads off-box if RCLONE_REMOTE is set.
set -euo pipefail
umask 077
DEST=/var/backups/pulse
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
TMP_ARCHIVE="$DEST/.pulse-$STAMP.tar.gz.tmp"
ARCHIVE="$DEST/pulse-$STAMP.tar.gz"
mkdir -p "$DEST" && chmod 700 "$DEST"

cleanup() {
  rm -f "$TMP_ARCHIVE"
  systemctl start pulse || true
}

systemctl stop pulse
trap cleanup EXIT
tar -C /var/lib --exclude='pulse/foyer_cache' -czf "$TMP_ARCHIVE" pulse
mv "$TMP_ARCHIVE" "$ARCHIVE"
systemctl start pulse
trap - EXIT

ls -1t "$DEST"/pulse-*.tar.gz | tail -n +8 | xargs -r rm --
if [[ -n "${RCLONE_REMOTE:-}" ]]; then
  rclone copy "$ARCHIVE" "$RCLONE_REMOTE"
fi
