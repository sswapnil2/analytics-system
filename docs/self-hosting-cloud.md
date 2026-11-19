# Self-hosting Pulse on a small cloud server

One `pulse-node` process behind Caddy (automatic HTTPS) on a small Linux VM, with a domain on
Cloudflare. Config files are in [`deploy/`](../deploy). Prices were checked in November 2026; check
the provider's pricing page before you buy.

## Pick a host

Pulse stores about 100 bytes per event, so 10 million events take about 1 GB of disk. For small
traffic, 1–2 vCPU and 1 GB of RAM is enough.

| Option | What you get | Monthly cost | Catch |
|---|---|---|---|
| **Hetzner Cloud CAX11** (recommended) | 2 ARM vCPU, 4 GB RAM, 40 GB NVMe, 20 TB traffic, EU or US | about €3.79, plus €0.50 for IPv4 (≈ €4.30, about $5) | Needs a card. Automatic backups cost 20% extra (≈ €0.76) |
| Oracle Cloud Always Free (Ampere A1) | up to 4 ARM OCPU, 24 GB RAM, 200 GB disk, 10 TB egress | $0 | ARM capacity is often "out of capacity" in popular regions. Idle free instances can be reclaimed unless you upgrade the account to Pay As You Go (still $0 within the free limits). Ubuntu images ship strict iptables rules |
| AWS Lightsail 1 GB (x86) | 2 vCPU, 1 GB RAM, 40 GB SSD, 2 TB, IPv4 included | $7 | x86 binary. Add swap |
| AWS EC2 t4g.micro (ARM) | 2 vCPU, 1 GB RAM | about $6.13, plus $3.65 for public IPv4 and $0.64 for 8 GB gp3 (≈ $10.40) | New accounts (since July 2025) get up to $200 in credits for 6 months, not the old 12 months free |
| Fly.io | shared-cpu-1x 1 GB, plus a 10 GB volume and IPv4 | about $9 | No free allowance for new orgs, and it doesn't use the systemd/Caddy setup below |

Cloudflare is the DNS provider and registrar here, not the host: it can't run a Rust binary with a disk.

**Domain:** Cloudflare Registrar sells domains at cost. A `.com` costs about $10.44 a year and renews
at the same price. It requires Cloudflare DNS, whose free plan is all you need.

**Recommended total:** Hetzner CAX11 plus a `.com` ≈ €4.30/month + $10.44/year, about **$6 a month**.
For $0, use Oracle's free tier with the same steps, since both are ARM (`aarch64`).

## 1. Steps you do yourself (accounts, payment, domain)

1. **Domain:** go to dash.cloudflare.com → *Domain Registration* → *Register Domains*, search for the
   name and buy it.
2. **Server (Hetzner):** go to console.hetzner.cloud → *New project* → *Add Server*:
   - Location: closest to your users.
   - Image: **Ubuntu 24.04**.
   - Type: *Shared vCPU* → **Arm64** → **CAX11**.
   - Networking: IPv4 + IPv6.
   - SSH key: paste `~/.ssh/id_ed25519.pub`.
   - Name: `pulse`.

   Then go to *Firewalls* → *Create Firewall* with inbound **TCP 22, TCP 80, TCP 443** (optionally
   UDP 443 for HTTP/3) and apply it to the server.

   **Oracle instead:** *Compute → Instances → Create*, Image Ubuntu 24.04, Shape *Ampere
   VM.Standard.A1.Flex* (1 OCPU / 6 GB is plenty). In the VCN's *Security List*, add ingress rules for
   TCP 80 and 443.
3. **DNS:** go to Cloudflare → your domain → *DNS* → *Add record*:
   - `A`, name `analytics`, value = the server's IPv4, **Proxy status: DNS only** (grey cloud).
   - `AAAA`, name `analytics`, value = the server's IPv6, DNS only.

   DNS only lets Caddy get its certificate directly. You can turn the proxy on later with SSL/TLS set
   to *Full (strict)*.

Everything below uses `analytics.example.com`; replace it with your domain.

## 2. Get or build the binary (not on the VM)

The release build takes about 15 minutes and a lot of RAM, so don't compile it on a small VM.

**GitHub Releases (simplest):** download `pulse-linux-aarch64.tar.gz` (or `pulse-linux-x86_64.tar.gz`) and `SHA256SUMS` from the [latest release](https://github.com/sswapnil2/analytics-system/releases/latest) and verify the checksum:

```bash
sha256sum -c --ignore-missing SHA256SUMS
```

**Or build a branch on GitHub Actions:** trigger `.github/workflows/release.yml` manually (non-tag runs build and smoke-test without publishing):

```bash
gh workflow run release.yml --ref <branch>
gh run watch
gh run download --name pulse-linux-aarch64      # or pulse-linux-x86_64
```

**Or cross-compile from a Mac** (not tested in CI):

```bash
brew install zig && cargo install cargo-zigbuild
rustup target add aarch64-unknown-linux-gnu
npm --prefix web ci && npm --prefix web run build
cargo zigbuild --release -p pulse-node --target aarch64-unknown-linux-gnu.2.35
# binary: target/aarch64-unknown-linux-gnu/release/pulse-node
```

Copy it to the server:

```bash
scp pulse-linux-aarch64.tar.gz root@analytics.example.com:/tmp/
```

## 3. Install on the server

```bash
ssh root@analytics.example.com

# Firewall on the box too (Hetzner firewall above is the first layer)
apt-get update && apt-get -y upgrade
apt-get install -y ufw caddy        # Ubuntu 24.04 ships caddy in universe
ufw allow OpenSSH && ufw allow 80,443/tcp && ufw allow 443/udp && ufw --force enable
# Oracle images: ufw clashes with their iptables rules; instead run
#   iptables -I INPUT 6 -p tcp -m multiport --dports 80,443 -j ACCEPT && netfilter-persistent save

# Swap: needed on 1 GB boxes, harmless elsewhere
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# App files
useradd --system --home /var/lib/pulse --shell /usr/sbin/nologin pulse
mkdir -p /opt && tar --no-same-owner -C /opt -xzf /tmp/pulse-linux-aarch64.tar.gz      # -> /opt/pulse
chown -R root:root /opt/pulse && chmod 755 /opt/pulse/pulse-node

# Secrets: generated here, stored outside the data dir, never in git
install -d -m 700 /etc/pulse
install -m 600 /opt/pulse/pulse.env.example /etc/pulse/pulse.env
sed -i "s|^PULSE_ADMIN_TOKEN=.*|PULSE_ADMIN_TOKEN=sk_root_$(openssl rand -hex 24)|" /etc/pulse/pulse.env
sed -i "s|^PULSE_VAULT_KEK=.*|PULSE_VAULT_KEK=$(openssl rand -hex 32)|" /etc/pulse/pulse.env
sed -i "s|analytics.example.com|YOUR.DOMAIN|" /etc/pulse/pulse.env
cat /etc/pulse/pulse.env      # copy both values into your password manager NOW

# Service + HTTPS
cp /opt/pulse/pulse.service /etc/systemd/system/
sed "s|analytics.example.com|YOUR.DOMAIN|" /opt/pulse/Caddyfile > /etc/caddy/Caddyfile
systemctl daemon-reload && systemctl enable --now pulse && systemctl reload caddy
```

Check it:

```bash
journalctl -u pulse -n 50          # the first boot prints a pk_live_… ingest key once; save it
curl -s https://YOUR.DOMAIN/healthz          # -> ok
```

Open `https://YOUR.DOMAIN` and sign in with `PULSE_ADMIN_TOKEN`. To create more keys, follow
[quickstart step 4](quickstart.md#4-get-an-ingest-key) with your domain in place of `127.0.0.1:8080`.

What this setup gives you:
- `pulse-node` listens only on `127.0.0.1:8080`, and Caddy is the only public listener.
- `PULSE_CORS_ORIGINS` is your `https://` domain. To call the admin or query API from another web
  app, add that app's origin, comma-separated. Ingest from your sites doesn't need it.
- `PULSE_ALLOW_NEW_ORGS=false`, so a public node doesn't let strangers create organisations. The
  first sign-up still works with the root token as its setup code.
- `PULSE_VAULT_KEK` is set, so no `.pulse_cluster.kek` file is written into the data directory. The
  key isn't in the data backups either, so keep a copy in your password manager. Without it, restored
  sealed secrets can't be read.

## 4. Storage: local disk or a bucket

- **Local disk (default):** data goes in `/var/lib/pulse`. 40 GB holds hundreds of millions of events.
- **S3-compatible bucket:** uncomment the `PULSE_OBJECT_STORE_URL` / `AWS_*` lines in
  `/etc/pulse/pulse.env`, then run `systemctl restart pulse`.
  - Cloudflare R2: 10 GB free, no egress fees. Set `AWS_REGION=auto` and set `AWS_ENDPOINT` to the R2 endpoint.
  - Hetzner Object Storage, Backblaze B2 and AWS S3 also work; leave `AWS_ENDPOINT` unset for AWS.
  - Flushes and compaction make many small writes, so watch the request counts against the free tier.
  - Metadata (`pulse_meta.db`) stays on local disk either way.
  - Pick one before you ingest real data; switching later doesn't move existing data.

## 5. Backups

`deploy/backup.sh` stops the node for a few seconds, tars `/var/lib/pulse` into `/var/backups/pulse`,
keeps 7 copies and, if `RCLONE_REMOTE` is set, uploads the new one off the box:

```bash
install -m 700 /opt/pulse/backup.sh /usr/local/sbin/pulse-backup
echo '17 3 * * * root /usr/local/sbin/pulse-backup' > /etc/cron.d/pulse-backup
/usr/local/sbin/pulse-backup && ls -lh /var/backups/pulse     # test once
```

Off-box copies, cheapest first:
- Hetzner's server backups (+20%).
- An R2 bucket via `apt install rclone && rclone config`, then add `RCLONE_REMOTE=r2:pulse-backups`
  before the command in the cron line.

To restore:

```bash
systemctl stop pulse
rm -rf /var/lib/pulse
tar -C /var/lib -xzf <backup>
chown -R pulse:pulse /var/lib/pulse
systemctl start pulse
```

Restore needs the same `/etc/pulse/pulse.env`.

## 6. Upgrades

Build a new tarball, copy it over, then:

```bash
tar --no-same-owner -C /opt -xzf /tmp/pulse-linux-aarch64.tar.gz && systemctl restart pulse
```

The tarball doesn't contain `/etc/pulse/pulse.env` or `/var/lib/pulse`, so neither is touched.
