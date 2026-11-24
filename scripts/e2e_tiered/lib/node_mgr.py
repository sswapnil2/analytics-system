"""Isolated pulse-node lifecycle, cluster harness, CLI runner, and local disk/SQLite helpers."""

from __future__ import annotations

import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, IO

from lib.client import PulseHttpClient
from lib.mock_s3 import MockS3Server

VALID_NODE_ROLES: tuple[str, ...] = ("all", "ingest", "writer", "query", "compactor")
_PK_KEY_RE = re.compile(r"\b(pk_live_[A-Za-z0-9_]+)\b")
_FLAG_RE = re.compile(r"(--[a-zA-Z0-9][a-zA-Z0-9-]*)")
_SUBCMD_LINE_RE = re.compile(r"^(?:[ \t]{1,8})([a-z][a-z0-9_-]{1,31})\s{2,}\S", re.MULTILINE)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def free_port(host: str = "127.0.0.1") -> int:
    """Allocate an available ephemeral loopback TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def free_udp_port(host: str = "127.0.0.1") -> int:
    """Allocate an available ephemeral loopback UDP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def build_binary(
    *,
    release: bool = False,
    force: bool = False,
    repo_root: Path | None = None,
) -> Path:
    """Locate or build the `pulse-node` binary via `cargo build -p pulse-node --locked`."""
    root = Path(repo_root) if repo_root is not None else _repo_root()
    target_bin = root / "target" / ("release" if release else "debug") / "pulse-node"
    if not force:
        if target_bin.is_file():
            return target_bin
        alt_bin = root / "target" / ("debug" if release else "release") / "pulse-node"
        if alt_bin.is_file():
            return alt_bin

    cmd = ["cargo", "build", "-p", "pulse-node", "--locked"]
    if release:
        cmd.append("--release")
    proc = subprocess.run(cmd, cwd=str(root), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to build pulse-node ({' '.join(cmd)}), exit={proc.returncode}")
    if not target_bin.is_file():
        raise FileNotFoundError(f"Expected built binary at {target_bin}")
    return target_bin


@dataclass(frozen=True, slots=True)
class Capabilities:
    """Detected CLI subcommands and flags from `pulse-node --help`."""

    binary_path: Path
    help_text: str
    flags: frozenset[str]
    subcommands: frozenset[str]

    def has_flag(self, flag: str) -> bool:
        return flag in self.flags

    def has_subcommand(self, name: str) -> bool:
        return name in self.subcommands


def _chmod_writable_tree(root: Path) -> None:
    if not root.exists():
        return
    try:
        os.chmod(root, 0o755)
    except OSError:
        pass
    for dirpath, dirnames, filenames in os.walk(root):
        for d in dirnames:
            try:
                os.chmod(Path(dirpath) / d, 0o755)
            except OSError:
                pass
        for f in filenames:
            try:
                os.chmod(Path(dirpath) / f, 0o644)
            except OSError:
                pass


def _resolve_rel_path(data_dir: Path, rel_path: str) -> Path:
    clean = rel_path.lstrip("/")
    target = (data_dir / clean).resolve() if clean else data_dir.resolve()
    root_res = data_dir.resolve()
    if target != root_res and root_res not in target.parents:
        raise ValueError(f"Path escapes data_dir: {rel_path!r}")
    return target


@dataclass(slots=True)
class NodeInstance:
    """Handle to a managed or attached pulse-node instance."""

    role: str
    host: str
    port: int
    base_url: str
    data_dir: Path
    web_dir: Path | None
    admin_token: str | None
    proc: subprocess.Popen[bytes] | None
    log_path: Path | None
    manager: NodeManager | None = field(default=None, repr=False)
    project_id: str | None = None
    quic_addr: str | None = None
    no_quic: bool = True
    seed: bool = False
    start_env: dict[str, str] = field(default_factory=dict, repr=False)
    extra_args: list[str] = field(default_factory=list, repr=False)
    _log_fh: IO[bytes] | None = field(default=None, repr=False)

    def client(self, *, api_key: str | None = None, timeout: float = 15.0) -> PulseHttpClient:
        """Create a PulseHttpClient bound to this node's URL and admin token."""
        tok = self.admin_token
        if tok is None:
            token_file = self.data_dir / "root_admin_token"
            if token_file.exists():
                tok = token_file.read_text(encoding="utf-8").strip()
        return PulseHttpClient(self.base_url, admin_token=tok, api_key=api_key, timeout=timeout)

    def read_logs(self, tail_bytes: int = 65536) -> str:
        """Read the tail (or full if tail_bytes <= 0) of this node's log file."""
        if self.log_path is None or not self.log_path.exists():
            return ""
        raw = self.log_path.read_bytes()
        if tail_bytes > 0 and len(raw) > tail_bytes:
            raw = raw[-tail_bytes:]
        return raw.decode("utf-8", errors="replace")

    def root_admin_token(self) -> str:
        """Return explicit admin_token or read `<data_dir>/root_admin_token`."""
        if self.admin_token:
            return self.admin_token
        token_file = self.data_dir / "root_admin_token"
        if not token_file.exists():
            raise FileNotFoundError(f"Root admin token file not found at {token_file}")
        return token_file.read_text(encoding="utf-8").strip()

    def extract_boot_ingest_key(self) -> str | None:
        """Extract the first-boot `pk_live_...` key from node logs, if present."""
        logs = self.read_logs(tail_bytes=262_144)
        match = _PK_KEY_RE.search(logs)
        return match.group(1) if match else None

    def stop(self, *, graceful: bool = True, timeout: float = 20.0) -> int | None:
        assert self.manager is not None
        return self.manager.stop(self, graceful=graceful, timeout=timeout)

    def kill(self) -> int | None:
        assert self.manager is not None
        return self.manager.kill(self)

    def restart(self, **kwargs: Any) -> NodeInstance:
        assert self.manager is not None
        return self.manager.restart(self, **kwargs)

    def wait_ready(self, *, timeout: float = 60.0) -> None:
        assert self.manager is not None
        self.manager.wait_ready(self, timeout=timeout)

    def local_files(self, rel_prefix: str = "") -> list[str]:
        """List sorted POSIX relative file paths under `data_dir` matching `rel_prefix`."""
        root = self.data_dir.resolve()
        if not root.exists():
            return []
        norm_prefix = rel_prefix.lstrip("/")
        found: list[str] = []
        for p in root.rglob("*"):
            if p.is_file():
                rel = p.relative_to(root).as_posix()
                if not norm_prefix or rel.startswith(norm_prefix):
                    found.append(rel)
        found.sort()
        return found

    def read_local_bytes(self, rel_path: str) -> bytes:
        target = _resolve_rel_path(self.data_dir, rel_path)
        return target.read_bytes()

    def write_local_bytes(self, rel_path: str, data: bytes) -> Path:
        target = _resolve_rel_path(self.data_dir, rel_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return target

    def chmod_local(self, rel_path: str, mode: int) -> None:
        target = _resolve_rel_path(self.data_dir, rel_path)
        os.chmod(target, mode)

    def rm_local(self, rel_path: str) -> None:
        target = _resolve_rel_path(self.data_dir, rel_path)
        if not target.exists() and not target.is_symlink():
            return
        if target.is_dir() and not target.is_symlink():
            _chmod_writable_tree(target)
            shutil.rmtree(target)
        else:
            target.unlink(missing_ok=True)

    def sqlite_query(
        self,
        sql: str,
        params: tuple[Any, ...] | list[Any] = (),
        *,
        db_path: Path | str | None = None,
        readonly: bool = True,
    ) -> list[dict[str, Any]]:
        assert self.manager is not None
        return self.manager.sqlite_query(
            sql,
            params,
            db_path=db_path or (self.data_dir / "pulse_meta.db"),
            readonly=readonly,
        )


class NodeManager:
    """Manages isolated pulse-node processes, CLI invocations, and temp directories."""

    def __init__(self, *, repo_root: Path | None = None, binary: Path | str | None = None) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else _repo_root()
        env_bin = os.environ.get("E2E_TIERED_BINARY", "").strip()
        if binary is not None:
            self._explicit_binary: Path | None = Path(binary)
        elif env_bin:
            self._explicit_binary = Path(env_bin)
        else:
            self._explicit_binary = None
        self._instances: list[NodeInstance] = []
        self._owned_temp_dirs: list[tempfile.TemporaryDirectory[str]] = []
        self._capabilities_cache: Capabilities | None = None
        self._postgres_urls: set[str] = set()

    @staticmethod
    def free_port(host: str = "127.0.0.1") -> int:
        return free_port(host)

    @staticmethod
    def free_udp_port(host: str = "127.0.0.1") -> int:
        return free_udp_port(host)

    def build_binary(self, *, release: bool = False, force: bool = False) -> Path:
        return build_binary(release=release, force=force, repo_root=self.repo_root)

    def resolve_binary(self, explicit: Path | str | None = None) -> Path:
        candidate = Path(explicit) if explicit is not None else self._explicit_binary
        if candidate is not None:
            if candidate.is_file():
                return candidate
            raise FileNotFoundError(f"Specified pulse-node binary not found: {candidate}")

        for rel in ("target/release/pulse-node", "target/debug/pulse-node"):
            p = self.repo_root / rel
            if p.is_file():
                return p
        raise FileNotFoundError(
            f"Could not find pulse-node binary under {self.repo_root / 'target'}"
        )

    def make_temp_data_dir(self, prefix: str = "pulse_tiered_e2e_") -> Path:
        td = tempfile.TemporaryDirectory(prefix=prefix)
        self._owned_temp_dirs.append(td)
        return Path(td.name)

    def probe_capabilities(self) -> Capabilities:
        if self._capabilities_cache is not None:
            return self._capabilities_cache
        bin_path = self.resolve_binary()
        res = subprocess.run(
            [str(bin_path), "--help"],
            cwd=str(self.repo_root),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        help_text = (res.stdout or "") + "\n" + (res.stderr or "")
        flags = frozenset(_FLAG_RE.findall(help_text))
        subcommands: set[str] = set()
        in_subcmd_section = False
        for line in help_text.splitlines():
            stripped = line.strip()
            if stripped.upper().startswith(("COMMANDS:", "SUBCOMMANDS:")):
                in_subcmd_section = True
                continue
            if in_subcmd_section:
                if not stripped:
                    in_subcmd_section = False
                    continue
                if stripped.upper().endswith(":"):
                    in_subcmd_section = False
                    continue
                m = _SUBCMD_LINE_RE.match(line)
                if m:
                    subcommands.add(m.group(1))
        caps = Capabilities(
            binary_path=bin_path,
            help_text=help_text,
            flags=flags,
            subcommands=frozenset(subcommands),
        )
        self._capabilities_cache = caps
        return caps

    def run_cli(
        self,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
        data_dir: Path | str | None = None,
        timeout: float = 30.0,
    ) -> subprocess.CompletedProcess[str]:
        """Run a `pulse-node` CLI subcommand or flag and capture stdout/stderr."""
        bin_path = self.resolve_binary()
        proc_env = dict(os.environ)
        proc_env.setdefault("PULSE_ALLOW_PRIVATE_ENDPOINTS", "1")
        proc_env.setdefault("PULSE_ALLOW_EPHEMERAL_KEK", "1")
        if data_dir is not None:
            proc_env["PULSE_DATA_DIR"] = str(data_dir)
        if env:
            proc_env.update(env)
        return subprocess.run(
            [str(bin_path), *args],
            cwd=str(self.repo_root),
            env=proc_env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def wait_ready(self, inst: NodeInstance, *, timeout: float = 60.0) -> None:
        client = PulseHttpClient(inst.base_url, timeout=2.0)
        deadline = time.monotonic() + timeout
        last_err = "not checked"
        while time.monotonic() < deadline:
            if inst.proc is not None:
                rc = inst.proc.poll()
                if rc is not None:
                    tail = inst.read_logs(tail_bytes=8192)
                    raise RuntimeError(
                        f"pulse-node ({inst.role}) exited prematurely with code {rc}.\nLogs:\n{tail}"
                    )
            try:
                hz = client.healthz()
                if hz.status == 200:
                    rz = client.readyz()
                    if rz.status == 200:
                        return
                    last_err = f"/readyz status={rz.status} body={rz.text(errors='replace')[:200]}"
                else:
                    last_err = f"/healthz status={hz.status}"
            except OSError as exc:
                last_err = str(exc)
            time.sleep(0.1)

        tail = inst.read_logs(tail_bytes=8192)
        raise TimeoutError(
            f"Timed out waiting {timeout:.1f}s for pulse-node ({inst.role}) at {inst.base_url} "
            f"(last error: {last_err}).\nLogs:\n{tail}"
        )

    def start_node(
        self,
        *,
        host: str = "127.0.0.1",
        port: int | None = None,
        data_dir: Path | str | None = None,
        web_dir: Path | str | None = None,
        project_id: str | None = None,
        role: str = "all",
        quic_addr: str | None = None,
        no_quic: bool = True,
        seed: bool = False,
        hot_store: str | None = None,
        object_store_url: str | None = None,
        aws_endpoint: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_region: str | None = None,
        database_url: str | None = None,
        byok_master_key: str | None = None,
        admin_token: str | None = None,
        metrics_token: str | None = None,
        upload_interval_secs: int | float | None = None,
        writer_lease_ttl_secs: int | None = None,
        allow_ephemeral_kek: bool = True,
        allow_private_endpoints: bool = True,
        node_name: str | None = None,
        drain_ms: int = 0,
        mock_s3: MockS3Server | None = None,
        extra_env: dict[str, str] | None = None,
        extra_args: list[str] | None = None,
        wait_ready: bool = True,
        expect_fail: bool = False,
        wait_timeout: float = 60.0,
    ) -> NodeInstance | tuple[int, str, str]:
        """Spawn a `pulse-node` process (or verify startup failure when `expect_fail=True`)."""
        if role not in VALID_NODE_ROLES:
            raise ValueError(f"Invalid role {role!r}; expected one of {VALID_NODE_ROLES}")

        bin_path = self.resolve_binary()
        resolved_data_dir = Path(data_dir) if data_dir is not None else self.make_temp_data_dir()
        resolved_data_dir.mkdir(parents=True, exist_ok=True)

        if web_dir is not None:
            resolved_web_dir: Path | None = Path(web_dir)
        else:
            default_web = self.repo_root / "web" / "dist"
            resolved_web_dir = default_web if default_web.is_dir() else None

        chosen_port = port if port is not None else self.free_port(host)
        cli_args: list[str] = [
            "--role",
            role,
            "--host",
            host,
            "--port",
            str(chosen_port),
            "--data-dir",
            str(resolved_data_dir),
        ]
        if resolved_web_dir is not None:
            cli_args.extend(["--web-dir", str(resolved_web_dir)])
        if project_id is not None:
            cli_args.extend(["--project-id", project_id])
        if quic_addr is not None:
            cli_args.extend(["--quic-addr", quic_addr])
        elif no_quic:
            cli_args.append("--no-quic")
        cli_args.append("--seed" if seed else "--no-seed")

        env = dict(os.environ)
        # Clear inherited test-hostile env vars unless explicitly set for this node
        for k in (
            "PULSE_HOT_STORE",
            "PULSE_OBJECT_STORE_URL",
            "AWS_ENDPOINT",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "DATABASE_URL",
            "PULSE_BYOK_MASTER_KEY",
            "PULSE_ADMIN_TOKEN",
            "PULSE_METRICS_TOKEN",
            "PULSE_UPLOAD_INTERVAL_SECS",
            "PULSE_WRITER_LEASE_TTL_SECS",
            "PULSE_NODE_NAME",
        ):
            env.pop(k, None)

        env["PULSE_ALLOW_PRIVATE_ENDPOINTS"] = "1" if allow_private_endpoints else "0"
        env["PULSE_ALLOW_EPHEMERAL_KEK"] = "1" if allow_ephemeral_kek else "0"
        env["PULSE_SHUTDOWN_DRAIN_MS"] = str(drain_ms)

        if mock_s3 is not None:
            env.update(mock_s3.env_vars())
        if hot_store is not None:
            env["PULSE_HOT_STORE"] = hot_store
        if object_store_url is not None:
            env["PULSE_OBJECT_STORE_URL"] = object_store_url
            if object_store_url.startswith("s3://"):
                env.setdefault("AWS_ACCESS_KEY_ID", "test")
                env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
                env.setdefault("AWS_REGION", "us-east-1")
        if aws_endpoint is not None:
            env["AWS_ENDPOINT"] = aws_endpoint
        if aws_access_key_id is not None:
            env["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        if aws_secret_access_key is not None:
            env["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        if aws_region is not None:
            env["AWS_REGION"] = aws_region
        if database_url is not None:
            env["DATABASE_URL"] = database_url
        if byok_master_key is not None:
            env["PULSE_BYOK_MASTER_KEY"] = byok_master_key
        if admin_token is not None:
            env["PULSE_ADMIN_TOKEN"] = admin_token
        if metrics_token is not None:
            env["PULSE_METRICS_TOKEN"] = metrics_token
        if upload_interval_secs is not None:
            env["PULSE_UPLOAD_INTERVAL_SECS"] = str(upload_interval_secs)
        if writer_lease_ttl_secs is not None:
            env["PULSE_WRITER_LEASE_TTL_SECS"] = str(writer_lease_ttl_secs)
        if node_name is not None:
            env["PULSE_NODE_NAME"] = node_name
            caps = self.probe_capabilities()
            if caps.has_flag("--node-name"):
                cli_args.extend(["--node-name", node_name])
        if extra_args:
            cli_args.extend(extra_args)
        if extra_env:
            env.update(extra_env)

        effective_db_url = env.get("DATABASE_URL", "").strip()
        if effective_db_url.startswith(("postgres://", "postgresql://")):
            configured_pg_urls = {
                os.environ.get(var, "").strip()
                for var in ("E2E_POSTGRES_URL", "DATABASE_URL")
                if os.environ.get(var, "").strip()
            }
            if effective_db_url in configured_pg_urls and effective_db_url not in self._postgres_urls:
                self._reset_postgres_db(effective_db_url)
                self._postgres_urls.add(effective_db_url)

        if expect_fail:
            proc = subprocess.Popen(
                [str(bin_path), *cli_args],
                cwd=str(self.repo_root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            fail_timeout = min(wait_timeout, 20.0)
            try:
                stdout_txt, stderr_txt = proc.communicate(timeout=fail_timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout_txt, stderr_txt = proc.communicate(timeout=5.0)
                raise AssertionError(
                    f"Expected pulse-node startup failure within {fail_timeout:.1f}s, "
                    f"but process stayed running.\nstdout={stdout_txt}\nstderr={stderr_txt}"
                )
            if proc.returncode == 0:
                raise AssertionError(
                    f"Expected pulse-node startup failure, but exited 0.\nstdout={stdout_txt}\nstderr={stderr_txt}"
                )
            return int(proc.returncode), stdout_txt, stderr_txt

        log_path = resolved_data_dir / f"node_{role}_{chosen_port}.log"
        log_fh = log_path.open("ab")
        proc = subprocess.Popen(
            [str(bin_path), *cli_args],
            cwd=str(self.repo_root),
            env=env,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        inst = NodeInstance(
            role=role,
            host=host,
            port=chosen_port,
            base_url=f"http://{host}:{chosen_port}",
            data_dir=resolved_data_dir,
            web_dir=resolved_web_dir,
            admin_token=admin_token,
            proc=proc,
            log_path=log_path,
            manager=self,
            project_id=project_id,
            quic_addr=quic_addr,
            no_quic=no_quic,
            seed=seed,
            start_env=dict(env),
            extra_args=list(extra_args or []),
            _log_fh=log_fh,
        )
        self._instances.append(inst)
        if wait_ready:
            try:
                self.wait_ready(inst, timeout=wait_timeout)
            except Exception:
                self.stop(inst, graceful=False, timeout=5.0)
                raise
        return inst

    def stop(self, inst: NodeInstance, *, graceful: bool = True, timeout: float = 20.0) -> int | None:
        proc = inst.proc
        rc: int | None = None
        if proc is not None:
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM if graceful else signal.SIGKILL)
                    rc = proc.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    rc = proc.wait(timeout=5.0)
            else:
                rc = proc.returncode
        if inst._log_fh is not None:
            try:
                inst._log_fh.close()
            except OSError:
                pass
            inst._log_fh = None
        return rc

    def kill(self, inst: NodeInstance) -> int | None:
        return self.stop(inst, graceful=False, timeout=5.0)

    def restart(
        self,
        inst: NodeInstance,
        *,
        graceful_stop: bool = True,
        reuse_port: bool = False,
        wait_timeout: float = 60.0,
        hot_store: str | None = None,
        extra_env: dict[str, str] | None = None,
        extra_args: list[str] | None = None,
        expect_fail: bool = False,
        wait_ready: bool = True,
    ) -> NodeInstance | tuple[int, str, str]:
        effective_token = inst.admin_token
        if effective_token is None:
            token_path = inst.data_dir / "root_admin_token"
            if token_path.exists():
                effective_token = token_path.read_text(encoding="utf-8").strip()
        self.stop(inst, graceful=graceful_stop)

        merged_env = dict(inst.start_env)
        if hot_store is not None:
            merged_env["PULSE_HOT_STORE"] = hot_store
        if extra_env:
            merged_env.update(extra_env)

        return self.start_node(
            role=inst.role,
            host=inst.host,
            port=inst.port if reuse_port else None,
            data_dir=inst.data_dir,
            web_dir=inst.web_dir,
            project_id=inst.project_id,
            quic_addr=inst.quic_addr,
            no_quic=inst.no_quic,
            seed=inst.seed,
            admin_token=effective_token,
            extra_env=merged_env,
            extra_args=extra_args if extra_args is not None else inst.extra_args,
            wait_ready=wait_ready,
            expect_fail=expect_fail,
            wait_timeout=wait_timeout,
        )

    @staticmethod
    def sqlite_query(
        sql: str,
        params: tuple[Any, ...] | list[Any] = (),
        *,
        db_path: Path | str,
        readonly: bool = True,
    ) -> list[dict[str, Any]]:
        p = Path(db_path)
        if not p.exists():
            raise FileNotFoundError(f"SQLite database not found: {p}")
        if readonly:
            conn = sqlite3.connect(f"file:{p.resolve()}?mode=ro", uri=True, timeout=5.0)
        else:
            conn = sqlite3.connect(str(p.resolve()), timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            if not readonly:
                conn.commit()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _reset_postgres_db(pg_url: str) -> None:
        psql_bin = shutil.which("psql")
        if psql_bin is None:
            for candidate in (
                "/opt/homebrew/opt/postgresql@17/bin/psql",
                "/opt/homebrew/bin/psql",
            ):
                if Path(candidate).is_file():
                    psql_bin = candidate
                    break
        if psql_bin is None:
            raise RuntimeError("psql binary not found on PATH or in Homebrew PostgreSQL paths")
        sql = (
            "DO $$ DECLARE tbls text; BEGIN "
            "SELECT string_agg(quote_ident(schemaname) || '.' || quote_ident(tablename), ', ') "
            "INTO tbls FROM pg_tables "
            "WHERE schemaname = 'public' AND tablename <> '_sqlx_migrations'; "
            "IF tbls IS NOT NULL THEN "
            "EXECUTE 'TRUNCATE TABLE ' || tbls || ' RESTART IDENTITY CASCADE'; "
            "END IF; END $$;"
        )
        res = subprocess.run(
            [psql_bin, pg_url, "-v", "ON_ERROR_STOP=1", "-c", sql],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if res.returncode != 0:
            raise RuntimeError(
                f"Failed to reset Postgres DB via psql (rc={res.returncode}): {res.stderr.strip() or res.stdout.strip()}"
            )

    def cleanup(self) -> None:
        for inst in reversed(self._instances):
            try:
                self.stop(inst, graceful=True, timeout=10.0)
            except OSError:
                pass
        self._instances.clear()
        for pg_url in self._postgres_urls:
            self._reset_postgres_db(pg_url)
        self._postgres_urls.clear()
        for td in reversed(self._owned_temp_dirs):
            p = Path(td.name)
            _chmod_writable_tree(p)
            try:
                td.cleanup()
            except OSError:
                shutil.rmtree(p, ignore_errors=True)
        self._owned_temp_dirs.clear()

    def __enter__(self) -> NodeManager:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.cleanup()


class ClusterHarness:
    """High-level harness managing MockS3Server + single/split/W3 pulse-node topologies."""

    def __init__(
        self,
        *,
        use_mock_s3: bool = True,
        bucket: str = "pulse-e2e",
        binary: Path | str | None = None,
        admin_token: str | None = None,
    ) -> None:
        self.node_mgr = NodeManager(binary=binary)
        self.mock_s3: MockS3Server | None = MockS3Server(bucket=bucket) if use_mock_s3 else None
        self.admin_token = admin_token or f"sk_root_e2e_{int(time.time() * 1000)}"
        self.nodes: list[NodeInstance] = []

    def __enter__(self) -> ClusterHarness:
        if self.mock_s3 is not None:
            self.mock_s3.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.cleanup()

    def start_node(self, **kwargs: Any) -> NodeInstance | tuple[int, str, str]:
        if self.mock_s3 is not None and "mock_s3" not in kwargs and "object_store_url" not in kwargs:
            kwargs["mock_s3"] = self.mock_s3
        kwargs.setdefault("admin_token", self.admin_token)
        res = self.node_mgr.start_node(**kwargs)
        if isinstance(res, NodeInstance):
            self.nodes.append(res)
        return res

    def start_split_cluster(
        self,
        *,
        data_dir: Path | str | None = None,
        hot_store: str = "local",
        **kwargs: Any,
    ) -> dict[str, NodeInstance]:
        shared_dir = Path(data_dir) if data_dir is not None else self.node_mgr.make_temp_data_dir("pulse_split_")
        shared_dir.mkdir(parents=True, exist_ok=True)
        compactor = self.start_node(role="compactor", data_dir=shared_dir, hot_store=hot_store, **kwargs)
        ingest = self.start_node(role="ingest", data_dir=shared_dir, hot_store=hot_store, **kwargs)
        query = self.start_node(role="query", data_dir=shared_dir, hot_store=hot_store, **kwargs)
        assert isinstance(compactor, NodeInstance)
        assert isinstance(ingest, NodeInstance)
        assert isinstance(query, NodeInstance)
        return {"compactor": compactor, "ingest": ingest, "query": query}

    def start_w3_cluster(
        self,
        *,
        num_writers: int = 2,
        num_storage: int = 2,
        database_url: str | None = None,
        hot_store: str = "local",
        **kwargs: Any,
    ) -> dict[str, list[NodeInstance]]:
        """Start a multi-node W3 topology (stateless ingest writers + storage nodes)."""
        shared_meta = database_url
        if shared_meta is None:
            meta_dir = self.node_mgr.make_temp_data_dir("pulse_w3_meta_")
            shared_meta = f"sqlite://{(meta_dir / 'shared_meta.db').resolve()}"

        storages: list[NodeInstance] = []
        for idx in range(num_storage):
            s_dir = self.node_mgr.make_temp_data_dir(f"pulse_w3_storage_{idx}_")
            node = self.start_node(
                role="all",
                data_dir=s_dir,
                database_url=shared_meta,
                hot_store=hot_store,
                node_name=f"storage-{idx}",
                **kwargs,
            )
            assert isinstance(node, NodeInstance)
            storages.append(node)

        writers: list[NodeInstance] = []
        for idx in range(num_writers):
            w_dir = self.node_mgr.make_temp_data_dir(f"pulse_w3_writer_{idx}_")
            node = self.start_node(
                role="writer",
                data_dir=w_dir,
                database_url=shared_meta,
                hot_store=hot_store,
                node_name=f"writer-{idx}",
                **kwargs,
            )
            assert isinstance(node, NodeInstance)
            writers.append(node)

        return {"storages": storages, "writers": writers}

    def cleanup(self) -> None:
        self.node_mgr.cleanup()
        if self.mock_s3 is not None:
            self.mock_s3.stop()
        self.nodes.clear()
