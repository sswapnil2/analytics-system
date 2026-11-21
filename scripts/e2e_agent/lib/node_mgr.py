"""Isolated pulse-node lifecycle, split-role cluster, SQLite, and object-store inspector."""

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

from lib.client import PulseClient

VALID_NODE_ROLES: tuple[str, ...] = ("all", "ingest", "query", "compactor")
_PK_KEY_RE = re.compile(r"\b(pk_live_[A-Za-z0-9_]+)\b")


def sqlite_tables(data_dir: Path | str) -> list[str]:
    """Read-only inspection of user tables in `<data_dir>/pulse_meta.db`."""
    db_path = Path(data_dir) / "pulse_meta.db"
    if not db_path.exists():
        return []
    uri = f"file:{db_path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        return [str(r[0]) for r in rows]
    finally:
        conn.close()


def list_agent_artifacts(data_dir: Path | str) -> dict[str, list[Path]]:
    """List agent WAL, segment, and manifest files under `<data_dir>/object_store/`."""
    obj_root = Path(data_dir) / "object_store"
    out: dict[str, list[Path]] = {
        "agent_wal": [],
        "agent_segments": [],
        "agent_manifests": [],
    }
    for key in out:
        sub = obj_root / key
        if sub.is_dir():
            out[key] = sorted(p for p in sub.rglob("*") if p.is_file())
    return out


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
    seed: bool = False
    no_quic: bool = True
    drain_ms: int = 0
    _log_fh: IO[bytes] | None = field(default=None, repr=False)

    def read_logs(self, tail_bytes: int = 65536) -> str:
        """Read the tail of this node's log file."""
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

    def sqlite_tables(self) -> list[str]:
        """Return sorted SQLite table names in `<data_dir>/pulse_meta.db`."""
        return sqlite_tables(self.data_dir)

    def object_store_root(self) -> Path:
        """Return `<data_dir>/object_store` path."""
        return self.data_dir / "object_store"

    def list_agent_artifacts(self) -> dict[str, list[Path]]:
        """List `agent_wal`, `agent_segments`, and `agent_manifests` files on disk."""
        return list_agent_artifacts(self.data_dir)


class NodeManager:
    """Manages isolated pulse-node processes and temp directories with guaranteed cleanup."""

    def __init__(self, *, repo_root: Path | None = None, binary: Path | str | None = None) -> None:
        self.repo_root = (
            Path(repo_root)
            if repo_root is not None
            else Path(__file__).resolve().parents[3]
        )
        self._explicit_binary = Path(binary) if binary is not None else None
        self._instances: list[NodeInstance] = []
        self._owned_temp_dirs: list[tempfile.TemporaryDirectory[str]] = []

    def resolve_binary(self, explicit: Path | str | None = None) -> Path:
        """Locate the `pulse-node` binary (explicit -> target/release -> target/debug)."""
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

    @staticmethod
    def free_port(host: str = "127.0.0.1") -> int:
        """Allocate an available ephemeral loopback TCP port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, 0))
            return int(sock.getsockname()[1])

    def make_temp_data_dir(self, prefix: str = "pulse_e2e_") -> Path:
        """Create a managed TemporaryDirectory that cleanup() removes automatically."""
        td = tempfile.TemporaryDirectory(prefix=prefix)
        self._owned_temp_dirs.append(td)
        return Path(td.name)

    def build_cli_args(
        self,
        *,
        role: str = "all",
        host: str = "127.0.0.1",
        port: int,
        data_dir: Path,
        web_dir: Path | None = None,
        no_quic: bool = True,
        seed: bool = False,
        project_id: str | None = None,
    ) -> list[str]:
        """Construct pulse-node CLI arguments."""
        if role not in VALID_NODE_ROLES:
            raise ValueError(f"Invalid pulse-node role {role!r}; expected one of {VALID_NODE_ROLES}")
        args = [
            "--role",
            role,
            "--host",
            host,
            "--port",
            str(port),
            "--data-dir",
            str(data_dir),
        ]
        if web_dir is not None:
            args.extend(["--web-dir", str(web_dir)])
        if no_quic:
            args.append("--no-quic")
        if seed:
            args.append("--seed")
        if project_id:
            args.extend(["--project-id", project_id])
        return args

    @staticmethod
    def build_env(
        *,
        admin_token: str | None = None,
        drain_ms: int = 0,
        extra_env: dict[str, str] | None = None,
        base_env: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Construct child process environment with `PULSE_ALLOW_PRIVATE_ENDPOINTS=1`."""
        env = dict(os.environ if base_env is None else base_env)
        env["PULSE_ALLOW_PRIVATE_ENDPOINTS"] = "1"
        env["PULSE_SHUTDOWN_DRAIN_MS"] = str(drain_ms)
        if admin_token is not None:
            env["PULSE_ADMIN_TOKEN"] = admin_token
        if extra_env:
            env.update(extra_env)
        return env

    def wait_ready(self, inst: NodeInstance, *, timeout: float = 60.0) -> None:
        """Poll `/healthz` and `/readyz` until HTTP 200 or fail fast on process exit."""
        client = PulseClient(inst.base_url, timeout=2.0)
        deadline = time.monotonic() + timeout
        last_err: str = "not checked"

        while time.monotonic() < deadline:
            if inst.proc is not None:
                rc = inst.proc.poll()
                if rc is not None:
                    tail = inst.read_logs(tail_bytes=8192)
                    raise RuntimeError(
                        f"pulse-node ({inst.role}) exited prematurely with code {rc}.\nLogs:\n{tail}"
                    )
            try:
                hz = client.get_healthz()
                if hz.status == 200:
                    rz = client.get_readyz()
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
        role: str = "all",
        *,
        host: str = "127.0.0.1",
        port: int | None = None,
        data_dir: Path | str | None = None,
        web_dir: Path | str | None = None,
        seed: bool = False,
        no_quic: bool = True,
        admin_token: str | None = None,
        drain_ms: int = 0,
        project_id: str | None = None,
        extra_env: dict[str, str] | None = None,
        wait_timeout: float = 60.0,
    ) -> NodeInstance:
        """Spawn an isolated pulse-node process and wait for `/readyz` == 200."""
        bin_path = self.resolve_binary()
        resolved_data_dir = Path(data_dir) if data_dir is not None else self.make_temp_data_dir()
        resolved_data_dir.mkdir(parents=True, exist_ok=True)

        if web_dir is not None:
            resolved_web_dir: Path | None = Path(web_dir)
        else:
            default_web = self.repo_root / "web" / "dist"
            resolved_web_dir = default_web if default_web.is_dir() else None

        chosen_port = port if port is not None else self.free_port(host)
        cli_args = self.build_cli_args(
            role=role,
            host=host,
            port=chosen_port,
            data_dir=resolved_data_dir,
            web_dir=resolved_web_dir,
            no_quic=no_quic,
            seed=seed,
            project_id=project_id,
        )
        env = self.build_env(
            admin_token=admin_token,
            drain_ms=drain_ms,
            extra_env=extra_env,
        )

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
            seed=seed,
            no_quic=no_quic,
            drain_ms=drain_ms,
            _log_fh=log_fh,
        )
        self._instances.append(inst)
        try:
            self.wait_ready(inst, timeout=wait_timeout)
        except Exception:
            self.stop(inst, graceful=False, timeout=5.0)
            raise
        return inst

    def start_all(self, **kwargs: Any) -> NodeInstance:
        """Start a single `--role all` node."""
        return self.start_node(role="all", **kwargs)

    def start_seeded(self, **kwargs: Any) -> NodeInstance:
        """Start a `--role all --seed` node."""
        kwargs.setdefault("seed", True)
        return self.start_node(role="all", **kwargs)

    def start_split_cluster(
        self,
        *,
        data_dir: Path | str | None = None,
        admin_token: str | None = None,
        wait_timeout: float = 60.0,
        **kwargs: Any,
    ) -> dict[str, NodeInstance]:
        """Start compactor, ingest, and query nodes sharing `data_dir` and `admin_token`."""
        shared_dir = Path(data_dir) if data_dir is not None else self.make_temp_data_dir("pulse_split_")
        shared_dir.mkdir(parents=True, exist_ok=True)
        shared_token = admin_token or f"sk_root_e2e_{int(time.time() * 1000)}"

        compactor = self.start_node(
            role="compactor",
            data_dir=shared_dir,
            admin_token=shared_token,
            wait_timeout=wait_timeout,
            **kwargs,
        )
        ingest = self.start_node(
            role="ingest",
            data_dir=shared_dir,
            admin_token=shared_token,
            wait_timeout=wait_timeout,
            **kwargs,
        )
        query = self.start_node(
            role="query",
            data_dir=shared_dir,
            admin_token=shared_token,
            wait_timeout=wait_timeout,
            **kwargs,
        )
        return {"compactor": compactor, "ingest": ingest, "query": query}

    def stop(self, inst: NodeInstance, *, graceful: bool = True, timeout: float = 20.0) -> int | None:
        """Stop a managed pulse-node process (SIGTERM graceful drain, SIGKILL fallback)."""
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
        """Force-kill a managed pulse-node immediately with SIGKILL (for crash-recovery tests)."""
        return self.stop(inst, graceful=False, timeout=5.0)

    def restart(
        self,
        inst: NodeInstance,
        *,
        graceful_stop: bool = True,
        reuse_port: bool = False,
        wait_timeout: float = 60.0,
    ) -> NodeInstance:
        """Stop `inst` and start a replacement pointing at the same `data_dir` and `admin_token`."""
        effective_token = inst.admin_token
        if effective_token is None:
            token_path = inst.data_dir / "root_admin_token"
            if token_path.exists():
                effective_token = token_path.read_text(encoding="utf-8").strip()
        self.stop(inst, graceful=graceful_stop)
        return self.start_node(
            role=inst.role,
            host=inst.host,
            port=inst.port if reuse_port else None,
            data_dir=inst.data_dir,
            web_dir=inst.web_dir,
            seed=inst.seed,
            no_quic=inst.no_quic,
            admin_token=effective_token,
            drain_ms=inst.drain_ms,
            wait_timeout=wait_timeout,
        )

    def attach_external(
        self,
        base_url: str,
        *,
        data_dir: Path | str | None = None,
        admin_token: str | None = None,
        wait_timeout: float = 30.0,
    ) -> NodeInstance:
        """Attach to an already-running external node without spawning a subprocess."""
        parsed = urllib_parse_host_port(base_url)
        resolved_dir = Path(data_dir) if data_dir is not None else Path(".")
        inst = NodeInstance(
            role="all",
            host=parsed[0],
            port=parsed[1],
            base_url=base_url.rstrip("/"),
            data_dir=resolved_dir,
            web_dir=None,
            admin_token=admin_token,
            proc=None,
            log_path=None,
        )
        self.wait_ready(inst, timeout=wait_timeout)
        return inst

    def cleanup(self) -> None:
        """Terminate all spawned node processes and remove managed temp directories."""
        for inst in reversed(self._instances):
            self.stop(inst, graceful=True, timeout=10.0)
        self._instances.clear()
        for td in reversed(self._owned_temp_dirs):
            try:
                td.cleanup()
            except OSError:
                shutil.rmtree(td.name, ignore_errors=True)
        self._owned_temp_dirs.clear()

    def __enter__(self) -> NodeManager:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.cleanup()


def urllib_parse_host_port(base_url: str) -> tuple[str, int]:
    from urllib.parse import urlsplit

    parsed = urlsplit(base_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return host, port
