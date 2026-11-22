"""HTTP-only multi-role, multi-org, and project API key bootstrap for E2E tests."""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lib.client import PulseClient


@dataclass(frozen=True, slots=True)
class ProjectCreds:
    """Isolated project identifier and API keys."""

    id: str
    name: str
    pk_key: str
    sk_key: str


@dataclass(slots=True)
class TestContext:
    """Isolated test context injected into every E2E test function."""

    base_url: str
    data_dir: Path | None
    root_admin_token: str
    owner_token: str
    admin_token: str
    member_token: str
    viewer_token: str
    other_org_token: str
    proj_a: ProjectCreds
    proj_b: ProjectCreds
    other_proj: ProjectCreds
    client: PulseClient
    mock_llm: Any = None
    mock_webhook: Any = None
    mock_llm_url: str | None = None
    mock_webhook_url: str | None = None
    node_manager: Any = None
    run_id: str = ""
    _id_counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1), repr=False)

    def unique_id(self, prefix: str = "e2e") -> str:
        """Return a collision-free identifier scoped to this test run."""
        seq = next(self._id_counter)
        base = self.run_id or "run"
        return f"{prefix}_{base}_{seq:05d}"

    def reset_mocks(self) -> None:
        """Reset recorded requests and restore default healthy responses on mock servers."""
        if self.mock_llm is not None:
            self.mock_llm.clear()
            self.mock_llm.status_code = 200
            self.mock_llm.raw_body_override = None
            self.mock_llm.latency_s = 0.0
            self.mock_llm.custom_responder = None
            self.mock_llm.response_content = (
                '{"score": 0.9, "passed": true, "reasoning": "Mock LLM judge pass"}'
            )
        if self.mock_webhook is not None:
            self.mock_webhook.clear()
            self.mock_webhook.status_code = 200
            self.mock_webhook.latency_s = 0.0
            self.mock_webhook.signing_secret = None


class AuthBootstrap:
    """Bootstraps Owner/Admin/Member/Viewer, second org, and isolated projects over HTTP."""

    @staticmethod
    def parse_invite_token(invite_url: str) -> str:
        """Extract the 64-hex invite token from `<public_url>/#/invite/<token>`."""
        marker = "/#/invite/"
        if marker not in invite_url:
            raise ValueError(f"Unexpected invite_url format: {invite_url!r}")
        token = invite_url.rsplit(marker, 1)[1].strip()
        if not token:
            raise ValueError(f"Empty invite token in invite_url: {invite_url!r}")
        return token

    @staticmethod
    def parse_verify_token(verify_url: str) -> str:
        """Extract the 64-hex email verification token from `<public_url>/#/verify/<token>`."""
        marker = "/#/verify/"
        if marker not in verify_url:
            raise ValueError(f"Unexpected verify_url format: {verify_url!r}")
        token = verify_url.rsplit(marker, 1)[1].strip()
        if not token:
            raise ValueError(f"Empty verification token in verify_url: {verify_url!r}")
        return token

    @classmethod
    def read_outbox_verify_token(
        cls,
        data_dir: Path | str,
        email: str,
        *,
        timeout: float = 5.0,
    ) -> str:
        """Poll `<data_dir>/outbox` for the email verification link and return its token."""
        outbox_path = Path(data_dir) / "outbox"
        target_email = email.strip().lower()
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if outbox_path.exists():
                lines = outbox_path.read_text(encoding="utf-8", errors="replace").splitlines()
                for line in reversed(lines):
                    parts = line.strip().split()
                    if len(parts) >= 4 and parts[1] == "verify" and parts[2].lower() == target_email:
                        return cls.parse_verify_token(parts[3])
            time.sleep(0.05)

        raise TimeoutError(
            f"Timed out waiting {timeout:.1f}s for verify line for {email} in {outbox_path}"
        )

    @classmethod
    def _create_project_with_keys(
        cls,
        client: PulseClient,
        *,
        token: str,
        name: str,
    ) -> ProjectCreds:
        resp = client.request(
            "POST",
            "/v1/projects",
            json_body={"name": name, "timezone": "UTC"},
            token=token,
        ).raise_for_status()
        data = resp.json()
        project = data["project"]
        proj_id = str(project["id"])
        pk_key = str(data["api_key"]["plaintext_key"])

        sk_resp = client.request(
            "POST",
            f"/v1/projects/{proj_id}/api-keys",
            json_body={"label": f"{name} Admin Key", "role": "admin"},
            token=token,
        ).raise_for_status()
        sk_key = str(sk_resp.json()["plaintext_key"])
        return ProjectCreds(id=proj_id, name=name, pk_key=pk_key, sk_key=sk_key)

    @classmethod
    def bootstrap_context(
        cls,
        client: PulseClient,
        data_dir: Path | str | None,
        admin_token: str,
        *,
        run_id: str | None = None,
        mock_llm: Any = None,
        mock_webhook: Any = None,
        node_manager: Any = None,
    ) -> TestContext:
        """Create Owner/Admin/Member/Viewer, an isolated second org, and projects A/B over HTTP."""
        rid = run_id or f"{int(time.time() * 1000)}"
        password = f"PulseE2E!{rid}"
        if len(password) < 10:
            password = f"{password}0123456789"[:14]
        resolved_dir = Path(data_dir) if data_dir is not None else None

        # 1. Bootstrap Owner in org_default (fresh signup with setup_code, or RootAdmin invite fallback)
        owner_email = f"owner+{rid}@e2e.local"
        signup_resp = client.request(
            "POST",
            "/v1/auth/signup",
            json_body={
                "email": owner_email,
                "password": password,
                "name": "E2E Owner",
                "setup_code": admin_token,
            },
        )
        if signup_resp.is_success() and isinstance(signup_resp.json(), dict) and "token" in signup_resp.json():
            owner_token = str(signup_resp.json()["token"])
        else:
            inv_resp = client.request(
                "POST",
                "/v1/org/invites",
                json_body={"email": owner_email, "role": "owner"},
                token=admin_token,
            ).raise_for_status()
            inv_token = cls.parse_invite_token(str(inv_resp.json()["invite_url"]))
            acc_resp = client.request(
                "POST",
                "/v1/auth/invites/accept",
                json_body={"token": inv_token, "password": password, "name": "E2E Owner"},
            ).raise_for_status()
            owner_token = str(acc_resp.json()["token"])

        # 2. Invite & accept Admin, Member, Viewer in org_default
        role_tokens: dict[str, str] = {}
        for role in ("admin", "member", "viewer"):
            email = f"{role}+{rid}@e2e.local"
            inv_resp = client.request(
                "POST",
                "/v1/org/invites",
                json_body={"email": email, "role": role},
                token=owner_token,
            ).raise_for_status()
            inv_tok = cls.parse_invite_token(str(inv_resp.json()["invite_url"]))
            acc_resp = client.request(
                "POST",
                "/v1/auth/invites/accept",
                json_body={"token": inv_tok, "password": password, "name": f"E2E {role.title()}"},
            ).raise_for_status()
            role_tokens[role] = str(acc_resp.json()["token"])

        # 3. Bootstrap isolated second organization + project via signup + outbox verification
        other_email = f"other+{rid}@other.local"
        other_signup = client.request(
            "POST",
            "/v1/auth/signup",
            json_body={
                "email": other_email,
                "password": password,
                "name": "Other Org Owner",
                "org_name": f"Other Org {rid}",
            },
        ).raise_for_status().json()

        if "token" in other_signup:
            other_org_token = str(other_signup["token"])
        else:
            if resolved_dir is None:
                raise RuntimeError(
                    "data_dir is required to read outbox verification token for second org signup"
                )
            verify_tok = cls.read_outbox_verify_token(resolved_dir, other_email)
            verify_resp = client.request(
                "POST",
                "/v1/auth/verify-email",
                json_body={"token": verify_tok},
            ).raise_for_status()
            other_org_token = str(verify_resp.json()["token"])

        other_proj = cls._create_project_with_keys(
            client,
            token=other_org_token,
            name=f"Other Org Project {rid}",
        )

        # 4. Create two isolated projects in org_default (proj_a & proj_b)
        proj_a = cls._create_project_with_keys(
            client,
            token=owner_token,
            name=f"Agent Project A {rid}",
        )
        proj_b = cls._create_project_with_keys(
            client,
            token=owner_token,
            name=f"Agent Project B {rid}",
        )

        return TestContext(
            base_url=client.base_url,
            data_dir=resolved_dir,
            root_admin_token=admin_token,
            owner_token=owner_token,
            admin_token=role_tokens["admin"],
            member_token=role_tokens["member"],
            viewer_token=role_tokens["viewer"],
            other_org_token=other_org_token,
            proj_a=proj_a,
            proj_b=proj_b,
            other_proj=other_proj,
            client=client,
            mock_llm=mock_llm,
            mock_webhook=mock_webhook,
            mock_llm_url=mock_llm.url if mock_llm is not None else None,
            mock_webhook_url=mock_webhook.url if mock_webhook is not None else None,
            node_manager=node_manager,
            run_id=rid,
        )
