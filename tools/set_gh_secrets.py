#!/usr/bin/env python3
"""
Upload Aegis runtime credentials as ENCRYPTED GitHub Actions repository
secrets (libsodium sealed box) — secrets never touch source code and are
only readable by GitHub's Actions runtime.

Usage: python tools/set_gh_secrets.py
Reads ./secrets.env (gitignored) and merges with os.environ (env wins).
"""

from __future__ import annotations

import base64
import json
import os
import pathlib
import sys
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]

SECRETS = [
    "BYBIT_API_KEY", "BYBIT_API_SECRET", "BYBIT_TESTNET", "BYBIT_SYMBOL",
    "RESEND_API_KEY", "RECEIVER_EMAIL", "ALERT_FROM_EMAIL",
]


def api(method: str, url: str, token: str, body: dict | None = None) -> tuple[int, bytes]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "aegis-bootstrap/1.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def main() -> None:
    import nacl.public  # pynacl

    env: dict[str, str] = {}
    for candidate in (ROOT / "secrets.env", ROOT / ".env"):
        if candidate.exists():
            for line in candidate.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")

    token = os.environ.get("GITHUB_TOKEN") or env["GITHUB_TOKEN"]
    if not token:
        sys.exit("GITHUB_TOKEN missing")

    status, body = api("GET", "https://api.github.com/repos/abdullah2025jalal/aegis-universal-bot/actions/secrets/public-key", token)
    if status != 200:
        sys.exit(f"public key lookup failed: HTTP {status} {body.decode()}")
    pk = json.loads(body)
    sealed_box = nacl.public.SealedBox(
        nacl.public.PublicKey(base64.b64decode(pk["key"])))

    for name in SECRETS:
        value = os.environ.get(name) or env.get(name, "")
        if not value:
            print(f"skip {name} (empty)")
            continue
        encrypted = base64.b64encode(sealed_box.encrypt(value.encode("utf-8"))).decode("utf-8")
        status, resp = api("PUT",
                           f"https://api.github.com/repos/abdullah2025jalal/aegis-universal-bot/actions/secrets/{name}",
                           token, {"encrypted_value": encrypted, "key_id": pk["key_id"]})
        print(f"{name}: HTTP {status}" + ("" if status in (201, 204) else f" {resp.decode()[:200]}"))


if __name__ == "__main__":
    main()
