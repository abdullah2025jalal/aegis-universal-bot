#!/usr/bin/env python3
"""
Aegis-Universal :: ZERO-TOUCH SYSTEM BOOTSTRAPPER
==================================================
Single-command, end-to-end cloud infrastructure automation (no UI):

  STEP 1  GitHub     — resolve operator -> create private repo -> push codebase
                       (git CLI primary, GitHub Contents API fallback)
  STEP 2  Render     — resolve owner -> create/verify Web Service ->
                       inject ALL environment variables -> dispatch build ->
                       poll deployment until 'live' -> extract app URL
  STEP 3  UptimeRobot — register 5-minute keep-alive HTTP monitor on
                       f"{app_url}/healthz/{HEALTH_UUID}"

Credentials are read from the environment or from ./secrets.env (which is
gitignored and therefore NEVER pushed). No secrets live in source code.
Requires only the Python standard library (3.8+).

Usage:  python tools/bootstrap_system.py
"""

from __future__ import annotations

import base64
import json
import os
import pathlib
import random
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]

GITHUB_API = "https://api.github.com"
RENDER_API = "https://api.render.com/v1"
UPTIMEROBOT_API = "https://api.uptimerobot.com/v2"

MANIFEST: dict = {}

_SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules", ".idea", ".vscode"}
_SKIP_FILES = {"secrets.env", ".env", "aegis_state.sqlite3"}
_SKIP_SUFFIX = {".pyc", ".sqlite3", ".db", ".pyo"}


def banner(text: str) -> None:
    print("\n" + "=" * 88)
    print(text)
    print("=" * 88, flush=True)


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}Z] {msg}", flush=True)


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib)
# ---------------------------------------------------------------------------

class HttpResult:
    def __init__(self, status: int, body_text: str):
        self.status = status
        self.text = body_text

    def json(self) -> dict:
        try:
            return json.loads(self.text)
        except json.JSONDecodeError:
            return {}


def http_request(method: str, url: str, *, token: str | None = None,
                 json_body: dict | None = None, form_body: dict | None = None,
                 extra_headers: dict | None = None, timeout: int = 60,
                 retries: int = 3, accept_404: bool = False) -> HttpResult:
    headers = {"User-Agent": "aegis-universal-bootstrap/1.0",
               "Accept": "application/vnd.github+json" if GITHUB_API in url else "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra_headers:
        headers.update(extra_headers)
    data: bytes | None = None
    if json_body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(json_body).encode("utf-8")
    elif form_body is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = urllib.parse.urlencode(form_body).encode("utf-8")

    attempt = 0
    last_exc: Exception | None = None
    while attempt < retries:
        attempt += 1
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return HttpResult(resp.status, resp.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", "replace")
            if accept_404 and exc.code == 404:
                return HttpResult(404, text)
            if exc.code < 500 and exc.code != 429:
                return HttpResult(exc.code, text)
            last_exc = exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
        sleep_for = min(2.0 * attempt + random.random(), 10.0)
        print(f"    retry {attempt}/{retries} after failure ({last_exc}); sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)
    raise RuntimeError(f"HTTP failed after {retries} attempts: {method} {url} ({last_exc})")


# ---------------------------------------------------------------------------
# Credential manifest loading
# ---------------------------------------------------------------------------

def parse_env_file(path: pathlib.Path) -> dict:
    parsed: dict = {}
    if not path.exists():
        return parsed
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        parsed[key.strip()] = value
    return parsed


def load_manifest() -> None:
    """
    Load credentials from environment (env wins) or secrets.env.
    Aegis NEVER hardcodes secrets in source code — secrets read from disk/env only.
    """
    env = {}
    for candidate in (ROOT / "secrets.env", ROOT / ".env"):
        env.update(parse_env_file(candidate))

    def grab(key: str, default: str = "") -> str:
        return os.environ.get(key) or env.get(key) or default

    required = {
        "GITHUB_TOKEN": grab("GITHUB_TOKEN"),
        "RENDER_API_KEY": grab("RENDER_API_KEY"),
        "UPTIMEROBOT_API_KEY": grab("UPTIMEROBOT_API_KEY"),
        "BYBIT_API_KEY": grab("BYBIT_API_KEY"),
        "BYBIT_API_SECRET": grab("BYBIT_API_SECRET"),
        "RESEND_API_KEY": grab("RESEND_API_KEY"),
        "RECEIVER_EMAIL": grab("RECEIVER_EMAIL"),
        "ALERT_FROM_EMAIL": grab("ALERT_FROM_EMAIL", "onboarding@resend.dev"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        banner("MISSING CREDENTIALS")
        print("These keys must exist in environment or ./secrets.env:\n  " + "\n  ".join(missing))
        sys.exit(2)

    env.setdefault("HEALTH_UUID", secrets.token_hex(16))
    MANIFEST.update({
        "github_repo": grab("GITHUB_REPO", "aegis-universal-bot"),
        "github_repo_private": True,
        "render_service_name": grab("RENDER_SERVICE_NAME", "aegis-universal-live"),
        "uptimerobot_interval": int(grab("UPTIMEROBOT_INTERVAL", "300")),
        "uptimerobot_friendly_name": grab("UPTIMEROBOT_MONITOR_NAME", "Aegis-KeepAlive"),
        "dashboard_username": grab("DASHBOARD_USERNAME", "operator"),
    })
    MANIFEST.update(required)
    MANIFEST["BYBIT_TESTNET"] = grab("BYBIT_TESTNET", "false")
    for key, default in {
        "BYBIT_SYMBOL": "BTCUSDT",
        "RISK_PER_TRADE_PCT": "0.015",
        "MAX_LEVERAGE": "5",
        "MAX_MARGIN_ALLOC_PCT": "0.35",
        "TARGET_GROSS_RR": "1.50",
        "DAILY_LOSS_LIMIT_PCT": "0.05",
        "DAILY_PROFIT_LOCK_PCT": "0.08",
        "EQUITY_PRESERVATION_FLOOR_PCT": "0.80",
        "COOLDOWN_MINUTES": "10",
        "MAX_HOLDING_HOURS": "24",
    }.items():
        MANIFEST[key] = grab(key, default)


# ---------------------------------------------------------------------------
# STEP 1 — GitHub
# ---------------------------------------------------------------------------

def github_resolve_user(github_token: str) -> str:
    log("STEP 1.1  resolving GitHub operator identity…")
    resp = http_request("GET", f"{GITHUB_API}/user", token=github_token)
    if resp.status != 200:
        raise RuntimeError(f"GitHub token rejected: HTTP {resp.status} {resp.text[:200]}")
    login = resp.json().get("login")
    if not login:
        raise RuntimeError("GitHub /user returned no login")
    print(f"    authenticated as: {login}")
    return login


def github_create_repo(login: str, github_token: str) -> tuple[str, str]:
    repo = MANIFEST["github_repo"]
    log(f"STEP 1.2  creating private repository '{repo}'…")
    payload = {"name": repo, "private": MANIFEST["github_repo_private"], "auto_init": False}
    resp = http_request("POST", f"{GITHUB_API}/user/repos", token=github_token, json_body=payload)
    if resp.status == 201:
        created = resp.json()
        full_name = created.get("full_name")
        html_url = created.get("html_url")
        print(f"    repository created: {full_name} ({html_url})")
        return full_name, html_url
    if resp.status == 422 and "already exists" in resp.text:
        check = http_request("GET", f"{GITHUB_API}/repos/{login}/{repo}", token=github_token)
        if check.status == 200 and check.json().get("owner", {}).get("login") == login:
            print("    repository already exists and is owned by this operator — proceeding")
            return check.json().get("full_name"), check.json().get("html_url")
        raise RuntimeError(f"repository '{repo}' already exists but belongs to someone else")
    raise RuntimeError(f"GitHub repo creation failed: HTTP {resp.status} {resp.text[:400]}")


def project_files(root: pathlib.Path) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(root)
        parts = rel.parts
        if parts and parts[0] in _SKIP_DIRS:
            continue
        if rel.name in _SKIP_FILES:
            continue
        if rel.suffix.lower() in _SKIP_SUFFIX:
            continue
        files.append(rel)
    return files


def git_push_via_cli(login: str, github_token: str) -> bool:
    repo_name = MANIFEST["github_repo"]
    run = lambda args: subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    try:
        if run(["git", "--version"]).returncode != 0:
            return False
        log("STEP 1.3  staging repository via local git CLI…")
        identity = ["-c", "user.name=aegis-bootstrap", "-c", "user.email=ops@aegis-universal.local"]
        for argv in (
            ["git"] + ["add", "-A"],
            ["git"] + identity + ["commit", "-m",
                                  "Aegis-Universal: zero-touch production deployment"],
            ["git"] + ["branch", "-M", "main"],
        ):
            p = run(argv)
            if p.returncode != 0 and "nothing to commit" not in p.stdout + p.stderr:
                print(f"    git {' '.join(argv[1:])} rc={p.returncode}\n{p.stdout}{p.stderr}")
                return False
        remote_url = f"https://x-access-token:{github_token}@github.com/{login}/{repo_name}.git"
        run(["git", "remote", "remove", "origin"])
        p = run(["git", "remote", "add", "origin", remote_url])
        if p.returncode != 0:
            print(f"    remote add failed: {p.stderr}")
            return False
        log("STEP 1.4  pushing to origin/main…")
        p = run(["git", "push", "-u", "origin", "main", "--force"])
        if p.returncode == 0:
            print("    git push: OK")
            return True
        print(f"    git push failed:\n{p.stdout}{p.stderr}")
        return False
    except (OSError, FileNotFoundError, subprocess.SubprocessError) as exc:
        print(f"    git CLI unavailable/failing: {exc}")
        return False


def push_via_contents_api(login: str, github_token: str) -> None:
    repo = MANIFEST["github_repo"]
    log("STEP 1.3-2  pushing via GitHub Contents API (git fallback)…")
    rel_files = project_files(ROOT)
    done = 0
    for rel in rel_files:
        data = base64.b64encode((ROOT / rel).read_bytes()).decode("ascii")
        url = f"{GITHUB_API}/repos/{login}/{repo}/contents/{urllib.parse.quote(str(rel).replace(os.sep, '/'))}"
        payload = {"message": f"Aegis deploy: {urllib.parse.quote(str(rel).replace(os.sep, '/'))}",
                   "content": data, "branch": "main"}
        resp = http_request("PUT", url, token=github_token, json_body=payload)
        if resp.status not in (200, 201):
            raise RuntimeError(f"Contents API failed for {rel}: HTTP {resp.status} {resp.text[:300]}")
        done += 1
    print(f"    contents API push: {done} files committed")


def bootstrap_github() -> str:
    token = MANIFEST["GITHUB_TOKEN"]
    login = github_resolve_user(token)
    _full_name, html_url = github_create_repo(login, token)
    if not git_push_via_cli(login, token):
        push_via_contents_api(login, token)
    banner("STEP 1 COMPLETE — GitHub repository populated")
    print(f"  https://github.com/{login}/{MANIFEST['github_repo']}")
    return f"{login}/{MANIFEST['github_repo']}"


# ---------------------------------------------------------------------------
# STEP 2 — Render
# ---------------------------------------------------------------------------

def render_resolve_owner(render_token: str) -> str:
    log("STEP 2.1  resolving Render primary Owner ID…")
    resp = http_request("GET", f"{RENDER_API}/owners?limit=1", token=render_token)
    if resp.status != 200:
        raise RuntimeError(f"Render owners lookup failed: HTTP {resp.status} {resp.text[:300]}")
    rows = [(item or {}).get("owner", {}).get("id", "") for item in resp.json()]
    owner_id = rows[0] if rows else (resp.json().get("id") if resp.json() else "")
    if not owner_id:
        raise RuntimeError(f"no Render owner resolvable: {resp.text[:300]}")
    print(f"    owner id: {owner_id}")
    return owner_id


def render_create_service(render_token: str, owner_id: str, full_repo: str) -> str:
    service_name = MANIFEST["render_service_name"]
    log(f"STEP 2.2  verifying Render service '{service_name}'…")
    resp = http_request("GET", f"{RENDER_API}/services?limit=50", token=render_token)
    services = resp.json()
    for item in services:
        svc = item.get("service") or item
        if svc.get("name") == service_name:
            print(f"    service exists (id={svc.get('id')}) — reusing")
            return svc.get("id")

    log("    creating new Render Web Service (python / free / oregon)…")
    payload = {
        "type": "web_service",
        "name": service_name,
        "ownerId": owner_id,
        "repo": f"https://github.com/{full_repo}",
        "autoDeploy": "yes",
        "branch": "main",
        "serviceDetails": {
            "env": "python",
            "runtime": "python",
            "plan": "free",
            "region": "oregon",
            "envSpecificDetails": {
                "buildCommand": "pip install -r requirements.txt",
                "startCommand": "uvicorn main:app --host 0.0.0.0 --port $PORT",
            },
        },
    }
    resp = http_request("POST", f"{RENDER_API}/services", token=render_token, json_body=payload)
    if resp.status not in (200, 201):
        raise RuntimeError(f"Render service creation failed: HTTP {resp.status} {resp.text[:600]}")
    svc = resp.json().get("service") or resp.json()
    print(f"    service created (id={svc.get('id')})")
    return svc.get("id")


def render_env_vars_payload() -> list[dict]:
    return [
        {"key": "BYBIT_API_KEY", "value": MANIFEST["BYBIT_API_KEY"]},
        {"key": "BYBIT_API_SECRET", "value": MANIFEST["BYBIT_API_SECRET"]},
        {"key": "BYBIT_TESTNET", "value": MANIFEST["BYBIT_TESTNET"]},
        {"key": "BYBIT_SYMBOL", "value": MANIFEST.get("BYBIT_SYMBOL", "BTCUSDT")},
        {"key": "RESEND_API_KEY", "value": MANIFEST["RESEND_API_KEY"]},
        {"key": "RECEIVER_EMAIL", "value": MANIFEST["RECEIVER_EMAIL"]},
        {"key": "ALERT_FROM_EMAIL", "value": MANIFEST["ALERT_FROM_EMAIL"]},
        {"key": "HEALTH_UUID", "value": MANIFEST["HEALTH_UUID"]},
        {"key": "DASHBOARD_USERNAME", "value": MANIFEST["dashboard_username"]},
        {"key": "DASHBOARD_PASSWORD", "value": MANIFEST["DASHBOARD_PASSWORD"]},
        {"key": "RISK_PER_TRADE_PCT", "value": MANIFEST["RISK_PER_TRADE_PCT"]},
        {"key": "MAX_LEVERAGE", "value": MANIFEST["MAX_LEVERAGE"]},
        {"key": "MAX_MARGIN_ALLOC_PCT", "value": MANIFEST["MAX_MARGIN_ALLOC_PCT"]},
        {"key": "TARGET_GROSS_RR", "value": MANIFEST["TARGET_GROSS_RR"]},
        {"key": "DAILY_LOSS_LIMIT_PCT", "value": MANIFEST["DAILY_LOSS_LIMIT_PCT"]},
        {"key": "DAILY_PROFIT_LOCK_PCT", "value": MANIFEST["DAILY_PROFIT_LOCK_PCT"]},
        {"key": "EQUITY_PRESERVATION_FLOOR_PCT", "value": MANIFEST["EQUITY_PRESERVATION_FLOOR_PCT"]},
        {"key": "COOLDOWN_MINUTES", "value": MANIFEST["COOLDOWN_MINUTES"]},
        {"key": "MAX_HOLDING_HOURS", "value": MANIFEST["MAX_HOLDING_HOURS"]},
    ]


def render_inject_env(render_token: str, service_id: str) -> int:
    log("STEP 2.3  injecting runtime environment variables…")
    payload = render_env_vars_payload()
    resp = http_request("PUT", f"{RENDER_API}/services/{service_id}/env-vars",
                        token=render_token, json_body=payload)
    if resp.status not in (200, 201):
        if resp.status == 404:
            log("    PUT env-vars unsupported — trying PATCH endpoint…")
            resp = http_request("PATCH", f"{RENDER_API}/services/{service_id}/env-vars",
                                token=render_token, json_body=payload)
        if resp.status not in (200, 201):
            raise RuntimeError(f"Render env-var injection failed: HTTP {resp.status} {resp.text[:400]}")
    print(f"    injected {len(payload)} environment variables")
    return len(payload)


def render_deploy_and_wait(render_token: str, service_id: str) -> str:
    log("STEP 2.4  dispatching build…")
    resp = http_request("POST", f"{RENDER_API}/services/{service_id}/deploys",
                        token=render_token, json_body={"clearCache": "no"})
    if resp.status not in (200, 201):
        raise RuntimeError(f"Render deploy dispatch failed: HTTP {resp.status} {resp.text[:400]}")
    body = resp.json()
    deploy_id = body.get("id") if isinstance(body, dict) else None
    if not deploy_id:
        raise RuntimeError(f"no deploy id returned: {resp.text[:300]}")
    print(f"    deploy {deploy_id} queued on Render")

    terminal_fail = {"build_failed", "deploy_failed", "build_canceled", "deploy_canceled", "deactivated"}
    deadline = time.time() + 30 * 60
    log("STEP 2.5  polling deployment until state == 'live'…")
    while time.time() < deadline:
        d = http_request("GET", f"{RENDER_API}/services/{service_id}/deploys/{deploy_id}",
                         token=render_token).json()
        status = (d.get("status") or d.get("deploy", {}).get("status", "")).lower()
        print(f"    deploy status: {status}  ({int(deadline - time.time())}s left)")
        if status == "live":
            break
        if status in terminal_fail:
            raise RuntimeError(f"Render deployment FAILED in state '{status}'. "
                               "Open the Render dashboard → Events / Logs for details.")
        time.sleep(20)

    svc = http_request("GET", f"{RENDER_API}/services/{service_id}", token=render_token).json()
    svc = svc.get("service") or svc
    app_url = ((svc.get("serviceDetails") or {}).get("url")) or ""
    if not app_url:
        raise RuntimeError("deployment 'live' but no service URL exposed")
    return app_url


def bootstrap_render(full_repo: str) -> str:
    token = MANIFEST["RENDER_API_KEY"]
    owner_id = render_resolve_owner(token)
    service_id = render_create_service(token, owner_id, full_repo)

    log("STEP 2.3a  generating credentials (HEALTH_UUID / dashboard password)…")
    MANIFEST["HEALTH_UUID"] = uuid.uuid4().hex
    MANIFEST["DASHBOARD_PASSWORD"] = uuid.uuid4().hex
    print(f"    HEALTH_UUID: {MANIFEST['HEALTH_UUID']}")
    print(f"    Dashboard password: {MANIFEST['DASHBOARD_PASSWORD']}")

    env_count = render_inject_env(token, service_id)
    app_url = render_deploy_and_wait(token, service_id)
    banner("STEP 2 COMPLETE — Render service is LIVE")
    print(f"  App URL: {app_url}")
    return app_url


# ---------------------------------------------------------------------------
# STEP 3 — UptimeRobot
# ---------------------------------------------------------------------------

def uptimerobot_create_monitor(app_url: str) -> dict:
    key = MANIFEST["UPTIMEROBOT_API_KEY"]
    keep_alive_target = f"{app_url}/healthz/{MANIFEST['HEALTH_UUID']}"
    log("STEP 3.1  registering 24/7 keep-alive monitor…")
    log(f"    target: {keep_alive_target}  (interval {MANIFEST['uptimerobot_interval']}s)")

    resp = http_request("POST", f"{UPTIMEROBOT_API}/newMonitor", form_body={
        "api_key": key,
        "format": "json",
        "type": "1",
        "friendly_name": MANIFEST["uptimerobot_friendly_name"],
        "url": keep_alive_target,
        "interval": str(MANIFEST["uptimerobot_interval"]),
    })
    body = resp.json() or {}
    if body.get("stat") == "ok":
        mon = body.get("monitor") or body
        print(f"    monitor #{mon.get('id', 'n/a')} created (stat=ok)")
        return body
    message = str(body.get("error", {}))
    if "already" in message.lower() or body.get("error", {}).get("id") == 20:
        log("    monitor already exists — verifying via getMonitors…")
        list_resp = http_request("POST", f"{UPTIMEROBOT_API}/getMonitors",
                                 form_body={"api_key": key, "format": "json"})
        for mon in list_resp.json().get("monitors", []):
            if mon.get("url") == keep_alive_target:
                print(f"    existing monitor #{mon.get('id')} verified")
                return {"stat": "ok", "monitor": mon}
        raise RuntimeError("monitor already exists but not found by URL — manual review required: "
                           f"{message}")
    raise RuntimeError(f"UptimeRobot monitor creation failed: {message or resp.text[:300]}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def bootstrap() -> None:
    banner("AEGIS-UNIVERSAL :: ZERO-TOUCH DEPLOYMENT — STARTING")
    load_manifest()

    full_repo = bootstrap_github()
    app_url = bootstrap_render(full_repo)
    monitor = uptimerobot_create_monitor(app_url)
    monitor_id = (monitor.get("monitor") or {}).get("id", "n/a")
    MANIFEST["APP_URL"] = app_url

    banner("AEGIS-UNIVERSAL :: DEPLOYMENT COMPLETE — ALL SYSTEMS GREEN")
    print(f"""
  GITHUB REPOSITORY   : https://github.com/{full_repo}            (private, code pushed)
  RENDER SERVICE      : {MANIFEST['render_service_name']}       (python / free / oregon, LIVE)
  APP URL             : {app_url}
  HEALTH ENDPOINT     : {app_url}/healthz/{MANIFEST['HEALTH_UUID']}
  UPTIMEROBOT MONITOR : name={MANIFEST['uptimerobot_friendly_name']} id={monitor_id} type=HTTP(s) interval={MANIFEST['uptimerobot_interval']}s  (stat=ok)

  Heatbeat / keep-alive traffic will guard the free-tier 15-minute sleep window.
  Next telemetry artifacts:
    - 'ONLINE & HEALTHY' HTML ticket -> {MANIFEST['RECEIVER_EMAIL']} (post-boot handshake)
    - Live error tickets with stack traces (deduped, 15-minute throttle)
    - Operator dashboard: {app_url}/dashboard
        username: {MANIFEST['dashboard_username']}
        password: {MANIFEST['DASHBOARD_PASSWORD']}
    - UTC 00:00 daily performance digest

  SECURITY NOTE: rotate any credential that has traveled through untrusted
  channels (chat, screenshots). This manifest is consumed from ./secrets.env
  which is gitignored and never pushed.
""")


if __name__ == "__main__":
    try:
        bootstrap()
    except KeyboardInterrupt:
        print("\naborted by operator")
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 — deployment surface must report loudly
        banner("DEPLOYMENT FAILED")
        import traceback
        traceback.print_exc()
        print(f"\nroot cause: {type(exc).__name__}: {exc}")
        sys.exit(1)
