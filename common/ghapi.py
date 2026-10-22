"""کلاینت مینیمال GitHub REST با کتابخانه‌ی استاندارد (برای بات و Coordinator؛ بدون وابستگی)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request

from . import protocol as P

API = "https://api.github.com"
ACTIVE = {"queued", "in_progress", "waiting", "requested", "pending"}


def api(method: str, path: str, token: str, body: dict | None = None, timeout: int = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "sync-worker",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"message": raw.decode("utf-8", "replace")[:200]}


def dispatch_listener(repo: str, token: str, ref: str = "main") -> bool:
    status, _ = api("POST", f"/repos/{repo}/actions/workflows/{P.WORKFLOW_FILE}/dispatches", token, {"ref": ref})
    return status == 204


def active_runs(repo: str, token: str) -> list[dict]:
    status, body = api("GET", f"/repos/{repo}/actions/workflows/{P.WORKFLOW_FILE}/runs?per_page=10", token)
    if status != 200 or not body:
        raise RuntimeError(f"خواندن runها ناموفق بود (HTTP {status})")
    return [r for r in body.get("workflow_runs", []) if r.get("status") in ACTIVE]
