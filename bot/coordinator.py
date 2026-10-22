"""Coordinator (هر ۵ دقیقه): شبکه‌ی ایمنی برای زنده نگه داشتن زنجیره‌ی Listener.

- اگر Listener در حال اجرا (یا در صف) است → کاری نمی‌کند
- اگر اجرا نیست و heartbeat سرور لوکال تازه است → Listener را دوباره dispatch می‌کند
- اگر heartbeat لوکال کهنه است (کاربر پروژه را بسته) یا کاربر «قطع» را زده یا session باطل است → کاری نمی‌کند
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

from common import ghapi
from common import protocol as P
from common.gitbus import BusError, GitBus


def log(msg: str) -> None:
    print(msg, flush=True)


def local_is_alive(bus: GitBus, timeout: float) -> tuple[bool, str]:
    hb = P.loads(bus.read(P.LOCAL_HB), None)
    if not isinstance(hb, dict):
        return False, "heartbeat لوکال وجود ندارد"
    if hb.get("active") is False:
        return False, "کاربر اتصال را قطع کرده"
    age = P.now() - float(hb.get("ts", 0))
    if age > timeout:
        return False, f"heartbeat لوکال کهنه است ({int(age)} ثانیه)"
    return True, "ok"


def session_blocked(bus: GitBus) -> bool:
    status = P.loads(bus.read(P.STATUS), {}) or {}
    if status.get("state") != "session_invalid":
        return False
    refreshed = P.loads(bus.read(P.SESSION_REFRESHED), {}) or {}
    return float(refreshed.get("ts", 0)) <= float(status.get("ts", 0))


def main() -> int:
    repo, token = os.environ.get("REPO", ""), os.environ.get("GH_TOKEN", "")
    ref = os.environ.get("REF_NAME", "main")
    if not repo or not token:
        log("REPO / GH_TOKEN تنظیم نشده")
        return 1

    tmp = Path(tempfile.mkdtemp(prefix="coord-"))
    try:
        bus = GitBus(tmp / "bus", f"https://github.com/{repo}.git", token)
        try:
            if not bus.init():
                log("برنچ bus وجود ندارد؛ کاری نیست.")
                return 0
        except BusError as e:
            log(f"خواندن bus ناموفق: {str(e)[:150]}")
            return 1

        cfg = P.merge_bus_config(P.loads(bus.read(P.CONFIG), {}))
        alive, why = local_is_alive(bus, float(cfg["local_timeout"]) + 60)
        if not alive:
            log(f"نیازی به راه‌اندازی نیست: {why}")
            return 0
        if session_blocked(bus):
            log("session باطل است؛ تا آپلود session تازه راه‌اندازی نمی‌شود.")
            return 0

        runs = ghapi.active_runs(repo, token)
        if runs:
            log(f"Listener فعال است (run {runs[0].get('id')}).")
            return 0

        log("Listener اجرا نیست ولی لوکال زنده است → dispatch")
        if ghapi.dispatch_listener(repo, token, ref):
            log("dispatch شد.")
            return 0
        log("dispatch ناموفق بود.")
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
