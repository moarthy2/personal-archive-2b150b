"""پوشش روی git خام برای برنچ bus.

تمام ترافیک داده فقط با clone/fetch/push روی github.com انجام می‌شود (نه api.github.com).

طراحی کلیدی: هر فایل فقط «یک مالک» دارد (فرمان‌ها = لوکال، داده‌ها = Listener)، پس به‌جای
merge/rebase از «بازپخش (replay)» استفاده می‌کنیم: عملیات نوشتن در حافظه می‌ماند، اگر push رد شد
کپی تازه‌ی ریموت گرفته می‌شود و همان عملیات دوباره روی آن اعمال می‌شود. این روش با بازنویسی
تاریخچه (squash / purge) هم سازگار است.
"""
from __future__ import annotations

import base64
import os
import shutil
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


class BusError(Exception):
    pass


class BusNetworkError(BusError):
    """خطای شبکه/موقت؛ عملیات دوباره امتحان می‌شود."""


class BusRejectedError(BusError):
    """ریموت محتوا را رد کرد (مثلاً secret-scanning)؛ تکرار فایده ندارد."""


@dataclass
class Op:
    kind: str                      # w=نوشتن  a=الحاق  d=حذف  c=کپی از فایل
    path: str
    data: bytes | None = None
    src: str | None = None
    cleanup: bool = False          # پس از موفقیت src پاک شود


_NET_PATTERNS = (
    "could not resolve host", "unable to access", "connection", "timed out", "timeout",
    "failed to connect", "ssl", "early eof", "rpc failed", "remote end hung up",
    "operation too slow", "http 5", "error: 5", "empty reply", "could not read from remote",
    "unexpected disconnect", "transfer closed", "gnutls", "tls", "network is unreachable",
)


def make_env(token: str | None) -> dict:
    env = os.environ.copy()
    cfgs = [
        ("http.postBuffer", "524288000"),
        ("http.lowSpeedLimit", "1000"),
        ("http.lowSpeedTime", "60"),
        ("commit.gpgsign", "false"),
        ("core.autocrlf", "false"),
        ("advice.detachedHead", "false"),
        ("gc.auto", "200"),
    ]
    if token:
        cred = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        cfgs.append(("http.extraheader", f"AUTHORIZATION: basic {cred}"))
    env["GIT_CONFIG_COUNT"] = str(len(cfgs))
    for i, (k, v) in enumerate(cfgs):
        env[f"GIT_CONFIG_KEY_{i}"] = k
        env[f"GIT_CONFIG_VALUE_{i}"] = v
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_AUTHOR_NAME"] = env["GIT_COMMITTER_NAME"] = "sync"
    env["GIT_AUTHOR_EMAIL"] = env["GIT_COMMITTER_EMAIL"] = "sync@localhost"
    return env


def run_git(args: list[str], cwd: str | Path, env: dict, timeout: int = 300, check: bool = True):
    try:
        r = subprocess.run(
            ["git", *args], cwd=str(cwd), env=env, capture_output=True, text=True,
            timeout=timeout, encoding="utf-8", errors="replace",
        )
    except FileNotFoundError as e:
        raise BusError("git نصب نیست (روی Termux: pkg install git)") from e
    except subprocess.TimeoutExpired as e:
        raise BusNetworkError(f"git {args[0]} زمان‌بر شد") from e
    if check and r.returncode != 0:
        raise classify_error(args[0], r)
    return r


def classify_error(cmd: str, r) -> BusError:
    err = (r.stderr or r.stdout or "").strip()
    low = err.lower()
    if "[remote rejected]" in low or "gh013" in low or "push declined" in low:
        return BusRejectedError(f"git {cmd}: {err[-400:]}")
    if any(p in low for p in _NET_PATTERNS):
        return BusNetworkError(f"git {cmd}: {err[-300:]}")
    return BusError(f"git {cmd}: {err[-400:]}")


class GitBus:
    def __init__(self, workdir: str | Path, remote_url: str, token: str | None = None,
                 branch: str = "bus", log: Callable[[str], None] | None = None):
        self.dir = Path(workdir)
        self.url = remote_url
        self.branch = branch
        self.env = make_env(token)
        self.log = log or (lambda m: None)
        self.lock = threading.RLock()        # برای خواندن/نوشتن درخت کاری
        self._oplock = threading.RLock()     # عملیات git (sync/flush/squash) هیچ‌وقت هم‌زمان اجرا نمی‌شوند
        self._plock = threading.Lock()
        self._pending: list[Op] = []

    # -- اجرای git ---------------------------------------------------------------
    def _git(self, *args: str, check: bool = True, timeout: int = 300):
        return run_git(list(args), self.dir, self.env, timeout, check)

    def _head(self) -> str | None:
        r = self._git("rev-parse", "-q", "--verify", "HEAD", check=False)
        return r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None

    # -- راه‌اندازی ---------------------------------------------------------------
    def init(self) -> bool:
        """کلون خالی را می‌سازد و اگر برنچ ریموت هست همگام می‌کند. True اگر برنچ ریموت وجود دارد."""
        self.dir.mkdir(parents=True, exist_ok=True)
        if not (self.dir / ".git").exists():
            self._git("init", "-q")
            self._git("symbolic-ref", "HEAD", f"refs/heads/{self.branch}")
            self._git("remote", "add", "origin", self.url)
        else:
            self._git("remote", "set-url", "origin", self.url)
        self.sync()
        return self._head() is not None

    # -- همگام‌سازی -----------------------------------------------------------------
    def _fetch(self) -> str | None:
        b = self.branch
        r = self._git("fetch", "-q", "--depth=1", "--no-tags", "origin",
                      f"+refs/heads/{b}:refs/remotes/origin/{b}", check=False, timeout=600)
        if r.returncode != 0:
            if "couldn't find remote ref" in (r.stderr or ""):
                return None
            raise classify_error("fetch", r)
        r2 = self._git("rev-parse", "-q", "--verify", f"refs/remotes/origin/{b}", check=False)
        return r2.stdout.strip() or None

    def sync(self) -> bool:
        """آخرین وضعیت ریموت را می‌گیرد. True اگر درخت کاری تغییر کرد."""
        with self._oplock:
            sha = self._fetch()
            if not sha or sha == self._head():
                return False
            with self.lock:
                self._git("reset", "-q", "--hard", sha)
                self._git("clean", "-fdq")
            return True

    # -- خواندن ----------------------------------------------------------------------
    def abs(self, rel: str) -> Path:
        p = (self.dir / rel).resolve()
        if self.dir.resolve() not in p.parents and p != self.dir.resolve():
            raise BusError(f"مسیر نامعتبر: {rel}")
        return p

    def read(self, rel: str) -> bytes | None:
        with self.lock:
            try:
                return self.abs(rel).read_bytes()
            except (FileNotFoundError, IsADirectoryError):
                return None

    def exists(self, rel: str) -> bool:
        return self.abs(rel).exists()

    def listdir(self, rel: str) -> list[str]:
        with self.lock:
            p = self.abs(rel)
            return sorted(x.name for x in p.iterdir()) if p.is_dir() else []

    # -- نوشتن (صف عملیات) ---------------------------------------------------------------
    def stage(self, kind: str, path: str, data: bytes | None = None, src: str | None = None,
              cleanup: bool = False) -> None:
        self.abs(path)  # اعتبارسنجی مسیر
        with self._plock:
            self._pending.append(Op(kind, path, data, src, cleanup))

    def pending_count(self) -> int:
        with self._plock:
            return len(self._pending)

    def _requeue(self, ops: list[Op]) -> None:
        with self._plock:
            self._pending = ops + self._pending

    def _apply(self, ops: list[Op]) -> None:
        for op in ops:
            p = self.abs(op.path)
            if op.kind == "w":
                p.parent.mkdir(parents=True, exist_ok=True)
                tmp = p.with_name(p.name + ".tmp~")
                tmp.write_bytes(op.data or b"")
                os.replace(tmp, p)
            elif op.kind == "a":
                p.parent.mkdir(parents=True, exist_ok=True)
                with open(p, "ab") as f:
                    f.write(op.data or b"")
            elif op.kind == "c":
                p.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(op.src, p)
            elif op.kind == "d":
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                elif p.exists():
                    p.unlink()

    def _cleanup(self, ops: list[Op]) -> None:
        for op in ops:
            if op.cleanup and op.src:
                try:
                    os.unlink(op.src)
                except OSError:
                    pass

    def _commit(self, message: str) -> bool:
        self._git("add", "-A")
        if self._git("diff", "--cached", "--quiet", check=False).returncode == 0:
            return False
        self._git("commit", "-q", "-m", message)
        return True

    def _undo(self, base: str | None) -> None:
        with self.lock:
            if base:
                self._git("reset", "-q", "--hard", base, check=False)
            else:
                self._git("update-ref", "-d", f"refs/heads/{self.branch}", check=False)
                self._git("read-tree", "--empty", check=False)
            self._git("clean", "-fdq", check=False)

    def _push(self) -> str:
        r = self._git("push", "-q", "origin", f"HEAD:refs/heads/{self.branch}", check=False, timeout=900)
        if r.returncode == 0:
            return "ok"
        low = (r.stderr or "").lower()
        # رقابت هم‌زمان روی ref (روی GitHub هم رخ می‌دهد): موقت است و مثل non-ff دوباره بازپخش می‌شود
        if "cannot lock ref" in low or "failed to update ref" in low or "reference already exists" in low:
            return "nonff"
        if "[remote rejected]" not in low and ("[rejected]" in low or "non-fast-forward" in low
                                               or "fetch first" in low or "stale info" in low):
            return "nonff"
        raise classify_error("push", r)

    def flush(self, message: str = "sync", max_attempts: int = 5) -> bool:
        """عملیات صف‌شده را commit و push می‌کند. True اگر چیزی push شد."""
        with self._oplock:
            return self._flush(message, max_attempts)

    def _flush(self, message: str, max_attempts: int) -> bool:
        with self._plock:
            ops, self._pending = self._pending, []
        if not ops:
            return False
        try:
            for attempt in range(max_attempts):
                if attempt:
                    self.sync()
                with self.lock:
                    base = self._head()
                    self._apply(ops)
                    committed = self._commit(message)
                if not committed:
                    self._cleanup(ops)
                    return False
                try:
                    res = self._push()
                except BaseException:
                    self._undo(base)
                    raise
                if res == "ok":
                    self._cleanup(ops)
                    return True
                self._undo(base)
            raise BusNetworkError("push مدام رد می‌شود (تداخل با نویسنده‌ی دیگر)")
        except BusRejectedError:
            self._cleanup(ops)      # دسته حذف می‌شود؛ تکرارش بی‌فایده است
            raise
        except BusError:
            self._requeue(ops)
            raise

    def squash(self, extra_ops: list[Op] | None = None, message: str = "compact") -> None:
        """تاریخچه را به یک commit یتیم (orphan) تبدیل و force-push می‌کند."""
        with self._oplock:
            self._squash(extra_ops, message)

    def _squash(self, extra_ops: list[Op] | None, message: str) -> None:
        with self._plock:
            pend, self._pending = self._pending, []
        ops = pend + list(extra_ops or [])
        old = None
        try:
            with self.lock:
                old = self._head()
                self._apply(ops)
                self._git("add", "-A")
                tree = self._git("write-tree").stdout.strip()
                commit = self._git("commit-tree", tree, "-m", message).stdout.strip()
                self._git("update-ref", f"refs/heads/{self.branch}", commit)
            r = self._git("push", "-q", "--force", "origin", f"{commit}:refs/heads/{self.branch}",
                          check=False, timeout=900)
            if r.returncode != 0:
                with self.lock:
                    if old:
                        self._git("update-ref", f"refs/heads/{self.branch}", old, check=False)
                        self._git("reset", "-q", "--hard", old, check=False)
                raise classify_error("push --force", r)
            self._cleanup(ops)
            self._git("reflog", "expire", "--expire=now", "--all", check=False)
            self._git("gc", "-q", "--prune=now", check=False, timeout=600)
        except BusError:
            self._requeue(pend)
            raise
