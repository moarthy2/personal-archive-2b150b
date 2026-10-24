from __future__ import annotations
import base64
import contextlib
import http.client
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

class BusError(Exception):
    pass

class BusNetworkError(BusError):
    pass

class BusRejectedError(BusError):
    pass

@dataclass(slots=True)
class Op:
    kind: str
    path: str
    data: bytes | None = None
    src: str | None = None
    cleanup: bool = False
_NET_PATTERNS = ('could not resolve host', 'unable to access', 'connection', 'timed out', 'timeout', 'failed to connect', 'ssl', 'early eof', 'rpc failed', 'remote end hung up', 'operation too slow', 'http 5', 'error: 5', 'empty reply', 'could not read from remote', 'unexpected disconnect', 'transfer closed', 'gnutls', 'tls', 'network is unreachable')
GITATTRIBUTES_CONTENT = b'* -delta -text\n'
_PROXY_ENV_KEYS = ('HTTPS_PROXY', 'https_proxy', 'HTTP_PROXY', 'http_proxy', 'ALL_PROXY', 'all_proxy')

def make_env(token: str | None, author_name: str='git-sync', author_email: str='noreply@github.com') -> dict:
    env = os.environ.copy()
    cfgs = [('protocol.version', '2'), ('init.defaultBranch', 'main'), ('advice.defaultBranchName', 'false'), ('advice.detachedHead', 'false'), ('status.showUntrackedFiles', 'no'), ('core.preloadIndex', 'true'), ('core.fscache', 'true'), ('http.postBuffer', '16777216'), ('http.lowSpeedLimit', '1000'), ('http.lowSpeedTime', '60'), ('commit.gpgsign', 'false'), ('core.autocrlf', 'false'), ('core.compression', '0'), ('core.bigFileThreshold', '64k'), ('pack.window', '0'), ('pack.depth', '0'), ('gc.auto', '0')]
    if token:
        cred = base64.b64encode(f'x-access-token:{token}'.encode()).decode()
        cfgs.append(('http.extraheader', f'AUTHORIZATION: basic {cred}'))
    env['GIT_CONFIG_COUNT'] = str(len(cfgs))
    for i, (k, v) in enumerate(cfgs):
        env[f'GIT_CONFIG_KEY_{i}'] = k
        env[f'GIT_CONFIG_VALUE_{i}'] = v
    env['GIT_TERMINAL_PROMPT'] = '0'
    env['GIT_AUTHOR_NAME'] = env['GIT_COMMITTER_NAME'] = author_name
    env['GIT_AUTHOR_EMAIL'] = env['GIT_COMMITTER_EMAIL'] = author_email
    return env

def run_git(args: list[str], cwd: str | Path, env: dict, timeout: int=300, check: bool=True):
    try:
        r = subprocess.run(['git', *args], cwd=str(cwd), env=env, capture_output=True, text=True, timeout=timeout, encoding='utf-8', errors='replace')
    except FileNotFoundError as e:
        raise BusError('git is not installed (on Termux: pkg install git)') from e
    except subprocess.TimeoutExpired as e:
        raise BusNetworkError(f'git {args[0]} timed out') from e
    if check and r.returncode != 0:
        raise classify_error(args[0], r)
    return r

def classify_error(cmd: str, r) -> BusError:
    err = (r.stderr or r.stdout or '').strip()
    low = err.lower()
    if '[remote rejected]' in low or 'gh013' in low or 'push declined' in low:
        return BusRejectedError(f'git {cmd}: {err[-400:]}')
    if any((p in low for p in _NET_PATTERNS)):
        return BusNetworkError(f'git {cmd}: {err[-300:]}')
    return BusError(f'git {cmd}: {err[-400:]}')

class GitBus:

    def __init__(self, workdir: str | Path, remote_url: str, token: str | None=None, branch: str='bus', log: Callable[[str], None] | None=None):
        self.dir = Path(workdir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._root = self.dir.resolve()
        self.url = remote_url
        self.branch = branch
        self.env = make_env(token)
        self.log = log or (lambda m: None)
        self.lock = threading.RLock()
        self._oplock = threading.RLock()
        self._plock = threading.Lock()
        self._pending: list[Op] = []
        self._head_cache: str | None = None
        self._squash_counter = 0
        self._peek_disable_until = 0.0
        self._ref_re = re.compile(b'([0-9a-f]{40})\\s+refs/heads/' + re.escape(branch.encode('utf-8')) + b'(?:[\\s\\x00]|$)')
        parsed = urlparse(remote_url)
        has_proxy = any((os.environ.get(k) for k in _PROXY_ENV_KEYS))
        self._http_host = parsed.hostname if parsed.scheme == 'https' and (not has_proxy) else None
        self._http_path = parsed.path if parsed.scheme == 'https' and (not has_proxy) else None
        self._http_conn: http.client.HTTPSConnection | None = None
        self._http_auth: str | None = None
        if token:
            cred = base64.b64encode(f'x-access-token:{token}'.encode()).decode()
            self._http_auth = f'Basic {cred}'

    def _git(self, *args: str, check: bool=True, timeout: int=300):
        return run_git(list(args), self._root, self.env, timeout, check)

    def _read_ref_file(self, rel_ref: str) -> str | None:
        ref_file = self._root / '.git' / rel_ref
        try:
            val = ref_file.read_text(encoding='ascii').strip()
            if len(val) == 40 and (not val.startswith('ref:')):
                return val
        except OSError:
            pass
        packed = self._root / '.git' / 'packed-refs'
        try:
            target = f' {rel_ref}'
            for line in packed.read_text(encoding='ascii').splitlines():
                if line.endswith(target) and len(line) >= 40 + len(target):
                    sha = line[:40]
                    if len(sha) == 40:
                        return sha
        except OSError:
            pass
        return None

    def _head(self) -> str | None:
        if self._head_cache:
            return self._head_cache
        sha = self._read_ref_file(f'refs/heads/{self.branch}')
        if sha:
            self._head_cache = sha
            return sha
        r = self._git('rev-parse', '-q', '--verify', 'HEAD', check=False)
        sha = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None
        self._head_cache = sha
        return sha

    def init(self) -> bool:
        self._root.mkdir(parents=True, exist_ok=True)
        if not (self._root / '.git').exists():
            self._git('init', '-q')
            self._git('symbolic-ref', 'HEAD', f'refs/heads/{self.branch}')
            self._git('remote', 'add', 'origin', self.url)
        else:
            self._git('remote', 'set-url', 'origin', self.url)
        info_attr = self._root / '.git' / 'info' / 'attributes'
        with contextlib.suppress(OSError):
            info_attr.parent.mkdir(parents=True, exist_ok=True)
            info_attr.write_bytes(GITATTRIBUTES_CONTENT)
        self._head_cache = None
        self.sync(force_fetch=True)
        return self._head() is not None

    def _open_peek_conn(self) -> http.client.HTTPSConnection:
        conn = http.client.HTTPSConnection(self._http_host, timeout=3.5)
        conn.connect()
        if conn.sock is not None:
            with contextlib.suppress(OSError):
                conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return conn

    def _peek_remote_sha(self) -> str | None:
        if not self._http_host or not self._http_path or time.monotonic() < self._peek_disable_until:
            return None
        headers = {'User-Agent': 'git/2.45.0', 'Accept': '*/*', 'Connection': 'keep-alive'}
        if self._http_auth:
            headers['Authorization'] = self._http_auth
        for attempt in range(2):
            try:
                if self._http_conn is None:
                    self._http_conn = self._open_peek_conn()
                self._http_conn.request('GET', f'{self._http_path}/info/refs?service=git-upload-pack', headers=headers)
                resp = self._http_conn.getresponse()
                body = resp.read()
                if resp.status != 200:
                    return None
                m = self._ref_re.search(body)
                if m:
                    return m.group(1).decode('ascii')
                return 'MISSING'
            except Exception:
                if self._http_conn is not None:
                    with contextlib.suppress(Exception):
                        self._http_conn.close()
                    self._http_conn = None
                if attempt == 1:
                    self._peek_disable_until = time.monotonic() + 45.0
        return None

    def _fetch(self, shallow: bool=False) -> str | None:
        b = self.branch
        args = ['fetch', '-q', '--no-tags', '--no-recurse-submodules']
        if shallow:
            args.append('--depth=1')
        args += ['origin', f'+refs/heads/{b}:refs/remotes/origin/{b}']
        r = self._git(*args, check=False, timeout=600)
        if r.returncode != 0:
            if "couldn't find remote ref" in (r.stderr or ''):
                return None
            if not shallow:
                r = self._git('fetch', '-q', '--depth=1', '--no-tags', '--no-recurse-submodules', 'origin', f'+refs/heads/{b}:refs/remotes/origin/{b}', check=False, timeout=600)
                if r.returncode != 0:
                    if "couldn't find remote ref" in (r.stderr or ''):
                        return None
                    raise classify_error('fetch', r)
            else:
                raise classify_error('fetch', r)
        sha = self._read_ref_file(f'refs/remotes/origin/{b}')
        if sha:
            return sha
        r2 = self._git('rev-parse', '-q', '--verify', f'refs/remotes/origin/{b}', check=False)
        return r2.stdout.strip() or None

    def sync(self, force_fetch: bool=False) -> bool:
        with self._oplock:
            cur = self._head()
            if not force_fetch and cur is not None:
                peek = self._peek_remote_sha()
                if peek == 'MISSING':
                    return False
                if peek is not None and peek == cur:
                    return False
            sha = self._fetch(shallow=cur is None)
            if not sha or sha == cur:
                return False
            with self.lock:
                self._git('reset', '-q', '--hard', sha)
                self._head_cache = sha
            return True

    def abs(self, rel: str) -> Path:
        p = (self._root / rel).resolve()
        if self._root not in p.parents and p != self._root:
            raise BusError(f'Invalid path: {rel}')
        return p

    def read(self, rel: str) -> bytes | None:
        with self.lock:
            try:
                return self.abs(rel).read_bytes()
            except (FileNotFoundError, IsADirectoryError):
                return None

    def exists(self, rel: str) -> bool:
        if self.abs(rel).exists():
            return True
        with self._plock:
            for op in reversed(self._pending):
                if op.path == rel:
                    return op.kind in ('w', 'a', 'c')
        return False

    def listdir(self, rel: str) -> list[str]:
        with self.lock:
            p = self.abs(rel)
            return sorted((x.name for x in p.iterdir())) if p.is_dir() else []

    def stage(self, kind: str, path: str, data: bytes | None=None, src: str | None=None, cleanup: bool=False) -> None:
        self.abs(path)
        with self._plock:
            if kind in ('w', 'd') and self._pending:
                self._pending = [op for op in self._pending if not (op.path == path and op.kind == 'w')]
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
            if op.kind == 'w':
                p.parent.mkdir(parents=True, exist_ok=True)
                tmp = p.with_name(p.name + '.tmp~')
                tmp.write_bytes(op.data or b'')
                os.replace(tmp, p)
            elif op.kind == 'a':
                p.parent.mkdir(parents=True, exist_ok=True)
                with open(p, 'ab') as f:
                    f.write(op.data or b'')
            elif op.kind == 'c':
                p.parent.mkdir(parents=True, exist_ok=True)
                if p.exists():
                    p.unlink()
                try:
                    os.link(op.src, p)
                except OSError:
                    shutil.copyfile(op.src, p)
            elif op.kind == 'd':
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                elif p.exists():
                    p.unlink()

    def _cleanup(self, ops: list[Op]) -> None:
        for op in ops:
            if op.cleanup and op.src:
                with contextlib.suppress(OSError):
                    os.unlink(op.src)

    def _commit(self, message: str) -> bool:
        self._git('add', '-A')
        r = self._git('commit', '-q', '--no-verify', '-m', message, check=False)
        if r.returncode != 0:
            out = ((r.stdout or '') + ' ' + (r.stderr or '')).lower()
            if 'nothing to commit' in out or 'nothing added to commit' in out:
                return False
            raise classify_error('commit', r)
        self._head_cache = self._read_ref_file(f'refs/heads/{self.branch}')
        return True

    def _undo(self, base: str | None) -> None:
        with self.lock:
            if base:
                self._git('reset', '-q', '--hard', base, check=False)
            else:
                self._git('update-ref', '-d', f'refs/heads/{self.branch}', check=False)
                self._git('read-tree', '--empty', check=False)
            self._git('clean', '-fdq', check=False)
            self._head_cache = base

    def _push(self) -> str:
        r = self._git('push', '-q', '--no-verify', 'origin', f'HEAD:refs/heads/{self.branch}', check=False, timeout=900)
        if r.returncode == 0:
            return 'ok'
        low = (r.stderr or '').lower()
        if 'cannot lock ref' in low or 'failed to update ref' in low or 'reference already exists' in low:
            return 'nonff'
        if '[remote rejected]' not in low and ('[rejected]' in low or 'non-fast-forward' in low or 'fetch first' in low or ('stale info' in low)):
            return 'nonff'
        raise classify_error('push', r)

    def flush(self, message: str='sync', max_attempts: int=5) -> bool:
        with self._oplock:
            return self._flush(message, max_attempts)

    def _flush(self, message: str, max_attempts: int) -> bool:
        with self._plock:
            ops, self._pending = (self._pending, [])
        if not ops:
            return False
        base = self._head()
        try:
            for attempt in range(max_attempts):
                if attempt:
                    sha = self._fetch(shallow=False)
                    with self.lock:
                        target = sha or base
                        if target:
                            self._git('reset', '-q', '--hard', target, check=False)
                        else:
                            self._git('update-ref', '-d', f'refs/heads/{self.branch}', check=False)
                            self._git('read-tree', '--empty', check=False)
                        self._git('clean', '-fdq', check=False)
                        self._head_cache = target
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
                if res == 'ok':
                    self._cleanup(ops)
                    return True
            self._undo(base)
            raise BusNetworkError('push keeps being rejected (conflict with another writer)')
        except BusRejectedError:
            file_ops = [op for op in ops if op.kind == 'c']
            meta_ops = [op for op in ops if op.kind != 'c']
            if file_ops and meta_ops:
                self._cleanup(file_ops)
                self._requeue(meta_ops)
            else:
                self._cleanup(ops)
            raise
        except BusError:
            self._requeue(ops)
            raise

    def squash(self, extra_ops: list[Op] | None=None, message: str='compact') -> None:
        with self._oplock:
            self._squash(extra_ops, message)

    def _squash(self, extra_ops: list[Op] | None, message: str) -> None:
        with self._plock:
            pend, self._pending = (self._pending, [])
        ops = pend + list(extra_ops or [])
        old = None
        try:
            for attempt in range(3):
                if attempt:
                    self.sync(force_fetch=True)
                with self.lock:
                    old = self._head()
                    self._apply(ops)
                    self._git('add', '-A')
                    tree = self._git('write-tree').stdout.strip()
                    commit = self._git('commit-tree', tree, '-m', message).stdout.strip()
                    self._git('update-ref', f'refs/heads/{self.branch}', commit)
                    self._head_cache = commit
                lease_arg = f'--force-with-lease=refs/heads/{self.branch}:{old}' if old else '--force'
                r = self._git('push', '-q', '--no-verify', lease_arg, 'origin', f'{commit}:refs/heads/{self.branch}', check=False, timeout=900)
                if r.returncode == 0:
                    self._cleanup(ops)
                    self._squash_counter += 1
                    if self._squash_counter % 4 == 1:
                        self._git('reflog', 'expire', '--expire=now', '--all', check=False)
                        self._git('gc', '-q', '--prune=now', check=False, timeout=300)
                    return
                with self.lock:
                    if old:
                        self._git('update-ref', f'refs/heads/{self.branch}', old, check=False)
                        self._git('reset', '-q', '--hard', old, check=False)
                        self._head_cache = old
                low = (r.stderr or '').lower()
                if 'stale info' in low or '[rejected]' in low or 'non-fast-forward' in low:
                    continue
                raise classify_error('push --force', r)
            raise BusNetworkError('squash push rejected due to concurrent remote updates')
        except BusError:
            self._requeue(pend)
            raise
