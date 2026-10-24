from __future__ import annotations
import contextlib
import http.client
import json
import socket
import threading
from . import protocol as P
API_HOST = 'api.github.com'
ACTIVE = frozenset({'queued', 'in_progress', 'waiting', 'requested', 'pending'})
_lock = threading.Lock()
_conn: http.client.HTTPSConnection | None = None

def _open_conn(timeout: int) -> http.client.HTTPSConnection:
    conn = http.client.HTTPSConnection(API_HOST, timeout=timeout)
    conn.connect()
    if conn.sock is not None:
        with contextlib.suppress(OSError):
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return conn

def api(method: str, path: str, token: str, body: dict | None=None, timeout: int=30):
    global _conn
    data = json.dumps(body, separators=(',', ':')).encode('utf-8') if body is not None else None
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'python-urllib/3.14', 'Connection': 'keep-alive'}
    if data is not None:
        headers['Content-Type'] = 'application/json'
        headers['Content-Length'] = str(len(data))
    elif method in ('POST', 'PUT', 'PATCH'):
        headers['Content-Length'] = '0'
    with _lock:
        for attempt in range(2):
            try:
                if _conn is None:
                    _conn = _open_conn(timeout)
                _conn.request(method, path, body=data, headers=headers)
                resp = _conn.getresponse()
                raw = resp.read()
                status = resp.status
                if not raw:
                    return (status, None)
                try:
                    return (status, json.loads(raw))
                except ValueError:
                    return (status, {'message': raw.decode('utf-8', 'replace')[:200]})
            except Exception:
                if _conn is not None:
                    with contextlib.suppress(Exception):
                        _conn.close()
                    _conn = None
                if attempt == 1:
                    raise

def dispatch_listener(repo: str, token: str, ref: str='main') -> bool:
    status, _ = api('POST', f'/repos/{repo}/actions/workflows/{P.WORKFLOW_FILE}/dispatches', token, {'ref': ref})
    return status == 204

def active_runs(repo: str, token: str) -> list[dict]:
    status, body = api('GET', f'/repos/{repo}/actions/workflows/{P.WORKFLOW_FILE}/runs?per_page=10', token)
    if status != 200 or not body:
        raise RuntimeError(f'Failed to read workflow runs (HTTP {status})')
    return [r for r in body.get('workflow_runs', []) if r.get('status') in ACTIVE]
