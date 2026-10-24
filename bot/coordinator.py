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
        return (False, 'no local heartbeat')
    if hb.get('active') is False:
        return (False, 'the user disconnected')
    age = P.now() - float(hb.get('ts', 0))
    if age > timeout:
        return (False, f'local heartbeat is stale ({int(age)} seconds)')
    return (True, 'ok')

def session_blocked(bus: GitBus) -> bool:
    status = P.loads(bus.read(P.STATUS), {}) or {}
    if status.get('state') != 'session_invalid':
        return False
    refreshed = P.loads(bus.read(P.SESSION_REFRESHED), {}) or {}
    return float(refreshed.get('ts', 0)) <= float(status.get('ts', 0))

def main() -> int:
    repo, token = (os.environ.get('REPO', ''), os.environ.get('GH_TOKEN', ''))
    ref = os.environ.get('REF_NAME', 'main')
    if not repo or not token:
        log('REPO / GH_TOKEN are not set')
        return 1
    try:
        runs = ghapi.active_runs(repo, token)
        if runs:
            log(f"A Listener is active (run {runs[0].get('id')}).")
            return 0
    except Exception as e:
        log(f'Failed to query workflow runs: {str(e)[:150]}')
        return 1
    tmp = Path(tempfile.mkdtemp(prefix='coord-'))
    try:
        bus = GitBus(tmp / 'bus', f'https://github.com/{repo}.git', token)
        try:
            if not bus.init():
                log('The bus branch does not exist; nothing to do.')
                return 0
        except BusError as e:
            log(f'Failed to read the bus: {str(e)[:150]}')
            return 1
        cfg = P.merge_bus_config(P.loads(bus.read(P.CONFIG), {}))
        alive, why = local_is_alive(bus, float(cfg['local_timeout']) + 60)
        if not alive:
            log(f'No need to start: {why}')
            return 0
        if session_blocked(bus):
            log('The session is invalid; not starting until a fresh session is uploaded.')
            return 0
        log('No Listener is running but the local server is alive → dispatch')
        if ghapi.dispatch_listener(repo, token, ref):
            log('Dispatched.')
            return 0
        log('Dispatch failed.')
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
if __name__ == '__main__':
    sys.exit(main())
