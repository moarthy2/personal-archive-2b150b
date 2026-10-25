from __future__ import annotations
import asyncio
import contextlib
import os
import shutil
import sys
import tempfile
import time
import traceback
from collections import OrderedDict
from pathlib import Path
from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession
from bot import commands as cmds
from common import ghapi
from common import protocol as P
from common.crypto import Codec, CryptoError, decrypt_bytes, decrypt_with_passphrase, encrypt_bytes, key_from_b64
from common.gitbus import BusError, BusNetworkError, BusRejectedError, GitBus
SESSION_ERRORS = tuple((getattr(errors, n) for n in ('AuthKeyUnregisteredError', 'SessionRevokedError', 'SessionExpiredError', 'UserDeactivatedError', 'UserDeactivatedBanError', 'AuthKeyDuplicatedError') if hasattr(errors, n)))
FLOOD_ERRORS = tuple((getattr(errors, n) for n in ('FloodWaitError', 'SlowModeWaitError') if hasattr(errors, n)))
VALIDATE_EVERY = 3 * 3600
CHATS_MIN_INTERVAL = 30
PROFILE_REFRESH_INTERVAL = 15 * 60
MAX_SEEN_CHATS = 100
MAX_SEEN_IDS_PER_CHAT = 2500
FAST_COMMANDS = frozenset({'send', 'edit', 'delete', 'mark_read'})

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

class Listener:

    def __init__(self) -> None:
        env = os.environ
        self.repo = env.get('REPO', '')
        self.token = env.get('GH_TOKEN', '')
        self.run_id = env.get('RUN_ID', 'local')
        self.ref = env.get('REF_NAME', 'main')
        self.poll = float(env.get('POLL_SECONDS', '0.45'))
        self.tmp = Path(tempfile.mkdtemp(prefix='listener-'))
        url = env.get('REMOTE_URL') or f'https://github.com/{self.repo}.git'
        self.bus = GitBus(self.tmp / 'bus', url, self.token or None, log=log)
        self.cfg = dict(P.DEFAULT_BUS_CONFIG)
        self.codec = Codec(None, False)
        self.content_key: bytes | None = None
        self.client: TelegramClient | None = None
        self.started_mono = time.monotonic()
        self.started_at = P.now()
        self.deadline = self.started_mono + float(env.get('HANDOFF_AFTER_SECONDS', P.HANDOFF_AFTER))
        self.wake = asyncio.Event()
        self.hb_seq = 0
        self.last_hb_mono = 0.0
        self.last_activity_mono = self.started_mono
        self.seen: OrderedDict[int, OrderedDict[int, None]] = OrderedDict()
        self.files_index: dict[str, dict] = {}
        self.index_dirty = False
        self.chats_dirty = False
        self.chats_list: list[dict] = []
        self.chat_ids: list[int] = []
        self.dialog_last_msg: dict[int, int] = {}
        self.last_chats = 0.0
        self.done_ids: set[str] = set()
        self.results: OrderedDict[str, dict] = OrderedDict()
        self.tasks: set[asyncio.Task] = set()
        self.local_key = None
        self.local_changed = self.started_mono
        self.flush_count = 0
        self.purge_marker = None
        self.session_invalid = False

    def stage_status(self, state: str, detail: str='') -> None:
        self.bus.stage('w', P.STATUS, P.dumps({'state': state, 'detail': detail, 'run_id': self.run_id, 'started_at': self.started_at, 'ts': P.now()}))

    def stage_heartbeat(self, handoff: bool=False) -> None:
        self.hb_seq += 1
        self.last_hb_mono = time.monotonic()
        self.bus.stage('w', P.ACTION_HB, P.dumps({'seq': self.hb_seq, 'ts': P.now(), 'run_id': self.run_id, 'started_at': self.started_at, 'handoff': handoff}))

    def refresh_config(self) -> None:
        self.cfg = P.merge_bus_config(P.loads(self.bus.read(P.CONFIG), {}))
        if self.cfg['encrypt'] and (not self.content_key):
            raise SystemExit('encrypt is enabled but the TG_CONTENT_KEY Secret does not exist.')
        P.set_path_key(self.content_key if self.cfg['encrypt'] else None)
        self.codec = Codec(self.content_key, self.cfg['encrypt'])

    def load_index(self) -> None:
        try:
            doc = self.codec.dec_json(self.bus.read(P.FILES_INDEX), {}) or {}
        except CryptoError:
            doc = {}
        self.files_index = dict(doc.get('files', {}))

    def load_results(self) -> None:
        try:
            doc = self.codec.dec_json(self.bus.read(P.RESULTS), {}) or {}
            raw_res = doc.get('results', {})
            if isinstance(raw_res, dict):
                self.results = OrderedDict(raw_res)
        except CryptoError:
            self.results = OrderedDict()

    def save_index(self) -> None:
        self.bus.stage('w', P.FILES_INDEX, self.codec.enc_json({'files': self.files_index}))
        self.index_dirty = False

    def flush_index(self) -> None:
        if self.index_dirty:
            self.save_index()

    def update_chat_preview(self, chat_id: int, rec: dict, incoming_unread: bool=False) -> None:
        if not self.chats_list:
            self.chats_dirty = True
            return
        target = None
        for c in self.chats_list:
            if int(c.get('id', 0)) == int(chat_id):
                target = c
                break
        if target is None:
            self.chats_dirty = True
            return
        if int(rec.get('date', 0)) >= int(target.get('last_date', 0)):
            target['last_date'] = int(rec.get('date', 0))
            target['last_text'] = (rec.get('text') or '')[:80]
            target['last_media'] = rec.get('media_type')
        if incoming_unread:
            target['unread'] = int(target.get('unread', 0)) + 1
        self.bus.stage('w', P.CHATS_INDEX, self.codec.enc_json({'updated': P.now(), 'chats': self.chats_list}))

    def mark_chat_read_in_memory(self, chat_id: int) -> None:
        if not self.chats_list:
            self.chats_dirty = True
            return
        for c in self.chats_list:
            if int(c.get('id', 0)) == int(chat_id):
                c['unread'] = 0
                self.bus.stage('w', P.CHATS_INDEX, self.codec.enc_json({'updated': P.now(), 'chats': self.chats_list}))
                return
        self.chats_dirty = True

    def _lru_chat_seen(self, chat_id: int) -> OrderedDict[int, None]:
        s = self.seen.get(chat_id)
        if s is not None:
            self.seen.move_to_end(chat_id)
            return s
        s = OrderedDict()
        for raw in self.codec.dec_lines(self.bus.read(P.messages_path(chat_id))):
            rec = P.unpack_record(raw)
            if rec.get('op') == 'msg':
                s[rec['id']] = None
        while len(s) > MAX_SEEN_IDS_PER_CHAT:
            s.popitem(last=False)
        self.seen[chat_id] = s
        while len(self.seen) > MAX_SEEN_CHATS:
            self.seen.popitem(last=False)
        return s

    def seen_set(self, chat_id: int) -> OrderedDict[int, None]:
        return self._lru_chat_seen(chat_id)

    def mark_seen(self, chat_id: int, message_id: int) -> None:
        s = self._lru_chat_seen(chat_id)
        s[message_id] = None
        if len(s) > MAX_SEEN_IDS_PER_CHAT:
            s.popitem(last=False)

    def note_file(self, chat_id: int, message_id: int, f: dict) -> None:
        fid = f['id']
        if fid in self.files_index:
            return
        self.files_index[fid] = {'file_id': fid, 'chat_id': chat_id, 'message_id': message_id, 'name': f['name'], 'size': f['size'], 'mime': f['mime'], 'status': 'remote'}
        self.index_dirty = True
        if self.cfg['auto_download_documents'] and f['size'] <= int(self.cfg['max_file_mb']) * 1024 * 1024:
            self.spawn(self._run(None, 'download_file', self._download({'chat_id': chat_id, 'message_id': message_id})))

    def stage_edit(self, chat_id: int, mid: int, text: str, edit_date: int) -> None:
        rec = {'op': 'edit', 'id': mid, 'chat_id': chat_id, 'text': text, 'edit_date': edit_date}
        self.bus.stage('a', P.messages_path(chat_id), self.codec.enc_line(P.pack_record(rec)))

    def stage_delete(self, chat_id: int, mid: int) -> None:
        rec = {'op': 'del', 'id': mid, 'chat_id': chat_id}
        self.bus.stage('a', P.messages_path(chat_id), self.codec.enc_line(P.pack_record(rec)))

    def add_result(self, cid: str | None, typ: str, ok: bool, out: dict | None=None, error: str='') -> None:
        if cid is None:
            return
        self.results[cid] = {'type': typ, 'ok': ok, 'error': error, 'out': out or {}, 'ts': P.now()}
        while len(self.results) > 100:
            self.results.popitem(last=False)
        self.bus.stage('w', P.RESULTS, self.codec.enc_json({'results': dict(self.results)}))

    def spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def _download(self, cmd: dict) -> dict:
        return await cmds.download_file(self, cmds._int(cmd.get('chat_id'), 'chat_id'), cmds._int(cmd.get('message_id'), 'message_id'))

    async def _run(self, cid: str | None, typ: str, coro) -> None:
        try:
            out = await coro
            self.flush_index()
            self.add_result(cid, typ, True, out)
        except asyncio.CancelledError:
            raise
        except SESSION_ERRORS as e:
            self.session_invalid = True
            self.flush_index()
            self.add_result(cid, typ, False, error=f'session invalidated: {type(e).__name__}')
        except FLOOD_ERRORS as e:
            wait_s = int(getattr(e, 'seconds', 5) or 5)
            self.flush_index()
            self.add_result(cid, typ, False, error=f'Telegram rate limit: wait {wait_s}s')
            await asyncio.sleep(min(wait_s, 30))
        except Exception as e:
            self.flush_index()
            self.add_result(cid, typ, False, error=f'{type(e).__name__}: {e}'[:300])
        finally:
            self.wake.set()

    async def process_commands(self) -> bool:
        fast_coros = []
        any_dispatched = False
        for name in self.bus.listdir(P.COMMANDS_DIR):
            if not P.is_command_file(name):
                continue
            cid = name[:-5]
            if cid in self.done_ids:
                continue
            path = f'{P.COMMANDS_DIR}/{name}'
            raw = self.bus.read(path)
            if raw is None:
                continue
            self.done_ids.add(cid)
            self.last_activity_mono = time.monotonic()
            self.bus.stage('d', path)
            any_dispatched = True
            try:
                cmd = self.codec.dec_json(raw)
                if not isinstance(cmd, dict):
                    raise ValueError('invalid command')
            except Exception as e:
                self.add_result(cid, '?', False, error=f'could not read command: {type(e).__name__}')
                continue
            typ = cmd.get('type')
            if not isinstance(typ, str):
                self.add_result(cid, '?', False, error='unknown command')
                continue
            if typ == 'download_file':
                self.spawn(self._run(cid, typ, self._download(cmd)))
            elif typ in FAST_COMMANDS and typ in cmds.HANDLERS:
                fast_coros.append(self._run(cid, typ, cmds.HANDLERS[typ](self, cmd)))
            elif typ in cmds.HANDLERS:
                self.spawn(self._run(cid, typ, cmds.HANDLERS[typ](self, cmd)))
            else:
                self.add_result(cid, str(typ), False, error='unknown command')
        if fast_coros:
            await asyncio.gather(*fast_coros)
        if self.tasks:
            await asyncio.wait(list(self.tasks), timeout=0.08)
        return any_dispatched

    def check_local(self) -> str | None:
        hb = P.loads(self.bus.read(P.LOCAL_HB), None)
        if isinstance(hb, dict):
            if hb.get('active') is False:
                return 'stopped_by_command'
            key = (hb.get('seq'), hb.get('ts'))
            if self.local_key is None:
                self.local_key = key
                age = max(0.0, P.now() - float(hb.get('ts', 0)))
                if age <= float(self.cfg['local_timeout']):
                    self.local_changed = time.monotonic() - age
            elif key != self.local_key:
                self.local_key, self.local_changed = (key, time.monotonic())
        stop = P.loads(self.bus.read(P.STOP_CMD), None)
        if isinstance(stop, dict):
            if float(stop.get('ts', 0)) >= self.started_at - 30:
                self.bus.stage('d', P.STOP_CMD)
                return 'stopped_by_command'
            self.bus.stage('d', P.STOP_CMD)
        if time.monotonic() - self.local_changed > float(self.cfg['local_timeout']):
            return 'stopped_by_local_timeout'
        return None

    def register_handlers(self) -> None:
        c = self.client

        @c.on(events.NewMessage())
        async def _new(ev):
            try:
                if await cmds.record_message(self, ev.message, allow_rpc=True, update_preview=True):
                    self.last_activity_mono = time.monotonic()
                    self.wake.set()
            except Exception as e:
                log(f'Failed to record message: {type(e).__name__}')

        @c.on(events.MessageEdited())
        async def _edit(ev):
            m = ev.message
            if m.chat_id is None or not m.edit_date or getattr(m, 'edit_hide', False):
                return
            if m.id in self.seen_set(m.chat_id):
                self.stage_edit(m.chat_id, m.id, m.raw_text or '', cmds._ts(m.edit_date))
                self.last_activity_mono = time.monotonic()
                self.wake.set()

        @c.on(events.MessageDeleted())
        async def _del(ev):
            any_del = False
            for mid in ev.deleted_ids:
                targets = [ev.chat_id] if ev.chat_id else [cid for cid, s in self.seen.items() if mid in s]
                for cid in targets:
                    if mid in self.seen_set(cid):
                        self.stage_delete(cid, mid)
                        any_del = True
            if any_del:
                self.last_activity_mono = time.monotonic()
                self.wake.set()

    def on_bus_changed(self) -> None:
        self.refresh_config()
        marker = P.loads(self.bus.read(P.PURGE_MARKER), {}).get('ts')
        if marker != self.purge_marker:
            log('Purge detected from the local side; resetting the internal cache.')
            self.purge_marker = marker
            self.seen.clear()
            self.load_index()
            self.load_results()

    async def sync(self) -> None:
        try:
            if await asyncio.to_thread(self.bus.sync):
                self.on_bus_changed()
        except BusNetworkError as e:
            log(f'git network: {str(e)[:120]}')

    async def flush(self, retries: int=1) -> bool:
        if self.bus.pending_count() == 0:
            return False
        if time.monotonic() - self.last_hb_mono >= 15.0:
            self.stage_heartbeat()
        for _ in range(retries):
            try:
                if await asyncio.to_thread(self.bus.flush, 'sync'):
                    self.flush_count += 1
                return True
            except BusRejectedError as e:
                log(f'The remote rejected the push; file dropped: {str(e)[:150]}')
                self.stage_status('running', 'push_rejected')
                return False
            except BusError as e:
                log(f'flush failed (will retry): {str(e)[:120]}')
                await asyncio.sleep(1.0)
        return False

    async def maybe_squash(self) -> None:
        if self.flush_count >= int(self.cfg['squash_every_commits']):
            try:
                await asyncio.to_thread(self.bus.squash, None, 'compact')
                log('The bus history was compacted.')
                self.flush_count = 0
            except BusError as e:
                log(f'squash failed: {str(e)[:120]}')

    def _load_session_string(self) -> str:
        base_session = decrypt_with_passphrase(os.environ['TG_SESSION_ENC'], os.environ['TG_SESSION_KEY']).decode()
        if self.content_key:
            raw_state = self.bus.read(P.SESSION_STATE)
            if raw_state:
                with contextlib.suppress(Exception):
                    refreshed = P.loads(self.bus.read(P.SESSION_REFRESHED), {}) or {}
                    status = P.loads(self.bus.read(P.STATUS), {}) or {}
                    if float(refreshed.get('ts', 0)) <= float(status.get('ts', 0)):
                        saved = decrypt_bytes(self.content_key, raw_state, b'session-state').decode('utf-8')
                        if saved:
                            return saved
        return base_session

    def _save_session_state(self) -> None:
        if self.client and self.content_key and (not self.session_invalid):
            with contextlib.suppress(Exception):
                s = self.client.session.save()
                if s:
                    self.bus.stage('w', P.SESSION_STATE, encrypt_bytes(self.content_key, s.encode('utf-8'), b'session-state'))

    async def setup(self) -> int | None:
        log(f'Listener starting (run {self.run_id})')
        try:
            if not await asyncio.to_thread(self.bus.init):
                log('The bus branch does not exist.')
                return 1
        except BusError as e:
            log(f'Could not access the bus: {str(e)[:200]}')
            return 1
        key = os.environ.get('TG_CONTENT_KEY')
        self.content_key = key_from_b64(key) if key else None
        self.refresh_config()
        self.load_index()
        self.load_results()
        self.purge_marker = P.loads(self.bus.read(P.PURGE_MARKER), {}).get('ts')
        self.stage_status('starting')
        self.stage_heartbeat()
        await self.flush(3)
        try:
            session = self._load_session_string()
        except (KeyError, CryptoError) as e:
            log(f'Session decryption failed: {type(e).__name__}')
            self.stage_status('error', 'session_decrypt_failed')
            await self.flush(3)
            return 1
        self.client = TelegramClient(StringSession(session), int(os.environ['TG_API_ID']), os.environ['TG_API_HASH'], device_model='Desktop', system_version='Windows 11 x64', app_version='4.16.8 x64', lang_code='en', system_lang_code='en-US')
        self.client.parse_mode = None
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                raise errors.AuthKeyUnregisteredError(request=None)
        except SESSION_ERRORS:
            return await self.fail_invalid()
        self.register_handlers()
        n = await cmds.refresh_chats(self, force=True)
        self.last_chats = time.monotonic()
        log(f'{n} chats synced.')
        await self.backfill()
        if self.index_dirty:
            self.save_index()
        self._save_session_state()
        self.stage_status('running')
        self.stage_heartbeat()
        await self.flush(3)
        return None

    async def backfill(self) -> None:
        count = int(self.cfg['startup_backfill_chats'])
        if count <= 0:
            return
        targets = []
        for cid in self.chat_ids[:count]:
            last_mid = self.dialog_last_msg.get(cid)
            if last_mid is not None and last_mid in self.seen_set(cid):
                continue
            targets.append(cid)
        if not targets:
            return
        sem = asyncio.Semaphore(5)
        limit = int(self.cfg.get('backfill_messages', 20))

        async def _one(cid: int) -> None:
            async with sem:
                await cmds.fetch_messages(self, cid, limit)
        try:
            await asyncio.gather(*(_one(cid) for cid in targets))
        except SESSION_ERRORS:
            self.session_invalid = True
        except Exception as e:
            log(f'backfill incomplete: {type(e).__name__}')

    async def fail_invalid(self) -> int:
        log('The session is invalid.')
        self.bus.stage('d', P.SESSION_STATE)
        self.stage_status('session_invalid', 'Log in again with tools/login.py')
        self.stage_heartbeat()
        await self.flush(3)
        await self.close()
        return 1

    async def close(self) -> None:
        for t in list(self.tasks):
            t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self._save_session_state()
        if self.client:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.client.disconnect(), 20)

    async def shutdown(self, reason: str) -> int:
        log(f'Stopping: {reason}')
        await self.close()
        self.stage_status(reason)
        self.stage_heartbeat()
        await self.flush(3)
        return 0

    async def handoff(self) -> int:
        log('Time limit approaching; handing off to the next job.')
        await self.close()
        self.stage_status('handoff')
        self.stage_heartbeat(handoff=True)
        await self.flush(3)
        for _ in range(3):
            try:
                if await asyncio.to_thread(ghapi.dispatch_listener, self.repo, self.token, self.ref):
                    log('A new job was triggered.')
                    return 0
            except Exception as e:
                log(f'dispatch failed: {type(e).__name__}')
            await asyncio.sleep(5)
        log('Dispatch failed; the Coordinator will restart it.')
        return 0

    async def run(self) -> int:
        code = await self.setup()
        if code is not None:
            return code
        last_validate = time.monotonic()
        errors_in_row = 0
        try:
            while True:
                t0 = time.monotonic()
                try:
                    await self.sync()
                    ran_cmd = await self.process_commands()
                    reason = self.check_local()
                    if self.session_invalid:
                        return await self.fail_invalid()
                    if self.index_dirty:
                        self.save_index()
                    if reason:
                        return await self.shutdown(reason)
                    if ran_cmd or self.bus.pending_count() > 0:
                        await self.flush()
                    t_now = time.monotonic()
                    if t_now - self.last_hb_mono >= P.ACTION_HB_INTERVAL:
                        self.stage_heartbeat()
                    refresh_due = t_now - self.last_chats >= PROFILE_REFRESH_INTERVAL
                    if not ran_cmd and (not self.tasks) and (self.chats_dirty and t_now - self.last_chats >= CHATS_MIN_INTERVAL or refresh_due):
                        await cmds.refresh_chats(self, force=False)
                        self.last_chats = time.monotonic()
                    if self.bus.pending_count() > 0:
                        await self.flush()
                    await self.maybe_squash()
                    if t_now >= self.deadline:
                        return await self.handoff()
                    if t_now - last_validate >= VALIDATE_EVERY:
                        last_validate = t_now
                        await self.client.get_me()
                    errors_in_row = 0
                except SESSION_ERRORS:
                    return await self.fail_invalid()
                except FLOOD_ERRORS as e:
                    wait_s = int(getattr(e, 'seconds', 5) or 5)
                    await asyncio.sleep(min(wait_s, 30))
                except Exception as e:
                    errors_in_row += 1
                    log(f'Error in loop ({errors_in_row}): {type(e).__name__}')
                    if os.environ.get('LISTENER_DEBUG'):
                        traceback.print_exc()
                    if errors_in_row >= 20:
                        self.stage_status('error', type(e).__name__)
                        await self.flush(3)
                        return 1
                active = time.monotonic() - self.last_activity_mono < 90.0 or bool(self.tasks)
                target_poll = min(self.poll, 0.4) if active else max(self.poll, 2.0)
                wait_s = max(0.05, target_poll - (time.monotonic() - t0))
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=wait_s)
                    self.wake.clear()
                    await asyncio.sleep(0.025)
                except asyncio.TimeoutError:
                    pass
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)

def main() -> None:
    sys.exit(asyncio.run(Listener().run()))
if __name__ == '__main__':
    main()
