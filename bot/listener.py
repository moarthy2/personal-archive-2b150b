"""Listener: اتصال واقعی به تلگرام (Telethon) داخل GitHub Actions.

ارتباط با سرور لوکال فقط از طریق برنچ bus (git) است:
  commands/*.json  ← فرمان‌های لوکال
  data/*           → نتیجه، پیام‌ها، فایل‌ها، heartbeat

اجرا:  python -m bot.listener
"""
from __future__ import annotations

import asyncio
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
from common.crypto import Codec, CryptoError, decrypt_with_passphrase, key_from_b64
from common.gitbus import BusError, BusNetworkError, BusRejectedError, GitBus

SESSION_ERRORS = tuple(
    getattr(errors, n) for n in (
        "AuthKeyUnregisteredError", "SessionRevokedError", "SessionExpiredError",
        "UserDeactivatedError", "UserDeactivatedBanError", "AuthKeyDuplicatedError",
    ) if hasattr(errors, n)
)
VALIDATE_EVERY = 3 * 3600
CHATS_MIN_INTERVAL = 30


def log(msg: str) -> None:
    # عمداً هیچ‌وقت متن پیام/نام چت/Secret لاگ نمی‌شود
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Listener:
    def __init__(self) -> None:
        env = os.environ
        self.repo = env.get("REPO", "")
        self.token = env.get("GH_TOKEN", "")
        self.run_id = env.get("RUN_ID", "local")
        self.ref = env.get("REF_NAME", "main")
        self.poll = float(env.get("POLL_SECONDS", "2.5"))
        self.tmp = Path(tempfile.mkdtemp(prefix="listener-"))
        url = env.get("REMOTE_URL") or f"https://github.com/{self.repo}.git"
        self.bus = GitBus(self.tmp / "bus", url, self.token or None, log=log)

        self.cfg = dict(P.DEFAULT_BUS_CONFIG)
        self.codec = Codec(None, False)
        self.content_key: bytes | None = None
        self.client: TelegramClient | None = None

        self.started_mono = time.monotonic()
        self.started_at = P.now()
        self.deadline = self.started_mono + float(env.get("HANDOFF_AFTER_SECONDS", P.HANDOFF_AFTER))

        self.hb_seq = 0
        self.seen: dict[int, set[int]] = {}
        self.files_index: dict[str, dict] = {}
        self.index_dirty = False
        self.chats_dirty = False
        self.chat_ids: list[int] = []
        self.last_chats = 0.0
        self.done_ids: set[str] = set()
        self.results: OrderedDict[str, dict] = OrderedDict()
        self.tasks: set[asyncio.Task] = set()
        self.local_key = None
        self.local_changed = self.started_mono
        self.flush_count = 0
        self.purge_marker = None
        self.session_invalid = False

    # ------------------------------------------------------------------ وضعیت/heartbeat
    def stage_status(self, state: str, detail: str = "") -> None:
        self.bus.stage("w", P.STATUS, P.dumps({"state": state, "detail": detail, "run_id": self.run_id,
                                              "started_at": self.started_at, "ts": P.now()}))

    def stage_heartbeat(self, handoff: bool = False) -> None:
        self.hb_seq += 1
        self.bus.stage("w", P.ACTION_HB, P.dumps({"seq": self.hb_seq, "ts": P.now(), "run_id": self.run_id,
                                                  "started_at": self.started_at, "handoff": handoff}))

    # ------------------------------------------------------------------ داده
    def refresh_config(self) -> None:
        self.cfg = P.merge_bus_config(P.loads(self.bus.read(P.CONFIG), {}))
        if self.cfg["encrypt"] and not self.content_key:
            raise SystemExit("encrypt فعال است ولی Secret با نام TG_CONTENT_KEY وجود ندارد.")
        self.codec = Codec(self.content_key, self.cfg["encrypt"])

    def load_index(self) -> None:
        try:
            doc = self.codec.dec_json(self.bus.read(P.FILES_INDEX), {}) or {}
        except CryptoError:
            doc = {}
        self.files_index = dict(doc.get("files", {}))

    def save_index(self) -> None:
        self.bus.stage("w", P.FILES_INDEX, self.codec.enc_json({"files": self.files_index}))
        self.index_dirty = False

    def flush_index(self) -> None:
        if self.index_dirty:
            self.save_index()

    def seen_set(self, chat_id: int) -> set[int]:
        s = self.seen.get(chat_id)
        if s is None:
            s = set()
            for rec in self.codec.dec_lines(self.bus.read(P.messages_path(chat_id))):
                if rec.get("op") == "msg":
                    s.add(rec["id"])
            self.seen[chat_id] = s
        return s

    def note_file(self, chat_id: int, message_id: int, f: dict) -> None:
        fid = f["id"]
        if fid in self.files_index:
            return
        self.files_index[fid] = {"file_id": fid, "chat_id": chat_id, "message_id": message_id, "name": f["name"],
                                 "size": f["size"], "mime": f["mime"], "status": "remote"}
        self.index_dirty = True
        if self.cfg["auto_download_documents"] and f["size"] <= int(self.cfg["max_file_mb"]) * 1024 * 1024:
            self.spawn(self._run(None, "download_file", self._download({"chat_id": chat_id, "message_id": message_id})))

    def stage_edit(self, chat_id: int, mid: int, text: str, edit_date: int) -> None:
        rec = {"op": "edit", "id": mid, "chat_id": chat_id, "text": text, "edit_date": edit_date}
        self.bus.stage("a", P.messages_path(chat_id), self.codec.enc_line(rec))

    def stage_delete(self, chat_id: int, mid: int) -> None:
        self.bus.stage("a", P.messages_path(chat_id), self.codec.enc_line({"op": "del", "id": mid, "chat_id": chat_id}))

    def add_result(self, cid: str | None, typ: str, ok: bool, out: dict | None = None, error: str = "") -> None:
        if cid is None:
            return
        self.results[cid] = {"type": typ, "ok": ok, "error": error, "out": out or {}, "ts": P.now()}
        while len(self.results) > 100:
            self.results.popitem(last=False)
        self.bus.stage("w", P.RESULTS, self.codec.enc_json({"results": dict(self.results)}))

    # ------------------------------------------------------------------ اجرای فرمان
    def spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def _download(self, cmd: dict) -> dict:
        return await cmds.download_file(self, cmds._int(cmd.get("chat_id"), "chat_id"),
                                        cmds._int(cmd.get("message_id"), "message_id"))

    async def _run(self, cid: str | None, typ: str, coro) -> None:
        try:
            out = await coro
            self.flush_index()          # ایندکس همیشه زودتر/هم‌زمان با نتیجه به لوکال برسد
            self.add_result(cid, typ, True, out)
        except asyncio.CancelledError:
            raise
        except SESSION_ERRORS as e:
            self.session_invalid = True
            self.flush_index()
            self.add_result(cid, typ, False, error=f"session باطل شد: {type(e).__name__}")
        except Exception as e:  # noqa: BLE001
            self.flush_index()
            self.add_result(cid, typ, False, error=f"{type(e).__name__}: {e}"[:300])

    async def process_commands(self) -> None:
        for name in self.bus.listdir(P.COMMANDS_DIR):
            if not P.is_command_file(name):
                continue
            cid = name[:-5]
            if cid in self.done_ids:
                continue
            path = f"{P.COMMANDS_DIR}/{name}"
            raw = self.bus.read(path)
            if raw is None:
                continue
            self.done_ids.add(cid)
            self.bus.stage("d", path)
            try:
                cmd = self.codec.dec_json(raw)
                if not isinstance(cmd, dict):
                    raise ValueError("فرمان نامعتبر")
            except Exception as e:  # noqa: BLE001
                self.add_result(cid, "?", False, error=f"فرمان خوانده نشد: {type(e).__name__}")
                continue
            typ = cmd.get("type")
            if typ == "download_file":
                self.spawn(self._run(cid, typ, self._download(cmd)))
            elif typ in cmds.HANDLERS:
                await self._run(cid, typ, cmds.HANDLERS[typ](self, cmd))
            else:
                self.add_result(cid, str(typ), False, error="فرمان ناشناخته")

    # ------------------------------------------------------------------ لوکال زنده است؟
    def check_local(self) -> str | None:
        hb = P.loads(self.bus.read(P.LOCAL_HB), None)
        if isinstance(hb, dict):
            if hb.get("active") is False:
                return "stopped_by_command"
            key = (hb.get("seq"), hb.get("ts"))
            if key != self.local_key:
                self.local_key, self.local_changed = key, time.monotonic()
        stop = P.loads(self.bus.read(P.STOP_CMD), None)
        if isinstance(stop, dict):
            if float(stop.get("ts", 0)) >= self.started_at - 30:
                self.bus.stage("d", P.STOP_CMD)
                return "stopped_by_command"
            self.bus.stage("d", P.STOP_CMD)   # فرمان stop کهنه؛ نادیده بگیر و پاک کن
        if time.monotonic() - self.local_changed > float(self.cfg["local_timeout"]):
            return "stopped_by_local_timeout"
        return None

    # ------------------------------------------------------------------ رویدادهای تلگرام
    def register_handlers(self) -> None:
        c = self.client

        @c.on(events.NewMessage())
        async def _new(ev):
            try:
                await cmds.record_message(self, ev.message)
            except Exception as e:  # noqa: BLE001
                log(f"خطا در ثبت پیام: {type(e).__name__}")

        @c.on(events.MessageEdited())
        async def _edit(ev):
            m = ev.message
            if m.chat_id is not None:
                self.stage_edit(m.chat_id, m.id, m.raw_text or "", cmds._ts(m.edit_date) or int(P.now()))

        @c.on(events.MessageDeleted())
        async def _del(ev):
            for mid in ev.deleted_ids:
                targets = [ev.chat_id] if ev.chat_id else [cid for cid, s in self.seen.items() if mid in s]
                for cid in targets:
                    self.stage_delete(cid, mid)

    # ------------------------------------------------------------------ تغییر برنچ
    def on_bus_changed(self) -> None:
        self.refresh_config()
        marker = P.loads(self.bus.read(P.PURGE_MARKER), {}).get("ts")
        if marker != self.purge_marker:
            log("پاکسازی از سمت لوکال شناسایی شد؛ کش داخلی ریست می‌شود.")
            self.purge_marker = marker
            self.seen.clear()
            self.load_index()

    async def sync(self) -> None:
        try:
            if await asyncio.to_thread(self.bus.sync):
                self.on_bus_changed()
        except BusNetworkError as e:
            log(f"شبکه‌ی git: {str(e)[:120]}")

    async def flush(self, retries: int = 1) -> bool:
        for _ in range(retries):
            try:
                if await asyncio.to_thread(self.bus.flush, "sync"):
                    self.flush_count += 1
                return True
            except BusRejectedError as e:
                log(f"ریموت push را رد کرد؛ دسته حذف شد: {str(e)[:150]}")
                self.stage_status("running", "push_rejected")
                return False
            except BusError as e:
                log(f"flush ناموفق (دوباره تلاش می‌شود): {str(e)[:120]}")
                await asyncio.sleep(3)
        return False

    async def maybe_squash(self) -> None:
        if self.flush_count >= int(self.cfg["squash_every_commits"]):
            try:
                await asyncio.to_thread(self.bus.squash, None, "compact")
                log("تاریخچه‌ی bus فشرده شد.")
                self.flush_count = 0
            except BusError as e:
                log(f"squash ناموفق: {str(e)[:120]}")

    # ------------------------------------------------------------------ چرخه‌ی عمر
    async def setup(self) -> int | None:
        """None = موفق؛ در غیر این صورت کد خروج."""
        log(f"شروع Listener (run {self.run_id})")
        try:
            if not await asyncio.to_thread(self.bus.init):
                log("برنچ bus وجود ندارد.")
                return 1
        except BusError as e:
            log(f"دسترسی به bus ممکن نشد: {str(e)[:200]}")
            return 1
        key = os.environ.get("TG_CONTENT_KEY")
        self.content_key = key_from_b64(key) if key else None
        self.refresh_config()
        self.load_index()
        self.purge_marker = P.loads(self.bus.read(P.PURGE_MARKER), {}).get("ts")

        self.stage_status("starting")
        self.stage_heartbeat()
        await self.flush(3)

        try:
            session = decrypt_with_passphrase(os.environ["TG_SESSION_ENC"], os.environ["TG_SESSION_KEY"]).decode()
        except (KeyError, CryptoError) as e:
            log(f"رمزگشایی session ناموفق: {type(e).__name__}")
            self.stage_status("error", "session_decrypt_failed")
            await self.flush(3)
            return 1
        print(f"::add-mask::{session}", flush=True)

        self.client = TelegramClient(StringSession(session), int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"],
                                     device_model="TG-Bridge (GitHub Actions)", app_version="1.0")
        self.client.parse_mode = None
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                raise errors.AuthKeyUnregisteredError(request=None)
        except SESSION_ERRORS:
            return await self.fail_invalid()
        self.register_handlers()
        n = await cmds.refresh_chats(self)
        log(f"{n} چت همگام شد.")
        await self.backfill()
        if self.index_dirty:
            self.save_index()
        self.stage_status("running")
        await self.flush(3)
        return None

    async def backfill(self) -> None:
        count = int(self.cfg["startup_backfill_chats"])
        if count <= 0:
            return
        try:
            for cid in self.chat_ids[:count]:
                await cmds.fetch_messages(self, cid, 20)
        except SESSION_ERRORS:
            self.session_invalid = True
        except Exception as e:  # noqa: BLE001
            log(f"backfill ناقص ماند: {type(e).__name__}")

    async def fail_invalid(self) -> int:
        log("session باطل است.")
        self.stage_status("session_invalid", "با tools/login.py دوباره وارد شوید")
        self.stage_heartbeat()
        await self.flush(3)
        await self.close()
        return 1

    async def close(self) -> None:
        for t in list(self.tasks):
            t.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.client:
            try:
                await asyncio.wait_for(self.client.disconnect(), 20)
            except Exception:  # noqa: BLE001
                pass

    async def shutdown(self, reason: str) -> int:
        log(f"توقف: {reason}")
        await self.close()
        self.stage_status(reason)
        self.stage_heartbeat()
        await self.flush(3)
        return 0

    async def handoff(self) -> int:
        log("سقف زمانی نزدیک است؛ تحویل به Job بعدی.")
        await self.close()
        self.stage_status("handoff")
        self.stage_heartbeat(handoff=True)
        await self.flush(3)
        for attempt in range(3):
            try:
                if await asyncio.to_thread(ghapi.dispatch_listener, self.repo, self.token, self.ref):
                    log("Job جدید trigger شد.")
                    return 0
            except Exception as e:  # noqa: BLE001
                log(f"dispatch ناموفق: {type(e).__name__}")
            await asyncio.sleep(5)
        log("dispatch نشد؛ Coordinator (هر ۵ دقیقه) دوباره راه‌اندازی می‌کند.")
        return 0

    async def run(self) -> int:
        code = await self.setup()
        if code is not None:
            return code
        last_hb = time.monotonic()
        last_validate = time.monotonic()
        errors_in_row = 0
        try:
            while True:
                t0 = time.monotonic()
                try:
                    await self.sync()
                    await self.process_commands()
                    reason = self.check_local()
                    if self.session_invalid:
                        return await self.fail_invalid()
                    if t0 - last_hb >= P.ACTION_HB_INTERVAL:
                        self.stage_heartbeat()
                        last_hb = t0
                    if self.chats_dirty and t0 - self.last_chats >= CHATS_MIN_INTERVAL:
                        await cmds.refresh_chats(self)
                        self.last_chats = t0
                    if self.index_dirty:
                        self.save_index()
                    if reason:
                        return await self.shutdown(reason)
                    await self.flush()
                    await self.maybe_squash()
                    if t0 >= self.deadline:
                        return await self.handoff()
                    if t0 - last_validate >= VALIDATE_EVERY:
                        last_validate = t0
                        await self.client.get_me()
                    errors_in_row = 0
                except SESSION_ERRORS:
                    return await self.fail_invalid()
                except Exception as e:  # noqa: BLE001
                    errors_in_row += 1
                    log(f"خطا در حلقه ({errors_in_row}): {type(e).__name__}")
                    if os.environ.get("LISTENER_DEBUG"):
                        traceback.print_exc()
                    if errors_in_row >= 20:
                        self.stage_status("error", type(e).__name__)
                        await self.flush(3)
                        return 1
                await asyncio.sleep(max(0.2, self.poll - (time.monotonic() - t0)))
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)


def main() -> None:
    sys.exit(asyncio.run(Listener().run()))


if __name__ == "__main__":
    main()
