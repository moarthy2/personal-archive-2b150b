"""اجرای فرمان‌ها و تبدیل پیام‌های تلگرام به رکوردهای bus.

قانون ثابت: عکس / گیف / ویدیو / استیکر هرگز دانلود نمی‌شوند (حتی thumbnail)؛
فقط «برچسب نوع» ذخیره می‌شود. تنها فایل‌های سند (Document) قابل دانلودند.
"""
from __future__ import annotations

import asyncio
import hashlib
import os

from telethon import utils
from telethon.tl.types import MessageMediaWebPage

from common import protocol as P
from common.crypto import encrypt_file

MAX_TEXT = 4096


# --------------------------------------------------------------------------------------
# پیام‌ها
# --------------------------------------------------------------------------------------
def classify_media(m) -> str | None:
    media = m.media
    if media is None or isinstance(media, MessageMediaWebPage):
        return None  # پیش‌نمایش لینک رسانه حساب نمی‌شود (و هیچ‌وقت دانلود نمی‌شود)
    if m.photo:
        return "photo"
    if m.gif:
        return "gif"
    if m.video or m.video_note:
        return "video"
    if m.sticker:
        return "sticker"
    if m.voice:
        return "voice"
    if m.audio:
        return "audio"
    if m.document:
        mime = (getattr(m.file, "mime_type", "") or "").lower()
        if mime.startswith("image/"):
            return "photo"     # عکسی که به‌صورت فایل فرستاده شده هم «عکس» است
        if mime.startswith("video/"):
            return "video"
        return "document"
    return "other"


def _ts(dt) -> int:
    return int(dt.timestamp()) if dt else 0


async def serialize(m) -> dict:
    try:
        sender = await m.get_sender()
    except Exception:  # noqa: BLE001
        sender = None
    media_type = classify_media(m)
    rec = {
        "op": "msg", "id": m.id, "chat_id": m.chat_id, "date": _ts(m.date), "out": bool(m.out),
        "sender_id": m.sender_id, "sender": (utils.get_display_name(sender) or None) if sender else None,
        "text": m.raw_text or "", "reply_to": m.reply_to_msg_id, "media_type": media_type,
        "fwd": bool(m.fwd_from),
    }
    if m.edit_date:
        rec["edit_date"] = _ts(m.edit_date)
    if m.action:
        rec["service"] = True
    if media_type == "document":
        f = m.file
        name = (f.name if f and f.name else None) or ("file" + ((f.ext or "") if f else ""))
        rec["file"] = {"id": P.file_id_for(m.chat_id, m.id), "name": P.safe_filename(name),
                       "size": int(getattr(f, "size", 0) or 0), "mime": getattr(f, "mime_type", None)}
    return rec


async def record_message(ctx, m) -> bool:
    cid = m.chat_id
    if cid is None:
        return False
    seen = ctx.seen_set(cid)
    if m.id in seen:
        return False
    rec = await serialize(m)
    seen.add(m.id)
    ctx.bus.stage("a", P.messages_path(cid), ctx.codec.enc_line(rec))
    ctx.chats_dirty = True
    if rec.get("file"):
        ctx.note_file(cid, m.id, rec["file"])
    return True


async def fetch_messages(ctx, chat_id: int, limit: int = 40, offset_id: int = 0) -> int:
    limit = max(1, min(int(limit), 200))
    msgs = []
    async for m in ctx.client.iter_messages(chat_id, limit=limit, offset_id=int(offset_id or 0)):
        msgs.append(m)
    new = 0
    for m in reversed(msgs):           # قدیمی → جدید
        new += 1 if await record_message(ctx, m) else 0
    return new


# --------------------------------------------------------------------------------------
# فهرست چت‌ها
# --------------------------------------------------------------------------------------
async def refresh_chats(ctx) -> int:
    chats = []
    async for d in ctx.client.iter_dialogs(limit=int(ctx.cfg["max_dialogs"])):
        if d.is_user:
            typ = "private"
        elif d.is_group:
            typ = "group"
        elif d.is_channel:
            typ = "channel"
        else:
            typ = "other"
        last = d.message
        chats.append({
            "id": d.id, "type": typ, "title": d.name or "", "unread": int(d.unread_count or 0),
            "last_date": _ts(last.date) if last else 0,
            "last_text": ((last.raw_text or "")[:80]) if last else "",
            "last_media": classify_media(last) if last else None,
            "pinned": bool(d.pinned), "archived": bool(d.archived),
        })
    ctx.bus.stage("w", P.CHATS_INDEX, ctx.codec.enc_json({"updated": P.now(), "chats": chats}))
    ctx.chat_ids = [c["id"] for c in chats]      # در حافظه؛ فایل ممکن است هنوز commit نشده باشد
    ctx.chats_dirty = False
    return len(chats)


# --------------------------------------------------------------------------------------
# دانلود سند
# --------------------------------------------------------------------------------------
def _sha256(path: str) -> tuple[str, int]:
    h, n = hashlib.sha256(), 0
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
            n += len(chunk)
    return h.hexdigest(), n


async def download_file(ctx, chat_id: int, message_id: int) -> dict:
    fid = P.file_id_for(chat_id, message_id)
    m = await ctx.client.get_messages(chat_id, ids=message_id)
    if m is None or classify_media(m) != "document":
        raise ValueError("این پیام سند (Document) نیست؛ عکس، گیف و ویدیو دانلود نمی‌شوند.")
    f = m.file
    size = int(getattr(f, "size", 0) or 0)
    cap = int(ctx.cfg["max_file_mb"]) * 1024 * 1024
    name = P.safe_filename((f.name if f and f.name else None) or ("file" + ((f.ext or "") if f else "")))
    rec = ctx.files_index.setdefault(fid, {})
    rec.update({"file_id": fid, "chat_id": chat_id, "message_id": message_id, "name": name,
                "size": size, "mime": getattr(f, "mime_type", None)})
    if size > cap:
        rec["status"] = "too_large_for_git"
        ctx.index_dirty = True
        return {"status": "too_large_for_git", "size": size, "limit": cap}

    rec["status"] = "downloading"
    src = str(ctx.tmp / f"dl-{fid}")
    path = await ctx.client.download_media(m, file=src)
    if not path:
        rec["status"] = "error"
        ctx.index_dirty = True
        raise RuntimeError("دانلود از تلگرام ناموفق بود")
    path = str(path)
    sha, real = await asyncio.to_thread(_sha256, path)
    if real > cap:
        os.unlink(path)
        rec.update(status="too_large_for_git", size=real)
        ctx.index_dirty = True
        return {"status": "too_large_for_git", "size": real, "limit": cap}

    encrypted = ctx.codec.encrypt
    rel = f"{P.FILES_DIR}/{fid}/" + ("blob.enc" if encrypted else name)
    if encrypted:
        enc = path + ".enc"
        await asyncio.to_thread(encrypt_file, ctx.codec.key, path, enc)
        os.unlink(path)
        ctx.bus.stage("c", rel, src=enc, cleanup=True)
    else:
        ctx.bus.stage("c", rel, src=path, cleanup=True)
    rec.update(status="ready", blob_path=rel, sha256=sha, size=real, encrypted=encrypted, updated=P.now())
    ctx.index_dirty = True
    return {"status": "ready", "size": real}


# --------------------------------------------------------------------------------------
# فرمان‌ها
# --------------------------------------------------------------------------------------
def _int(v, name="مقدار") -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        raise ValueError(f"{name} نامعتبر است") from None


async def h_list_chats(ctx, cmd):
    return {"chats": await refresh_chats(ctx)}


async def h_get_messages(ctx, cmd):
    return {"new": await fetch_messages(ctx, _int(cmd.get("chat_id"), "chat_id"),
                                        _int(cmd.get("limit", 40)), _int(cmd.get("offset_id", 0)))}


async def h_send(ctx, cmd):
    chat_id = _int(cmd.get("chat_id"), "chat_id")
    text = str(cmd.get("text", ""))
    if not text.strip() or len(text) > MAX_TEXT:
        raise ValueError("متن خالی است یا از ۴۰۹۶ کاراکتر بیشتر است")
    reply = cmd.get("reply_to")
    m = await ctx.client.send_message(chat_id, text, reply_to=_int(reply) if reply else None)
    await record_message(ctx, m)
    return {"message_id": m.id}


async def h_edit(ctx, cmd):
    chat_id, mid = _int(cmd.get("chat_id"), "chat_id"), _int(cmd.get("message_id"), "message_id")
    text = str(cmd.get("text", ""))
    if not text.strip() or len(text) > MAX_TEXT:
        raise ValueError("متن نامعتبر است")
    m = await ctx.client.edit_message(chat_id, mid, text)
    ctx.stage_edit(chat_id, mid, m.raw_text or text, _ts(m.edit_date) or int(P.now()))
    return {}


async def h_delete(ctx, cmd):
    chat_id = _int(cmd.get("chat_id"), "chat_id")
    ids = [_int(x) for x in (cmd.get("message_ids") or [])][:100]
    if not ids:
        raise ValueError("شناسه‌ای داده نشده")
    await ctx.client.delete_messages(chat_id, ids, revoke=True)
    for mid in ids:
        ctx.stage_delete(chat_id, mid)
    return {"deleted": len(ids)}


async def h_mark_read(ctx, cmd):
    await ctx.client.send_read_acknowledge(_int(cmd.get("chat_id"), "chat_id"))
    ctx.chats_dirty = True
    return {}


HANDLERS = {
    "list_chats": h_list_chats,
    "get_messages": h_get_messages,
    "send": h_send,
    "edit": h_edit,
    "delete": h_delete,
    "mark_read": h_mark_read,
}
