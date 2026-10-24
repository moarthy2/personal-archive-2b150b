from __future__ import annotations
import asyncio
import contextlib
import hashlib
import os
from telethon import utils
from telethon.tl.types import MessageMediaWebPage
from common import protocol as P
from common.crypto import encrypt_file, encrypt_file_with_sha256
MAX_TEXT = 4096
AVATAR_PIXELS = 48
AVATAR_MAX_BYTES = 12 * 1024
MAX_AVATARS_PER_REFRESH = 12

def classify_media(m) -> str | None:
    media = m.media
    if media is None or isinstance(media, MessageMediaWebPage):
        return None
    if m.photo:
        return 'photo'
    if m.gif:
        return 'gif'
    if m.video or m.video_note:
        return 'video'
    if m.sticker:
        return 'sticker'
    if m.voice:
        return 'voice'
    if m.audio:
        return 'audio'
    if m.document:
        mime = (getattr(m.file, 'mime_type', '') or '').lower()
        if mime.startswith('image/'):
            return 'photo'
        if mime.startswith('video/'):
            return 'video'
        return 'document'
    return 'other'

def _ts(dt) -> int:
    return int(dt.timestamp()) if dt else 0

async def serialize(m, allow_rpc: bool=False) -> dict:
    sender = getattr(m, '_sender', None)
    if sender is None and allow_rpc:
        with contextlib.suppress(Exception):
            sender = await m.get_sender()
    media_type = classify_media(m)
    rec = {'op': 'msg', 'id': m.id, 'chat_id': m.chat_id, 'date': _ts(m.date), 'out': bool(m.out), 'sender_id': m.sender_id, 'sender': utils.get_display_name(sender) or None if sender else None, 'text': m.raw_text or '', 'reply_to': m.reply_to_msg_id, 'media_type': media_type, 'fwd': bool(m.fwd_from)}
    if m.edit_date:
        rec['edit_date'] = _ts(m.edit_date)
    if m.action:
        rec['service'] = True
    if media_type == 'document':
        f = m.file
        name = (f.name if f and f.name else None) or 'file' + (f.ext or '' if f else '')
        rec['file'] = {'id': P.file_id_for(m.chat_id, m.id), 'name': P.safe_filename(name), 'size': int(getattr(f, 'size', 0) or 0), 'mime': getattr(f, 'mime_type', None)}
    return rec

async def record_message(ctx, m, allow_rpc: bool=False, update_preview: bool=False) -> bool:
    cid = m.chat_id
    if cid is None:
        return False
    seen = ctx.seen_set(cid)
    if m.id in seen:
        return False
    rec = await serialize(m, allow_rpc=allow_rpc)
    ctx.mark_seen(cid, m.id)
    ctx.bus.stage('a', P.messages_path(cid), ctx.codec.enc_line(P.pack_record(rec)))
    if update_preview:
        ctx.update_chat_preview(cid, rec, incoming_unread=not bool(m.out))
    if rec.get('file'):
        ctx.note_file(cid, m.id, rec['file'])
    return True

async def fetch_messages(ctx, chat_id: int, limit: int=40, offset_id: int=0) -> int:
    limit = max(1, min(int(limit), 200))
    msgs = []
    async for m in ctx.client.iter_messages(chat_id, limit=limit, offset_id=int(offset_id or 0)):
        msgs.append(m)
    new = 0
    for m in reversed(msgs):
        new += 1 if await record_message(ctx, m, allow_rpc=False, update_preview=False) else 0
    return new

def _avatar_version(dialog) -> str | None:
    photo = getattr(getattr(dialog, 'entity', None), 'photo', None)
    photo_id = getattr(photo, 'photo_id', None)
    return str(photo_id) if photo_id is not None else None

def _make_avatar(source: str, target: str) -> bool:
    try:
        from PIL import Image, ImageOps
        with Image.open(source) as image:
            image = ImageOps.fit(ImageOps.exif_transpose(image).convert('RGB'), (AVATAR_PIXELS, AVATAR_PIXELS), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
            image.save(target, 'JPEG', quality=28, subsampling=2, optimize=True, progressive=True)
        return os.path.getsize(target) <= AVATAR_MAX_BYTES
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(target)
        return False

async def _cache_avatar(ctx, dialog, previous: dict, budget: list[int], sem: asyncio.Semaphore) -> str | None:
    chat_id = int(dialog.id)
    version = _avatar_version(dialog)
    if not version:
        if previous.get('avatar'):
            ctx.bus.stage('d', P.avatar_path(chat_id, False))
            ctx.bus.stage('d', P.avatar_path(chat_id, True))
        return None
    encrypted = bool(ctx.codec.encrypt)
    destination = P.avatar_path(chat_id, encrypted)
    if previous.get('avatar') == version and ctx.bus.exists(destination):
        return version
    if budget[0] <= 0:
        return previous.get('avatar') if ctx.bus.exists(destination) else None
    entity = getattr(dialog, 'entity', None)
    if entity is None:
        return None
    budget[0] -= 1
    source = str(ctx.tmp / f'avatar-{abs(chat_id)}.source')
    target = str(ctx.tmp / f'avatar-{abs(chat_id)}.jpg')
    async with sem:
        try:
            downloaded = await ctx.client.download_profile_photo(entity, file=source, download_big=False)
        except Exception:
            return None
        ok = bool(downloaded) and await asyncio.to_thread(_make_avatar, str(downloaded), target)
        if not ok:
            with contextlib.suppress(OSError):
                os.unlink(str(downloaded or source))
            return None
        with contextlib.suppress(OSError):
            os.unlink(str(downloaded))
        if encrypted:
            encrypted_target = target + '.enc'
            await asyncio.to_thread(encrypt_file, ctx.codec.key, target, encrypted_target, f'av:{chat_id}'.encode())
            with contextlib.suppress(OSError):
                os.unlink(target)
            ctx.bus.stage('c', destination, src=encrypted_target, cleanup=True)
        else:
            ctx.bus.stage('c', destination, src=target, cleanup=True)
        other = P.avatar_path(chat_id, not encrypted)
        if other != destination and ctx.bus.exists(other):
            ctx.bus.stage('d', other)
        return version

async def refresh_chats(ctx, force: bool=True) -> int:
    try:
        old_doc = ctx.codec.dec_json(ctx.bus.read(P.CHATS_INDEX), {}) or {}
    except Exception:
        old_doc = {}
    old_list = old_doc.get('chats', []) if isinstance(old_doc.get('chats'), list) else []
    old_chats = {int(c.get('id')): c for c in old_list if isinstance(c, dict) and 'id' in c}
    dialogs = []
    chats = []
    latest_msg_ids: dict[int, int] = {}
    async for d in ctx.client.iter_dialogs(limit=int(ctx.cfg['max_dialogs'])):
        dialogs.append(d)
        if d.is_user:
            typ = 'private'
        elif d.is_group:
            typ = 'group'
        elif d.is_channel:
            typ = 'channel'
        else:
            typ = 'other'
        last = d.message
        if last and getattr(last, 'id', None) is not None:
            latest_msg_ids[int(d.id)] = int(last.id)
        chat = {'id': d.id, 'type': typ, 'title': d.name or '', 'unread': int(d.unread_count or 0), 'last_date': _ts(last.date) if last else 0, 'last_text': (last.raw_text or '')[:80] if last else '', 'last_media': classify_media(last) if last else None, 'pinned': bool(d.pinned), 'archived': bool(d.archived)}
        chats.append(chat)
    avatar_budget = [MAX_AVATARS_PER_REFRESH]
    sem = asyncio.Semaphore(4)
    avatars = await asyncio.gather(*(_cache_avatar(ctx, d, old_chats.get(int(d.id), {}), avatar_budget, sem) for d in dialogs))
    for chat, avatar in zip(chats, avatars):
        if avatar:
            chat['avatar'] = avatar
    ctx.chats_list = chats
    if force or chats != old_list or (not ctx.bus.exists(P.CHATS_INDEX)):
        ctx.bus.stage('w', P.CHATS_INDEX, ctx.codec.enc_json({'updated': P.now(), 'chats': chats}))
    ctx.chat_ids = [c['id'] for c in chats]
    ctx.dialog_last_msg = latest_msg_ids
    ctx.chats_dirty = avatar_budget[0] == 0
    return len(chats)

def _sha256(path: str) -> tuple[str, int]:
    h, n = (hashlib.sha256(), 0)
    with open(path, 'rb') as f:
        while (chunk := f.read(1024 * 1024)):
            h.update(chunk)
            n += len(chunk)
    return (h.hexdigest(), n)

async def download_file(ctx, chat_id: int, message_id: int) -> dict:
    fid = P.file_id_for(chat_id, message_id)
    m = await ctx.client.get_messages(chat_id, ids=message_id)
    if m is None or classify_media(m) != 'document':
        raise ValueError('This message is not a document; photos, GIFs and videos are not downloaded.')
    f = m.file
    size = int(getattr(f, 'size', 0) or 0)
    cap = int(ctx.cfg['max_file_mb']) * 1024 * 1024
    name = P.safe_filename((f.name if f and f.name else None) or 'file' + (f.ext or '' if f else ''))
    rec = ctx.files_index.setdefault(fid, {})
    rec.update({'file_id': fid, 'chat_id': chat_id, 'message_id': message_id, 'name': name, 'size': size, 'mime': getattr(f, 'mime_type', None)})
    if size > cap:
        rec['status'] = 'too_large_for_git'
        ctx.index_dirty = True
        return {'status': 'too_large_for_git', 'size': size, 'limit': cap}
    rec['status'] = 'downloading'
    ctx.index_dirty = True
    src = str(ctx.tmp / f'dl-{fid}')
    path = await ctx.client.download_media(m, file=src)
    if not path:
        rec['status'] = 'error'
        ctx.index_dirty = True
        raise RuntimeError('Download from Telegram failed')
    path = str(path)
    encrypted = ctx.codec.encrypt
    rel = f'{P.FILES_DIR}/{fid}/' + ('blob.enc' if encrypted else name)
    if encrypted:
        enc = path + '.enc'
        sha, real = await asyncio.to_thread(encrypt_file_with_sha256, ctx.codec.key, path, enc, f'file:{fid}'.encode())
        with contextlib.suppress(OSError):
            os.unlink(path)
        if real > cap:
            with contextlib.suppress(OSError):
                os.unlink(enc)
            rec.update(status='too_large_for_git', size=real)
            ctx.index_dirty = True
            return {'status': 'too_large_for_git', 'size': real, 'limit': cap}
        ctx.bus.stage('c', rel, src=enc, cleanup=True)
    else:
        sha, real = await asyncio.to_thread(_sha256, path)
        if real > cap:
            with contextlib.suppress(OSError):
                os.unlink(path)
            rec.update(status='too_large_for_git', size=real)
            ctx.index_dirty = True
            return {'status': 'too_large_for_git', 'size': real, 'limit': cap}
        ctx.bus.stage('c', rel, src=path, cleanup=True)
    rec.update(status='ready', blob_path=rel, sha256=sha, size=real, encrypted=encrypted, updated=P.now())
    ctx.index_dirty = True
    return {'status': 'ready', 'size': real}

def _int(v, name='value') -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        raise ValueError(f'Invalid {name}') from None

async def h_list_chats(ctx, cmd):
    return {'chats': await refresh_chats(ctx, force=True)}

async def h_get_messages(ctx, cmd):
    return {'new': await fetch_messages(ctx, _int(cmd.get('chat_id'), 'chat_id'), _int(cmd.get('limit', 40)), _int(cmd.get('offset_id', 0)))}

async def h_send(ctx, cmd):
    chat_id = _int(cmd.get('chat_id'), 'chat_id')
    text = str(cmd.get('text', ''))
    if not text.strip() or len(text) > MAX_TEXT:
        raise ValueError('The text is empty or longer than 4096 characters')
    reply = cmd.get('reply_to')
    m = await ctx.client.send_message(chat_id, text, reply_to=_int(reply) if reply else None)
    await record_message(ctx, m, allow_rpc=False, update_preview=True)
    return {'message_id': m.id}

async def h_edit(ctx, cmd):
    chat_id, mid = (_int(cmd.get('chat_id'), 'chat_id'), _int(cmd.get('message_id'), 'message_id'))
    text = str(cmd.get('text', ''))
    if not text.strip() or len(text) > MAX_TEXT:
        raise ValueError('Invalid text')
    m = await ctx.client.edit_message(chat_id, mid, text)
    ctx.stage_edit(chat_id, mid, m.raw_text or text, _ts(m.edit_date) or int(P.now()))
    return {}

async def h_delete(ctx, cmd):
    chat_id = _int(cmd.get('chat_id'), 'chat_id')
    ids = [_int(x) for x in cmd.get('message_ids') or []][:100]
    if not ids:
        raise ValueError('No ids were given')
    await ctx.client.delete_messages(chat_id, ids, revoke=True)
    for mid in ids:
        ctx.stage_delete(chat_id, mid)
    return {'deleted': len(ids)}

async def h_mark_read(ctx, cmd):
    cid = _int(cmd.get('chat_id'), 'chat_id')
    await ctx.client.send_read_acknowledge(cid)
    ctx.mark_chat_read_in_memory(cid)
    return {}
HANDLERS = {'list_chats': h_list_chats, 'get_messages': h_get_messages, 'send': h_send, 'edit': h_edit, 'delete': h_delete, 'mark_read': h_mark_read}
