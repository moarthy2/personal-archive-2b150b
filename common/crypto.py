"""رمزنگاری AES-256-GCM.

- رمزنگاری session با passphrase (PBKDF2-HMAC-SHA256 + AES-GCM) → متن base64
- رمزنگاری محتوای bus با یک کلید تصادفی ۳۲ بایتی (Codec)
- رمزنگاری جریانی فایل‌ها (chunk های ۱ مگابایتی؛ با تشخیص برش/دستکاری)
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
from typing import Iterator

try:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "پکیج «cryptography» نصب نیست. روی Termux: pkg install python-cryptography ؛ "
        "جاهای دیگر: pip install cryptography"
    ) from e

PBKDF2_ITERS = 600_000
PASS_MAGIC = b"TGP1"
BLOB_MAGIC = b"TGE1"
STREAM_MAGIC = b"TGS1"
LINE_PREFIX = "ENC1:"
CHUNK = 1024 * 1024


class CryptoError(Exception):
    pass


class NeedKeyError(CryptoError):
    pass


# --- passphrase (برای session) ----------------------------------------------
def _derive(passphrase: str, salt: bytes, iters: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", passphrase.encode("utf-8"), salt, iters, 32)


def encrypt_with_passphrase(plaintext: str | bytes, passphrase: str) -> str:
    data = plaintext.encode("utf-8") if isinstance(plaintext, str) else plaintext
    salt, nonce = os.urandom(16), os.urandom(12)
    key = _derive(passphrase, salt, PBKDF2_ITERS)
    ct = AESGCM(key).encrypt(nonce, data, b"tg-bridge/session")
    return base64.b64encode(PASS_MAGIC + struct.pack(">I", PBKDF2_ITERS) + salt + nonce + ct).decode("ascii")


def decrypt_with_passphrase(token: str, passphrase: str) -> bytes:
    try:
        raw = base64.b64decode(token.strip())
    except Exception as e:
        raise CryptoError("متن رمزشده معتبر نیست") from e
    if raw[:4] != PASS_MAGIC or len(raw) < 4 + 4 + 16 + 12 + 16:
        raise CryptoError("قالب متن رمزشده نامعتبر است")
    iters = struct.unpack(">I", raw[4:8])[0]
    if not (1000 <= iters <= 5_000_000):
        raise CryptoError("پارامتر KDF نامعتبر است")
    salt, nonce, ct = raw[8:24], raw[24:36], raw[36:]
    key = _derive(passphrase, salt, iters)
    try:
        return AESGCM(key).decrypt(nonce, ct, b"tg-bridge/session")
    except InvalidTag as e:
        raise CryptoError("passphrase اشتباه است یا داده خراب شده") from e


# --- کلید محتوا ---------------------------------------------------------------
def generate_key() -> bytes:
    return AESGCM.generate_key(bit_length=256)


def key_to_b64(key: bytes) -> str:
    return base64.b64encode(key).decode("ascii")


def key_from_b64(text: str) -> bytes:
    key = base64.b64decode(text.strip())
    if len(key) != 32:
        raise CryptoError("کلید باید ۳۲ بایت باشد")
    return key


# --- بلاب‌های کوچک -------------------------------------------------------------
def encrypt_bytes(key: bytes, data: bytes, aad: bytes = b"") -> bytes:
    nonce = os.urandom(12)
    return BLOB_MAGIC + nonce + AESGCM(key).encrypt(nonce, data, aad)


def decrypt_bytes(key: bytes, blob: bytes, aad: bytes = b"") -> bytes:
    if blob[:4] != BLOB_MAGIC or len(blob) < 4 + 12 + 16:
        raise CryptoError("قالب بلاب نامعتبر")
    try:
        return AESGCM(key).decrypt(blob[4:16], blob[16:], aad)
    except InvalidTag as e:
        raise CryptoError("رمزگشایی ناموفق (کلید اشتباه یا داده‌ی دستکاری‌شده)") from e


# --- فایل‌های بزرگ (جریانی) ------------------------------------------------------
def encrypt_file(key: bytes, src: str, dst: str) -> None:
    aes = AESGCM(key)
    prefix = os.urandom(8)
    with open(src, "rb") as f, open(dst, "wb") as out:
        out.write(STREAM_MAGIC + prefix)
        idx = 0
        buf = f.read(CHUNK)
        while True:
            nxt = f.read(CHUNK)
            last = not nxt
            out.write(aes.encrypt(prefix + struct.pack(">I", idx), buf, b"L" if last else b"M"))
            if last:
                break
            buf, idx = nxt, idx + 1


def iter_decrypt_file(key: bytes, path: str) -> Iterator[bytes]:
    aes = AESGCM(key)
    with open(path, "rb") as f:
        head = f.read(12)
        if head[:4] != STREAM_MAGIC or len(head) != 12:
            raise CryptoError("فایل رمزنگاری‌شده‌ی جریانی نیست")
        prefix = head[4:]
        idx = 0
        buf = f.read(CHUNK + 16)
        while True:
            nxt = f.read(CHUNK + 16)
            last = not nxt
            try:
                yield aes.decrypt(prefix + struct.pack(">I", idx), buf, b"L" if last else b"M")
            except InvalidTag as e:
                raise CryptoError("رمزگشایی فایل ناموفق (خراب یا ناقص)") from e
            if last:
                break
            buf, idx = nxt, idx + 1


def is_stream_encrypted(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == STREAM_MAGIC
    except OSError:
        return False


# --- Codec: رمزنگاری JSON و jsonl --------------------------------------------------
class Codec:
    """اگر encrypt=True و key موجود باشد، خروجی رمز می‌شود؛ ورودی‌ی رمزشده همیشه (با کلید) خوانده می‌شود."""

    def __init__(self, key: bytes | None = None, encrypt: bool = True):
        self.key = key
        self.encrypt = bool(encrypt and key)

    def _enc_text(self, raw: bytes) -> bytes:
        return (LINE_PREFIX + base64.b64encode(encrypt_bytes(self.key, raw)).decode("ascii")).encode("ascii")

    def _dec_text(self, data: bytes):
        if data.startswith(LINE_PREFIX.encode()):
            if not self.key:
                raise NeedKeyError("داده رمز شده ولی کلید محتوا در دسترس نیست")
            blob = base64.b64decode(data[len(LINE_PREFIX):])
            return decrypt_bytes(self.key, blob)
        return data

    def enc_json(self, obj) -> bytes:
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return self._enc_text(raw) if self.encrypt else raw

    def dec_json(self, data: bytes | None, default=None):
        if not data or not data.strip():
            return default
        raw = self._dec_text(data.strip())
        return json.loads(raw)

    def enc_line(self, obj) -> bytes:
        return self.enc_json(obj) + b"\n"

    def dec_lines(self, data: bytes | None) -> list:
        out = []
        if not data:
            return out
        for line in data.split(b"\n"):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(self._dec_text(line)))
            except NeedKeyError:
                raise
            except Exception:
                continue  # خط خراب/ناقص را نادیده بگیر
        return out
