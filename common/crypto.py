from __future__ import annotations
import base64
import hashlib
import json
import os
import struct
import zlib
from collections.abc import Iterator
try:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError as e:
    raise ImportError('The "cryptography" package is not installed. On Termux: pkg install python-cryptography; elsewhere: pip install cryptography') from e
PBKDF2_ITERS = 600000
PASS_MAGIC = b'TGP1'
BLOB_MAGIC = b'TGE1'
STREAM_MAGIC = b'TGS1'
LINE_PREFIX = '~1:'
LINE_PREFIX_BYTES = b'~1:'
LEGACY_LINE_PREFIX_BYTES = b'ENC1:'
CHUNK = 1024 * 1024
_FLAG_RAW = b'\x00'
_FLAG_ZLIB = b'\x01'
_U32_BE = struct.Struct('>I')

class CryptoError(Exception):
    pass

class NeedKeyError(CryptoError):
    pass

def _key_magic(key: bytes, ctx: bytes) -> bytes:
    return hashlib.blake2s(key, person=ctx).digest()[:4]

def _derive(passphrase: str, salt: bytes, iters: int) -> bytes:
    return hashlib.pbkdf2_hmac('sha256', passphrase.encode('utf-8'), salt, iters, 32)

def encrypt_with_passphrase(plaintext: str | bytes, passphrase: str) -> str:
    data = plaintext.encode('utf-8') if isinstance(plaintext, str) else plaintext
    salt, nonce = (os.urandom(16), os.urandom(12))
    key = _derive(passphrase, salt, PBKDF2_ITERS)
    hdr = _key_magic(key, b'pass-hdr')
    ct = AESGCM(key).encrypt(nonce, data, hdr)
    return base64.b64encode(hdr + _U32_BE.pack(PBKDF2_ITERS) + salt + nonce + ct).decode('ascii')

def decrypt_with_passphrase(token: str, passphrase: str) -> bytes:
    try:
        raw = base64.b64decode(token.strip())
    except Exception as e:
        raise CryptoError('The encrypted text is not valid') from e
    if len(raw) < 4 + 4 + 16 + 12 + 16:
        raise CryptoError('The encrypted text has an invalid format')
    hdr = raw[:4]
    iters = _U32_BE.unpack_from(raw, 4)[0]
    if not 1000 <= iters <= 5000000:
        raise CryptoError('Invalid KDF parameter')
    salt, nonce, ct = (raw[8:24], raw[24:36], raw[36:])
    key = _derive(passphrase, salt, iters)
    expected_hdr = _key_magic(key, b'pass-hdr')
    if hdr == expected_hdr:
        aad = expected_hdr
    elif hdr == PASS_MAGIC:
        aad = b'tg-bridge/session'
    else:
        raise CryptoError('Wrong passphrase, or the data is corrupted')
    try:
        return AESGCM(key).decrypt(nonce, ct, aad)
    except (InvalidTag, ValueError) as e:
        raise CryptoError('Wrong passphrase, or the data is corrupted') from e

def generate_key() -> bytes:
    return AESGCM.generate_key(bit_length=256)

def key_to_b64(key: bytes) -> str:
    return base64.b64encode(key).decode('ascii')

def key_from_b64(text: str) -> bytes:
    key = base64.b64decode(text.strip())
    if len(key) != 32:
        raise CryptoError('The key must be 32 bytes')
    return key

def _pack_payload(data: bytes) -> bytes:
    if len(data) >= 48:
        comp = zlib.compress(data, level=6)
        if len(comp) + 1 < len(data):
            return _FLAG_ZLIB + comp
    return _FLAG_RAW + data

def _unpack_payload(pt: bytes) -> bytes:
    if not pt:
        return b''
    first = pt[0]
    if first == 1:
        try:
            return zlib.decompress(memoryview(pt)[1:])
        except zlib.error as e:
            raise CryptoError('Decompression failed (corrupted payload)') from e
    if first == 0:
        return pt[1:]
    return pt

def encrypt_bytes(key: bytes, data: bytes, aad: bytes=b'', *, _aes: AESGCM | None=None, _magic: bytes | None=None) -> bytes:
    aes = _aes or AESGCM(key)
    nonce = os.urandom(12)
    magic = _magic or _key_magic(key, b'blob-v2')
    return magic + nonce + aes.encrypt(nonce, _pack_payload(data), aad)

def decrypt_bytes(key: bytes, blob: bytes, aad: bytes=b'', *, _aes: AESGCM | None=None, _magic: bytes | None=None) -> bytes:
    if len(blob) < 4 + 12 + 16:
        raise CryptoError('Invalid blob format')
    mv = memoryview(blob)
    hdr = bytes(mv[:4])
    magic_v2 = _magic or _key_magic(key, b'blob-v2')
    if hdr != magic_v2 and hdr != BLOB_MAGIC:
        raise CryptoError('Invalid blob format or wrong key')
    aes = _aes or AESGCM(key)
    try:
        pt = aes.decrypt(bytes(mv[4:16]), bytes(mv[16:]), aad)
    except (InvalidTag, ValueError) as e:
        raise CryptoError('Decryption failed (wrong key or tampered data)') from e
    return _unpack_payload(pt) if hdr == magic_v2 else pt

def encrypt_file(key: bytes, src: str, dst: str, aad: bytes=b'') -> None:
    encrypt_file_with_sha256(key, src, dst, aad=aad)

def encrypt_file_with_sha256(key: bytes, src: str, dst: str, aad: bytes=b'') -> tuple[str, int]:
    aes = AESGCM(key)
    prefix = os.urandom(8)
    magic = _key_magic(key, b'strm-v2')
    h = hashlib.sha256()
    total = 0
    tag_more = b'M:' + aad if aad else b'M'
    tag_last = b'L:' + aad if aad else b'L'
    with open(src, 'rb') as f, open(dst, 'wb') as out:
        out.write(magic + prefix)
        idx = 0
        buf = f.read(CHUNK)
        if buf:
            h.update(buf)
            total += len(buf)
        while True:
            nxt = f.read(CHUNK)
            if nxt:
                h.update(nxt)
                total += len(nxt)
            last = not nxt
            out.write(aes.encrypt(prefix + _U32_BE.pack(idx), buf, tag_last if last else tag_more))
            if last:
                break
            buf, idx = (nxt, idx + 1)
    return (h.hexdigest(), total)

def iter_decrypt_file(key: bytes, path: str, aad: bytes=b'') -> Iterator[bytes]:
    aes = AESGCM(key)
    magic_v2 = _key_magic(key, b'strm-v2')
    tag_more = b'M:' + aad if aad else b'M'
    tag_last = b'L:' + aad if aad else b'L'
    with open(path, 'rb') as f:
        head = f.read(12)
        if len(head) != 12 or (head[:4] != magic_v2 and head[:4] != STREAM_MAGIC):
            raise CryptoError('Not a stream-encrypted file')
        prefix = head[4:]
        idx = 0
        buf = f.read(CHUNK + 16)
        while True:
            if len(buf) < 16:
                raise CryptoError('File decryption failed (corrupted or truncated)')
            nxt = f.read(CHUNK + 16)
            last = not nxt
            nonce = prefix + _U32_BE.pack(idx)
            try:
                if aad:
                    try:
                        yield aes.decrypt(nonce, buf, tag_last if last else tag_more)
                    except (InvalidTag, ValueError):
                        yield aes.decrypt(nonce, buf, b'L' if last else b'M')
                else:
                    yield aes.decrypt(nonce, buf, b'L' if last else b'M')
            except (InvalidTag, ValueError) as e:
                raise CryptoError('File decryption failed (corrupted or truncated)') from e
            if last:
                break
            buf, idx = (nxt, idx + 1)

def is_stream_encrypted(path: str, key: bytes | None=None) -> bool:
    try:
        with open(path, 'rb') as f:
            hdr = f.read(4)
            if len(hdr) != 4:
                return False
            if hdr == STREAM_MAGIC:
                return True
            if key and hdr == _key_magic(key, b'strm-v2'):
                return True
            return path.endswith('.enc') and (not hdr.startswith(b'\xff\xd8'))
    except OSError:
        return False

class Codec:

    def __init__(self, key: bytes | None=None, encrypt: bool=True):
        self.key = key
        self.encrypt = bool(encrypt and key)
        self._aes = AESGCM(key) if key else None
        self._magic = _key_magic(key, b'blob-v2') if key else None

    def _enc_text(self, raw: bytes) -> bytes:
        blob = encrypt_bytes(self.key, raw, _aes=self._aes, _magic=self._magic)
        return LINE_PREFIX_BYTES + base64.b64encode(blob)

    def _dec_text(self, data: bytes) -> bytes:
        if data.startswith(LINE_PREFIX_BYTES):
            if not self.key:
                raise NeedKeyError('The data is encrypted but the content key is not available')
            blob = base64.b64decode(memoryview(data)[len(LINE_PREFIX_BYTES):])
            return decrypt_bytes(self.key, blob, _aes=self._aes, _magic=self._magic)
        if data.startswith(LEGACY_LINE_PREFIX_BYTES):
            if not self.key:
                raise NeedKeyError('The data is encrypted but the content key is not available')
            blob = base64.b64decode(memoryview(data)[len(LEGACY_LINE_PREFIX_BYTES):])
            return decrypt_bytes(self.key, blob, _aes=self._aes, _magic=self._magic)
        return data

    def enc_json(self, obj) -> bytes:
        raw = json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        return self._enc_text(raw) if self.encrypt else raw

    def dec_json(self, data: bytes | None, default=None):
        if not data or not data.strip():
            return default
        raw = self._dec_text(data.strip())
        return json.loads(raw)

    def enc_line(self, obj) -> bytes:
        return self.enc_json(obj) + b'\n'

    def dec_lines(self, data: bytes | None) -> list:
        out = []
        if not data:
            return out
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(self._dec_text(line)))
            except NeedKeyError:
                raise
            except Exception:
                continue
        return out
