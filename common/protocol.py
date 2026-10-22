"""ثابت‌ها و توابع مشترک بین سرور لوکال و بات (فقط کتابخانه‌ی استاندارد).

مسیرهای زیر همگی «داخل برنچ bus» هستند، نه روی main.
"""
from __future__ import annotations

import json
import re
import time
import uuid

BRANCH = "bus"
WORKFLOW_FILE = "listener.yml"
COORDINATOR_FILE = "coordinator.yml"
META_FILE = ".bridge-meta.json"
CODE_VERSION = "1.0.0"

# --- مسیرها داخل برنچ bus ---------------------------------------------------
CONFIG = "config.json"                       # تنظیمات (متن ساده، بدون داده‌ی حساس)
CHATS_INDEX = "data/chats_index.json"        # رمز می‌شود
FILES_INDEX = "data/files_index.json"        # رمز می‌شود
MESSAGES_DIR = "data/messages"               # هر خط رمز می‌شود
FILES_DIR = "data/files"                     # بلاب‌ها (در حالت رمز: chunked AES-GCM)
RESULTS = "data/results.json"                # رمز می‌شود
STATUS = "data/status.json"                  # متن ساده (بدون محتوای چت)
ACTION_HB = "data/action_heartbeat.json"     # متن ساده
PURGE_MARKER = "data/purge_marker.json"      # متن ساده

COMMANDS_DIR = "commands"
LOCAL_HB = "commands/local_heartbeat.json"   # متن ساده
STOP_CMD = "commands/stop.json"              # متن ساده
SESSION_REFRESHED = "commands/session_refreshed.json"
_RESERVED_COMMAND_FILES = {"local_heartbeat.json", "stop.json", "session_refreshed.json"}

# --- زمان‌بندی ---------------------------------------------------------------
ACTION_HB_INTERVAL = 20      # ثانیه
LOCAL_HB_INTERVAL = 30       # ثانیه
STALE_AFTER = 120            # ثانیه؛ بیش از این بدون تغییر = قطع
HANDOFF_AFTER = 5 * 3600 + 45 * 60   # ۵ ساعت و ۴۵ دقیقه

DEFAULT_BUS_CONFIG = {
    "encrypt": True,                 # رمزنگاری محتوای bus/data (برای ریپوی public اجباری)
    "max_file_mb": 90,               # سقف حجم فایل سند برای commit در git
    "auto_download_documents": False,
    "startup_backfill_chats": 15,    # تعداد چت‌هایی که هنگام شروع پیام‌های اخیرشان همگام می‌شود
    "backfill_messages": 20,
    "max_dialogs": 200,
    "local_timeout": STALE_AFTER,
    "squash_every_commits": 1000,
}

# --- ساعت --------------------------------------------------------------------
_clock_offset = 0.0
_clock_synced = False


def now() -> float:
    """زمان فعلی با اصلاح اختلاف ساعت دستگاه (تا مقایسه‌ی heartbeat بین دو سمت درست باشد)."""
    return time.time() + _clock_offset


def set_clock_offset(offset: float) -> None:
    global _clock_offset, _clock_synced
    _clock_offset = float(offset)
    _clock_synced = True


def clock_synced() -> bool:
    return _clock_synced


# --- JSON --------------------------------------------------------------------
def dumps(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def loads(data: bytes | str | None, default=None):
    if not data:
        return default
    try:
        return json.loads(data)
    except (ValueError, TypeError):
        return default


# --- شناسه‌ها و مسیرها -------------------------------------------------------
def new_command_id() -> str:
    return f"{int(time.time() * 1000):013d}-{uuid.uuid4().hex[:8]}"


def command_path(cid: str) -> str:
    if not re.fullmatch(r"[0-9]{13}-[0-9a-f]{8}", cid):
        raise ValueError("bad command id")
    return f"{COMMANDS_DIR}/{cid}.json"


def is_command_file(name: str) -> bool:
    return name.endswith(".json") and name not in _RESERVED_COMMAND_FILES and not name.startswith(".")


def messages_path(chat_id) -> str:
    return f"{MESSAGES_DIR}/{int(chat_id)}.jsonl"


_FILE_ID_RE = re.compile(r"^-?\d+_\d+$")


def valid_file_id(file_id: str) -> bool:
    return bool(_FILE_ID_RE.match(file_id or ""))


def file_id_for(chat_id, message_id) -> str:
    return f"{int(chat_id)}_{int(message_id)}"


def safe_filename(name: str | None, fallback: str = "file") -> str:
    name = (name or "").replace("\\", "/").split("/")[-1]
    name = re.sub(r"[\x00-\x1f\x7f<>:\"|?*]", "_", name).strip(" .")
    if not name:
        name = fallback
    if len(name) > 120:
        stem, dot, ext = name.rpartition(".")
        name = (stem[:100] + dot + ext[:10]) if dot and len(ext) <= 10 else name[:120]
    return name


def merge_bus_config(raw: dict | None) -> dict:
    cfg = dict(DEFAULT_BUS_CONFIG)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k not in cfg:
                continue
            default = cfg[k]
            if isinstance(default, bool):
                ok = isinstance(v, bool)
            elif isinstance(default, (int, float)):
                ok = isinstance(v, (int, float)) and not isinstance(v, bool)
            else:
                ok = isinstance(v, type(default))
            if ok:
                cfg[k] = v
    return cfg
