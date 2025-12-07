# main.py — PART 1 (تا قبل از poller / admin callbacks / entrypoint)
# اصلاح‌شده برای: شیت-padding، backoff، wipe-all, ایمیل اجباری برای اولین ورود،
# سازگاری env varهای مختلف برای channel ids، و دکمهٔ تک‌کلیک بررسی عضویت.

import os
import json
import time
import asyncio
import logging
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from aiohttp import web
from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from google.oauth2 import service_account
import gspread
import base64
import binascii
from gspread.exceptions import APIError

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
logger = logging.getLogger("telegram-sub-bot")

# -------------------------
# Config via ENV (with fallback names)
# -------------------------
def env_any(*names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

TOKEN = env_any("TELEGRAM_TOKEN", "BOT_TOKEN")
ADMIN_TELEGRAM_ID = env_any("ADMIN_TELEGRAM_ID", "PLATFORM_ADMIN")
SPREADSHEET_ID = env_any("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_ENV = env_any("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = env_any("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
PORT = int(env_any("PORT", default="8000"))
# channel envs: accept several naming conventions (you had mismatch previously)
NORMAL_CHANNEL_ID = env_any("NORMAL_CHANNEL_ID", "CHANNEL_NORMAL_ID", "CHANNEL_NORMAL")
PREMIUM_CHANNEL_ID = env_any("PREMIUM_CHANNEL_ID", "CHANNEL_PREMIUM_CHANNEL_ID", "CHANNEL_PREMIUM")
TEST_CHANNEL_ID = env_any("TEST_CHANNEL_ID", "CHANNEL_TEST_CHANNEL_ID", "CHANNEL_TEST")
REQUIRED_CHANNELS = env_any("REQUIRED_CHANNELS", "")  # comma separated
INSTANCE_MODE = env_any("INSTANCE_MODE", "polling").lower()

# optional: list of admin ids (comma-separated)
ADMINS_ENV = env_any("ADMINS", "")
ADMINS: List[int] = []
if ADMINS_ENV:
    for s in ADMINS_ENV.split(","):
        s = s.strip()
        if s:
            try:
                ADMINS.append(int(s))
            except Exception:
                pass
# ensure ADMIN_TELEGRAM_ID numeric if given as plain id
try:
    if ADMIN_TELEGRAM_ID and str(ADMIN_TELEGRAM_ID).isdigit():
        ADMIN_TELEGRAM_ID = str(int(ADMIN_TELEGRAM_ID))
except Exception:
    pass

# basic validation
if not TOKEN:
    logger.error("Missing TELEGRAM_TOKEN env var. Set TELEGRAM_TOKEN.")
    raise SystemExit("Missing TELEGRAM_TOKEN")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID env var.")
    raise SystemExit("Missing SPREADSHEET_ID")

REQUIRED_CHANNELS_LIST = [c.strip() for c in REQUIRED_CHANNELS.split(",") if c.strip()]

# -------------------------
# Google Sheets setup (gspread) — robust loader
# -------------------------
def load_google_creds_info() -> Dict[str, Any]:
    if GOOGLE_CREDENTIALS_ENV:
        s = GOOGLE_CREDENTIALS_ENV.strip()
        # raw json
        try:
            data = json.loads(s)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return data
        except Exception:
            logger.debug("GOOGLE_CREDENTIALS raw parse failed.")
        # try substring
        try:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = s[start:end+1]
                data = json.loads(candidate)
                logger.info("Recovered JSON substring from GOOGLE_CREDENTIALS.")
                return data
        except Exception:
            logger.debug("substring recovery failed")
        # try base64
        try:
            decoded = base64.b64decode(s, validate=True)
            data = json.loads(decoded.decode("utf-8"))
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64).")
            return data
        except Exception:
            logger.debug("GOOGLE_CREDENTIALS not valid base64 or parse failed.")
    # fallback to file
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info("Loaded Google credentials from file %s", GOOGLE_SERVICE_ACCOUNT_FILE)
                return data
        except Exception as e:
            logger.exception("Failed to load GOOGLE_SERVICE_ACCOUNT file: %s", e)
    logger.error("No Google credentials found. Set GOOGLE_CREDENTIALS or upload service-account.json.")
    raise SystemExit("Missing Google credentials")

gc = None
try:
    creds_info = load_google_creds_info()
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    logger.info("gspread client initialized.")
except Exception as e:
    logger.exception("Failed to initialize gspread: %s", e)
    raise SystemExit("Failed to init Google Sheets client")

# -------------------------
# Sheet names and headers
# -------------------------
USERS_SHEET = "Users"
PURCHASES_SHEET = "Purchases"
REFERRALS_SHEET = "Referrals"
SUPPORT_SHEET = "Support"
SUBS_SHEET = "Subscription"
CONFIG_SHEET = "Config"

HEADERS = {
    USERS_SHEET: ["telegram_id", "full_name", "email", "referral_code", "referred_by", "status", "purchase_status", "expires_at", "created_at", "last_seen", "notes"],
    PURCHASES_SHEET: ["telegram_id", "full_name", "email", "product", "amount", "transaction_info", "status", "request_at", "activated_at", "expires_at", "joined_at", "left_at", "admin_note"],
    REFERRALS_SHEET: ["telegram_id", "referral_code", "referred_count", "created_at"],
    SUPPORT_SHEET: ["ticket_id", "telegram_id", "full_name", "subject", "message", "status", "created_at", "response", "responded_at"],
    SUBS_SHEET: ["telegram_id", "product", "activated_at", "expires_at", "active"],
    CONFIG_SHEET: ["key", "value"],
}

# -------------------------
# Helpers for robust Sheets access
# -------------------------
_SPREADSHEET_CACHE = None
_WORKSHEET_CACHE: dict = {}
_open_by_key_last = {"key": None, "ts": 0.0}

def pad_row_to_header(row: List[Any], ws_name: str) -> List[str]:
    """Normalize row length to header length (stringify values)."""
    header = HEADERS.get(ws_name, [])
    row_out = ["" if x is None else str(x) for x in list(row) if row is not None]
    while len(row_out) < len(header):
        row_out.append("")
    # keep extra columns if longer than header (rare)
    return row_out[:max(len(header), len(row_out))]

def open_sheet_with_backoff(spreadsheet_key: str, max_retries: int = 6, base_wait: float = 1.0):
    global _SPREADSHEET_CACHE, _open_by_key_last
    try:
        if _SPREADSHEET_CACHE and _open_by_key_last.get("key") == spreadsheet_key:
            return _SPREADSHEET_CACHE
    except Exception:
        pass
    attempt = 0
    wait = base_wait
    while attempt < max_retries:
        attempt += 1
        try:
            sh = gc.open_by_key(spreadsheet_key)
            _SPREADSHEET_CACHE = sh
            _open_by_key_last["key"] = spreadsheet_key
            _open_by_key_last["ts"] = time.time()
            return sh
        except APIError as e:
            txt = str(e)
            if any(x in txt for x in ("429", "Quota exceeded", "rateLimitExceeded", "userRateLimitExceeded")):
                logger.warning("open_sheet_with_backoff attempt %s due to quota/rate-limit. sleeping %.1fs", attempt, wait)
                time.sleep(wait + random.random() * 0.5)
                wait = min(wait * 2, 60)
                continue
            logger.exception("open_sheet_with_backoff non-retryable APIError: %s", e)
            raise
        except Exception as e:
            logger.exception("open_sheet_with_backoff unexpected error: %s", e)
            time.sleep(wait)
            wait = min(wait * 2, 60)
    raise RuntimeError("open_sheet_with_backoff failed after retries")

def open_sheet(ws_name: str):
    """Open worksheet robustly and ensure header exists (non-destructive)."""
    global _WORKSHEET_CACHE
    try:
        if ws_name in _WORKSHEET_CACHE:
            try:
                _ = _WORKSHEET_CACHE[ws_name].title
                return _WORKSHEET_CACHE[ws_name]
            except Exception:
                _WORKSHEET_CACHE.pop(ws_name, None)

        sh = open_sheet_with_backoff(SPREADSHEET_ID)
        try:
            w = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            logger.info("Worksheet %s not found. Creating.", ws_name)
            w = sh.add_worksheet(title=ws_name, rows="1000", cols="20")
        # ensure header (non-destructive)
        try:
            vals = w.get_all_values()
            if not vals or (len(vals) == 0) or (not vals[0] or str(vals[0][0]).strip() == ""):
                header = HEADERS.get(ws_name, [])
                if header:
                    try:
                        w.insert_row(header, index=1)
                        logger.info("Inserted header for %s", ws_name)
                    except Exception:
                        try:
                            w.update("A1:1", [header])
                            logger.info("Updated header for %s via update", ws_name)
                        except Exception:
                            logger.exception("Failed to set header for %s", ws_name)
        except APIError as e:
            logger.exception("open_sheet: APIError checking header for %s: %s", ws_name, e)
            raise
        _WORKSHEET_CACHE[ws_name] = w
        return w
    except Exception:
        logger.exception("Failed to open spreadsheet: %s", ws_name)
        raise

# helper wrappers
async def sheets_get_all(ws_name: str) -> List[List[str]]:
    try:
        w = open_sheet(ws_name)
        return w.get_all_values()
    except APIError as e:
        txt = str(e)
        logger.exception("sheets_get_all APIError for %s: %s", ws_name, e)
        # transient handling: return [] so caller can handle
        return []
    except Exception:
        logger.exception("sheets_get_all failed for %s", ws_name)
        return []

async def sheets_append(ws_name: str, row: List[Any]) -> bool:
    try:
        r = pad_row_to_header(row, ws_name)
        w = open_sheet(ws_name)
        w.append_row(r, value_input_option="USER_ENTERED")
        _WORKSHEET_CACHE.pop(ws_name, None)
        return True
    except APIError as e:
        txt = str(e)
        logger.exception("sheets_append APIError for %s: %s", ws_name, e)
        return False
    except Exception:
        logger.exception("sheets_append failed for %s", ws_name)
        return False

async def sheets_append_return_index(ws_name: str, row: List[Any]) -> int:
    ok = await sheets_append(ws_name, row)
    if not ok:
        return -1
    vals = await sheets_get_all(ws_name)
    return len(vals)

async def sheets_update_row(ws_name: str, row_idx: int, values: List[Any]) -> bool:
    try:
        vals = pad_row_to_header(values, ws_name)
        w = open_sheet(ws_name)
        cell_range = f"A{row_idx}:{gspread.utils.rowcol_to_a1(row_idx, len(vals))}"
        w.update(range_name=cell_range, values=[vals])
        _WORKSHEET_CACHE.pop(ws_name, None)
        return True
    except APIError as e:
        logger.exception("sheets_update_row APIError for %s row %s: %s", ws_name, row_idx, e)
        return False
    except Exception:
        logger.exception("sheets_update_row failed for %s row %s", ws_name, row_idx)
        return False

def fix_sheet_header(ws_name: str, force_clear: bool = False) -> bool:
    """Non-destructive attempt to ensure header is present and correct.
       If force_clear=True the sheet is cleared and header written (use with care)."""
    try:
        sh = open_sheet_with_backoff(SPREADSHEET_ID)
        try:
            w = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            w = sh.add_worksheet(title=ws_name, rows="1000", cols="20")
        header = HEADERS.get(ws_name, [])
        if force_clear:
            w.clear()
            if header:
                w.insert_row(header, index=1)
            logger.info("Force-cleared and wrote header for %s", ws_name)
            return True
        vals = w.get_all_values()
        first = vals[0] if vals and len(vals) > 0 else []
        if not first or (str(first[0]).strip().lower() != (header[0].lower() if header else "")):
            # insert header non-destructively
            try:
                w.insert_row(header, index=1)
                logger.info("Inserted header row for %s (non-destructive).", ws_name)
                return True
            except Exception:
                try:
                    w.clear()
                    w.insert_row(header, index=1)
                    logger.info("Cleared and wrote header for %s as fallback.", ws_name)
                    return True
                except Exception:
                    logger.exception("Could not repair header for %s", ws_name)
                    return False
        return True
    except Exception:
        logger.exception("fix_sheet_header failed for %s", ws_name)
        return False

# -------------------------
# Helpers (time, referral, parse)
# -------------------------
def now_iso() -> str:
    try:
        return datetime.now(tz=ZoneInfo("Asia/Tehran")).replace(microsecond=0).isoformat()
    except Exception:
        return datetime.utcnow().replace(microsecond=0).isoformat()

def parse_iso_or_none(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def generate_referral_code(length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def find_user_by_referral_code(code: str) -> Optional[str]:
    try:
        w = open_sheet(USERS_SHEET)
        vals = w.get_all_values()
        for row in vals[1:]:
            if len(row) > 3 and row[3].strip().upper() == code.strip().upper():
                return row[0]
    except Exception:
        logger.exception("find_user_by_referral_code error")
    return None

def increment_referral_count(referrer_id: str):
    try:
        w = open_sheet(REFERRALS_SHEET)
        vals = w.get_all_values()
        header = HEADERS.get(REFERRALS_SHEET, [])
        for idx, row in enumerate(vals[1:], start=2):
            if len(row) > 0 and str(row[0]) == str(referrer_id):
                while len(row) < len(header):
                    row.append("")
                try:
                    cur = int(row[2]) if row[2] else 0
                except Exception:
                    cur = 0
                row[2] = str(cur + 1)
                w.update(f"A{idx}:{gspread.utils.rowcol_to_a1(idx, len(row))}", [pad_row_to_header(row, REFERRALS_SHEET)])
                return
        new_row = [str(referrer_id), "", "1", now_iso()]
        new_row = pad_row_to_header(new_row, REFERRALS_SHEET)
        w.append_row(new_row, value_input_option="USER_ENTERED")
    except Exception:
        logger.exception("increment_referral_count failed for %s", referrer_id)

# -------------------------
# Bot & dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# utils: is_admin
def is_admin(user_id: int) -> bool:
    if ADMINS:
        return int(user_id) in ADMINS
    if ADMIN_TELEGRAM_ID:
        try:
            return str(user_id) == str(ADMIN_TELEGRAM_ID)
        except Exception:
            pass
    return False

async def notify_admins(text: str, reply_markup=None):
    """Notify configured admins (ADMINS list first, fallback to ADMIN_TELEGRAM_ID)."""
    try:
        if ADMINS:
            for aid in ADMINS:
                try:
                    await bot.send_message(int(aid), text, reply_markup=reply_markup)
                except Exception:
                    logger.exception("Failed to notify admin %s", aid)
        elif ADMIN_TELEGRAM_ID:
            try:
                await bot.send_message(int(ADMIN_TELEGRAM_ID), text, reply_markup=reply_markup)
            except Exception:
                logger.exception("Failed to notify single ADMIN_TELEGRAM_ID")
    except Exception:
        logger.exception("notify_admins failed")

# send_and_record: single last bot message per user (helps avoid stacking many messages)
_last_bot_messages: Dict[int, int] = {}  # user_id -> message_id

async def send_and_record(user_id: int, text: str, **kwargs):
    try:
        prev = _last_bot_messages.get(user_id)
        if prev:
            try:
                await bot.delete_message(chat_id=user_id, message_id=prev)
            except Exception:
                pass
        msg = await bot.send_message(user_id, text, **kwargs)
        try:
            _last_bot_messages[user_id] = msg.message_id
        except Exception:
            pass
        return msg
    except Exception:
        logger.exception("send_and_record failed for %s", user_id)
        try:
            return await bot.send_message(user_id, text, **kwargs)
        except Exception:
            return None

# -------------------------
# Invite and trial management
# -------------------------
scheduled_removals: Dict[int, asyncio.Task] = {}

async def create_temporary_invite(chat_id: str, expire_seconds: int = 600, member_limit: int = 1) -> Optional[str]:
    try:
        expire_date = int((datetime.utcnow() + timedelta(seconds=expire_seconds)).timestamp())
        link = await bot.create_chat_invite_link(chat_id=chat_id, expire_date=expire_date, member_limit=member_limit)
        invite_url = link.invite_link
        logger.info("Created invite link for chat %s", chat_id)
        return invite_url
    except Exception as e:
        logger.exception("Failed to create invite link for %s: %s", chat_id, e)
        return None

async def remove_user_from_chat(chat_id: str, user_id: int) -> bool:
    try:
        await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        await asyncio.sleep(0.6)
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        logger.info("Removed user %s from chat %s", user_id, chat_id)
        return True
    except Exception as e:
        logger.exception("Failed to remove user %s from chat %s: %s", user_id, chat_id, e)
        return False

async def schedule_remove_after(chat_id: str, user_id: int, delay_seconds: int = 600, purchase_row_idx: Optional[int] = None):
    if user_id in scheduled_removals:
        scheduled_removals[user_id].cancel()
    async def job():
        try:
            await asyncio.sleep(delay_seconds)
            ok = await remove_user_from_chat(chat_id, user_id)
            if ok and purchase_row_idx and purchase_row_idx > 0:
                try:
                    rows = await sheets_get_all(PURCHASES_SHEET)
                    if rows and len(rows) >= purchase_row_idx:
                        row = rows[purchase_row_idx - 1]
                        while len(row) < len(HEADERS[PURCHASES_SHEET]):
                            row.append("")
                        left_col_index = HEADERS[PURCHASES_SHEET].index("left_at")
                        row[left_col_index] = now_iso()
                        await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, pad_row_to_header(row, PURCHASES_SHEET))
                except Exception:
                    logger.exception("Failed to update left_at for purchase row %s", purchase_row_idx)
            try:
                kb = build_main_keyboard()
                await bot.send_message(user_id, "⏳ مدت تست کانال به پایان رسید. برای ادامه اشتراک‌ها به منو مراجعه کنید.", reply_markup=kb)
            except Exception:
                pass
        except asyncio.CancelledError:
            logger.info("Scheduled removal for %s cancelled.", user_id)
        finally:
            scheduled_removals.pop(user_id, None)
    task = asyncio.create_task(job())
    scheduled_removals[user_id] = task

# -------------------------
# Keyboards & UI
# -------------------------
def build_main_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["تست کانال معمولی", "خرید کانال معمولی"],
        ["خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم"]
    ]
    for row in buttons:
        keyboard.row(*[types.KeyboardButton(b) for b in row])
    return keyboard

def admin_confirm_keyboard(purchase_row_index: int, user_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("تأیید خرید ✅", callback_data=f"confirm:{purchase_row_index}:{user_id}"))
    kb.add(types.InlineKeyboardButton("رد خرید ❌", callback_data=f"reject:{purchase_row_index}:{user_id}"))
    return kb

# -------------------------
# Membership helpers و دکمهٔ بررسی عضویت تک‌کلیک
# -------------------------
async def is_member_of(chat_id: str, user_id: int) -> bool:
    try:
        mem = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = getattr(mem, "status", None)
        return status not in ("left", "kicked", None)
    except Exception as e:
        # لاگ دقیق (Member list is inaccessible هم برمی‌گردد)
        logger.warning("is_member_of error for chat %s user %s: %s", chat_id, user_id, e)
        return False

async def enforce_required_channels(user_id: int) -> Tuple[bool, List[str]]:
    not_member = []
    if not REQUIRED_CHANNELS_LIST:
        return True, []
    for ch in REQUIRED_CHANNELS_LIST:
        ch = ch.strip()
        if not ch:
            continue
        try:
            ok = await is_member_of(ch, user_id)
            if not ok:
                not_member.append(ch)
        except Exception as e:
            logger.exception("enforce_required_channels check error for %s %s", ch, e)
            not_member.append(ch)
    return (len(not_member) == 0, not_member)

# -------------------------
# ensure_user_row_and_return (safe + padding)
# -------------------------
def ensure_user_row_and_return(user: types.User, email: Optional[str] = None) -> Tuple[int, List[str]]:
    w = open_sheet(USERS_SHEET)
    values = w.get_all_values()
    header = values[0] if values else HEADERS[USERS_SHEET]
    for idx, row in enumerate(values[1:], start=2):
        try:
            if len(row) > 0 and str(row[0]) == str(user.id):
                changed = False
                while len(row) < len(header):
                    row.append("")
                if email and (not row[2]):
                    row[2] = email
                    changed = True
                if not row[1]:
                    row[1] = user.full_name or ""
                    changed = True
                if row[9] != now_iso():
                    row[9] = now_iso()
                    changed = True
                if changed:
                    w.update(f"A{idx}:{gspread.utils.rowcol_to_a1(idx, len(row))}", [pad_row_to_header(row, USERS_SHEET)])
                return idx, pad_row_to_header(row, USERS_SHEET)
        except Exception:
            continue
    referral = generate_referral_code()
    created_at = now_iso()
    new_row = [str(user.id), user.full_name or "", email or "", referral, "", "active", "none", "", created_at, now_iso(), ""]
    w.append_row(pad_row_to_header(new_row, USERS_SHEET), value_input_option="USER_ENTERED")
    vals2 = w.get_all_values()
    return len(vals2), pad_row_to_header(vals2[-1], USERS_SHEET)

# -------------------------
# Handlers: /start, membership check, email, platform_info, support, test, buy
# -------------------------
# جایگزین /start handler با لاگ و گزارش بهتر
import traceback

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        # parse possible referral: /start REF
        parts = (message.text or "").split()
        ref_code = parts[1].strip() if len(parts) > 1 else ""

        # quick env sanity checks (fail early with user-friendly msgs)
        missing_env = []
        for name in ("SPREADSHEET_ID",):
            if not globals().get(name) and not os.getenv(name):
                missing_env.append(name)
        if missing_env:
            logger.error("Missing required env(s): %s", missing_env)
            await message.answer("⚠️ پیکربندی ربات ناقص است. لطفاً ادمین را مطلع کنید.")
            # notify admin with details if configured
            try:
                if ADMIN_TELEGRAM_ID:
                    await bot.send_message(int(ADMIN_TELEGRAM_ID),
                                           f"Missing envs for /start: {missing_env} (user {message.from_user.id})")
            except Exception:
                logger.exception("Failed to notify admin about missing envs")
            return

        # membership check (wrap to catch any unexpected sheet/bot errors)
        try:
            ok_membership, missing = await enforce_required_channels(message.from_user.id)
        except Exception:
            tb = traceback.format_exc()
            logger.exception("enforce_required_channels failed in /start: %s", tb)
            # inform admin
            try:
                if ADMIN_TELEGRAM_ID:
                    await bot.send_message(int(ADMIN_TELEGRAM_ID), f"Error in enforce_required_channels (/start):\n{tb}")
            except Exception:
                logger.exception("Failed to notify admin about enforce_required_channels error")
            await message.answer("خطا در بررسی عضویت. ادمین اطلاع داده شد. لطفاً بعدا تلاش کنید.")
            return

        if not ok_membership:
            # build inline keyboard with channel links and a check button
            kb = types.InlineKeyboardMarkup(row_width=1)
            for ch in missing:
                if isinstance(ch, str) and ch.startswith("@"):
                    kb.add(types.InlineKeyboardButton(text=f"عضویت در {ch}", url=f"https://t.me/{ch.lstrip('@')}"))
                else:
                    # numeric id or unknown -> show text + try t.me link
                    try:
                        username_try = str(ch).lstrip("-100")
                        kb.add(types.InlineKeyboardButton(text=f"عضویت در {ch}", url=f"https://t.me/{username_try}"))
                    except Exception:
                        kb.add(types.InlineKeyboardButton(text=str(ch), callback_data="noop"))
            kb.add(types.InlineKeyboardButton("✅ بررسی عضویت", callback_data="check_membership"))
            if ADMIN_TELEGRAM_ID:
                try:
                    kb.add(types.InlineKeyboardButton("👤 تماس با ادمین", url=f"https://t.me/{ADMIN_TELEGRAM_ID.lstrip('@')}" if isinstance(ADMIN_TELEGRAM_ID, str) else f"https://t.me/{ADMIN_TELEGRAM_ID}"))
                except Exception:
                    pass

            await send_and_record(message.from_user.id,
                                  "⚠️ برای استفاده از ربات باید در کانال(های) زیر عضو شوید. پس از عضویت روی «✅ بررسی عضویت» بزنید:",
                                  reply_markup=kb, disable_web_page_preview=True)
            # store referral if present (best-effort)
            if ref_code:
                try:
                    idx, urow = ensure_user_row_and_return(message.from_user)
                    while len(urow) < len(HEADERS[USERS_SHEET]):
                        urow.append("")
                    urow[HEADERS[USERS_SHEET].index("referred_by")] = ref_code
                    open_sheet(USERS_SHEET).update(f"A{idx}:{gspread.utils.rowcol_to_a1(idx, len(urow))}", [pad_row_to_header(urow, USERS_SHEET)])
                except Exception:
                    logger.exception("Failed to write referral on start (non-fatal)")
            return

        # membership ok -> ensure user row
        try:
            idx, urow = ensure_user_row_and_return(message.from_user)
        except Exception:
            tb = traceback.format_exc()
            logger.exception("ensure_user_row_and_return failed: %s", tb)
            try:
                if ADMIN_TELEGRAM_ID:
                    await bot.send_message(int(ADMIN_TELEGRAM_ID), f"Error ensure_user_row_and_return (/start):\n{tb}")
            except Exception:
                pass
            await message.answer("خطا در ثبت داخلی اطلاعات. ادمین مطلع شد.")
            return

        # if user has no active subscription -> show buy buttons
        try:
            if not has_active_subscription(message.from_user.id):
                kb_buy = types.ReplyKeyboardMarkup(resize_keyboard=True)
                kb_buy.row("خرید کانال معمولی", "خرید کانال ویژه")
                await send_and_record(message.from_user.id, "⚠️ اشتراک شما فعال نیست یا منقضی شده است. برای ادامه لطفاً اشتراک تهیه کنید:", reply_markup=kb_buy)
                return
        except Exception:
            logger.exception("has_active_subscription failed; allowing user to continue as fallback")

        # require email if empty (first-time user)
        try:
            email_col = HEADERS[USERS_SHEET].index("email")
            cur_email = ""
            if len(urow) > email_col:
                cur_email = urow[email_col]
            if not cur_email:
                await send_and_record(message.from_user.id, "لطفاً ایمیل خود را ارسال کنید (مثال: name@example.com)")
                return
        except Exception:
            logger.exception("Email-check in /start failed (non-fatal)")

        # all good -> main menu
        kb = build_main_keyboard()
        await send_and_record(message.from_user.id, "👋 خوش آمدید! منوی اصلی:", reply_markup=kb)
    except Exception:
        tb = traceback.format_exc()
        logger.exception("Unhandled error in /start: %s", tb)
        # notify admin with traceback for faster debugging
        try:
            if ADMIN_TELEGRAM_ID:
                await bot.send_message(int(ADMIN_TELEGRAM_ID), f"Unhandled error in /start (user {message.from_user.id}):\n{tb}")
        except Exception:
            logger.exception("Failed to notify admin about unhandled /start exception")
        # user-friendly message
        await message.answer("خطا در شروع. خطا به ادمین گزارش شد. لطفاً بعداً تلاش کنید.")

@dp.callback_query_handler(lambda c: c.data == "check_membership")
async def cb_check_membership(callback_query: types.CallbackQuery):
    user = callback_query.from_user
    try:
        ok_membership, missing = await enforce_required_channels(user.id)
        if ok_membership:
            ensure_user_row_and_return(user)
            kb = build_main_keyboard()
            try:
                await callback_query.message.edit_text("✅ عضویت شما در کانال(ها) تأیید شد. منوی اصلی برایتان ارسال شد.")
            except Exception:
                try:
                    await callback_query.answer("عضویت تأیید شد.", show_alert=False)
                except Exception:
                    pass
            try:
                await send_and_record(user.id, "✅ عضویت شما در کانال(ها) تأیید شد. منوی اصلی:", reply_markup=kb)
            except Exception:
                pass
        else:
            kb2 = types.InlineKeyboardMarkup(row_width=1)
            for ch in missing:
                if isinstance(ch, str) and ch.startswith("@"):
                    kb2.add(types.InlineKeyboardButton(text=f"عضویت در {ch}", url=f"https://t.me/{ch.lstrip('@')}"))
                else:
                    kb2.add(types.InlineKeyboardButton(text=str(ch), url=f"https://t.me/{str(ch).lstrip('-100')}"))
            kb2.add(types.InlineKeyboardButton("🔁 بررسی مجدد", callback_data="check_membership"))
            try:
                await callback_query.message.edit_text("⚠️ هنوز به کانال(های) زیر ملحق نشده‌اید. لطفاً ابتدا عضو شوید و سپس دوباره بررسی کنید:", reply_markup=kb2, disable_web_page_preview=True)
            except Exception:
                try:
                    await callback_query.answer("عضویت بررسی شد.", show_alert=False)
                    await send_and_record(user.id, "⚠️ هنوز عضو نشده‌اید. لطفا کانال‌ها را چک کنید.", reply_markup=kb2)
                except Exception:
                    pass
    except Exception as e:
        logger.exception("Error in cb_check_membership: %s", e)
        try:
            await callback_query.answer("خطا در بررسی عضویت. لطفا دوباره تلاش کنید.", show_alert=True)
        except Exception:
            pass

@dp.message_handler(lambda msg: msg.text is not None and "@" in msg.text and "." in msg.text)
async def handle_email(message: types.Message):
    email = message.text.strip()
    try:
        if len(email) < 5 or " " in email or "@" not in email:
            await message.answer("ایمیل وارد شده معتبر نیست. دوباره امتحان کنید.")
            return
        idx, urow = ensure_user_row_and_return(message.from_user)
        try:
            while len(urow) < len(HEADERS[USERS_SHEET]):
                urow.append("")
            urow[HEADERS[USERS_SHEET].index("email")] = email
            open_sheet(USERS_SHEET).update(f"A{idx}:{gspread.utils.rowcol_to_a1(idx, len(urow))}", [pad_row_to_header(urow, USERS_SHEET)])
        except Exception:
            logger.exception("Failed to write email to sheet")
            await message.answer("خطا در ذخیره ایمیل. لطفا بعدا تلاش کنید.")
            return
        kb = build_main_keyboard()
        await message.answer("✅ ایمیل ثبت شد! لطفاً از منوی زیر انتخاب کنید:", reply_markup=kb)
    except Exception as e:
        logger.exception("handle_email error: %s", e)
        await message.answer("❌ ثبت ایمیل با خطا مواجه شد. لطفاً بعداً تلاش کنید.")

@dp.message_handler(lambda msg: msg.text == "توضیحات پلتفرم")
async def platform_info(message: types.Message):
    try:
        w = open_sheet(CONFIG_SHEET)
        vals = w.get_all_values()
        content = ""
        for row in vals[1:]:
            if len(row) >= 2 and row[0] == "platform_info":
                content = row[1]
                break
        if not content:
            content = "📘 توضیحات پلتفرم هنوز تنظیم نشده است."
        await message.answer(content)
    except Exception:
        logger.exception("platform_info error")
        await message.answer("خطا در دریافت توضیحات پلتفرم.")

@dp.message_handler(lambda msg: msg.text == "پشتیبانی")
async def support(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    await message.answer("🧰 لطفاً سوال یا مشکل خود را به صورت یک پیام بفرستید تا تیکت ثبت شود.")

# -------------------------
# test_channel (قابل کپی/پیست)
# -------------------------
@dp.message_handler(lambda msg: msg.text == "تست کانال معمولی")
async def test_channel(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    # accept multiple env names earlier, so TEST_CHANNEL_ID should be set
    if not TEST_CHANNEL_ID:
        # try fallback to normal channel (you said test==normal sometimes)
        if NORMAL_CHANNEL_ID:
            tcid = NORMAL_CHANNEL_ID
        else:
            await message.answer("⚠️ کانال تست تنظیم نشده است. مقدار env TEST_CHANNEL_ID یا CHANNEL_TEST_CHANNEL_ID یا CHANNEL_TEST را بررسی کن.")
            return
    else:
        tcid = TEST_CHANNEL_ID

    # validate chat id
    try:
        chat = await bot.get_chat(tcid)
    except Exception as e:
        logger.exception("get_chat(TEST_CHANNEL_ID) failed: %s", e)
        await message.answer("⚠️ خطا در دسترسی به کانال تست. مطمئن شو ربات ادمین آن کانال است و شناسه درست است.")
        return

    invite = await create_temporary_invite(tcid, expire_seconds=20, member_limit=1)  # reduced to 20s for testing
    if not invite:
        await message.answer("⚠️ لینک دعوت ایجاد نشد. مطمئن شوید ربات ادمین آن کانال است و دسترسی can_invite_users دارد.")
        return

    await send_and_record(message.from_user.id, "⏳ لینک عضویت موقت برای شما ایجاد شد (۲۰ ثانیه):\n" + invite, disable_web_page_preview=True)

    joined_at = now_iso()
    row = [str(message.from_user.id), message.from_user.full_name or "", "", "trial", "0", "test_invite", "trial", joined_at, "", "", joined_at, ""]
    row_idx = await sheets_append_return_index(PURCHASES_SHEET, pad_row_to_header(row, PURCHASES_SHEET))
    if row_idx <= 0:
        logger.error("Failed to append purchase row for trial user %s", message.from_user.id)
        await send_and_record(message.from_user.id, "⚠️ ثبت داخلی تست با خطا مواجه شد.")
        return
    await schedule_remove_after(tcid, message.from_user.id, delay_seconds=20, purchase_row_idx=row_idx)

@dp.message_handler(lambda msg: msg.text == "خرید کانال معمولی")
async def buy_normal(message: types.Message):
    idx, _ = ensure_user_row_and_return(message.from_user)
    created_at = now_iso()
    row = [str(message.from_user.id), message.from_user.full_name or "", "", "normal", "", "", "awaiting_tx", created_at, "", "", ""]
    row_idx = await sheets_append_return_index(PURCHASES_SHEET, pad_row_to_header(row, PURCHASES_SHEET))
    if row_idx <= 0:
        await message.answer("⚠️ خطا در ثبت سفارش. لطفاً بعداً تلاش کنید.")
        return
    await message.answer(f"💳 سفارش ثبت شد. شناسه سفارش داخلی: {row_idx}\nلطفاً پس از پرداخت، شناسه یا اطلاعات تراکنش را همین‌جا ارسال کنید.")

@dp.message_handler(lambda msg: msg.text == "خرید کانال ویژه")
async def buy_premium(message: types.Message):
    idx, _ = ensure_user_row_and_return(message.from_user)
    created_at = now_iso()
    row = [str(message.from_user.id), message.from_user.full_name or "", "", "premium", "", "", "awaiting_tx", created_at, "", "", ""]
    row_idx = await sheets_append_return_index(PURCHASES_SHEET, pad_row_to_header(row, PURCHASES_SHEET))
    if row_idx <= 0:
        await message.answer("⚠️ خطا در ثبت سفارش. لطفاً بعداً تلاش کنید.")
        return
    await message.answer(f"💳 سفارش ویژه ثبت شد. شناسه سفارش داخلی: {row_idx}\nلطفاً پس از پرداخت، شناسه یا اطلاعات تراکنش را همین‌جا ارسال کنید.")

# -------------------------
# catch-all text handler (transaction info OR support ticket)
# Single handler — avoids duplicates and uses sheet-padding.
# -------------------------
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def catch_all_text(message: types.Message):
    try:
        # ignore commands
        if message.text and message.text.startswith("/"):
            return
        ents = getattr(message, "entities", None)
        if ents:
            for e in ents:
                try:
                    if getattr(e, "type", "") == "bot_command":
                        return
                except Exception:
                    continue
    except Exception:
        pass

    text = (message.text or "").strip()
    if not text:
        return

    # heuristic transaction: contains digits and length >= 6
    if len(text) >= 6 and any(ch.isdigit() for ch in text):
        created_at = now_iso()
        try:
            rows = await sheets_get_all(PURCHASES_SHEET)
            pending_idx = None
            # search most recent awaiting_tx/pending for this user
            if rows and len(rows) > 1:
                for rev_i, row in enumerate(reversed(rows[1:]), start=1):
                    real_idx = len(rows) - rev_i + 1
                    try:
                        status = (row[HEADERS[PURCHASES_SHEET].index("status")] if len(row) > HEADERS[PURCHASES_SHEET].index("status") else "").lower()
                    except Exception:
                        status = ""
                    if str(row[0]) == str(message.from_user.id) and status in ("awaiting_tx", "pending"):
                        pending_idx = real_idx
                        break
            if pending_idx:
                row = rows[pending_idx - 1]
                while len(row) < len(HEADERS[PURCHASES_SHEET]):
                    row.append("")
                row[HEADERS[PURCHASES_SHEET].index("transaction_info")] = text
                row[HEADERS[PURCHASES_SHEET].index("status")] = "pending"
                row[HEADERS[PURCHASES_SHEET].index("request_at")] = created_at
                ok = await sheets_update_row(PURCHASES_SHEET, pending_idx, pad_row_to_header(row, PURCHASES_SHEET))
                if ok:
                    await message.answer("✅ تراکنش شما ثبت شد و در انتظار بررسی ادمین است. به زودی اطلاع‌رسانی می‌شود.")
                else:
                    await message.answer("❌ ثبت تراکنش انجام نشد. لطفاً دوباره تلاش کنید.")
            else:
                new_row = [str(message.from_user.id), message.from_user.full_name or "", "", "unknown", "", text, "pending", created_at, "", "", ""]
                ok = await sheets_append(PURCHASES_SHEET, pad_row_to_header(new_row, PURCHASES_SHEET))
                if ok:
                    await message.answer("✅ تراکنش شما ثبت شد و در انتظار بررسی ادمین است. به زودی اطلاع‌رسانی می‌شود.")
                else:
                    await message.answer("❌ ثبت تراکنش انجام نشد. لطفاً دوباره تلاش کنید.")
        except Exception:
            logger.exception("Error recording transaction")
            await message.answer("❌ ثبت تراکنش انجام نشد. لطفاً دوباره تلاش کنید.")
        return

    # otherwise support ticket
    try:
        ticket_id = str(uuid.uuid4())[:8]
        created_at = now_iso()
        ticket_row = [ticket_id, str(message.from_user.id), message.from_user.full_name or "", "کاربر-پیام", text, "open", created_at, "", ""]
        ok = await sheets_append(SUPPORT_SHEET, pad_row_to_header(ticket_row, SUPPORT_SHEET))
        if ok:
            await message.answer("✅ تیکت شما ثبت شد. پاسخ از طریق همین ربات ارسال خواهد شد.")
            # notify admins
            try:
                await notify_admins(f"🎫 تیکت جدید: {ticket_id}\nUser: {message.from_user.id}\nMessage: {text}")
            except Exception:
                logger.exception("Could not notify admins of support ticket.")
        else:
            await message.answer("❌ ثبت تیکت انجام نشد. لطفاً دوباره تلاش کنید.")
    except Exception:
        logger.exception("support ticket handling failed")
        await message.answer("❌ ثبت تیکت انجام نشد. لطفاً دوباره تلاش کنید.")

# -------------------------
# Admin helper: wipe all data and recreate headers (USE WITH CARE)
# -------------------------
@dp.message_handler(commands=["wipe_all_sheets"])
async def wipe_all_sheets_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("فقط ادمین می‌تواند از این دستور استفاده کند.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].strip().lower() != "confirm":
        await message.reply("استفاده: /wipe_all_sheets confirm\n(این عمل داده‌های همه شیت‌ها را پاک می‌کند و فقط header را بازنویسی می‌کند!)")
        return
    try:
        sh = open_sheet_with_backoff(SPREADSHEET_ID)
        for s in HEADERS.keys():
            try:
                try:
                    w = sh.worksheet(s)
                    w.clear()
                    w.insert_row(HEADERS[s], index=1)
                except gspread.WorksheetNotFound:
                    w = sh.add_worksheet(title=s, rows="1000", cols="20")
                    w.insert_row(HEADERS[s], index=1)
            except Exception:
                logger.exception("Failed to wipe/create sheet %s", s)
        # clear caches
        _WORKSHEET_CACHE.clear()
        _SPREADSHEET_CACHE = None
        await message.reply("تمام شیت‌ها پاک و headerها بازنویسی شد.")
    except Exception:
        logger.exception("wipe_all_sheets failed")
        await message.reply("خطا در پاکسازی شیت‌ها. لاگ‌ها را بررسی کن.")

# === END OF PART 1 ===
# بعد از پیست و ذخیره این بخش، به من بگو "اوکی" تا بخش دوم (poller, admin callbacks, rebuild_subs, entrypoint) را بفرستم.
# Admin notify rate-limiting
ADMIN_NOTIFY_INTERVAL_SECONDS = int(os.getenv("ADMIN_NOTIFY_INTERVAL_SECONDS", "10"))
_last_admin_notify_time: Dict[str, float] = {}

async def notify_admin_pending(pending_row: List[str]):
    """
    Notify admin about a newly created pending purchase (best-effort, rate-limited).
    The poller does the full actionable notify (with inline buttons) — این تابع فقط اطلاع‌رسانی اولیه است.
    """
    if not ADMIN_TELEGRAM_ID:
        logger.warning("No ADMIN_TELEGRAM_ID configured; skipping admin notify.")
        return
    now_t = time.time()
    last = _last_admin_notify_time.get(str(ADMIN_TELEGRAM_ID), 0)
    if now_t - last < ADMIN_NOTIFY_INTERVAL_SECONDS:
        logger.info("Admin notify rate-limited; skipping.")
        return
    _last_admin_notify_time[str(ADMIN_TELEGRAM_ID)] = now_t
    user_id = pending_row[0] if len(pending_row) > 0 else ""
    trans_info = pending_row[5] if len(pending_row) > 5 else ""
    created_at = pending_row[7] if len(pending_row) > 7 else ""
    msg = f"🔔 تراکنش جدید ثبت شد\nUser: {user_id}\nInfo: {trans_info}\nTime: {created_at}"
    try:
        await bot.send_message(int(ADMIN_TELEGRAM_ID), msg)
    except Exception:
        logger.exception("Failed to send admin notify.")

# Poller: scan Purchases sheet, notify admin about pending rows with inline confirm/reject
# >>> جایگزین کامل تابع poll_pending_notify_admin با این بلوک کن <<<

async def poll_pending_notify_admin():
    """
    Poll Purchases sheet for pending purchases and notify admin with inline confirm/reject.
    Reduced read frequency and protected against Sheets API quota errors.
    """
    await asyncio.sleep(2)
    while True:
        try:
            rows = await sheets_get_all(PURCHASES_SHEET)
            header = HEADERS.get(PURCHASES_SHEET, [])
            def hidx(name, default):
                try:
                    return header.index(name)
                except Exception:
                    return default

            status_i = hidx("status", 6)
            admin_note_i = hidx("admin_note", 12)
            trans_i = hidx("transaction_info", 5)
            request_i = hidx("request_at", 7)

            if rows and len(rows) > 1:
                for idx, row in enumerate(rows[1:], start=2):
                    try:
                        status = (row[status_i] if len(row) > status_i else "").lower()
                        admin_note = (row[admin_note_i] if len(row) > admin_note_i else "")
                        if status == "pending" and not admin_note:
                            if not (ADMINS or ADMIN_TELEGRAM_ID):
                                break
                            user_id = 0
                            try:
                                user_id = int(row[0]) if row and str(row[0]).isdigit() else 0
                            except Exception:
                                user_id = 0
                            msg = (
                                f"🔔 Pending purchase (row {idx})\n"
                                f"User: {row[0] if len(row)>0 else ''}\n"
                                f"Name: {row[1] if len(row)>1 else ''}\n"
                                f"Info: {row[trans_i] if len(row)>trans_i else ''}\n"
                                f"Time: {row[request_i] if len(row)>request_i else ''}"
                            )
                            try:
                                target_admin = None
                                # prefer sending to all admins
                                if ADMINS:
                                    await notify_admins(msg, reply_markup=admin_confirm_keyboard(idx, user_id))
                                else:
                                    await bot.send_message(int(ADMIN_TELEGRAM_ID), msg, reply_markup=admin_confirm_keyboard(idx, user_id))
                                # mark admin_note column (avoid re-notify)
                                while len(row) <= admin_note_i:
                                    row.append("")
                                row[admin_note_i] = now_iso()
                                await sheets_update_row(PURCHASES_SHEET, idx, pad_row_to_header(row, PURCHASES_SHEET))
                            except APIError as e:
                                logger.exception("Failed to notify admin about pending row %s due to APIError: %s", idx, e)
                                # if sheets update failed due to quota, skip and let backoff in sheets_update_row handle it
                            except Exception:
                                logger.exception("Failed to notify admin about pending row %s", idx)
                    except Exception:
                        logger.exception("Error processing purchase row %s", idx)
            # delay between polling rounds — increased to reduce Sheets reads
            await asyncio.sleep(60)
        except Exception as e:
            logger.exception("poll_pending_notify_admin loop error: %s", e)
            # on unexpected errors, back off a bit
            await asyncio.sleep(60)

# Callback handler for confirm/reject
# -------------------------
# 8) process_admin_confirmation (کامل قابل کپی/پیست)
# -------------------------
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("confirm:") or c.data.startswith("reject:")))
async def process_admin_confirmation(callback_query: types.CallbackQuery):
    data = callback_query.data
    parts = data.split(":")
    action = parts[0] if parts else ""
    try:
        purchase_row_idx = int(parts[1])
        target_user_id = int(parts[2])
    except Exception:
        await callback_query.answer("فرمت داده نامعتبر.", show_alert=True)
        return

    try:
        rows = await sheets_get_all(PURCHASES_SHEET)
        if not rows or purchase_row_idx - 1 >= len(rows):
            await callback_query.answer("ردیف موجود نیست یا قبلا تغییر کرده.", show_alert=True)
            return
        row = rows[purchase_row_idx - 1]
        expected_len = len(HEADERS.get(PURCHASES_SHEET, []))
        while len(row) < expected_len:
            row.append("")

        header = HEADERS.get(PURCHASES_SHEET, [])
        def idx_of(col_name, default=-1):
            try:
                return header.index(col_name)
            except Exception:
                return default

        status_idx = idx_of("status", 6)
        trans_info_idx = idx_of("transaction_info", 5)
        activated_idx = idx_of("activated_at", 8)
        expires_idx = idx_of("expires_at", 9)
        admin_note_idx = idx_of("admin_note", 12)

        if action == "confirm":
            row[status_idx] = "confirmed"
            activated = now_iso()
            try:
                expires_dt = datetime.now(tz=ZoneInfo("Asia/Tehran")) + timedelta(days=30*6)
                expires = expires_dt.replace(microsecond=0).isoformat()
            except Exception:
                expires = (datetime.utcnow() + timedelta(days=30*6)).replace(microsecond=0).isoformat()

            row[activated_idx] = activated
            row[expires_idx] = expires
            row[admin_note_idx] = now_iso()
            await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, pad_row_to_header(row, PURCHASES_SHEET))

            plan = "premium" if ("premium" in (row[3] or "").lower()) else "normal"
            try:
                sub_row = [str(target_user_id), plan, activated, expires, "yes"]
                await sheets_append(SUBS_SHEET, pad_row_to_header(sub_row, SUBS_SHEET))
            except Exception:
                logger.exception("Failed to append subscription row")

            # update Users sheet purchase_status/expires and ensure referral code exists
            try:
                u_w = open_sheet(USERS_SHEET)
                u_vals = u_w.get_all_values()
                for i, ur in enumerate(u_vals[1:], start=2):
                    if len(ur) > 0 and str(ur[0]) == str(target_user_id):
                        while len(ur) < len(HEADERS[USERS_SHEET]):
                            ur.append("")
                        try:
                            ur[HEADERS[USERS_SHEET].index("purchase_status")] = "active"
                            ur[HEADERS[USERS_SHEET].index("expires_at")] = expires
                        except Exception:
                            pass
                        if not ur[HEADERS[USERS_SHEET].index("referral_code")]:
                            ur[HEADERS[USERS_SHEET].index("referral_code")] = generate_referral_code()
                        u_w.update(f"A{i}:{gspread.utils.rowcol_to_a1(i, len(ur))}", [pad_row_to_header(ur, USERS_SHEET)])
                        break
            except Exception:
                logger.exception("Failed to update Users after confirm.")

            # referral detection from transaction_info
            try:
                trans_info = row[trans_info_idx] if trans_info_idx >= 0 else ""
                if trans_info:
                    for token in (trans_info or "").split():
                        candidate = token.strip()
                        if not candidate:
                            continue
                        ref_found = find_user_by_referral_code(candidate) if 'find_user_by_referral_code' in globals() else None
                        if ref_found and str(ref_found) != str(target_user_id):
                            try:
                                increment_referral_count(ref_found)
                            except Exception:
                                logger.exception("increment_referral_count failed")
                            # write referred_by in Users
                            try:
                                u_w = open_sheet(USERS_SHEET)
                                u_vals = u_w.get_all_values()
                                for j, ur in enumerate(u_vals[1:], start=2):
                                    if len(ur) > 0 and str(ur[0]) == str(target_user_id):
                                        while len(ur) < len(HEADERS[USERS_SHEET]):
                                            ur.append("")
                                        ur[HEADERS[USERS_SHEET].index("referred_by")] = str(ref_found)
                                        u_w.update(f"A{j}:{gspread.utils.rowcol_to_a1(j, len(ur))}", [pad_row_to_header(ur, USERS_SHEET)])
                                        break
                            except Exception:
                                logger.exception("Failed to set referred_by for user %s", target_user_id)
                            break
            except Exception:
                logger.exception("Referral detection failed for purchase row %s", purchase_row_idx)

            # DM user and create invite(s) — record admin_note if invite fails
            try:
                referral = ""
                try:
                    uvals = open_sheet(USERS_SHEET).get_all_values()
                    for ur in uvals[1:]:
                        if ur and len(ur) > 0 and str(ur[0]) == str(target_user_id):
                            referral = ur[3] if len(ur) > 3 else ""
                            break
                except Exception:
                    pass

                await bot.send_message(target_user_id, "🎉 پرداخت شما تایید شد. اشتراک شما فعال شد.\nکد معرفی شما: " + (referral or generate_referral_code()))

                if plan == "premium":
                    chat_list = [NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID]
                else:
                    chat_list = [NORMAL_CHANNEL_ID]

                for ch in chat_list:
                    if not ch:
                        continue
                    try:
                        link = await create_temporary_invite(ch, expire_seconds=60*60*24, member_limit=1)
                    except Exception as e:
                        link = None
                        logger.exception("create_temporary_invite failed: %s", e)
                    if link:
                        await bot.send_message(target_user_id, f"لینک عضویت در کانال: {link}")
                    else:
                        try:
                            row[admin_note_idx] = (row[admin_note_idx] or "") + f" invite_error:{ch}:{now_iso()}"
                            await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, pad_row_to_header(row, PURCHASES_SHEET))
                        except Exception:
                            logger.exception("Failed to write admin_note for invite error")
            except Exception:
                logger.exception("Failed to DM user on confirm.")

            await callback_query.answer("خرید تأیید شد.")
        else:
            # reject
            row[status_idx] = "rejected"
            row[admin_note_idx] = now_iso()
            await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, pad_row_to_header(row, PURCHASES_SHEET))
            try:
                await bot.send_message(target_user_id, "❌ خرید شما تایید نشد. لطفاً با پشتیبانی تماس بگیرید.")
            except Exception:
                logger.exception("Could not notify user about rejected payment.")
            await callback_query.answer("خرید رد شد.")
    except Exception as e:
        logger.exception("Error processing admin callback: %s", e)
        try:
            await callback_query.answer("خطا در پردازش.", show_alert=True)
        except Exception:
            pass
            
async def rebuild_schedules_from_subscriptions():
    """
    Rebuild scheduled expiry jobs from subscriptions sheet on startup.
    Ensures mark_subscription_expired is called after removal.
    """
    try:
        rows = await sheets_get_all(SUBS_SHEET)
        if not rows or len(rows) <= 1:
            logger.info("No subscriptions to rebuild.")
            return
        for idx, row in enumerate(rows[1:], start=2):
            try:
                if not row or len(row) == 0:
                    continue
                # basic values
                user_id = int(row[0])
                plan = row[1] if len(row) > 1 else ""
                expires_at = row[3] if len(row) > 3 else ""
                expires_dt = parse_iso_or_none(expires_at)
                if not expires_dt:
                    logger.error("rebuild row err: Invalid isoformat string: %r", expires_at)
                    continue

                # make everything timezone-aware (Asia/Tehran) for comparison
                now = datetime.now(tz=ZoneInfo("Asia/Tehran"))
                if expires_dt.tzinfo is None:
                    try:
                        expires_dt = expires_dt.replace(tzinfo=ZoneInfo("Asia/Tehran"))
                    except Exception:
                        pass

                if expires_dt <= now:
                    # already expired: remove and mark expired
                    if plan == "premium":
                        for ch in [NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID]:
                            if ch:
                                asyncio.create_task(remove_user_from_chat(ch, user_id))
                    else:
                        if NORMAL_CHANNEL_ID:
                            asyncio.create_task(remove_user_from_chat(NORMAL_CHANNEL_ID, user_id))
                    # mark active = no in sheet
                    try:
                        while len(row) < len(HEADERS.get(SUBS_SHEET, [])):
                            row.append("")
                        if "active" in HEADERS.get(SUBS_SHEET, []):
                            row[HEADERS[SUBS_SHEET].index("active")] = "no"
                        await sheets_update_row(SUBS_SHEET, idx, row)
                    except Exception:
                        pass
                else:
                    delay = (expires_dt - now).total_seconds()
                    async def expire_job(chat_ids, uid, d, exp_iso):
                        try:
                            await asyncio.sleep(d)
                            for ch in chat_ids:
                                if ch:
                                    await remove_user_from_chat(ch, uid)
                            try:
                                await bot.send_message(uid, "⏳ اشتراک شما به پایان رسید. جهت تمدید یا خرید مجدد به من مراجعه کنید.")
                            except Exception:
                                pass
                            # mark subscription expired in sheet
                            try:
                                await mark_subscription_expired(uid, exp_iso)
                            except Exception:
                                logger.exception("Failed to mark subscription expired for %s", uid)
                        except Exception:
                            logger.exception("Error in expire_job for user %s", uid)

                    chat_ids = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if plan == "premium" else [NORMAL_CHANNEL_ID]
                    asyncio.create_task(expire_job([ch for ch in chat_ids if ch], user_id, delay, expires_at))
            except Exception:
                logger.exception("Error rebuilding subscription row: %s", row)
    except Exception:
        logger.exception("rebuild_schedules_from_subscriptions failed")

# Rebuild scheduled expiries from Subscription sheet on startup
async def mark_subscription_expired(user_id: int, expires_at_iso: str):
    """
    Mark subscription row active=no in SUBS_SHEET for given user and expires_at (if matches).
    """
    try:
        rows = await sheets_get_all(SUBS_SHEET)
        if not rows:
            return
        header = rows[0]
        for idx, row in enumerate(rows[1:], start=2):
            try:
                if len(row) > 0 and str(row[0]) == str(user_id):
                    # if expires_at_iso provided, check match (loose)
                    if expires_at_iso and len(row) > 3 and row[3] and row[3] != expires_at_iso:
                        continue
                    # ensure columns
                    while len(row) < len(header):
                        row.append("")
                    if "active" in header:
                        row[header.index("active")] = "no"
                    else:
                        row[-1] = "no"
                    await sheets_update_row(SUBS_SHEET, idx, row)
                    return
            except Exception:
                continue
    except Exception:
        logger.exception("mark_subscription_expired failed for %s", user_id)

# Admin command: reply to ticket
@dp.message_handler(commands=["reply_ticket"])
async def admin_reply_ticket(message: types.Message):
    if not ADMIN_TELEGRAM_ID or str(message.from_user.id) != str(ADMIN_TELEGRAM_ID):
        await message.answer("فقط ادمین مجاز است.")
        return
    parts = message.text.split(" ", 2)
    if len(parts) < 3:
        await message.answer("استفاده: /reply_ticket <ticket_id> <response message>")
        return
    ticket_id = parts[1].strip()
    response_text = parts[2].strip()
    try:
        w = open_sheet(SUPPORT_SHEET)
        vals = w.get_all_values()
        found = False
        for idx, row in enumerate(vals[1:], start=2):
            if len(row) > 0 and row[0] == ticket_id:
                while len(row) < 9:
                    row.append("")
                row[7] = response_text
                row[8] = now_iso()
                w.update(f"A{idx}:I{idx}", [row])
                # send message to user
                try:
                    await bot.send_message(int(row[1]), f"📩 پاسخ پشتیبانی به تیکت {ticket_id}:\n\n{response_text}")
                except Exception:
                    logger.exception("Could not DM user for ticket response.")
                found = True
                break
        if found:
            await message.answer("✅ پاسخ ارسال و ثبت شد.")
        else:
            await message.answer("تیکت پیدا نشد.")
    except Exception:
        logger.exception("admin_reply_ticket error")
        await message.answer("خطا در ارسال پاسخ.")

# -------------------------
# /reset_sheet <SheetName> CONFIRM — پاک کند و header را بازنویسی کند (فقط ادمین)
# -------------------------
@dp.message_handler(commands=["reset_sheet"])
async def reset_sheet_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("فقط ادمین می‌تواند از این دستور استفاده کند.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3 or parts[2].strip().lower() != "confirm":
        await message.reply("استفاده: /reset_sheet <SheetName> confirm\nمثال: /reset_sheet Users confirm\n(این عمل همه‌چیز را حذف می‌کند!)")
        return
    sheet = parts[1].strip()
    if sheet not in HEADERS:
        await message.reply("نام شیت معتبر نیست. لیست شیت‌ها: " + ", ".join(HEADERS.keys()))
        return
    ok = fix_sheet_header(sheet, force_clear=True)
    if ok:
        await message.reply(f"شیت {sheet} پاک و header بازنویسی شد.")
    else:
        await message.reply(f"خطا در ریست کردن شیت {sheet}. لاگ‌ها را بررسی کن.")

# -------------------------
# /ensure_headers — بررسی و به‌صورت non-destructive header را قرار می‌دهد
# -------------------------
@dp.message_handler(commands=["ensure_headers"])
async def ensure_headers_handler(message: types.Message):
    if not ADMIN_TELEGRAM_ID or str(message.from_user.id) != str(ADMIN_TELEGRAM_ID):
        await message.reply("فقط ادمین مجاز است.")
        return
    results = []
    for s in HEADERS.keys():
        ok = fix_sheet_header(s, force_clear=False)
        results.append(f"{s}: {'OK' if ok else 'FAILED'}")
    await message.reply("نتیجه بررسی هدرها:\n" + "\n".join(results))

# Webserver (health) & startup
async def start_webserver():
    app = web.Application()
    async def handle_root(req):
        return web.Response(text="OK")
    app.router.add_get("/", handle_root)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)

async def on_startup(dp_obj):
    try:
        if INSTANCE_MODE == "polling":
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted on startup (polling mode).")
    except Exception:
        logger.exception("Failed to delete webhook on startup.")

    try:
        # start simple health webserver
        asyncio.create_task(start_webserver())
    except Exception:
        logger.exception("Failed to start webserver.")

    # Debug: log key envs for troubleshooting
    try:
        logger.info("ENV CHECK: TEST_CHANNEL_ID=%s NORMAL_CHANNEL_ID=%s PREMIUM_CHANNEL_ID=%s SPREADSHEET_ID=%s",
                    TEST_CHANNEL_ID, NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID, SPREADSHEET_ID)
    except Exception:
        pass

    # Ensure sheet headers exist (non-destructive). This helps avoid append_row writing into wrong columns.
    for sname in [USERS_SHEET, PURCHASES_SHEET, REFERRALS_SHEET, SUPPORT_SHEET, SUBS_SHEET, CONFIG_SHEET]:
        try:
            ok = fix_sheet_header(sname, force_clear=False)
            if not ok:
                logger.warning("fix_sheet_header reported problem for %s", sname)
        except Exception:
            logger.exception("fix_sheet_header exception for %s", sname)

    # Ensure worksheets exist (open/create) AFTER header fixes
    for sname in [USERS_SHEET, PURCHASES_SHEET, REFERRALS_SHEET, SUPPORT_SHEET, SUBS_SHEET, CONFIG_SHEET]:
        try:
            open_sheet(sname)
        except Exception:
            logger.exception("Failed to ensure sheet exists: %s", sname)

    # Start background tasks AFTER sheets ensured
    try:
        asyncio.create_task(poll_pending_notify_admin())
        asyncio.create_task(rebuild_schedules_from_subscriptions())
    except Exception:
        logger.exception("Failed to create background tasks.")

# Robust polling wrapper
# -------------------------
# 10) run_polling_with_retries (کامل قابل کپی/پیست)
# -------------------------
def run_polling_with_retries(skip_updates: bool = True, max_retries: int = 20):
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Starting aiogram polling (attempt %d)...", attempt)
            executor.start_polling(dp, skip_updates=skip_updates, on_startup=on_startup)
            logger.info("executor.start_polling returned normally.")
            break
        except TerminatedByOtherGetUpdates as e:
            logger.error("TerminatedByOtherGetUpdates: %s", e)
            if ADMIN_TELEGRAM_ID:
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(bot.send_message(int(ADMIN_TELEGRAM_ID),
                        "⚠️ خطای اجرا: یک instance دیگر این بات با همان توکن در حال اجرا است. لطفاً تنها یک instance فعال داشته باشید یا توکن را تغییر دهید."))
                except Exception:
                    logger.exception("Could not notify admin about TerminatedByOtherGetUpdates.")
            import sys
            sys.exit(1)
        except Exception as e:
            logger.exception("Unhandled exception in polling: %s", e)
            wait = min(60, 5 * attempt)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached for polling.")
                break
                
# Entry point
if __name__ == "__main__":
    logger.info("=== BOT STARTING ===")
    print("=== BOT STARTING ===")
    if INSTANCE_MODE == "webhook":
        logger.info("INSTANCE_MODE=webhook requested but not configured; falling back to polling.")
    run_polling_with_retries(skip_updates=True, max_retries=20)

