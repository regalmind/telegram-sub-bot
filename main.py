# main.py — بخش ۱ از ۲
# Telegram subscription bot — full implementation (split into two parts)
# بخش اول شامل تنظیمات، راه‌اندازی Google Sheets (gspread)، توابع کمکی،
# بوت و هندلرهای ابتدایی (start, email, platform info, support, test, buy)
# بعد از اینکه این را پیست کردی و گفتی "اوکی"، بخش دوم را می‌فرستم.

import os
import json
import time
import asyncio
import logging
import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web
from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates, ChatNotFound
from google.oauth2 import service_account
import gspread
import base64
import binascii
import uuid
import traceback

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
logger = logging.getLogger("telegram-sub-bot")

# -------------------------
# Config via ENV
# -------------------------
# Required env vars (names used by this code):
# TELEGRAM_TOKEN
# ADMIN_TELEGRAM_ID
# NORMAL_CHANNEL_ID (e.g. -1001234567890)
# PREMIUM_CHANNEL_ID
# TEST_CHANNEL_ID
# SPREADSHEET_ID
# GOOGLE_CREDENTIALS  (either raw JSON, substring, or base64 JSON) OR GOOGLE_SERVICE_ACCOUNT (file path)
# PORT (optional, default 8000)
# REQUIRED_CHANNELS (optional, comma-separated channel IDs to require membership)
# INSTANCE_MODE (optional: "polling" or "webhook") - default polling

TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID") or os.getenv("PLATFORM_ADMIN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
PORT = int(os.getenv("PORT", "8000"))
NORMAL_CHANNEL_ID = os.getenv("NORMAL_CHANNEL_ID")
PREMIUM_CHANNEL_ID = os.getenv("PREMIUM_CHANNEL_ID")
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID")
REQUIRED_CHANNELS = os.getenv("REQUIRED_CHANNELS", "")  # comma separated
INSTANCE_MODE = os.getenv("INSTANCE_MODE", "polling").lower()  # polling or webhook

# basic validation
if not TOKEN:
    logger.error("Missing TELEGRAM_TOKEN env var. Set TELEGRAM_TOKEN.")
    raise SystemExit("Missing TELEGRAM_TOKEN")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID env var.")
    raise SystemExit("Missing SPREADSHEET_ID")

# Normalize required channels to list
REQUIRED_CHANNELS_LIST = [c.strip() for c in REQUIRED_CHANNELS.split(",") if c.strip()]

# -------------------------
# Google Sheets setup (gspread)
# -------------------------
def load_google_creds_info() -> Dict[str, Any]:
    # Try GOOGLE_CREDENTIALS env (raw json or base64) or file
    if GOOGLE_CREDENTIALS_ENV:
        s = GOOGLE_CREDENTIALS_ENV.strip()
        # try raw json
        try:
            data = json.loads(s)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return data
        except Exception:
            logger.debug("GOOGLE_CREDENTIALS raw parse failed; trying substring/base64.")
        # try substring
        try:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = s[start:end+1]
                data = json.loads(candidate)
                logger.info("Recovered JSON substring from GOOGLE_CREDENTIALS.")
                return data
        except Exception as e:
            logger.debug("substring recovery failed: %s", e)
        # try base64
        try:
            decoded = base64.b64decode(s, validate=True)
            try:
                data = json.loads(decoded.decode("utf-8"))
                logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64).")
                return data
            except Exception as e:
                logger.warning("Base64 decoded but JSON parse failed: %s", e)
        except (binascii.Error, ValueError) as e:
            logger.debug("Not valid base64: %s", e)

    # fallback: try file
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

# create gspread client
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
# Sheet names and headers (matching your described sheets)
# -------------------------
USERS_SHEET = "Users"
PURCHASES_SHEET = "Purchases"
REFERRALS_SHEET = "Referrals"
SUPPORT_SHEET = "Support"
SUBS_SHEET = "Subscription"
CONFIG_SHEET = "Config"  # for editable platform info and keys

HEADERS = {
    USERS_SHEET: ["telegram_id", "full_name", "email", "referral_code", "referred_by", "status", "purchase_status", "expires_at", "created_at", "last_seen", "notes"],
    PURCHASES_SHEET: ["telegram_id", "full_name", "email", "product", "amount", "transaction_info", "status", "request_at", "activated_at", "expires_at", "admin_note"],
    REFERRALS_SHEET: ["telegram_id", "referral_code", "referred_count", "created_at"],
    SUPPORT_SHEET: ["ticket_id", "telegram_id", "full_name", "subject", "message", "status", "created_at", "response", "responded_at"],
    SUBS_SHEET: ["telegram_id", "product", "activated_at", "expires_at", "active"],
    CONFIG_SHEET: ["key", "value"],
}

# helper: open spreadsheet and ensure worksheet exists + header
def open_sheet(ws_name: str):
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        logger.exception("Failed to open spreadsheet: %s", e)
        raise
    try:
        try:
            w = sh.worksheet(ws_name)
        except gspread.WorksheetNotFound:
            logger.info("Worksheet %s not found. Creating.", ws_name)
            w = sh.add_worksheet(title=ws_name, rows="1000", cols="20")
        # ensure header
        values = w.get_all_values()
        if not values or (len(values) == 0) or (values[0] == []):
            header = HEADERS.get(ws_name, [])
            if header:
                w.insert_row(header, index=1)
                logger.info("Wrote header for %s", ws_name)
        else:
            # if header exists but not matching expected, keep existing to avoid data loss
            pass
        return w
    except Exception as e:
        logger.exception("Failed to open/create worksheet %s: %s", ws_name, e)
        raise

# convenience wrappers
async def sheets_append(ws_name: str, row: List[Any]) -> bool:
    # small sanitize
    row = [str(x)[:2000] if x is not None else "" for x in row]
    try:
        w = open_sheet(ws_name)
        w.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception:
        logger.exception("sheets_append failed for sheet %s", ws_name)
        return False

async def sheets_get_all(ws_name: str) -> List[List[str]]:
    try:
        w = open_sheet(ws_name)
        vals = w.get_all_values()
        return vals
    except Exception:
        logger.exception("sheets_get_all failed for %s", ws_name)
        return []

async def sheets_update_range(ws_name: str, start_row: int, start_col: int, values: List[List[Any]]) -> bool:
    # gspread uses A1 notation; convert
    try:
        w = open_sheet(ws_name)
        cell_range = gspread.utils.rowcol_to_a1(start_row, start_col) + ":" + gspread.utils.rowcol_to_a1(start_row + len(values) - 1, start_col + (len(values[0]) - 1))
        w.update(cell_range, values)
        return True
    except Exception:
        logger.exception("sheets_update_range failed for %s", ws_name)
        return False

async def sheets_update_row(ws_name: str, row_idx: int, values: List[Any]) -> bool:
    try:
        w = open_sheet(ws_name)
        cell_range = f"A{row_idx}:{gspread.utils.rowcol_to_a1(row_idx, len(values))}"
        w.update(cell_range, [values])
        return True
    except Exception:
        logger.exception("sheets_update_row failed for %s row %s", ws_name, row_idx)
        return False

# -------------------------
# Helpers (time, referral, parse)
# -------------------------
def now_iso() -> str:
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

def ensure_user_row_and_return(user: types.User, email: Optional[str] = None) -> Tuple[int, List[str]]:
    """
    Ensure user exists in Users sheet. Returns (row_index (1-based), row_values)
    This is synchronous wrapper around gspread operations (safe to call from async tasks).
    """
    w = open_sheet(USERS_SHEET)
    values = w.get_all_values()
    header = values[0] if values else HEADERS[USERS_SHEET]
    # search for user
    for idx, row in enumerate(values[1:], start=2):
        try:
            if len(row) > 0 and str(row[0]) == str(user.id):
                # update name/email/last_seen if needed
                changed = False
                # ensure enough columns
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
                    w.update(f"A{idx}:K{idx}", [row])
                return idx, row
        except Exception:
            continue
    # append new
    referral = generate_referral_code()
    created_at = now_iso()
    new_row = [str(user.id), user.full_name or "", email or "", referral, "", "active", "none", "", created_at, now_iso(), ""]
    w.append_row(new_row, value_input_option="USER_ENTERED")
    vals2 = w.get_all_values()
    return len(vals2), vals2[-1]

# -------------------------
# Bot & dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

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
        await asyncio.sleep(0.8)
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        logger.info("Removed user %s from chat %s", user_id, chat_id)
        return True
    except Exception as e:
        logger.exception("Failed to remove user %s from chat %s: %s", user_id, chat_id, e)
        return False

async def schedule_remove_after(chat_id: str, user_id: int, delay_seconds: int = 600):
    if user_id in scheduled_removals:
        scheduled_removals[user_id].cancel()
    async def job():
        try:
            await asyncio.sleep(delay_seconds)
            ok = await remove_user_from_chat(chat_id, user_id)
            if ok:
                try:
                    kb = build_main_keyboard()
                    await bot.send_message(user_id, "⏳ مدت تست کانال به پایان رسید. برای ادامه اشتراک‌ها به منو مراجعه کنید.", reply_markup=kb)
                except Exception as e:
                    logger.debug("Could not DM user after removal: %s", e)
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
# Membership check (required channels)
# -------------------------
async def is_member_of(chat_id: str, user_id: int) -> bool:
    try:
        mem = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = mem.status  # 'creator', 'administrator', 'member', 'restricted', 'left', 'kicked'
        return status not in ("left", "kicked")
    except Exception as e:
        logger.exception("is_member_of error for chat %s user %s: %s", chat_id, user_id, e)
        return False

async def enforce_required_channels(user_id: int) -> Tuple[bool, List[str]]:
    not_member = []
    for ch in REQUIRED_CHANNELS_LIST:
        try:
            ok = await is_member_of(ch, user_id)
            if not ok:
                not_member.append(ch)
        except Exception:
            not_member.append(ch)
    return (len(not_member) == 0, not_member)

# -------------------------
# Handlers: start, email, menu
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        ok_membership, missing = await enforce_required_channels(message.from_user.id)
        if not ok_membership:
            await message.answer("⚠️ برای استفاده از ربات باید در کانال‌های مربوطه عضو باشید. لطفا ابتدا در کانال‌های زیر عضو شوید:\n" + "\n".join(missing))
            return
        ensure_user_row_and_return(message.from_user)
        kb = build_main_keyboard()
        await message.answer("👋 خوش آمدید! منوی اصلی:", reply_markup=kb)
    except Exception as e:
        logger.exception("Error in /start: %s", e)
        await message.answer("خطا در شروع. لطفا بعداً تلاش کنید.")

@dp.message_handler(lambda msg: msg.text is not None and "@" in msg.text and "." in msg.text)
async def handle_email(message: types.Message):
    email = message.text.strip()
    try:
        # basic format check
        if len(email) < 5 or " " in email:
            await message.answer("ایمیل وارد شده معتبر نیست.")
            return
        row_idx, _ = ensure_user_row_and_return(message.from_user, email=email)
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
    except Exception as e:
        logger.exception("platform_info error: %s", e)
        await message.answer("خطا در دریافت توضیحات پلتفرم.")

@dp.message_handler(lambda msg: msg.text == "پشتیبانی")
async def support(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    await message.answer("🧰 لطفاً سوال یا مشکل خود را به صورت یک پیام بفرستید تا تیکت ثبت شود.")

@dp.message_handler(lambda msg: msg.text == "تست کانال معمولی")
async def test_channel(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    if not TEST_CHANNEL_ID:
        await message.answer("⚠️ کانال تست تنظیم نشده است. با ادمین تماس بگیرید.")
        return
    invite = await create_temporary_invite(TEST_CHANNEL_ID, expire_seconds=600, member_limit=1)
    if not invite:
        await message.answer("⚠️ لینک دعوت ایجاد نشد. مطمئن شوید ربات ادمین کانال تست است.")
        return
    await message.answer("⏳ لینک عضویت موقت برای شما ایجاد شد (۱۰ دقیقه):\n" + invite, disable_web_page_preview=True)
    await schedule_remove_after(TEST_CHANNEL_ID, message.from_user.id, delay_seconds=600)
    await sheets_append(PURCHASES_SHEET, [str(message.from_user.id), message.from_user.full_name or "", "", "trial", "0", "test_invite", "trial", now_iso(), "", "", ""])

@dp.message_handler(lambda msg: msg.text == "خرید کانال معمولی")
async def buy_normal(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    await message.answer("💳 لطفاً مبلغ مربوط به اشتراک را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nپس از پرداخت، اطلاعات تراکنش (شناسه تراکنش یا متن اطلاعات) را ارسال کنید.\nتوجه: پس از تایید پرداخت، اشتراک شما فعال خواهد شد.")

@dp.message_handler(lambda msg: msg.text == "خرید کانال ویژه")
async def buy_premium(message: types.Message):
    ensure_user_row_and_return(message.from_user)
    await message.answer("🌟 برای خرید اشتراک ویژه، لطفاً مبلغ را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nپس از پرداخت، اطلاعات تراکنش را ارسال نمایید.\nتوجه: پس از تایید پرداخت، اشتراک ویژه برای شما فعال خواهد شد.")

# catch-all text handler: either transaction info or support ticket
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def catch_all_text(message: types.Message):
    text = (message.text or "").strip()
    if not text:
        return
    # heuristic: transaction if contains digits and length >= 6
    if len(text) >= 6 and any(ch.isdigit() for ch in text):
        created_at = now_iso()
        # append to Purchases sheet with status pending
        row = [str(message.from_user.id), message.from_user.full_name or "", "", "unknown", "", text, "pending", created_at, "", "", ""]
        ok = await sheets_append(PURCHASES_SHEET, row)
        if ok:
            await message.answer("✅ تراکنش شما ثبت شد و در انتظار بررسی ادمین است. به زودی اطلاع‌رسانی می‌شود.")
            # admin will be notified by poller (بخش دوم کد)
        else:
            await message.answer("❌ ثبت تراکنش انجام نشد. لطفاً دوباره تلاش کنید.")
    else:
        # support ticket
        ticket_id = str(uuid.uuid4())[:8]
        created_at = now_iso()
        ticket_row = [ticket_id, str(message.from_user.id), message.from_user.full_name or "", "کاربر-پیام", text, "open", created_at, "", ""]
        ok = await sheets_append(SUPPORT_SHEET, ticket_row)
        if ok:
            await message.answer("✅ تیکت شما ثبت شد. پاسخ از طریق همین ربات ارسال خواهد شد.")
            if ADMIN_TELEGRAM_ID:
                try:
                    await bot.send_message(int(ADMIN_TELEGRAM_ID), f"🎫 تیکت جدید: {ticket_id}\nUser: {message.from_user.id}\nMessage: {text}")
                except Exception:
                    logger.exception("Could not notify admin of support ticket.")
        else:
            await message.answer("❌ ثبت تیکت انجام نشد. لطفاً دوباره تلاش کنید.")

# === پایان بخش ۱ از ۲ ===
# پس از اینکه این قطعه را در فایل main.py پیست کردی، به من بگو "اوکی"
# تا ادامه (بخش ۲) را فوراً بفرستم تا کنار این پیست کنی.
# main.py — بخش 2 از 2 (ادامه)
# ادامهٔ توابع پشت‌صحنه: نوتیفای ادمین، poller، callback handler، بازسازی اشتراک‌ها،
# دستورات ادمین، وب‌سرور و entrypoint.

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
    این نسخه به‌طور واضح try/except ها را بسته و از خطاهای Indentation جلوگیری می‌کند.
    """
    await asyncio.sleep(2)
    while True:
        try:
            rows = await sheets_get_all(PURCHASES_SHEET)
            if rows and len(rows) > 1:
                for idx, row in enumerate(rows[1:], start=2):
                    # هر ردیف را با try جداگانه پوشش می‌دهیم تا خطای یک ردیف حلقه را نشکند
                    try:
                        status = (row[6] if len(row) > 6 else "").lower()
                        admin_note = (row[10] if len(row) > 10 else "")
                        if status == "pending" and not admin_note:
                            if not ADMIN_TELEGRAM_ID:
                                # اگر ادمین تنظیم نشده، نوتیفای کردن بی‌معنی است
                                break
                            user_id = 0
                            try:
                                user_id = int(row[0]) if row and str(row[0]).isdigit() else 0
                            except Exception:
                                user_id = 0
                            msg = f"🔔 Pending purchase (row {idx})\nUser: {row[0] if len(row)>0 else ''}\nName: {row[1] if len(row)>1 else ''}\nInfo: {row[5] if len(row)>5 else ''}\nTime: {row[7] if len(row)>7 else ''}"
                            try:
                                await bot.send_message(int(ADMIN_TELEGRAM_ID), msg, reply_markup=admin_confirm_keyboard(idx, user_id))
                                # mark notified time in admin_note column (index 10 / col K)
                                while len(row) < 11:
                                    row.append("")
                                row[10] = now_iso()
                                await sheets_update_row(PURCHASES_SHEET, idx, row)
                            except Exception:
                                logger.exception("Failed to notify admin about pending row %s", idx)
                    except Exception:
                        # خطای پردازش یک ردیف را لاگ کن و ادامه بده
                        logger.exception("Error processing purchase row %s", idx)
            # صبر بین هر دور polling
            await asyncio.sleep(12)
        except Exception as e:
            logger.exception("poll_pending_notify_admin loop error: %s", e)
            # اگر خطای کلی رخ داد، با تأخیر بیشتری تلاش کن
            await asyncio.sleep(20)

# Callback handler for confirm/reject
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("confirm:") or c.data.startswith("reject:")))
async def process_admin_confirmation(callback_query: types.CallbackQuery):
    data = callback_query.data
    parts = data.split(":")
    action = parts[0]
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
        # ensure row has enough columns
        while len(row) < 11:
            row.append("")
        if action == "confirm":
            # set status = confirmed
            row[6] = "confirmed"
            activated = now_iso()
            expires = (datetime.utcnow() + timedelta(days=30*6)).replace(microsecond=0).isoformat()
            row[8] = activated
            row[9] = expires
            await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, row)
            # append subscription
            plan = "premium" if ("premium" in (row[3] or "").lower()) else "normal"
            sub_row = [str(target_user_id), plan, activated, expires, "yes"]
            await sheets_append(SUBS_SHEET, sub_row)
            # update Users sheet for this user
            try:
                u_w = open_sheet(USERS_SHEET)
                u_vals = u_w.get_all_values()
                found_idx = None
                for i, ur in enumerate(u_vals[1:], start=2):
                    if len(ur) > 0 and str(ur[0]) == str(target_user_id):
                        found_idx = i
                        u_row = ur
                        break
                if found_idx:
                    while len(u_row) < 11:
                        u_row.append("")
                    u_row[6] = "active"
                    u_row[7] = expires
                    if not u_row[3]:
                        u_row[3] = generate_referral_code()
                    u_w.update(f"A{found_idx}:K{found_idx}", [u_row])
            except Exception:
                logger.exception("Failed to update Users after confirm.")
            # DM user with referral + invite links
            try:
                referral = ""
                try:
                    uvals = open_sheet(USERS_SHEET).get_all_values()
                    for ur in uvals[1:]:
                        if ur and len(ur) > 0 and str(ur[0]) == str(target_user_id):
                            referral = ur[3] if len(ur) > 3 else ""
                except Exception:
                    pass
                await bot.send_message(target_user_id, "🎉 پرداخت شما تایید شد. اشتراک شما فعال شد.\nکد معرفی شما: " + (referral or generate_referral_code()))
                if plan == "premium":
                    for ch in [NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID]:
                        if ch:
                            link = await create_temporary_invite(ch, expire_seconds=60*60*24, member_limit=1)
                            if link:
                                await bot.send_message(target_user_id, f"لینک عضویت در کانال: {link}")
                else:
                    if NORMAL_CHANNEL_ID:
                        link = await create_temporary_invite(NORMAL_CHANNEL_ID, expire_seconds=60*60*24, member_limit=1)
                        if link:
                            await bot.send_message(target_user_id, f"لینک عضویت در کانال معمولی: {link}")
            except Exception:
                logger.exception("Failed to DM user on confirm.")
            await callback_query.answer("خرید تأیید شد.")
        else:
            # reject
            row[6] = "rejected"
            await sheets_update_row(PURCHASES_SHEET, purchase_row_idx, row)
            try:
                await bot.send_message(target_user_id, "❌ خرید شما تایید نشد. لطفاً با پشتیبانی تماس بگیرید یا اطلاعات تراکنش را بررسی کنید.")
            except Exception:
                logger.exception("Could not notify user about rejected payment.")
            await callback_query.answer("خرید رد شد.")
    except Exception as e:
        logger.exception("Error processing admin callback: %s", e)
        await callback_query.answer("خطا در پردازش.")

# Rebuild scheduled expiries from Subscription sheet on startup
async def rebuild_schedules_from_subscriptions():
    try:
        rows = await sheets_get_all(SUBS_SHEET)
        if not rows or len(rows) <= 1:
            logger.info("No subscriptions to rebuild.")
            return
        for idx, row in enumerate(rows[1:], start=2):
            try:
                user_id = int(row[0])
                plan = row[1] if len(row) > 1 else ""
                expires_at = row[2] if len(row) > 2 else ""
                expires_dt = parse_iso_or_none(expires_at)
                if not expires_dt:
                    logger.error("rebuild row err: Invalid isoformat string: %r", expires_at)
                    continue
                now = datetime.utcnow()
                if expires_dt <= now:
                    # already expired: remove and mark expired
                    if plan == "premium":
                        for ch in [NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID]:
                            if ch:
                                asyncio.create_task(remove_user_from_chat(ch, user_id))
                    else:
                        if NORMAL_CHANNEL_ID:
                            asyncio.create_task(remove_user_from_chat(NORMAL_CHANNEL_ID, user_id))
                    # mark active=no
                    try:
                        row[4] = "no"
                        await sheets_update_row(SUBS_SHEET, idx, row)
                    except Exception:
                        pass
                else:
                    delay = (expires_dt - now).total_seconds()
                    async def expire_job(chat_ids, uid, d):
                        await asyncio.sleep(d)
                        for ch in chat_ids:
                            if ch:
                                await remove_user_from_chat(ch, uid)
                        try:
                            await bot.send_message(uid, "⏳ اشتراک شما به پایان رسید. جهت تمدید یا خرید مجدد به من مراجعه کنید.")
                        except Exception:
                            pass
                    chat_ids = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if plan == "premium" else [NORMAL_CHANNEL_ID]
                    asyncio.create_task(expire_job([ch for ch in chat_ids if ch], user_id, delay))
            except Exception:
                logger.exception("Error rebuilding subscription row: %s", row)
    except Exception:
        logger.exception("rebuild_schedules_from_subscriptions failed")

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
        asyncio.create_task(start_webserver())
    except Exception:
        logger.exception("Failed to start webserver.")
    # ensure sheets exist BEFORE starting pollers
    for sname in [USERS_SHEET, PURCHASES_SHEET, REFERRALS_SHEET, SUPPORT_SHEET, SUBS_SHEET, CONFIG_SHEET]:
        try:
            open_sheet(sname)
        except Exception:
            logger.exception("Failed to ensure sheet exists: %s", sname)
    # start background tasks AFTER sheets ensured
    try:
        asyncio.create_task(poll_pending_notify_admin())
        asyncio.create_task(rebuild_schedules_from_subscriptions())
    except Exception:
        logger.exception("Failed to create background tasks.")

# Robust polling wrapper
def run_polling_with_retries(skip_updates: bool = True, max_retries: int = 10):
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Starting aiogram polling (attempt %d)...", attempt)
            executor.start_polling(dp, skip_updates=skip_updates, on_startup=on_startup)
            logger.info("executor.start_polling returned normally.")
            break
        except TerminatedByOtherGetUpdates as e:
            logger.warning("TerminatedByOtherGetUpdates: %s", e)
            logger.error("Detected another running instance (same token). Exiting. Ensure only one bot instance is running.")
            break
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

