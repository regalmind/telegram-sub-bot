# main.py
# کامل، مقاوم‌شده و طبق خواسته‌ها
# نیازمند aiogram, google-api-python-client, google-auth, aiohttp
import os
import json
import base64
import binascii
import logging
import asyncio
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates, ChatNotFound
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiohttp import web
import http.client

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
logger = logging.getLogger("telegram-sub-bot")

# -------------------------
# Environment variables
# -------------------------
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or os.getenv("SPREADSHEET")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
PORT = int(os.environ.get("PORT", "8000"))
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID")
NORMAL_CHANNEL_ID = os.getenv("NORMAL_CHANNEL_ID")
PREMIUM_CHANNEL_ID = os.getenv("PREMIUM_CHANNEL_ID")
REFERRAL_PREFIX = os.getenv("REFERRAL_PREFIX", "REF-")
# Optional limits
ADMIN_NOTIFY_INTERVAL_SECONDS = int(os.getenv("ADMIN_NOTIFY_INTERVAL_SECONDS", "10"))  # rate-limit admin messages

# Validation
if not TOKEN:
    logger.error("Missing BOT_TOKEN / TELEGRAM_TOKEN env var.")
    raise SystemExit("Missing BOT_TOKEN / TELEGRAM_TOKEN")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID env var.")
    raise SystemExit("Missing SPREADSHEET_ID")
if not ADMIN_TELEGRAM_ID:
    logger.warning("ADMIN_TELEGRAM_ID not set. Admin notifications disabled (they will fail).")

# -------------------------
# Google credentials loader (robust)
# -------------------------
def load_google_creds() -> Dict[str, Any]:
    if GOOGLE_CREDENTIALS_ENV:
        s = GOOGLE_CREDENTIALS_ENV.strip()
        # try raw JSON
        try:
            data = json.loads(s)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return data
        except json.JSONDecodeError:
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

    # fallback to file
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info("Loaded Google credentials from file %s", GOOGLE_SERVICE_ACCOUNT_FILE)
                return data
        except Exception as e:
            logger.exception("Failed to load GOOGLE_SERVICE_ACCOUNT file: %s", e)

    logger.error("No Google credentials found.")
    raise SystemExit("Missing Google credentials")

# -------------------------
# Initialize Google Sheets client
# -------------------------
sheets = None
try:
    creds_info = load_google_creds()
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sheets_service = build("sheets", "v4", credentials=creds)
    sheets = sheets_service.spreadsheets()
    logger.info("Google Sheets client initialized.")
except Exception:
    logger.exception("Failed to initialize Google Sheets client; continuing with sheets=None")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# -------------------------
# Utilities: robust Google Sheets operations with retries
# -------------------------
def _sheets_values_get(spreadsheet_id: str, range_name: str) -> Dict[str, Any]:
    # blocking, may raise exceptions
    if sheets is None:
        raise RuntimeError("Sheets client not initialized")
    return sheets.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()

def _sheets_values_append(spreadsheet_id: str, range_name: str, values: List[List[Any]]):
    if sheets is None:
        raise RuntimeError("Sheets client not initialized")
    return sheets.values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()

def _sheets_values_update(spreadsheet_id: str, range_name: str, values: List[List[Any]]):
    if sheets is None:
        raise RuntimeError("Sheets client not initialized")
    return sheets.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()

async def run_in_executor(func, *args, retries=3, delay=1.0):
    loop = asyncio.get_running_loop()
    attempt = 0
    while True:
        attempt += 1
        try:
            return await loop.run_in_executor(None, func, *args)
        except http.client.IncompleteRead as e:
            logger.warning("Executor IncompleteRead attempt %d: %s", attempt, e)
            if attempt >= retries:
                raise
            await asyncio.sleep(delay * attempt)
        except Exception as e:
            logger.exception("Executor call attempt %d failed: %s", attempt, e)
            if attempt >= retries:
                raise
            await asyncio.sleep(delay * attempt)

async def sheets_get(range_name: str) -> Optional[List[List[Any]]]:
    try:
        res = await run_in_executor(_sheets_values_get, SPREADSHEET_ID, range_name)
        return res.get("values", [])
    except Exception as e:
        logger.exception("sheets_get failed for %s: %s", range_name, e)
        return None

async def sheets_append(range_name: str, values: List[List[Any]]) -> bool:
    try:
        await run_in_executor(_sheets_values_append, SPREADSHEET_ID, range_name, values)
        return True
    except Exception as e:
        logger.exception("sheets_append failed for %s: %s", range_name, e)
        return False

async def sheets_update(range_name: str, values: List[List[Any]]) -> bool:
    try:
        await run_in_executor(_sheets_values_update, SPREADSHEET_ID, range_name, values)
        return True
    except Exception as e:
        logger.exception("sheets_update failed for %s: %s", range_name, e)
        return False

# -------------------------
# Sheet schema helpers
# -------------------------
# Users sheet columns:
USERS_SHEET = "Users"
USERS_RANGE = f"{USERS_SHEET}!A:K"  # flexible width
# Subscriptions sheet columns:
SUBS_SHEET = "Subscriptions"
SUBS_RANGE = f"{SUBS_SHEET}!A:J"
# Payments / Pending sheet:
PENDING_SHEET = "PendingPayments"
PENDING_RANGE = f"{PENDING_SHEET}!A:K"

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def parse_iso_or_none(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

# -------------------------
# Referral codes / user utilities
# -------------------------
import random
import string
def generate_referral_code(length: int = 6) -> str:
    chars = string.ascii_uppercase + string.digits
    return REFERRAL_PREFIX + ''.join(random.choice(chars) for _ in range(length))

async def find_user_row_by_id(user_id: int) -> Optional[Tuple[int, List[str]]]:
    rows = await sheets_get(USERS_RANGE)
    if not rows:
        return None
    # header detection
    header = rows[0]
    id_col = 0
    for idx, row in enumerate(rows[1:], start=2):
        try:
            if str(user_id) == str(row[id_col]):
                return idx, row
        except Exception:
            continue
    return None

async def ensure_user_in_sheet(user: types.User, email: Optional[str]=None) -> bool:
    """
    Ensure user exists in Users sheet. If exists, update missing fields.
    Columns (recommended):
    A: user_id, B: full_name, C: email, D: referral_code, E: referred_by, F: status, G: purchase_status, H: expires_at, I: created_at, J: last_seen
    """
    rows = await sheets_get(USERS_RANGE)
    header = []
    if rows:
        header = rows[0]
    else:
        # create header
        header = ["user_id","full_name","email","referral_code","referred_by","status","purchase_status","expires_at","created_at","last_seen","notes"]
        await sheets_append(USERS_RANGE, [header])
        rows = [header]
    # search
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) > 0 and str(row[0]) == str(user.id):
            # update missing email/name/last_seen
            updated = False
            if email and (len(row) < 3 or not row[2]):
                row[2:3] = [email]
                updated = True
            if len(row) < 2 or not row[1]:
                name = f"{user.full_name or ''}".strip()
                row[1:2] = [name]
                updated = True
            # update last seen
            if len(row) < 10 or row[9] != now_iso():
                row[9:10] = [now_iso()]
                updated = True
            if updated:
                # write back entire row: we need A{idx}:K{idx}
                range_row = f"{USERS_SHEET}!A{idx}:K{idx}"
                await sheets_update(range_row, [row])
            return True
    # not found -> append
    name = f"{user.full_name or ''}".strip()
    referral_code = generate_referral_code()
    created_at = now_iso()
    new_row = [str(user.id), name, email or "", referral_code, "", "active", "none", "", created_at, now_iso(), ""]
    ok = await sheets_append(f"{USERS_SHEET}!A:K", [new_row])
    return ok

# -------------------------
# Invite link & trial scheduling
# -------------------------
# Keep in-memory schedule to allow cancellations if bot restarts quickly.
scheduled_removals: Dict[int, asyncio.Task] = {}  # key = user_id, value = task

async def create_temporary_invite(chat_id: str, expire_seconds: int = 600, member_limit: int = 1) -> Optional[str]:
    """
    Create an invite link that expires after expire_seconds.
    Requires bot to be admin in chat (channel) with invite link permission.
    Returns invite link (str) or None.
    """
    try:
        expire_date = int((datetime.utcnow() + timedelta(seconds=expire_seconds)).timestamp())
        # aiogram's method:
        link = await bot.create_chat_invite_link(chat_id=chat_id, expire_date=expire_date, member_limit=member_limit)
        invite_url = link.invite_link
        logger.info("Created invite link for chat %s, expires in %d seconds", chat_id, expire_seconds)
        return invite_url
    except Exception as e:
        logger.exception("Failed to create invite link for %s: %s", chat_id, e)
        return None

async def remove_user_from_chat(chat_id: str, user_id: int) -> bool:
    """
    Remove user from chat by banning then unbanning (so they are kicked).
    """
    try:
        await bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
        # short sleep to ensure removal
        await asyncio.sleep(1)
        await bot.unban_chat_member(chat_id=chat_id, user_id=user_id, only_if_banned=True)
        logger.info("Kicked user %s from chat %s (ban->unban).", user_id, chat_id)
        return True
    except Exception as e:
        logger.exception("Failed to remove user %s from chat %s: %s", user_id, chat_id, e)
        return False

async def schedule_remove_after(chat_id: str, user_id: int, delay_seconds: int = 600):
    """
    Schedule removal and notify user after removal.
    """
    # Cancel existing
    if user_id in scheduled_removals:
        task = scheduled_removals[user_id]
        task.cancel()
    async def job():
        try:
            await asyncio.sleep(delay_seconds)
            ok = await remove_user_from_chat(chat_id, user_id)
            # notify user inside bot (if possible)
            try:
                if ok:
                    await bot.send_message(user_id, f"⏳ مدت تست کانال به پایان رسید. برای ادامه اشتراک‌ها و خریدها به منو مراجعه کنید.", reply_markup=types.ReplyKeyboardRemove())
                    kb = build_main_keyboard()
                    await bot.send_message(user_id, "برای خرید از منو استفاده کنید:", reply_markup=kb)
            except Exception as e:
                logger.debug("Could not DM user after removal: %s", e)
        except asyncio.CancelledError:
            logger.info("Scheduled removal for %s cancelled.", user_id)
        finally:
            scheduled_removals.pop(user_id, None)
    task = asyncio.create_task(job())
    scheduled_removals[user_id] = task

# -------------------------
# Keyboards / menu
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

def admin_confirm_keyboard(user_id: int, pending_row_index: int):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("تأیید خرید ✅", callback_data=f"confirm:{pending_row_index}:{user_id}"))
    kb.add(types.InlineKeyboardButton("رد خرید ❌", callback_data=f"reject:{pending_row_index}:{user_id}"))
    return kb

# -------------------------
# Handlers: core flows
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # If user already in sheet, don't ask for email again.
    logger.info("Received /start from %s", message.from_user.id)
    rows = await sheets_get(USERS_RANGE)
    found = False
    if rows:
        for row in rows[1:]:
            try:
                if str(row[0]) == str(message.from_user.id):
                    found = True
                    break
            except Exception:
                continue
    if found:
        kb = build_main_keyboard()
        await message.answer("👋 خوش آمدید مجدد! منوی اصلی در ادامه برای شما فرستاده شد:", reply_markup=kb)
    else:
        await message.answer("👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:", reply_markup=types.ReplyKeyboardRemove())
        await message.answer("✉️ منتظر ایمیل شما هستم...")

@dp.message_handler(lambda msg: msg.text is not None and "@" in msg.text and "." in msg.text)
async def handle_email(message: types.Message):
    email = message.text.strip()
    ok = await ensure_user_in_sheet(message.from_user, email=email)
    if ok:
        kb = build_main_keyboard()
        await message.answer("✅ ایمیل ثبت شد! لطفاً از منوی زیر انتخاب کنید:", reply_markup=kb)
    else:
        await message.answer("❌ ثبت ایمیل با خطا مواجه شد. لطفاً بعداً تلاش کنید.")

@dp.message_handler(lambda msg: msg.text == "تست کانال معمولی")
async def test_channel(message: types.Message):
    # check user exists
    await ensure_user_in_sheet(message.from_user)
    if not TEST_CHANNEL_ID:
        await message.answer("⚠️ کانال تست تنظیم نشده است. با ادمین تماس بگیرید.")
        return
    # generate invite link
    invite = await create_temporary_invite(TEST_CHANNEL_ID, expire_seconds=600, member_limit=1)
    if not invite:
        await message.answer("⚠️ لینک دعوت ایجاد نشد. مطمئن شوید ربات ادمین کانال تست است.")
        return
    await message.answer("⏳ لینک عضویت موقت برای شما ایجاد شد (۱۰ دقیقه):\n" + invite, disable_web_page_preview=True)
    # schedule removal in 10 minutes
    await schedule_remove_after(TEST_CHANNEL_ID, message.from_user.id, delay_seconds=600)

@dp.message_handler(lambda msg: msg.text == "خرید کانال معمولی")
async def buy_normal(message: types.Message):
    await ensure_user_in_sheet(message.from_user)
    await message.answer("💳 لطفاً مبلغ مربوط به اشتراک را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nپس از پرداخت، اطلاعات تراکنش (شناسه تراکنش) را ارسال کنید.\nتوجه: پس از تایید پرداخت، کد رفرال شما ارسال خواهد شد.")

@dp.message_handler(lambda msg: msg.text == "خرید کانال ویژه")
async def buy_premium(message: types.Message):
    await ensure_user_in_sheet(message.from_user)
    await message.answer("🌟 برای خرید اشتراک ویژه، لطفاً مبلغ را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nپس از پرداخت، اطلاعات تراکنش را ارسال نمایید.\nتوجه: پس از تایید پرداخت، کد رفرال شما ارسال خواهد شد. با خرید ویژه هر دو کانال اضافه خواهد شد.")

@dp.message_handler(lambda msg: msg.text == "پشتیبانی")
async def support(message: types.Message):
    await ensure_user_in_sheet(message.from_user)
    await message.answer("🧰 لطفاً سوال یا مشکل خود را ارسال کنید تا برای شما تیکت ایجاد شود.")
    # next messages from user should be recorded as support ticket — implement simple append
    # we register a short-lived state by storing in-memory mapping (simple approach)
    # For brevity implement as: any text after pressing پشتیبانی will be appended as ticket if within 2 minutes.
    # (Simpler implementation: direct append and notify admin.)

@dp.message_handler(lambda msg: msg.text == "توضیحات پلتفرم")
async def platform_info(message: types.Message):
    await message.answer("📘 توضیحات پلتفرم به‌زودی در این بخش قرار خواهد گرفت.")

# Generic message handler: capture transaction strings or support messages
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def catch_all_text(message: types.Message):
    text = message.text.strip()
    # Heuristics: if text looks like a payment transaction id (e.g., digits/letters) -> append to PendingPayments
    # Otherwise treat as support ticket content (if message length reasonable)
    if len(text) >= 6 and any(ch.isdigit() for ch in text):
        # assume payment info
        created_at = now_iso()
        row = [str(message.from_user.id), message.from_user.full_name or "", text, "pending", created_at, "", "", "", "", ""]
        ok = await sheets_append(f"{PENDING_SHEET}!A:K", [row])
        if ok:
            await message.answer("✅ تراکنش شما ثبت شد و در انتظار بررسی ادمین است. به زودی اطلاع‌رسانی می‌شود.")
            # notify admin (rate-limited)
            await notify_admin_pending(row)
        else:
            await message.answer("❌ ثبت تراکنش انجام نشد. لطفاً دوباره تلاش کنید.")
    else:
        # treat as support ticket
        created_at = now_iso()
        ticket_row = [str(message.from_user.id), message.from_user.full_name or "", text, created_at, "open"]
        await sheets_append("Support!A:E", [ticket_row])
        await message.answer("✅ تیکت شما ثبت شد. پاسخ از طریق این ربات ارسال خواهد شد.")

# -------------------------
# Admin notifications with rate-limiting
# -------------------------
_last_admin_notify_time: Dict[str, float] = {}

async def notify_admin_pending(pending_row: List[str]):
    if not ADMIN_TELEGRAM_ID:
        logger.warning("No ADMIN_TELEGRAM_ID configured; skipping admin notify.")
        return
    now = time.time()
    last = _last_admin_notify_time.get(str(ADMIN_TELEGRAM_ID), 0)
    if now - last < ADMIN_NOTIFY_INTERVAL_SECONDS:
        logger.info("Admin notify rate-limited; skipping.")
        return
    _last_admin_notify_time[str(ADMIN_TELEGRAM_ID)] = now
    user_id = pending_row[0]
    trans_info = pending_row[2] if len(pending_row) > 2 else ""
    created_at = pending_row[4] if len(pending_row) > 4 else ""
    msg = f"🔔 تراکنش جدید ثبت شد\nUser: {user_id}\nInfo: {trans_info}\nTime: {created_at}"
    try:
        await bot.send_message(int(ADMIN_TELEGRAM_ID), msg)
    except ChatNotFound:
        logger.exception("Admin chat not found when notifying pending.")
    except Exception as e:
        logger.exception("Failed to notify admin: %s", e)

# -------------------------
# Poll pending payments periodically and post inline confirm/reject
# -------------------------
async def poll_pending_notify_admin():
    await asyncio.sleep(2)
    while True:
        try:
            rows = await sheets_get(PENDING_RANGE)
            if rows and len(rows) > 1:
                for idx, row in enumerate(rows[1:], start=2):
                    status = row[3] if len(row) > 3 else "pending"
                    if status.lower() == "pending":
                        # send admin inline buttons to confirm/reject
                        if not ADMIN_TELEGRAM_ID:
                            break
                        msg = f"🔔 Pending payment #{idx-1}\nUser: {row[0]}\nName: {row[1]}\nInfo: {row[2]}\nTime: {row[4]}"
                        try:
                            await bot.send_message(int(ADMIN_TELEGRAM_ID), msg, reply_markup=admin_confirm_keyboard(int(row[0]), idx))
                        except Exception as e:
                            logger.exception("Failed to notify admin about pending row %s: %s", idx, e)
                        # mark as notified by updating a column "notified" to timestamp to avoid double sending
                        range_row = f"{PENDING_SHEET}!L{idx}:L{idx}"
                        await sheets_update(range_row, [[now_iso()]])
            await asyncio.sleep(15)
        except Exception as e:
            logger.exception("poll_pending_notify_admin loop error: %s", e)
            await asyncio.sleep(20)

# Callback handler for admin confirm/reject
@dp.callback_query_handler(lambda c: c.data and (c.data.startswith("confirm:") or c.data.startswith("reject:")))
async def process_admin_confirmation(callback_query: types.CallbackQuery):
    data = callback_query.data
    parts = data.split(":")
    action = parts[0]
    pending_row_idx = int(parts[1])
    target_user_id = int(parts[2])
    try:
        # read that row
        rows = await sheets_get(PENDING_RANGE)
        if not rows or pending_row_idx - 1 >= len(rows):
            await callback_query.answer("ردیف موجود نیست یا قبلا تغییر کرده.", show_alert=True)
            return
        row = rows[pending_row_idx-1]
        if action == "confirm":
            # mark as confirmed in sheet
            # update status column (D) to "confirmed"
            range_row = f"{PENDING_SHEET}!D{pending_row_idx}:D{pending_row_idx}"
            await sheets_update(range_row, [["confirmed"]])
            # add to subscriptions sheet: fill with user, plan (detect from notes), expires_at (6 months), created_at, status
            plan = "normal"
            if len(row) > 5 and row[5]:
                plan = row[5]
            expires = (datetime.utcnow() + timedelta(days=30*6)).replace(microsecond=0).isoformat()
            sub_row = [str(target_user_id), plan, "confirmed", expires, now_iso(), row[2] if len(row)>2 else ""]
            await sheets_append(f"{SUBS_SHEET}!A:F", [sub_row])
            # update Users sheet purchase_status/expires_at and send welcome & referral code
            # find user row
            user_lookup = await find_user_row_by_id(target_user_id)
            if user_lookup:
                idx, urow = user_lookup
                # set purchase_status, expires_at
                if len(urow) < 8:
                    while len(urow) < 8:
                        urow.append("")
                urow[6] = "active"
                urow[7] = expires
                # if no referral_code set, keep existing; send referral code message
                # write back
                range_u = f"{USERS_SHEET}!A{idx}:K{idx}"
                await sheets_update(range_u, [urow])
            # DM user
            try:
                await bot.send_message(target_user_id, "🎉 پرداخت شما تایید شد. تبریک! اشتراک شما فعال شد.\nکد معرفی شما: " + (urow[3] if user_lookup else generate_referral_code()))
                # if premium, add both channels
                # add user to normal or premium channel(s) if bot can (via invite link creation)
                if plan == "premium":
                    # add to both channels by creating unlimited invite link (member_limit=1 then schedule removal?), better create permanent admin invite? Here create single use invite for the user
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
            except Exception as e:
                logger.exception("Failed to DM user on confirm: %s", e)
            await callback_query.answer("خرید تأیید شد.")
        else:
            # reject
            range_row = f"{PENDING_SHEET}!D{pending_row_idx}:D{pending_row_idx}"
            await sheets_update(range_row, [["rejected"]])
            try:
                await bot.send_message(target_user_id, "❌ خرید شما تایید نشد. لطفاً با پشتیبانی تماس بگیرید یا اطلاعات تراکنش را بررسی کنید.")
            except Exception:
                logger.exception("Could not notify user about rejected payment.")
            await callback_query.answer("خرید رد شد.")
    except Exception as e:
        logger.exception("Error processing admin callback: %s", e)
        await callback_query.answer("خطا در پردازش.")

# -------------------------
# Subscription schedule rebuild (on startup)
# -------------------------
async def rebuild_schedules_from_subscriptions():
    # read subs sheet and schedule removals when expiry arrives
    rows = await sheets_get(SUBS_RANGE)
    if not rows or len(rows) <= 1:
        logger.info("No subscriptions to rebuild.")
        return
    header = rows[0]
    for idx, row in enumerate(rows[1:], start=2):
        try:
            user_id = int(row[0])
            plan = row[1] if len(row)>1 else ""
            status = row[2] if len(row)>2 else ""
            expires_at = row[3] if len(row)>3 else ""
            expires_dt = parse_iso_or_none(expires_at)
            if not expires_dt:
                # if expires_at isn't parseable, skip and log
                logger.error("rebuild row err: Invalid isoformat string: %r", expires_at)
                continue
            now = datetime.utcnow()
            if expires_dt <= now:
                # subscription already expired -> ensure removed
                # here we will attempt to remove from channels
                if plan == "premium":
                    for ch in [NORMAL_CHANNEL_ID, PREMIUM_CHANNEL_ID]:
                        if ch:
                            asyncio.create_task(remove_user_from_chat(ch, user_id))
                else:
                    if NORMAL_CHANNEL_ID:
                        asyncio.create_task(remove_user_from_chat(NORMAL_CHANNEL_ID, user_id))
                # optionally update sheet status
                await sheets_update(f"{SUBS_SHEET}!C{idx}:C{idx}", [["expired"]])
            else:
                # schedule a task to remove at expiration
                delay = (expires_dt - now).total_seconds()
                async def expire_job(chat_ids, uid):
                    await asyncio.sleep(delay)
                    for ch in chat_ids:
                        if ch:
                            await remove_user_from_chat(ch, uid)
                    # DM user
                    try:
                        await bot.send_message(uid, "⏳ اشتراک شما به پایان رسید. جهت تمدید یا خرید مجدد به من مراجعه کنید.")
                    except Exception as e:
                        logger.debug("Could not DM user on subscription expiry: %s", e)
                chat_ids = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if plan=="premium" else [NORMAL_CHANNEL_ID]
                asyncio.create_task(expire_job([ch for ch in chat_ids if ch], user_id))
        except Exception as e:
            logger.exception("rebuild_schedules_from_subscriptions error: %s", e)

# -------------------------
# Startup/Shutdown hooks
# -------------------------
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
    # delete webhook if exists
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted on startup.")
    except Exception:
        logger.exception("Failed to delete webhook on startup.")

    # start webserver
    try:
        asyncio.create_task(start_webserver())
    except Exception:
        logger.exception("Failed to start webserver.")

    # start background poller for pending payments
    asyncio.create_task(poll_pending_notify_admin())

    # rebuild subscription schedules
    asyncio.create_task(rebuild_schedules_from_subscriptions())

# -------------------------
# Robust polling wrapper
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
            logger.warning("TerminatedByOtherGetUpdates: %s", e)
            wait = min(60, 5*attempt)
            logger.info("Sleeping %d seconds before retrying...", wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached for TerminatedByOtherGetUpdates.")
                break
        except Exception as e:
            logger.exception("Unhandled exception in polling: %s", e)
            wait = min(60, 5*attempt)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached for polling.")
                break

# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    logger.info("=== BOT STARTING ===")
    print("=== BOT STARTING ===")
    run_polling_with_retries(skip_updates=True, max_retries=20)
