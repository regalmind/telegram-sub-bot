# main.py
# نسخهٔ کامل و عملیاتی — همه‌ی جزئیات مورد درخواست پیاده‌سازی شده
# - خواندن Google credentials از env یا فایل
# - پشتیبانی از BOT_TOKEN / TELEGRAM_TOKEN
# - نوشتن امن در Google Sheets (run_in_executor)
# - حذف webhook و drop_pending_updates
# - health endpoint با aiohttp
# - ساخت خودکار شیت‌ها و هدرها
# - ثبت ایمیل، رفرال، خرید، تیکت پشتیبانی
# - تولید کد رفرال، ارسال لینک دعوت، عضویت تست 10 دقیقه‌ای با زمانبندی حذف
# - فعال‌سازی اشتراک (normal / premium) با ثبت در شیت و زمانبندی حذف
# - poll کننده برای فعال‌سازی خریدهای تاییدشده و ارسال پاسخ پشتیبانی
# - مدیریت خطا، retry، backoff و لاگ کامل
# کاملاً آمادهٔ deploy روی Render یا اجرا محلی پس از تنظیم متغیرهای محیطی.

import os
import json
import base64
import binascii
import logging
import asyncio
import time
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates, TelegramAPIError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiohttp import web

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("telegram-sub-bot")

# -------------------------
# Env vars (set these in Render or your environment)
# -------------------------
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or os.getenv("SPREADSHEET")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
PORT = int(os.getenv("PORT", "8000"))

# Channels and admin
REQUIRED_CHANNELS = os.getenv("REQUIRED_CHANNELS", "")  # comma-separated IDs or @usernames
CHANNEL_TEST = os.getenv("CHANNEL_TEST", "")            # id or @username
CHANNEL_NORMAL = os.getenv("CHANNEL_NORMAL", "")
CHANNEL_PREMIUM = os.getenv("CHANNEL_PREMIUM", "")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")      # optional admin notification

# Basic validation
if not TOKEN:
    logger.error("Missing BOT_TOKEN / TELEGRAM_TOKEN")
    raise SystemExit("Missing BOT_TOKEN / TELEGRAM_TOKEN")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID")
    raise SystemExit("Missing SPREADSHEET_ID")

# -------------------------
# Google creds loader (robust)
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
            logger.debug("GOOGLE_CREDENTIALS not raw JSON; attempt substring/base64.")
        # try to extract JSON substring (if user pasted extra text)
        try:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = s[start:end+1]
                data = json.loads(candidate)
                logger.info("Recovered JSON substring from GOOGLE_CREDENTIALS.")
                return data
        except Exception as e:
            logger.debug("recover substring failed: %s", e)
        # try base64
        try:
            decoded = base64.b64decode(s, validate=True)
            data = json.loads(decoded.decode("utf-8"))
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64).")
            return data
        except Exception as e:
            logger.debug("base64 decode parse failed: %s", e)

    # fallback to file
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info("Loaded Google credentials from file: %s", GOOGLE_SERVICE_ACCOUNT_FILE)
                return data
        except Exception as e:
            logger.exception("Failed to load/parse GOOGLE_SERVICE_ACCOUNT file '%s': %s", GOOGLE_SERVICE_ACCOUNT_FILE, e)

    logger.error("No valid Google credentials found. Provide GOOGLE_CREDENTIALS or upload service-account.json.")
    raise SystemExit("Missing Google credentials")

# -------------------------
# Initialize Google Sheets client
# -------------------------
try:
    creds_info = load_google_creds()
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sheets_service = build("sheets", "v4", credentials=creds)
    sheets = sheets_service.spreadsheets()
    logger.info("Google Sheets client initialized.")
except Exception as e:
    logger.exception("Failed to initialize Google Sheets client: %s", e)
    sheets = None

# -------------------------
# Bot & dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# -------------------------
# Helper wrappers for blocking Sheets calls (run in executor)
# -------------------------
def _sheets_get_meta(spreadsheet_id: str):
    return sheets.get(spreadsheetId=spreadsheet_id).execute()

def _sheets_values_get(spreadsheet_id: str, range_name: str):
    return sheets.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()

def _sheets_values_append(spreadsheet_id: str, range_name: str, values: List[List[Any]]):
    return sheets.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="USER_ENTERED", body={"values": values}).execute()

def _sheets_values_update(spreadsheet_id: str, range_name: str, values: List[List[Any]], value_input_option="RAW"):
    return sheets.values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option, body={"values": values}).execute()

def _sheets_batch_update(spreadsheet_id: str, body: Dict[str, Any]):
    return sheets.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

async def run_in_executor(func, *args, retries: int = 2, delay: float = 1.0):
    loop = asyncio.get_running_loop()
    for attempt in range(1, retries + 2):
        try:
            return await loop.run_in_executor(None, func, *args)
        except Exception as e:
            logger.exception("Executor call attempt %d failed: %s", attempt, e)
            if attempt <= retries:
                await asyncio.sleep(delay * attempt)
            else:
                raise

async def sheets_meta(spreadsheet_id: str):
    return await run_in_executor(_sheets_get_meta, spreadsheet_id)

async def sheets_values_get(spreadsheet_id: str, range_name: str):
    return await run_in_executor(_sheets_values_get, spreadsheet_id, range_name)

async def sheets_values_append(spreadsheet_id: str, range_name: str, row: List[Any]):
    return await run_in_executor(_sheets_values_append, spreadsheet_id, range_name, [row])

async def sheets_values_update(spreadsheet_id: str, range_name: str, rows: List[List[Any]], value_input_option="RAW"):
    return await run_in_executor(_sheets_values_update, spreadsheet_id, range_name, rows, value_input_option)

async def sheets_batch_update(spreadsheet_id: str, body: Dict[str, Any]):
    return await run_in_executor(_sheets_batch_update, spreadsheet_id, body)

# -------------------------
# Default sheets & headers
# -------------------------
DEFAULT_SHEET_TITLES = ["Users", "Purchases", "Referrals", "Support", "Subscriptions"]
DEFAULT_HEADERS = {
    "Users": ["telegram_id", "full_name", "email", "registered_at", "referrer", "referral_code", "notes"],
    "Purchases": ["telegram_id", "full_name", "email", "product", "amount", "transaction_info", "status", "requested_at", "activated_at", "expires_at", "admin_note"],
    "Referrals": ["telegram_id", "referral_code", "referred_count", "created_at"],
    "Support": ["ticket_id", "telegram_id", "full_name", "subject", "message", "status", "created_at", "response", "responded_at"],
    "Subscriptions": ["telegram_id", "product", "activated_at", "expires_at", "active"]
}

async def ensure_sheets_and_headers() -> bool:
    """
    Create missing sheets and headers if necessary. Idempotent.
    """
    if sheets is None:
        logger.error("sheets client not initialized.")
        return False
    try:
        meta = await sheets_meta(SPREADSHEET_ID)
        titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
        requests = []
        for title in DEFAULT_SHEET_TITLES:
            if title not in titles:
                requests.append({"addSheet": {"properties": {"title": title}}})
                logger.info("Will create sheet: %s", title)
        if requests:
            await sheets_batch_update(SPREADSHEET_ID, {"requests": requests})
            # small sleep to let Google finalize
            await asyncio.sleep(0.5)
        # ensure headers
        for title in DEFAULT_SHEET_TITLES:
            header_range = f"{title}!A1:{chr(65 + len(DEFAULT_HEADERS[title]) - 1)}1"
            existing = await sheets_values_get(SPREADSHEET_ID, f"{title}!A1:Z1")
            if not existing or not existing.get("values"):
                await sheets_values_update(SPREADSHEET_ID, header_range, [DEFAULT_HEADERS[title]])
                logger.info("Header written to %s", title)
        return True
    except Exception as e:
        logger.exception("ensure_sheets_and_headers failed: %s", e)
        return False

# -------------------------
# Utilities: channels, parsing env
# -------------------------
def parse_channel_list(s: str) -> List[str]:
    if not s:
        return []
    return [it.strip() for it in s.split(",") if it.strip()]

REQUIRED_CHANNELS_LIST = parse_channel_list(REQUIRED_CHANNELS)

# -------------------------
# Referral code generator
# -------------------------
def make_referral_code() -> str:
    return "R" + secrets.token_hex(4).upper()

# -------------------------
# Chat membership helpers
# -------------------------
async def is_member_of(chat_id_or_username: str, user_id: int) -> bool:
    try:
        chat = int(chat_id_or_username) if str(chat_id_or_username).lstrip("-").isdigit() else chat_id_or_username
        member = await bot.get_chat_member(chat, user_id)
        status = getattr(member, "status", None) or (member.get("status") if isinstance(member, dict) else None)
        logger.debug("get_chat_member(%s,%d) -> %s", chat_id_or_username, user_id, status)
        return status in ("member", "creator", "administrator")
    except TelegramAPIError as e:
        logger.warning("get_chat_member API error for %s user %d: %s", chat_id_or_username, user_id, e)
        return False
    except Exception as e:
        logger.exception("is_member_of unexpected: %s", e)
        return False

async def create_invite_link(chat_id_or_username: str, expire_seconds: Optional[int] = None, member_limit: Optional[int] = None) -> Optional[str]:
    """
    Create an invite link. Fallback to export_chat_invite_link if create_chat_invite_link not available.
    Returns invite URL or None.
    """
    try:
        chat = int(chat_id_or_username) if str(chat_id_or_username).lstrip("-").isdigit() else chat_id_or_username
        # prefer create_chat_invite_link (newer API). aiogram may return ChatInviteLink object.
        params = {}
        if expire_seconds:
            params["expire_date"] = int(time.time()) + int(expire_seconds)
        if member_limit:
            params["member_limit"] = int(member_limit)
        try:
            # some aiogram versions: create_chat_invite_link(chat_id, **params)
            link_obj = await bot.create_chat_invite_link(chat, **params) if params else await bot.create_chat_invite_link(chat)
            # unify
            if isinstance(link_obj, str):
                return link_obj
            if hasattr(link_obj, "invite_link"):
                return getattr(link_obj, "invite_link")
            if isinstance(link_obj, dict):
                return link_obj.get("invite_link")
        except Exception:
            # fallback to export_chat_invite_link (may return string)
            try:
                link = await bot.export_chat_invite_link(chat)
                return link
            except Exception as e2:
                logger.exception("export_chat_invite_link failed: %s", e2)
                return None
    except Exception as e:
        logger.exception("create_invite_link unexpected: %s", e)
        return None

# -------------------------
# Scheduling removal tasks (in-memory)
# -------------------------
scheduled_tasks: Dict[str, asyncio.Task] = {}  # key: f"{telegram_id}:{chat}"

async def schedule_removal(telegram_id: int, chat_id_or_username: str, when: datetime):
    """
    Schedule a ban (removal) at 'when'. We store tasks in memory; on restart subscriptions sheet can be used to rebuild schedules.
    """
    key = f"{telegram_id}:{chat_id_or_username}"
    # cancel previous if exists
    prev = scheduled_tasks.get(key)
    if prev and not prev.done():
        prev.cancel()
    delay = (when - datetime.utcnow()).total_seconds()
    if delay < 0:
        delay = 0
    async def _job():
        try:
            await asyncio.sleep(delay)
            chat = int(chat_id_or_username) if str(chat_id_or_username).lstrip("-").isdigit() else chat_id_or_username
            # Ban to remove; use ban_chat_member (aiogram), may require bot admin
            try:
                await bot.ban_chat_member(chat, telegram_id)
                logger.info("Removed user %d from %s (ban)", telegram_id, chat_id_or_username)
            except Exception as e:
                logger.exception("Failed to ban user %d from %s: %s", telegram_id, chat_id_or_username, e)
        except asyncio.CancelledError:
            logger.info("Scheduled removal for %s cancelled", key)
        except Exception as e:
            logger.exception("schedule_removal job exception: %s", e)
    task = asyncio.create_task(_job())
    scheduled_tasks[key] = task
    return task

# -------------------------
# Activate subscription: send invite links & record in Subscriptions sheet + schedule removal
# -------------------------
async def activate_subscription_for_user(telegram_id: int, product: str, months: int = 6):
    """
    product: "normal" or "premium"
    months: duration
    """
    channels_to_invite = []
    if product == "normal":
        if CHANNEL_NORMAL:
            channels_to_invite.append(CHANNEL_NORMAL)
    elif product == "premium":
        if CHANNEL_NORMAL:
            channels_to_invite.append(CHANNEL_NORMAL)
        if CHANNEL_PREMIUM:
            channels_to_invite.append(CHANNEL_PREMIUM)
    activated_at = datetime.utcnow()
    expires_at = activated_at + timedelta(days=30 * months)
    # For each channel, create invite link and send to user
    for ch in channels_to_invite:
        try:
            link = await create_invite_link(ch, expire_seconds=60 * 60 * 24)  # 24h
            if link:
                await bot.send_message(telegram_id, f"✅ اشتراک شما برای کانال {ch} فعال شد. برای پیوستن از لینک زیر استفاده کنید:\n{link}")
            else:
                await bot.send_message(telegram_id, f"✅ اشتراک شما فعال شد، اما نتوانستم لینک ایجاد کنم. لطفاً دستی عضو شوید: {ch}")
        except Exception as e:
            logger.exception("Error inviting user %d to %s: %s", telegram_id, ch, e)
    # record in Subscriptions sheet
    try:
        await sheets_values_append(SPREADSHEET_ID, "Subscriptions!A:E", [telegram_id, product, activated_at.isoformat(), expires_at.isoformat(), "TRUE"])
    except Exception as e:
        logger.exception("Failed to write Subscriptions row: %s", e)
    # schedule removal for each channel (we schedule removal on CHANNEL_NORMAL for normal, for premium schedule both)
    for ch in channels_to_invite:
        try:
            await schedule_removal(telegram_id, ch, expires_at)
        except Exception as e:
            logger.exception("Failed to schedule removal for %d from %s: %s", telegram_id, ch, e)

# -------------------------
# Background pollers
# -------------------------
POLL_INTERVAL = 20  # seconds

async def poll_purchases_and_activate():
    """
    Look for purchases with status 'confirmed' (case-insensitive) and not yet activated.
    We'll read Purchases!A:K (headers assumed).
    If a row has status confirmed and activated_at empty, activate subscription and write activated_at/expires_at.
    """
    while True:
        try:
            data = await sheets_values_get(SPREADSHEET_ID, "Purchases!A2:K")
            rows = data.get("values", []) if data else []
            for idx, r in enumerate(rows, start=2):
                try:
                    status = r[6].strip().lower() if len(r) > 6 and r[6] else ""
                    activated_at = r[8] if len(r) > 8 and r[8] else ""
                    if status in ("confirmed", "approved") and not activated_at:
                        telegram_id = int(r[0]) if len(r) > 0 and r[0].isdigit() else None
                        product = r[3] if len(r) > 3 else None
                        months = 6
                        if telegram_id and product:
                            await activate_subscription_for_user(telegram_id, product, months=months)
                            activated_iso = datetime.utcnow().isoformat()
                            expires_iso = (datetime.utcnow() + timedelta(days=30*months)).isoformat()
                            # update cells I (activated_at) and J (expires_at), columns are 9 and 10 -> I/J
                            update_range = f"Purchases!I{idx}:J{idx}"
                            await sheets_values_update(SPREADSHEET_ID, update_range, [[activated_iso, expires_iso]])
                            logger.info("Activated purchase row %d for user %s", idx, telegram_id)
                except Exception as e:
                    logger.exception("Error processing purchase row %d: %s", idx, e)
        except Exception as e:
            logger.exception("poll_purchases_and_activate top-level error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

async def poll_support_responses():
    """
    Scan Support sheet for responses added by admin and send them to users.
    When response present and status != 'responded', send message and mark status/responded_at.
    """
    while True:
        try:
            data = await sheets_values_get(SPREADSHEET_ID, "Support!A2:I")
            rows = data.get("values", []) if data else []
            for idx, r in enumerate(rows, start=2):
                try:
                    status = r[5].strip().lower() if len(r) > 5 and r[5] else ""
                    telegram_id = int(r[1]) if len(r) > 1 and r[1].isdigit() else None
                    response_text = r[7] if len(r) > 7 else ""
                    responded_at = r[8] if len(r) > 8 else ""
                    if response_text and status != "responded":
                        if telegram_id:
                            await bot.send_message(telegram_id, f"📬 پاسخ پشتیبانی:\n\n{response_text}")
                        # mark responded and set responded_at
                        update_range = f"Support!F{idx}:I{idx}"
                        await sheets_values_update(SPREADSHEET_ID, update_range, [["responded", "", response_text, datetime.utcnow().isoformat()]])
                        logger.info("Sent support response for row %d", idx)
                except Exception as e:
                    logger.exception("Error processing support row %d: %s", idx, e)
        except Exception as e:
            logger.exception("poll_support_responses top-level error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

# -------------------------
# In-memory user flow state (simple)
# -------------------------
user_flow_state: Dict[int, Dict[str, Any]] = {}

def get_state(user_id: int) -> Dict[str, Any]:
    return user_flow_state.setdefault(user_id, {"stage": None})

# -------------------------
# Handlers: registration, referral, main menu, test, purchase, support, platform
# -------------------------
def build_main_keyboard() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("تست کانال معمولی", "خرید کانال معمولی")
    kb.row("خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم")
    return kb

@dp.message_handler(commands=["start"])
async def on_start(message: types.Message):
    uid = message.from_user.id
    # Ensure sheets exists
    await ensure_sheets_and_headers()
    # Check required channels membership
    missing = []
    for ch in REQUIRED_CHANNELS_LIST:
        member = await is_member_of(ch, uid)
        if not member:
            missing.append(ch)
    if missing:
        msg = "برای استفاده از ربات لطفاً ابتدا در کانال‌های زیر عضو شوید:\n"
        for ch in missing:
            link = await create_invite_link(ch, expire_seconds=60*60*24)
            if link:
                msg += f"- {ch}: {link}\n"
            else:
                msg += f"- {ch}\n"
        msg += "\nپس از عضویت، /start را مجدداً ارسال کنید."
        await message.answer(msg)
        return
    # proceed to request email
    get_state(uid)["stage"] = "awaiting_email"
    await message.answer("👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") == "awaiting_email" and m.text and "@" in m.text and "." in m.text)
async def receive_email(message: types.Message):
    uid = message.from_user.id
    email = message.text.strip()
    st = get_state(uid)
    st["email"] = email
    st["stage"] = "awaiting_referral"
    # Append basic user row (note: we'll also record referral later)
    await sheets_values_append(SPREADSHEET_ID, "Users!A:G", [uid, message.from_user.full_name, email, datetime.utcnow().isoformat(), "", "", "registered"])
    await message.answer("✅ ایمیل ثبت شد.\nاگر کد معرف دارید آن را وارد کنید، در غیر این صورت 'ندارم' را بنویسید.")

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") == "awaiting_referral" and m.text)
async def receive_referral(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()
    st = get_state(uid)
    ref_provided = ""
    if text.lower() in ("ندارم", "ندار", "no", "none", "skip"):
        ref_provided = ""
    else:
        ref_provided = text.strip()
    # generate own referral code
    my_code = make_referral_code()
    st["referrer"] = ref_provided
    st["my_referral_code"] = my_code
    st["stage"] = "main"
    # save referral info
    await sheets_values_append(SPREADSHEET_ID, "Referrals!A:D", [uid, my_code, 0, datetime.utcnow().isoformat()])
    # update Users sheet: append another row noting referral (simpler than find+update)
    await sheets_values_append(SPREADSHEET_ID, "Users!A:G", [uid, message.from_user.full_name, st.get("email", ""), datetime.utcnow().isoformat(), ref_provided, my_code, "complete"])
    await message.answer(f"✅ ثبت شد. کد رفرال شما: {my_code}\nلطفاً از منوی زیر انتخاب کنید:", reply_markup=build_main_keyboard())

@dp.message_handler(lambda m: m.text == "تست کانال معمولی")
async def handle_test_channel(message: types.Message):
    uid = message.from_user.id
    if not CHANNEL_TEST:
        await message.answer("کانال تست تنظیم نشده است. با مدیر تماس بگیرید.")
        return
    # create a one-time invite link limited to 1 member and 1 hour expiry
    link = await create_invite_link(CHANNEL_TEST, expire_seconds=60*60, member_limit=1)
    if not link:
        await message.answer("خطا در ایجاد لینک دعوت؛ لطفاً بعداً تلاش کنید یا با مدیر تماس بگیرید.")
        return
    await message.answer("✅ لینک تست ساخته شد — لطفاً با کلیک روی لینک وارد کانال تست شوید. شما 10 دقیقه آنجا خواهید بود.")
    await message.answer(link)
    # background poll for join then schedule removal
    async def wait_for_join_and_schedule():
        # check membership up to 5 minutes (30 checks every 10s)
        for _ in range(30):
            if await is_member_of(CHANNEL_TEST, uid):
                # record trial in Purchases sheet
                now = datetime.utcnow()
                expires = now + timedelta(minutes=10)
                await sheets_values_append(SPREADSHEET_ID, "Purchases!A:K", [uid, message.from_user.full_name, "", "trial", "0", "trial", "activated", now.isoformat(), now.isoformat(), expires.isoformat(), ""])
                # notify user
                try:
                    await bot.send_message(uid, f"🎉 شما به کانال تست اضافه شدید. مدت: 10 دقیقه. پس از پایان عضویت حذف خواهید شد.")
                except Exception:
                    pass
                # schedule removal
                await schedule_removal(uid, CHANNEL_TEST, expires)
                return
            await asyncio.sleep(10)
        # if not joined
        try:
            await bot.send_message(uid, "⚠️ به نظر می‌رسد که شما عضو کانال تست نشدید. لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
        except Exception:
            pass
    asyncio.create_task(wait_for_join_and_schedule())

@dp.message_handler(lambda m: m.text == "خرید کانال معمولی")
async def handle_buy_normal(message: types.Message):
    uid = message.from_user.id
    get_state(uid)["stage"] = "awaiting_payment_normal"
    await message.answer("💳 لطفاً مبلغ مربوط به کانال معمولی را به کارت زیر واریز کنید:\n`6037-9917-1234-5678`\nپس از پرداخت، اطلاعات تراکنش (شناسه، تاریخ، مبلغ و 4 رقم آخر کارت) را ارسال کنید.")

@dp.message_handler(lambda m: m.text == "خرید کانال ویژه")
async def handle_buy_premium(message: types.Message):
    uid = message.from_user.id
    get_state(uid)["stage"] = "awaiting_payment_premium"
    await message.answer("💳 لطفاً مبلغ مربوط به کانال ویژه را به کارت زیر واریز کنید:\n`6037-9917-1234-5678`\nپس از پرداخت، اطلاعات تراکنش را ارسال کنید. پس از تایید توسط مدیر، شما در هر دو کانال عضو خواهید شد.")

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") in ("awaiting_payment_normal", "awaiting_payment_premium") and m.text)
async def receive_payment_info(message: types.Message):
    uid = message.from_user.id
    st = get_state(uid)
    stage = st.get("stage")
    product = "normal" if stage == "awaiting_payment_normal" else "premium"
    txn_info = message.text.strip()
    # Append purchase as pending
    await sheets_values_append(SPREADSHEET_ID, "Purchases!A:K", [uid, message.from_user.full_name, st.get("email", ""), product, "", txn_info, "pending", datetime.utcnow().isoformat(), "", "", ""])
    st["stage"] = "main"
    await message.answer("✅ اطلاعات تراکنش شما ثبت شد. پس از تایید مدیریت، اشتراک شما فعال خواهد شد.")

@dp.message_handler(lambda m: m.text == "پشتیبانی")
async def support_start(message: types.Message):
    uid = message.from_user.id
    get_state(uid)["stage"] = "awaiting_support"
    await message.answer("🧰 لطفاً سوال یا مشکل خود را ارسال کنید تا تیکت ثبت شود.")

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") == "awaiting_support" and m.text)
async def support_receive(message: types.Message):
    uid = message.from_user.id
    body = message.text.strip()
    ticket_id = f"T{int(time.time())}{uid%1000}"
    await sheets_values_append(SPREADSHEET_ID, "Support!A:I", [ticket_id, uid, message.from_user.full_name, "پشتیبانی", body, "open", datetime.utcnow().isoformat(), "", ""])
    get_state(uid)["stage"] = "main"
    await message.answer(f"✅ تیکت ثبت شد. شناسه: {ticket_id}\nپاسخ مدیریت در همین صفحه در قسمت پاسخ درج خواهد شد و برای شما ارسال می‌شود.")

@dp.message_handler(lambda m: m.text == "توضیحات پلتفرم")
async def platform_info(message: types.Message):
    PLATFORM_TEXT = (
        "📘 توضیحات پلتفرم:\n\n"
        "این ربات برای مدیریت اشتراک کانال‌های آموزشی طراحی شده است. "
        "ثبت ایمیل و عضویت در کانال‌ها الزامی است. خریدها توسط مدیریت در شیت تایید می‌شوند."
    )
    await message.answer(PLATFORM_TEXT)

@dp.message_handler(lambda m: True)
async def fallback(message: types.Message):
    uid = message.from_user.id
    st = get_state(uid)
    if st.get("stage") in (None, "main"):
        await message.answer("برای ادامه لطفاً از منوی زیر انتخاب کنید.", reply_markup=build_main_keyboard())
    else:
        await message.answer("در این مرحله منتظر اطلاعات مورد نیاز هستیم. اگر می‌خواهید از ابتدا شروع کنید /start را ارسال کنید.")

# -------------------------
# Health server
# -------------------------
async def start_webserver():
    app = web.Application()
    async def root(req):
        return web.Response(text="OK")
    app.router.add_get("/", root)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server started on port %s", PORT)

# -------------------------
# On startup: webhook delete, ensure sheets, start polls & health server, rebuild schedules
# -------------------------
async def rebuild_schedules_from_subscriptions():
    try:
        data = await sheets_values_get(SPREADSHEET_ID, "Subscriptions!A2:E")
        rows = data.get("values", []) if data else []
        for r in rows:
            try:
                telegram_id = int(r[0]) if len(r) > 0 and str(r[0]).isdigit() else None
                product = r[1] if len(r) > 1 else None
                activated_at = r[2] if len(r) > 2 else None
                expires_at = r[3] if len(r) > 3 else None
                active = r[4] if len(r) > 4 else ""
                if telegram_id and expires_at:
                    expires_dt = datetime.fromisoformat(expires_at)
                    # schedule removal if expires in future
                    if expires_dt > datetime.utcnow():
                        # choose channel to remove depending on product (approx)
                        if product == "normal" and CHANNEL_NORMAL:
                            await schedule_removal(telegram_id, CHANNEL_NORMAL, expires_dt)
                        elif product == "premium":
                            if CHANNEL_NORMAL:
                                await schedule_removal(telegram_id, CHANNEL_NORMAL, expires_dt)
                            if CHANNEL_PREMIUM:
                                await schedule_removal(telegram_id, CHANNEL_PREMIUM, expires_dt)
            except Exception as e:
                logger.exception("rebuild_schedules_from_subscriptions row error: %s", e)
    except Exception as e:
        logger.exception("Failed to rebuild schedules: %s", e)

async def on_startup(dp_object):
    # remove webhook to prevent conflict with polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted (drop_pending_updates=True)")
    except Exception as e:
        logger.exception("Failed to delete webhook on startup: %s", e)
    # ensure sheets and headers
    ok = await ensure_sheets_and_headers()
    if not ok:
        logger.warning("ensure_sheets_and_headers returned False; proceeding but some features may fail.")
    # start health server
    try:
        asyncio.create_task(start_webserver())
    except Exception as e:
        logger.exception("Failed to start webserver: %s", e)
    # start background pollers
    try:
        asyncio.create_task(poll_purchases_and_activate())
        asyncio.create_task(poll_support_responses())
    except Exception as e:
        logger.exception("Failed to start pollers: %s", e)
    # rebuild scheduled removals from subscriptions sheet
    try:
        asyncio.create_task(rebuild_schedules_from_subscriptions())
    except Exception as e:
        logger.exception("Failed to rebuild schedules: %s", e)

# -------------------------
# Robust polling main (handle TerminatedByOtherGetUpdates)
# -------------------------
def run_polling_with_retries(skip_updates: bool = True, max_retries: int = 20):
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Starting polling (attempt %d)...", attempt)
            executor.start_polling(dp, skip_updates=skip_updates, on_startup=on_startup)
            logger.info("executor.start_polling returned normally.")
            break
        except TerminatedByOtherGetUpdates as e:
            wait = min(60, 5 * attempt)
            logger.warning("TerminatedByOtherGetUpdates detected: %s — sleeping %d seconds", e, wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached for TerminatedByOtherGetUpdates. Exiting.")
                break
        except Exception as e:
            wait = min(60, 5 * attempt)
            logger.exception("Unhandled exception starting polling: %s — sleeping %d seconds", e, wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached for polling. Exiting.")
                break

# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    logger.info("=== BOT STARTING ===")
    print("=== BOT STARTING ===")
    # re-parse required channels for runtime (in case parse function defined above)
    REQUIRED_CHANNELS_LIST = parse_channel_list(REQUIRED_CHANNELS)
    run_polling_with_retries(skip_updates=True, max_retries=20)
