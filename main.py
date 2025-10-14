# main.py (UPDATED)
# شامل اصلاح برای خطاهای rebuild_schedules_from_subscriptions و ارسال لیست pending به ادمین
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
# Env vars
# -------------------------
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or os.getenv("SPREADSHEET")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
PORT = int(os.getenv("PORT", "8000"))

REQUIRED_CHANNELS = os.getenv("REQUIRED_CHANNELS", "")
CHANNEL_TEST = os.getenv("CHANNEL_TEST", "")
CHANNEL_NORMAL = os.getenv("CHANNEL_NORMAL", "")
CHANNEL_PREMIUM = os.getenv("CHANNEL_PREMIUM", "")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID")) if os.getenv("ADMIN_TELEGRAM_ID") else None

if not TOKEN:
    logger.error("Missing BOT_TOKEN / TELEGRAM_TOKEN")
    raise SystemExit("Missing BOT_TOKEN / TELEGRAM_TOKEN")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID")
    raise SystemExit("Missing SPREADSHEET_ID")

# -------------------------
# Google creds loader
# -------------------------
def load_google_creds() -> Dict[str, Any]:
    if GOOGLE_CREDENTIALS_ENV:
        s = GOOGLE_CREDENTIALS_ENV.strip()
        try:
            data = json.loads(s)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return data
        except json.JSONDecodeError:
            logger.debug("GOOGLE_CREDENTIALS not raw JSON; try substring/base64.")
        try:
            start = s.find("{"); end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = s[start:end+1]
                data = json.loads(candidate)
                logger.info("Recovered JSON substring from GOOGLE_CREDENTIALS.")
                return data
        except Exception as e:
            logger.debug("recover substring failed: %s", e)
        try:
            decoded = base64.b64decode(s, validate=True)
            data = json.loads(decoded.decode("utf-8"))
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64).")
            return data
        except Exception as e:
            logger.debug("base64 decode parse failed: %s", e)
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info("Loaded Google credentials from file: %s", GOOGLE_SERVICE_ACCOUNT_FILE)
                return data
        except Exception as e:
            logger.exception("Failed to load/parse GOOGLE_SERVICE_ACCOUNT file '%s': %s", GOOGLE_SERVICE_ACCOUNT_FILE, e)
    logger.error("No valid Google credentials found.")
    raise SystemExit("Missing Google credentials")

# -------------------------
# Initialize Sheets
# -------------------------
try:
    creds_info = load_google_creds()
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sheets_service = build("sheets", "v4", credentials=creds)
    sheets = sheets_service.spreadsheets()
    logger.info("Google Sheets client initialized.")
except Exception as e:
    logger.exception("Failed to init Google Sheets client: %s", e)
    sheets = None

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# -------------------------
# Sheets wrappers
# -------------------------
def _sheets_get(spreadsheet_id: str):
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
    return await run_in_executor(_sheets_get, spreadsheet_id)
async def sheets_values_get(spreadsheet_id: str, range_name: str):
    return await run_in_executor(_sheets_values_get, spreadsheet_id, range_name)
async def sheets_values_append(spreadsheet_id: str, range_name: str, row: List[Any]):
    return await run_in_executor(_sheets_values_append, spreadsheet_id, range_name, [row])
async def sheets_values_update(spreadsheet_id: str, range_name: str, rows: List[List[Any]], value_input_option="RAW"):
    return await run_in_executor(_sheets_values_update, spreadsheet_id, range_name, rows, value_input_option)
async def sheets_batch_update(spreadsheet_id: str, body: Dict[str, Any]):
    return await run_in_executor(_sheets_batch_update, spreadsheet_id, body)

# -------------------------
# Defaults
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
        if requests:
            await sheets_batch_update(SPREADSHEET_ID, {"requests": requests})
            await asyncio.sleep(0.5)
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
# util helpers
# -------------------------
def parse_channel_list(s: str) -> List[str]:
    if not s:
        return []
    return [it.strip() for it in s.split(",") if it.strip()]
REQUIRED_CHANNELS_LIST = parse_channel_list(REQUIRED_CHANNELS)

async def find_user_row(telegram_id: int) -> Optional[Dict[str, Any]]:
    try:
        data = await sheets_values_get(SPREADSHEET_ID, "Users!A2:G")
        rows = data.get("values", []) if data else []
        for idx, r in enumerate(rows, start=2):
            if len(r) > 0 and str(r[0]) == str(telegram_id):
                return {"row_index": idx, "row": r}
        return None
    except Exception as e:
        logger.exception("find_user_row failed: %s", e)
        return None

async def update_user_row(row_index: int, row_values: List[Any]):
    maxlen = len(DEFAULT_HEADERS["Users"])
    row_values = (row_values + [""] * maxlen)[:maxlen]
    range_name = f"Users!A{row_index}:{chr(65+maxlen-1)}{row_index}"
    return await sheets_values_update(SPREADSHEET_ID, range_name, [row_values])

def make_referral_code() -> str:
    return "R" + secrets.token_hex(4).upper()

async def ensure_referral_for_user(telegram_id: int, user_full_name: str):
    try:
        data = await sheets_values_get(SPREADSHEET_ID, "Referrals!A2:D")
        rows = data.get("values", []) if data else []
        for r in rows:
            if len(r) > 0 and str(r[0]) == str(telegram_id):
                return r[1] if len(r) > 1 else None
        code = make_referral_code()
        await sheets_values_append(SPREADSHEET_ID, "Referrals!A:D", [telegram_id, code, 0, datetime.utcnow().isoformat()])
        u = await find_user_row(telegram_id)
        if u:
            row = u["row"]
            full_name = row[1] if len(row) > 1 else user_full_name
            email = row[2] if len(row) > 2 else ""
            registered_at = row[3] if len(row) > 3 else datetime.utcnow().isoformat()
            referrer = row[4] if len(row) > 4 else ""
            notes = row[6] if len(row) > 6 else ""
            new_row = [telegram_id, full_name, email, registered_at, referrer, code, notes]
            await update_user_row(u["row_index"], new_row)
        return code
    except Exception as e:
        logger.exception("ensure_referral_for_user failed: %s", e)
        return None

# -------------------------
# Chat helpers
# -------------------------
async def is_member_of(chat_id_or_username: str, user_id: int) -> bool:
    try:
        chat = int(chat_id_or_username) if str(chat_id_or_username).lstrip("-").isdigit() else chat_id_or_username
        member = await bot.get_chat_member(chat, user_id)
        status = getattr(member, "status", None) or (member.get("status") if isinstance(member, dict) else None)
        return status in ("member", "creator", "administrator")
    except TelegramAPIError as e:
        logger.warning("get_chat_member API error for %s user %d: %s", chat_id_or_username, user_id, e)
        return False
    except Exception as e:
        logger.exception("is_member_of unexpected: %s", e)
        return False

async def create_invite_link(chat_id_or_username: str, expire_seconds: Optional[int] = None, member_limit: Optional[int] = None) -> Optional[str]:
    try:
        chat = int(chat_id_or_username) if str(chat_id_or_username).lstrip("-").isdigit() else chat_id_or_username
        params = {}
        if expire_seconds:
            params["expire_date"] = int(time.time()) + int(expire_seconds)
        if member_limit:
            params["member_limit"] = int(member_limit)
        try:
            link_obj = await bot.create_chat_invite_link(chat, **params) if params else await bot.create_chat_invite_link(chat)
            if isinstance(link_obj, str):
                return link_obj
            if hasattr(link_obj, "invite_link"):
                return getattr(link_obj, "invite_link")
            if isinstance(link_obj, dict):
                return link_obj.get("invite_link")
        except Exception:
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
# Removal scheduling & banning
# -------------------------
scheduled_tasks: Dict[str, asyncio.Task] = {}

async def schedule_removal(telegram_id: int, chat_id_or_username: str, when: datetime):
    key = f"{telegram_id}:{chat_id_or_username}"
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
            try:
                await bot.ban_chat_member(chat, telegram_id)
                logger.info("Banned user %d from %s", telegram_id, chat_id_or_username)
                try:
                    await sheets_values_append(SPREADSHEET_ID, "Subscriptions!A:E", [telegram_id, chat_id_or_username, "", datetime.utcnow().isoformat(), "FALSE"])
                except Exception:
                    pass
                try:
                    await bot.send_message(telegram_id, "⏳ مدت شما به پایان رسید و از کانال حذف شدید. برای دسترسی کامل، از منوی ربات گزینهٔ خرید را انتخاب کنید.")
                    await bot.send_message(telegram_id, "📌 برای خرید سریع: از منوی اصلی گزینهٔ 'خرید کانال معمولی' یا 'خرید کانال ویژه' را انتخاب کنید.")
                except Exception:
                    logger.exception("Failed to send removal notice to user %d", telegram_id)
            except Exception as e:
                logger.exception("Failed to ban user %d from %s: %s", telegram_id, chat_id_or_username, e)
        except asyncio.CancelledError:
            logger.info("Scheduled removal for %s cancelled", key)
        except Exception as e:
            logger.exception("schedule_removal job exception: %s", e)
    task = asyncio.create_task(_job())
    scheduled_tasks[key] = task
    return task

async def activate_subscription_for_user(telegram_id: int, product: str, months: int = 6):
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
    for ch in channels_to_invite:
        try:
            chat = int(ch) if str(ch).lstrip("-").isdigit() else ch
            try:
                await bot.unban_chat_member(chat, telegram_id)
            except Exception:
                pass
            link = await create_invite_link(ch, expire_seconds=60 * 60 * 24)
            if link:
                await bot.send_message(telegram_id, f"✅ اشتراک شما برای کانال {ch} فعال شد. از لینک زیر برای پیوستن استفاده کنید:\n{link}")
            else:
                await bot.send_message(telegram_id, f"✅ اشتراک شما برای کانال {ch} فعال شد. لطفاً دستی عضو شوید: {ch}")
        except Exception as e:
            logger.exception("Error inviting user %d to %s: %s", telegram_id, ch, e)
    try:
        await sheets_values_append(SPREADSHEET_ID, "Subscriptions!A:E", [telegram_id, product, activated_at.isoformat(), expires_at.isoformat(), "TRUE"])
    except Exception as e:
        logger.exception("Failed to write Subscriptions row: %s", e)
    for ch in channels_to_invite:
        try:
            await schedule_removal(telegram_id, ch, expires_at)
        except Exception as e:
            logger.exception("Failed to schedule removal for %d from %s: %s", telegram_id, ch, e)
    try:
        u = await find_user_row(telegram_id)
        full_name = u["row"][1] if u and len(u["row"]) > 1 else ""
        code = await ensure_referral_for_user(telegram_id, full_name)
        if code:
            try:
                await bot.send_message(telegram_id, f"🎉 خرید شما تایید شد! کد معرفی شما: `{code}`", parse_mode="Markdown")
            except Exception:
                pass
    except Exception as e:
        logger.exception("Failed to ensure/give referral code: %s", e)

# -------------------------
# Background pollers
# -------------------------
POLL_INTERVAL = 20
pending_notified_rows = set()

async def poll_purchases_and_activate():
    while True:
        try:
            data = await sheets_values_get(SPREADSHEET_ID, "Purchases!A2:K")
            rows = data.get("values", []) if data else []
            for idx, r in enumerate(rows, start=2):
                try:
                    status = r[6].strip().lower() if len(r) > 6 and r[6] else ""
                    activated_at = r[8] if len(r) > 8 and r[8] else ""
                    if status in ("confirmed", "approved") and not activated_at:
                        telegram_id = int(r[0]) if len(r) > 0 and str(r[0]).isdigit() else None
                        product = r[3] if len(r) > 3 else None
                        months = 6
                        if telegram_id and product:
                            await activate_subscription_for_user(telegram_id, product, months=months)
                            activated_iso = datetime.utcnow().isoformat()
                            expires_iso = (datetime.utcnow() + timedelta(days=30*months)).isoformat()
                            await sheets_values_update(SPREADSHEET_ID, f"Purchases!I{idx}:J{idx}", [[activated_iso, expires_iso]])
                            logger.info("Activated purchase row %d for user %s", idx, telegram_id)
                except Exception as e:
                    logger.exception("Error processing purchase row %d: %s", idx, e)
        except Exception as e:
            logger.exception("poll_purchases_and_activate top-level error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

async def poll_support_responses():
    while True:
        try:
            data = await sheets_values_get(SPREADSHEET_ID, "Support!A2:I")
            rows = data.get("values", []) if data else []
            for idx, r in enumerate(rows, start=2):
                try:
                    status = r[5].strip().lower() if len(r) > 5 and r[5] else ""
                    telegram_id = int(r[1]) if len(r) > 1 and str(r[1]).isdigit() else None
                    response_text = r[7] if len(r) > 7 else ""
                    responded_at = r[8] if len(r) > 8 else ""
                    if response_text and status != "responded":
                        if telegram_id:
                            await bot.send_message(telegram_id, f"📬 پاسخ پشتیبانی:\n\n{response_text}")
                        update_range = f"Support!F{idx}:I{idx}"
                        await sheets_values_update(SPREADSHEET_ID, update_range, [["responded", "", response_text, datetime.utcnow().isoformat()]])
                        logger.info("Sent support response for row %d", idx)
                except Exception as e:
                    logger.exception("Error processing support row %d: %s", idx, e)
        except Exception as e:
            logger.exception("poll_support_responses top-level error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

# New: poll pending purchases and notify admin once per row (dedup)
async def poll_pending_notify_admin():
    global pending_notified_rows
    while True:
        try:
            if ADMIN_TELEGRAM_ID:
                data = await sheets_values_get(SPREADSHEET_ID, "Purchases!A2:K")
                rows = data.get("values", []) if data else []
                current_pending = set()
                for idx, r in enumerate(rows, start=2):
                    status = r[6].strip().lower() if len(r) > 6 and r[6] else ""
                    if status in ("pending", "awaiting", "payment"):
                        current_pending.add(idx)
                        if idx not in pending_notified_rows:
                            # send summary for this pending row
                            telegram_id = r[0] if len(r) > 0 else ""
                            full_name = r[1] if len(r) > 1 else ""
                            product = r[3] if len(r) > 3 else ""
                            txn = r[5] if len(r) > 5 else ""
                            msg = f"🔔 Pending Purchase (row {idx})\nUser: {full_name} ({telegram_id})\nProduct: {product}\nTransaction info: {txn}\nUse /confirm {idx} to approve."
                            try:
                                await bot.send_message(ADMIN_TELEGRAM_ID, msg)
                                pending_notified_rows.add(idx)
                            except Exception:
                                logger.exception("Failed to notify admin about pending row %d", idx)
                # remove rows no longer pending from notified set
                pending_notified_rows = {i for i in pending_notified_rows if i in current_pending}
        except Exception as e:
            logger.exception("poll_pending_notify_admin error: %s", e)
        await asyncio.sleep(60)

# -------------------------
# User flow state
# -------------------------
user_flow_state: Dict[int, Dict[str, Any]] = {}
def get_state(user_id: int) -> Dict[str, Any]:
    return user_flow_state.setdefault(user_id, {"stage": None})

def build_main_keyboard() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("تست کانال معمولی", "خرید کانال معمولی")
    kb.row("خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم")
    return kb

# -------------------------
# Handlers
# -------------------------
@dp.message_handler(commands=["start"])
async def on_start(message: types.Message):
    uid = message.from_user.id
    await ensure_sheets_and_headers()
    u = await find_user_row(uid)
    if u:
        get_state(uid)["stage"] = "main"
        await message.answer("👋 خوش آمدید مجدد! من شما را شناختم. از منوی زیر انتخاب کنید:", reply_markup=build_main_keyboard())
        return
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
    get_state(uid)["stage"] = "awaiting_email"
    await message.answer("👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") == "awaiting_email" and m.text and "@" in m.text and "." in m.text)
async def receive_email(message: types.Message):
    uid = message.from_user.id
    email = message.text.strip()
    u = await find_user_row(uid)
    if u:
        row = u["row"]
        full_name = message.from_user.full_name or (row[1] if len(row)>1 else "")
        registered_at = row[3] if len(row)>3 and row[3] else datetime.utcnow().isoformat()
        referrer = row[4] if len(row)>4 else ""
        referral_code = row[5] if len(row)>5 else ""
        notes = row[6] if len(row)>6 else ""
        new_row = [uid, full_name, email, registered_at, referrer, referral_code, notes]
        await update_user_row(u["row_index"], new_row)
    else:
        await sheets_values_append(SPREADSHEET_ID, "Users!A:G", [uid, message.from_user.full_name, email, datetime.utcnow().isoformat(), "", "", "registered"])
    get_state(uid)["stage"] = "awaiting_referral"
    await message.answer("✅ ایمیل ثبت شد.\nاگر کد معرف دارید آن را وارد کنید؛ در غیر این صورت 'ندارم' را بنویسید.")

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") == "awaiting_referral" and m.text)
async def receive_referral(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()
    ref_provided = "" if text.lower() in ("ندارم","ندار","no","none","skip") else text
    u = await find_user_row(uid)
    if u:
        row = u["row"]
        full_name = row[1] if len(row)>1 else message.from_user.full_name
        email = row[2] if len(row)>2 else ""
        registered_at = row[3] if len(row)>3 else datetime.utcnow().isoformat()
        referral_code = row[5] if len(row)>5 else ""
        notes = row[6] if len(row)>6 else ""
        new_row = [uid, full_name, email, registered_at, ref_provided, referral_code, notes]
        await update_user_row(u["row_index"], new_row)
    else:
        await sheets_values_append(SPREADSHEET_ID, "Users!A:G", [uid, message.from_user.full_name, "", datetime.utcnow().isoformat(), ref_provided, "", "registered"])
    get_state(uid)["stage"] = "main"
    await message.answer("✅ ثبت شد. اکنون می‌توانید از منو استفاده کنید:", reply_markup=build_main_keyboard())

@dp.message_handler(lambda m: m.text == "تست کانال معمولی")
async def handle_test_channel(message: types.Message):
    uid = message.from_user.id
    if not CHANNEL_TEST:
        await message.answer("کانال تست تنظیم نشده است. با مدیر تماس بگیرید.")
        return
    link = await create_invite_link(CHANNEL_TEST, expire_seconds=60*60, member_limit=1)
    if not link:
        await message.answer("خطا در ایجاد لینک دعوت؛ لطفاً بعداً تلاش کنید.")
        return
    await message.answer("✅ لینک تست ساخته شد — لطفاً با کلیک روی لینک وارد کانال تست شوید. شما ۱۰ دقیقه آنجا خواهید بود.")
    await message.answer(link)
    async def wait_for_join_and_schedule():
        joined = False
        join_time = None
        monitor_seconds = 20 * 60
        poll_interval = 8
        end_time = time.time() + monitor_seconds
        while time.time() < end_time:
            if await is_member_of(CHANNEL_TEST, uid):
                joined = True
                join_time = datetime.utcnow()
                break
            await asyncio.sleep(poll_interval)
        if joined and join_time:
            expires = join_time + timedelta(minutes=10)
            try:
                await sheets_values_append(SPREADSHEET_ID, "Purchases!A:K", [uid, message.from_user.full_name, "", "trial", "0", "trial", "activated", join_time.isoformat(), join_time.isoformat(), expires.isoformat(), ""])
            except Exception:
                logger.exception("Failed to append trial purchase")
            await schedule_removal(uid, CHANNEL_TEST, expires)
            try:
                await bot.send_message(uid, "🎉 شما به کانال تست اضافه شدید. مدت تست: ۱۰ دقیقه. پس از پایان حذف خواهید شد.")
            except Exception:
                pass
        else:
            try:
                await bot.send_message(uid, "⚠️ شما وارد کانال تست نشدید. لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            except Exception:
                pass
    asyncio.create_task(wait_for_join_and_schedule())

@dp.message_handler(lambda m: m.text == "خرید کانال معمولی")
async def handle_buy_normal(message: types.Message):
    uid = message.from_user.id
    get_state(uid)["stage"] = "awaiting_payment_normal"
    await message.answer("💳 مبلغ مربوط به کانال معمولی را واریز کنید به:\n`6037-9917-1234-5678`\nسپس اطلاعات تراکنش را ارسال کنید.")

@dp.message_handler(lambda m: m.text == "خرید کانال ویژه")
async def handle_buy_premium(message: types.Message):
    uid = message.from_user.id
    get_state(uid)["stage"] = "awaiting_payment_premium"
    await message.answer("💳 مبلغ مربوط به کانال ویژه را واریز کنید به:\n`6037-9917-1234-5678`\nسپس اطلاعات تراکنش را ارسال کنید.")

@dp.message_handler(lambda m: get_state(m.from_user.id).get("stage") in ("awaiting_payment_normal","awaiting_payment_premium") and m.text)
async def receive_payment_info(message: types.Message):
    uid = message.from_user.id
    st = get_state(uid)
    stage = st.get("stage")
    product = "normal" if stage == "awaiting_payment_normal" else "premium"
    txn_info = message.text.strip()
    await sheets_values_append(SPREADSHEET_ID, "Purchases!A:K", [uid, message.from_user.full_name, st.get("email",""), product, "", txn_info, "pending", datetime.utcnow().isoformat(), "", "", ""])
    st["stage"] = "main"
    await message.answer("✅ اطلاعات تراکنش شما ثبت شد. پس از تأیید مدیریت اشتراک شما فعال خواهد شد.")

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
    await message.answer(f"✅ تیکت ثبت شد. شناسه: {ticket_id}\nپاسخ مدیریت در همین شیت درج و برای شما ارسال خواهد شد.")

@dp.message_handler(lambda m: m.text == "توضیحات پلتفرم")
async def platform_info(message: types.Message):
    PLATFORM_TEXT = ("📘 توضیحات پلتفرم:\n\n"
                     "این ربات برای مدیریت اشتراک کانال‌های آموزشی طراحی شده است.\n"
                     "ثبت ایمیل و عضویت در کانال‌ها الزامی است. خریدها توسط مدیریت در شیت تأیید می‌شوند.")
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
# Admin commands
# -------------------------
@dp.message_handler(commands=["confirm"])
async def admin_confirm(message: types.Message):
    if ADMIN_TELEGRAM_ID is None or message.from_user.id != ADMIN_TELEGRAM_ID:
        await message.answer("فقط ادمین مجاز است.")
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("فرمت: /confirm <row_number>")
        return
    try:
        row_num = int(parts[1])
        rng = f"Purchases!A{row_num}:K{row_num}"
        data = await sheets_values_get(SPREADSHEET_ID, rng)
        vals = data.get("values", []) if data else []
        if not vals:
            await message.answer("ردیف پیدا نشد.")
            return
        r = vals[0]
        telegram_id = int(r[0]) if len(r)>0 and str(r[0]).isdigit() else None
        product = r[3] if len(r)>3 else None
        if telegram_id and product:
            await sheets_values_update(SPREADSHEET_ID, f"Purchases!G{row_num}:G{row_num}", [["confirmed"]])
            await activate_subscription_for_user(telegram_id, product, months=6)
            activated_iso = datetime.utcnow().isoformat()
            expires_iso = (datetime.utcnow() + timedelta(days=30*6)).isoformat()
            await sheets_values_update(SPREADSHEET_ID, f"Purchases!I{row_num}:J{row_num}", [[activated_iso, expires_iso]])
            await message.answer(f"خرید ردیف {row_num} برای کاربر {telegram_id} فعال شد.")
            # remove from pending_notified_rows if present
            try:
                pending_notified_rows.discard(row_num)
            except Exception:
                pass
        else:
            await message.answer("اطلاعات کاربر یا محصول معتبر نیست.")
    except Exception as e:
        logger.exception("admin_confirm error: %s", e)
        await message.answer("خطا در پردازش درخواست.")

@dp.message_handler(commands=["list_pending"])
async def admin_list_pending(message: types.Message):
    if ADMIN_TELEGRAM_ID is None or message.from_user.id != ADMIN_TELEGRAM_ID:
        await message.answer("فقط ادمین مجاز است.")
        return
    try:
        data = await sheets_values_get(SPREADSHEET_ID, "Purchases!A2:K")
        rows = data.get("values", []) if data else []
        lines = []
        for idx, r in enumerate(rows, start=2):
            status = r[6].strip().lower() if len(r) > 6 and r[6] else ""
            if status in ("pending", "awaiting", "payment"):
                telegram_id = r[0] if len(r)>0 else ""
                full_name = r[1] if len(r)>1 else ""
                product = r[3] if len(r)>3 else ""
                txn = r[5] if len(r)>5 else ""
                lines.append(f"{idx}: {full_name} ({telegram_id}) - {product} - {txn}")
        if not lines:
            await message.answer("ردیف پندینگ وجود ندارد.")
        else:
            await message.answer("Pending purchases:\n" + "\n".join(lines))
    except Exception as e:
        logger.exception("list_pending error: %s", e)
        await message.answer("خطا در خواندن پندینگ‌ها.")

# -------------------------
# Health server & restore schedules
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

async def rebuild_schedules_from_subscriptions():
    try:
        data = await sheets_values_get(SPREADSHEET_ID, "Subscriptions!A2:E")
        rows = data.get("values", []) if data else []
        for r in rows:
            try:
                telegram_id = int(r[0]) if len(r)>0 and str(r[0]).isdigit() else None
                product = r[1] if len(r)>1 else None
                # expires_at might be missing or malformed; try to find a parseable ISO string in row
                expires_at = None
                # check common columns that may contain iso datetime
                candidates = []
                if len(r) > 3:
                    candidates.append(r[3])
                # also look through entire row for iso-like strings
                candidates.extend([item for item in r if isinstance(item, str)])
                parsed = None
                for cand in candidates:
                    if not cand or not isinstance(cand, str):
                        continue
                    try:
                        parsed_dt = datetime.fromisoformat(cand)
                        parsed = parsed_dt
                        break
                    except Exception:
                        continue
                if not parsed:
                    # nothing to schedule for this row
                    continue
                expires_dt = parsed
                if telegram_id and expires_dt > datetime.utcnow():
                    if product == "normal" and CHANNEL_NORMAL:
                        await schedule_removal(telegram_id, CHANNEL_NORMAL, expires_dt)
                    elif product == "premium":
                        if CHANNEL_NORMAL:
                            await schedule_removal(telegram_id, CHANNEL_NORMAL, expires_dt)
                        if CHANNEL_PREMIUM:
                            await schedule_removal(telegram_id, CHANNEL_PREMIUM, expires_dt)
            except Exception as e:
                logger.exception("rebuild row err: %s", e)
    except Exception as e:
        logger.exception("rebuild_schedules failed: %s", e)

async def on_startup(dp_object):
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted on startup.")
    except Exception as e:
        logger.exception("delete_webhook failed: %s", e)
    ok = await ensure_sheets_and_headers()
    if not ok:
        logger.warning("ensure_sheets_and_headers returned False.")
    try:
        asyncio.create_task(start_webserver())
        asyncio.create_task(poll_purchases_and_activate())
        asyncio.create_task(poll_support_responses())
        asyncio.create_task(rebuild_schedules_from_subscriptions())
        asyncio.create_task(poll_pending_notify_admin())
    except Exception as e:
        logger.exception("Failed to start background tasks: %s", e)

def run_polling_with_retries(skip_updates: bool = True, max_retries: int = 20):
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Starting polling (attempt %d)...", attempt)
            executor.start_polling(dp, skip_updates=skip_updates, on_startup=on_startup)
            break
        except TerminatedByOtherGetUpdates as e:
            wait = min(60, 5 * attempt)
            logger.warning("TerminatedByOtherGetUpdates: %s — sleeping %d", e, wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached. Exiting.")
                break
        except Exception as e:
            wait = min(60, 5 * attempt)
            logger.exception("Unhandled exception in polling: %s — sleeping %d", e, wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Max retries reached. Exiting.")
                break

if __name__ == "__main__":
    logger.info("=== BOT STARTING ===")
    print("=== BOT STARTING ===")
    REQUIRED_CHANNELS_LIST = parse_channel_list(REQUIRED_CHANNELS)
    run_polling_with_retries(skip_updates=True, max_retries=20)
