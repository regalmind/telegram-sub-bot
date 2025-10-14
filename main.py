# main.py
# کامل، مقاوم‌شده و آماده برای Render (یا اجرا محلی)
# قابلیت‌ها:
# - خواندن Google credentials از GOOGLE_CREDENTIALS (raw JSON یا base64) یا از فایل نام‌برده در GOOGLE_SERVICE_ACCOUNT
# - پشتیبانی از BOT_TOKEN یا TELEGRAM_TOKEN
# - استفاده از google-api-python-client برای نوشتن به Google Sheets (append)
# - اجرای append در threadpool (run_in_executor) برای جلوگیری از بلاک شدن ایونت لوپ
# - حذف وبهوک و drop_pending_updates قبل از شروع polling
# - راه‌اندازی health HTTP endpoint با aiohttp (اگر وب سرویس باشی Render پورت را تشخیص می‌دهد)
# - لاگ‌های کامل برای debug
# - تلاش محدود در append و محافظت در مقابل خطاها
# - در صورت رخداد conflict با getUpdates، لاگ و تلاش مجدد (با backoff کوتاه)
# - منو/کیبورد مشابه نسخهٔ اولیه

import os
import json
import base64
import binascii
import logging
import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils.exceptions import TerminatedByOtherGetUpdates
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiohttp import web

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s"
)
logger = logging.getLogger("telegram-sub-bot")

# -------------------------
# Environment variables (names used earlier)
# -------------------------
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or os.getenv("SPREADSHEET")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
PORT = int(os.environ.get("PORT", "8000"))

# Validate minimal envs
if not TOKEN:
    logger.error("Missing BOT_TOKEN or TELEGRAM_TOKEN environment variable.")
    raise SystemExit("Missing BOT_TOKEN / TELEGRAM_TOKEN")

if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID (or SHEET_ID) environment variable.")
    raise SystemExit("Missing SPREADSHEET_ID")

# -------------------------
# Helper: load google creds robustly
# -------------------------
def load_google_creds() -> Dict[str, Any]:
    """
    Try:
      1) GOOGLE_CREDENTIALS as raw JSON
      2) GOOGLE_CREDENTIALS as a base64-encoded JSON
      3) GOOGLE_SERVICE_ACCOUNT_FILE path to a JSON file (default service-account.json)
    Raises SystemExit if cannot parse.
    """
    if GOOGLE_CREDENTIALS_ENV:
        s = GOOGLE_CREDENTIALS_ENV.strip()
        # try raw JSON first
        try:
            data = json.loads(s)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return data
        except json.JSONDecodeError:
            logger.debug("GOOGLE_CREDENTIALS raw JSON parse failed, will attempt substring or base64.")

        # try to recover JSON substring (if pasted with extra content)
        try:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                candidate = s[start:end+1]
                data = json.loads(candidate)
                logger.info("Recovered and parsed JSON substring from GOOGLE_CREDENTIALS.")
                return data
        except Exception as e:
            logger.debug("Failed to recover JSON substring: %s", e)

        # try base64 decode
        try:
            decoded = base64.b64decode(s, validate=True)
            try:
                data = json.loads(decoded.decode("utf-8"))
                logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64).")
                return data
            except Exception as e:
                logger.warning("base64 decoded but JSON parse/utf-8 failed: %s", e)
        except (binascii.Error, ValueError) as e:
            logger.debug("GOOGLE_CREDENTIALS not valid base64: %s", e)

    # fallback: try file
    if os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        try:
            with open(GOOGLE_SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info("Loaded Google credentials from file: %s", GOOGLE_SERVICE_ACCOUNT_FILE)
                return data
        except Exception as e:
            logger.exception("Failed to load/parse GOOGLE_SERVICE_ACCOUNT file '%s': %s", GOOGLE_SERVICE_ACCOUNT_FILE, e)

    logger.error("No valid Google credentials found. Set GOOGLE_CREDENTIALS (raw or base64) or upload a service-account JSON file and set GOOGLE_SERVICE_ACCOUNT.")
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
except Exception:
    logger.exception("Failed to initialize Google Sheets client.")
    sheets = None

# -------------------------
# Bot and Dispatcher
# -------------------------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)


# -------------------------
# Blocking append wrapper
# -------------------------
def _blocking_append(spreadsheet_id: str, range_name: str, values: List[List[Any]]):
    """
    Blocking call to sheets API. Should be run in executor.
    """
    if sheets is None:
        raise RuntimeError("Sheets client not initialized")
    return sheets.values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()


async def append_to_sheet(spreadsheet_id: str, range_name: str, row: List[Any], retries: int = 2, delay: float = 1.0):
    """
    Run blocking append in executor with a couple retries.
    """
    loop = asyncio.get_running_loop()
    for attempt in range(1, retries + 2):
        try:
            await loop.run_in_executor(None, _blocking_append, spreadsheet_id, range_name, [row])
            logger.info("Appended row to sheet %s: %s", spreadsheet_id, row)
            return True
        except Exception as e:
            logger.exception("Attempt %d: Failed to append to sheet: %s", attempt, e)
            if attempt <= retries:
                await asyncio.sleep(delay * attempt)
            else:
                return False


# -------------------------
# Keyboard building helper
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


# -------------------------
# Handlers (as in original)
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:", reply_markup=types.ReplyKeyboardRemove())
    await message.answer("✉️ منتظر ایمیل شما هستم...")

@dp.message_handler(lambda msg: msg.text is not None and "@" in msg.text and "." in msg.text)
async def handle_email(message: types.Message):
    email = message.text.strip()
    row = [message.from_user.id, message.from_user.full_name, email, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "شروع ثبت"]
    ok = await append_to_sheet(SPREADSHEET_ID, "Users!A:E", row)
    if ok:
        kb = build_main_keyboard()
        await message.answer("✅ ایمیل ثبت شد! لطفاً از منوی زیر انتخاب کنید:", reply_markup=kb)
    else:
        await message.answer("❌ ثبت ایمیل با خطا مواجه شد. لطفاً بعداً تلاش کنید.")

@dp.message_handler(lambda msg: msg.text == "تست کانال معمولی")
async def test_channel(message: types.Message):
    await message.answer("⏳ در حال افزودن موقت شما به کانال تست...")
    await message.answer("✅ شما به مدت ۱۰ دقیقه در کانال تست عضو خواهید بود.")

@dp.message_handler(lambda msg: msg.text == "خرید کانال معمولی")
async def buy_normal(message: types.Message):
    await message.answer("💳 لطفاً مبلغ مربوط به اشتراک را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nپس از پرداخت، اطلاعات تراکنش را ارسال کنید.")

@dp.message_handler(lambda msg: msg.text == "خرید کانال ویژه")
async def buy_premium(message: types.Message):
    await message.answer("🌟 برای خرید اشتراک ویژه، لطفاً مبلغ را به شماره کارت زیر واریز کنید:\n\n`6037-9917-1234-5678`\n\nو اطلاعات تراکنش را ارسال نمایید.")

@dp.message_handler(lambda msg: msg.text == "پشتیبانی")
async def support(message: types.Message):
    await message.answer("🧰 لطفاً سوال یا مشکل خود را ارسال کنید تا بررسی شود.")

@dp.message_handler(lambda msg: msg.text == "توضیحات پلتفرم")
async def platform_info(message: types.Message):
    await message.answer("📘 توضیحات پلتفرم به‌زودی در این بخش قرار خواهد گرفت.")


# -------------------------
# Health webserver (aiohttp)
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


# -------------------------
# on_startup: remove webhook, start webserver
# -------------------------
async def on_startup(dp_obj):
    try:
        # remove any webhook to avoid conflicts
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("delete_webhook(drop_pending_updates=True) called successfully.")
    except Exception:
        logger.exception("Failed to delete webhook on startup (continuing).")

    # start health server in background (if you actually want worker without webserver remove this)
    try:
        asyncio.create_task(start_webserver())
    except Exception:
        logger.exception("Failed to start health webserver (continuing).")


# -------------------------
# Robust start: handle TerminatedByOtherGetUpdates by backoff+retry
# -------------------------
def run_polling_with_retries(skip_updates: bool = True, max_retries: int = 10):
    """
    This wrapper calls executor.start_polling inside a loop and if it exits with a
    TerminatedByOtherGetUpdates exception or other fatal ones, it will wait and retry.
    Note: if there truly is another active instance, retries won't fix it. This makes logs clearer.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info("Starting aiogram polling (attempt %d)...", attempt)
            executor.start_polling(dp, skip_updates=skip_updates, on_startup=on_startup)
            logger.info("executor.start_polling returned (process exiting normally).")
            break
        except TerminatedByOtherGetUpdates as e:
            logger.warning("TerminatedByOtherGetUpdates detected: %s", e)
            # give operator time to stop other instances; exponential backoff
            wait = min(60, 5 * attempt)
            logger.info("Sleeping %d seconds before retrying polling...", wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Reached max retries for TerminatedByOtherGetUpdates. Exiting.")
                break
        except Exception as e:
            logger.exception("Unhandled exception in start_polling: %s", e)
            wait = min(60, 5 * attempt)
            logger.info("Sleeping %d seconds before retrying polling...", wait)
            time.sleep(wait)
            if attempt >= max_retries:
                logger.error("Reached max retries for start_polling. Exiting.")
                break


# -------------------------
# Entry point
# -------------------------
if __name__ == "__main__":
    logger.info("=== BOT STARTING ===")
    # print marker for easier Render log searching
    print("=== BOT STARTING ===")

    # run polling (this will block; wrapper will retry on certain exceptions)
    run_polling_with_retries(skip_updates=True, max_retries=10)
