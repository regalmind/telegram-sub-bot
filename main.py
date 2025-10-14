# main.py
# نسخهٔ کامل و مقاوم‌شده برای اجرا روی Render (و محلی)
# - خواندن GOOGLE_CREDENTIALS به صورت raw JSON یا base64 یا از فایل GOOGLE_SERVICE_ACCOUNT
# - استفاده از run_in_executor برای تماس blocking به Google Sheets
# - اضافه کردن یک health HTTP endpoint با aiohttp تا Render پورت را ببیند (در صورت وب سرویس)
# - پذیرش BOT_TOKEN یا TELEGRAM_TOKEN
# - لاگ‌گذاری کامل برای رفع خطاها

import os
import json
import base64
import binascii
import logging
import asyncio
from datetime import datetime

from aiogram import Bot, Dispatcher, executor, types
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiohttp import web

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------- Load environment variables ----------
# Accept either BOT_TOKEN or TELEGRAM_TOKEN
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

if not TOKEN:
    logger.error("Missing bot token. Set BOT_TOKEN or TELEGRAM_TOKEN in environment variables.")
    raise SystemExit("Missing BOT token")

if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID environment variable.")
    raise SystemExit("Missing SPREADSHEET_ID")

# ---------- Google credentials loader ----------
def load_google_creds():
    """
    Try to load Google service account credentials from:
    1) env GOOGLE_CREDENTIALS (raw JSON)
    2) env GOOGLE_CREDENTIALS (base64 encoded JSON)
    3) fallback: file path from GOOGLE_SERVICE_ACCOUNT (default: service-account.json)
    The function attempts safe parses and logs helpful messages.
    """
    creds_env = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_env:
        # 1) try raw JSON
        try:
            creds = json.loads(creds_env)
            logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (raw JSON).")
            return creds
        except json.JSONDecodeError as e_raw:
            logger.warning("GOOGLE_CREDENTIALS raw JSON parse failed: %s", e_raw)

        # 2) try to recover JSON substring if someone pasted extra text
        try:
            start = creds_env.find('{')
            end = creds_env.rfind('}')
            if start != -1 and end != -1 and end > start:
                candidate = creds_env[start:end+1]
                creds = json.loads(candidate)
                logger.info("Recovered JSON substring from GOOGLE_CREDENTIALS and parsed successfully.")
                return creds
        except Exception as e_sub:
            logger.warning("Failed to recover JSON substring from GOOGLE_CREDENTIALS: %s", e_sub)

        # 3) try base64 decode (validate=True to ensure valid base64)
        try:
            decoded = base64.b64decode(creds_env, validate=True)
            try:
                creds = json.loads(decoded.decode("utf-8"))
                logger.info("Loaded Google credentials from GOOGLE_CREDENTIALS (base64-decoded).")
                return creds
            except UnicodeDecodeError as e_ud:
                logger.warning("Base64 decoded but UTF-8 decode failed: %s", e_ud)
            except json.JSONDecodeError as e_b64json:
                logger.warning("Base64 decoded but JSON parse failed: %s", e_b64json)
        except (binascii.Error, ValueError) as e_b64:
            logger.warning("GOOGLE_CREDENTIALS is not valid base64: %s", e_b64)

    # 4) fallback: try to read a file whose path/name is in GOOGLE_SERVICE_ACCOUNT
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
    if os.path.exists(sa_path):
        try:
            with open(sa_path, "r", encoding="utf-8") as f:
                creds = json.load(f)
                logger.info("Loaded Google credentials from file: %s", sa_path)
                return creds
        except Exception as e_file:
            logger.exception("Failed to load/parse Google service account file '%s': %s", sa_path, e_file)

    logger.error("No valid Google credentials found. Set GOOGLE_CREDENTIALS (raw JSON or base64) or upload service-account.json and set GOOGLE_SERVICE_ACCOUNT.")
    raise SystemExit("Missing Google credentials")


# Load credentials (may raise SystemExit on failure)
creds_info = load_google_creds()

# Create credentials and Sheets service
try:
    creds = service_account.Credentials.from_service_account_info(creds_info)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    logger.info("Google Sheets service initialized.")
except Exception as e:
    logger.exception("Failed to initialize Google Sheets client: %s", e)
    raise

# ---------- Bot and dispatcher ----------
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# ---------- Synchronous append wrapped for executor ----------
def _sync_append(values):
    """
    Blocking call to Google Sheets API.
    """
    try:
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range="Users!A:E",
            valueInputOption="USER_ENTERED",
            body={"values": [values]}
        ).execute()
    except Exception:
        logger.exception("Exception during sheet append (blocking call).")
        raise

async def add_to_sheet(values):
    """
    Run blocking Google Sheets append in threadpool to avoid blocking the event loop.
    """
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _sync_append, values)
        logger.info("Appended row to sheet: %s", values)
    except Exception as e:
        logger.exception("Failed to append to Google Sheet: %s", e)

# ---------- Bot handlers ----------
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    # remove keyboard and ask for email
    await message.answer(
        "👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await message.answer("✉️ منتظر ایمیل شما هستم...")

@dp.message_handler(lambda msg: msg.text and "@" in msg.text and "." in msg.text)
async def get_email(message: types.Message):
    try:
        email = message.text.strip()
        await add_to_sheet([
            message.from_user.id,
            message.from_user.full_name,
            email,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "شروع ثبت"
        ])
        # reply with menu keyboard
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        buttons = [
            ["تست کانال معمولی", "خرید کانال معمولی"],
            ["خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم"]
        ]
        for row in buttons:
            keyboard.row(*[types.KeyboardButton(b) for b in row])
        await message.answer("✅ ایمیل ثبت شد! لطفاً از منوی زیر انتخاب کنید:", reply_markup=keyboard)
    except Exception:
        logger.exception("Error in get_email handler")
        await message.answer("متأسفم، در ثبت ایمیل مشکلی پیش آمد. لطفاً دوباره تلاش کنید.")

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

# ---------- Simple health webserver for Render (so Render sees an open port) ----------
async def start_webserver():
    async def handle(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", handle)

    # Render sets PORT env var for web services; fallback to 8000
    port = int(os.environ.get("PORT", "8000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health webserver started on port %s", port)
    # not blocking; just leaves the site running

# ---------- Entry point ----------
if __name__ == "__main__":
    # print small marker so logs are easy to find in Render
    print("=== BOT STARTING ===")

    loop = asyncio.get_event_loop()

    # Start webserver (so Render's port scan finds a listening port).
    # If you intentionally use Background Worker and don't want a webserver remove this call.
    try:
        loop.run_until_complete(start_webserver())
    except Exception:
        logger.exception("Failed to start health webserver; continuing without it.")

    # Start polling (this will block current thread and keep running)
    try:
        logger.info("Starting polling.")
        executor.start_polling(dp, skip_updates=True)
    except Exception:
        logger.exception("Unhandled exception in executor.start_polling")
