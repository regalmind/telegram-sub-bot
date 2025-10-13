# main.py (patched)
import logging
import os
import json
import base64
import asyncio
from aiogram import Bot, Dispatcher, executor, types
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot token (accept either BOT_TOKEN or TELEGRAM_TOKEN)
TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    logger.error("Missing bot token. Set BOT_TOKEN (or TELEGRAM_TOKEN) in environment variables.")
    raise SystemExit("Missing BOT token")

# Spreadsheet id
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
if not SPREADSHEET_ID:
    logger.error("Missing SPREADSHEET_ID environment variable.")
    raise SystemExit("Missing SPREADSHEET_ID")

# Load Google service account credentials (supports raw JSON, base64, or file)
def load_google_creds():
    creds_env = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_env:
        # try raw JSON
        try:
            return json.loads(creds_env)
        except json.JSONDecodeError:
            # try base64 decode
            try:
                decoded = base64.b64decode(creds_env).decode("utf-8")
                return json.loads(decoded)
            except Exception as e:
                logger.exception("GOOGLE_CREDENTIALS exists but cannot be parsed as JSON or base64 JSON: %s", e)
                raise

    # fallback: try file path from GOOGLE_SERVICE_ACCOUNT or default file
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "service-account.json")
    if os.path.exists(sa_path):
        with open(sa_path, "r", encoding="utf-8") as f:
            return json.load(f)

    logger.error("No valid Google credentials found. Set GOOGLE_CREDENTIALS (raw JSON or base64) or upload service-account.json and set GOOGLE_SERVICE_ACCOUNT.")
    raise SystemExit("Missing Google credentials")

creds_info = load_google_creds()
creds = service_account.Credentials.from_service_account_info(creds_info)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# synchronous append function (google client is blocking)
def _sync_append(values):
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Users!A:E",
        valueInputOption="USER_ENTERED",
        body={"values": [values]}
    ).execute()

async def add_to_sheet(values):
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, _sync_append, values)
    except Exception:
        logger.exception("Failed to append to Google Sheet.")

# Handlers (unchanged logic, small robustness guards)
@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["تست کانال معمولی", "خرید کانال معمولی"],
        ["خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم"]
    ]
    for row in buttons:
        keyboard.row(*[types.KeyboardButton(b) for b in row])

    await message.answer(
        "👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await message.answer("✉️ منتظر ایمیل شما هستم...")

@dp.message_handler(lambda msg: msg.text and "@" in msg.text and "." in msg.text)
async def get_email(message: types.Message):
    email = message.text.strip()
    await add_to_sheet([message.from_user.id, message.from_user.full_name, email, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "شروع ثبت"])
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["تست کانال معمولی", "خرید کانال معمولی"],
        ["خرید کان ویژه", "پشتیبانی", "توضیحات پلتفرم"]
    ]
    for row in buttons:
        keyboard.row(*[types.KeyboardButton(b) for b in row])
    await message.answer("✅ ایمیل ثبت شد! لطفاً از منوی زیر انتخاب کنید:", reply_markup=keyboard)

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

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
