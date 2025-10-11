import logging
import os
import json
from aiogram import Bot, Dispatcher, executor, types
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("BOT_TOKEN")

creds_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(creds_info)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

async def add_to_sheet(values):
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Users!A:E",
        valueInputOption="USER_ENTERED",
        body={"values": [values]}
    ).execute()

@dp.message_handler(commands=["start"])
async def start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["تست کانال معمولی", "خرید کانال معمولی"],
        ["خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم"]
    ]
    keyboard.add(*[types.KeyboardButton(b) for row in buttons for b in row])

    await message.answer(
        "👋 خوش آمدید!\nبرای ادامه لطفاً ایمیل خود را وارد کنید:",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await message.answer("✉️ منتظر ایمیل شما هستم...")

@dp.message_handler(lambda msg: "@" in msg.text and "." in msg.text)
async def get_email(message: types.Message):
    email = message.text.strip()
    await add_to_sheet([message.from_user.id, message.from_user.full_name, email, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "شروع ثبت"])
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = [
        ["تست کانال معمولی", "خرید کانال معمولی"],
        ["خرید کانال ویژه", "پشتیبانی", "توضیحات پلتفرم"]
    ]
    keyboard.add(*[types.KeyboardButton(b) for row in buttons for b in row])
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
