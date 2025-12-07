import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, executor

# ===============================
# CONFIG / ENV
# ===============================
TOKEN = os.getenv("BOT_TOKEN")  # توکن ربات
ADMIN_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))

# Sheets setup placeholders (باید تابع های واقعی sheets/headers رو داشته باشی)
HEADERS = {
    "Users": ["id", "name", "purchase_status", "expires_at", "referral_code", "referred_by"],
    "Purchases": ["user_id", "plan", "transaction_info", "status", "request_at", "activated_at", "expires_at", "admin_note"],
    "Support": ["ticket_id", "user_id", "message", "status", "response", "created_at", "response_at"],
    "Subs": ["user_id", "plan", "activated_at", "expires_at", "active"]
}

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram-sub-bot")

# ===============================
# BOT SETUP
# ===============================
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# ===============================
# MOCK FUNCTIONS (برای تست)
# ===============================
def fix_sheet_header(sheet_name, force_clear=False):
    # شبیه‌سازی بررسی header
    logger.info("fix_sheet_header called for %s", sheet_name)
    return True

async def wipe_sheet(sheet_name):
    logger.info("Sheet %s wiped", sheet_name)

async def reset_sheet(sheet_name):
    logger.info("Sheet %s reset with header", sheet_name)

# ===============================
# ADMIN COMMANDS
# ===============================
@dp.message_handler(commands=["ensure_headers"])
async def ensure_headers_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("فقط ادمین مجاز است.")
        return
    results = []
    for s in HEADERS.keys():
        ok = fix_sheet_header(s, force_clear=False)
        results.append(f"{s}: {'OK' if ok else 'FAILED'}")
    await message.reply("نتیجه بررسی هدرها:\n" + "\n".join(results))

@dp.message_handler(commands=["reset_sheet"])
async def reset_sheet_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("فقط ادمین مجاز است.")
        return
    parts = message.text.split()
    if len(parts) < 3 or parts[2].lower() != "confirm":
        await message.reply("استفاده: /reset_sheet <SheetName> confirm")
        return
    sheet_name = parts[1]
    if sheet_name not in HEADERS:
        await message.reply("شیت نامعتبر.")
        return
    await reset_sheet(sheet_name)
    await message.reply(f"✅ شیت {sheet_name} ریست شد.")

@dp.message_handler(commands=["wipe_all_sheets"])
async def wipe_all_sheets_handler(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("فقط ادمین مجاز است.")
        return
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() != "confirm":
        await message.reply("استفاده: /wipe_all_sheets confirm")
        return
    for sheet_name in HEADERS.keys():
        await wipe_sheet(sheet_name)
    await message.reply("✅ همه شیت‌ها پاک شدند.")

# ===============================
# START POLLING
# ===============================
if __name__ == "__main__":
    logger.info("=== BOT STARTING (ADMIN ONLY) ===")
    executor.start_polling(dp, skip_updates=True)
