import json, os
creds = json.loads(os.environ["GOOGLE_CREDENTIALS"])

import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Load config
with open("config.json") as f:
    config = json.load(f)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(config["GOOGLE_SERVICE_ACCOUNT"], scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(config["SPREADSHEET_ID"]).sheet1

# Telegram setup
TOKEN = config["TELEGRAM_TOKEN"]
logging.basicConfig(level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    sheet.append_row([user.id, user.username, "Joined"])
    await update.message.reply_text("👋 خوش اومدی! حساب تو در سیستم ثبت شد.")

app = ApplicationBuilder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

if __name__ == "__main__":
    print("Bot is running...")
    app.run_polling()

