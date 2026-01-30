"""
Telegram Subscription Bot - Part 1/3
Configuration, Google Sheets, and Core Functions
"""

import os
import json
import time
import asyncio
import logging
import random
import string
import uuid
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, types, executor
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.utils.exceptions import (
    MessageToDeleteNotFound, MessageCantBeDeleted,
    MessageNotModified, CantParseEntities
)
from google.oauth2 import service_account
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
import base64

# ============================================
# LOGGING CONFIGURATION
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("TelegramBot")

# ============================================
# ENVIRONMENT VARIABLES
# ============================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_ENV = os.getenv("GOOGLE_CREDENTIALS")

REQUIRED_CHANNELS = os.getenv("REQUIRED_CHANNELS", "")
NORMAL_CHANNEL_ID = os.getenv("NORMAL_CHANNEL_ID")
PREMIUM_CHANNEL_ID = os.getenv("PREMIUM_CHANNEL_ID")
TEST_CHANNEL_ID = os.getenv("TEST_CHANNEL_ID")

NORMAL_PRICE = float(os.getenv("NORMAL_PRICE", "5"))
PREMIUM_PRICE = float(os.getenv("PREMIUM_PRICE", "20"))

TETHER_WALLET = os.getenv("TETHER_WALLET", "")
CARD_NUMBER = os.getenv("CARD_NUMBER", "")
CARD_HOLDER = os.getenv("CARD_HOLDER", "")

PORT = int(os.getenv("PORT", "8000"))
INSTANCE_MODE = os.getenv("INSTANCE_MODE", "polling").lower()

# Validation
if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN is missing!")
if not SPREADSHEET_ID:
    raise SystemExit("❌ SPREADSHEET_ID is missing!")

REQUIRED_CHANNELS_LIST = [c.strip() for c in REQUIRED_CHANNELS.split(",") if c.strip()]

# ============================================
# GOOGLE SHEETS INITIALIZATION
# ============================================
def load_google_credentials() -> Dict[str, Any]:
    """Load Google credentials from env or file"""
    if GOOGLE_CREDENTIALS_ENV:
        try:
            return json.loads(GOOGLE_CREDENTIALS_ENV)
        except:
            try:
                decoded = base64.b64decode(GOOGLE_CREDENTIALS_ENV)
                return json.loads(decoded.decode("utf-8"))
            except Exception as e:
                logger.error(f"Failed to parse GOOGLE_CREDENTIALS: {e}")
    
    if os.path.exists("service-account.json"):
        with open("service-account.json", "r", encoding="utf-8") as f:
            return json.load(f)
    
    raise SystemExit("❌ No Google credentials found!")

try:
    creds_info = load_google_credentials()
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)
    logger.info("✅ Google Sheets initialized")
except Exception as e:
    logger.exception(f"Failed to initialize Google Sheets: {e}")
    raise SystemExit("Failed to init Google Sheets")

# ============================================
# SHEET STRUCTURE DEFINITIONS
# ============================================
SHEET_DEFINITIONS = {
    "Users": [
        "telegram_id", "username", "full_name", "email", 
        "referral_code", "referred_by", "wallet_balance", 
        "status", "created_at", "last_seen"
    ],
    "Subscriptions": [
        "telegram_id", "username", "subscription_type", 
        "status", "activated_at", "expires_at", "payment_method"
    ],
    "Purchases": [
        "purchase_id", "telegram_id", "username", "product",
        "amount_usd", "amount_irr", "payment_method", 
        "transaction_id", "status", "created_at", 
        "approved_at", "approved_by", "notes"
    ],
    "Referrals": [
        "referrer_id", "referred_id", "level", 
        "commission_usd", "status", "purchase_id", 
        "created_at", "paid_at"
    ],
    "Withdrawals": [
        "withdrawal_id", "telegram_id", "amount_usd", 
        "method", "wallet_address", "card_number", 
        "status", "requested_at", "processed_at", 
        "processed_by", "notes"
    ],
    "Tickets": [
        "ticket_id", "telegram_id", "username", 
        "subject", "message", "status", 
        "created_at", "response", "responded_at"
    ],
    "Config": [
        "key", "value", "description"
    ]
}

# ============================================
# GOOGLE SHEETS HELPERS
# ============================================
_sheet_cache = {}
_last_open_time = 0

def open_spreadsheet():
    """Open spreadsheet with caching"""
    global _last_open_time
    current_time = time.time()
    
    if _sheet_cache.get("spreadsheet") and (current_time - _last_open_time) < 60:
        return _sheet_cache["spreadsheet"]
    
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        _sheet_cache["spreadsheet"] = sh
        _last_open_time = current_time
        return sh
    except Exception as e:
        logger.exception(f"Failed to open spreadsheet: {e}")
        raise

def get_worksheet(sheet_name: str):
    """Get or create worksheet with proper headers"""
    try:
        sh = open_spreadsheet()
        
        try:
            ws = sh.worksheet(sheet_name)
        except WorksheetNotFound:
            logger.info(f"Creating worksheet: {sheet_name}")
            ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="30")
        
        headers = SHEET_DEFINITIONS.get(sheet_name, [])
        if headers:
            try:
                existing = ws.row_values(1)
                if not existing or existing[0] != headers[0]:
                    ws.update("A1", [headers])
                    logger.info(f"✅ Headers set for {sheet_name}")
            except Exception as e:
                logger.error(f"Failed to set headers for {sheet_name}: {e}")
        
        return ws
    except Exception as e:
        logger.exception(f"Failed to get worksheet {sheet_name}: {e}")
        raise

def pad_row(row: List[Any], sheet_name: str) -> List[str]:
    """Pad row to match header length"""
    headers = SHEET_DEFINITIONS.get(sheet_name, [])
    padded = [str(x) if x is not None else "" for x in row]
    
    while len(padded) < len(headers):
        padded.append("")
    
    return padded[:len(headers)]

async def append_row(sheet_name: str, row: List[Any]) -> bool:
    """Append row to sheet"""
    try:
        ws = get_worksheet(sheet_name)
        padded = pad_row(row, sheet_name)
        ws.append_row(padded, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        logger.exception(f"Failed to append row to {sheet_name}: {e}")
        return False

async def get_all_rows(sheet_name: str) -> List[List[str]]:
    """Get all rows from sheet"""
    try:
        ws = get_worksheet(sheet_name)
        return ws.get_all_values()
    except Exception as e:
        logger.exception(f"Failed to get rows from {sheet_name}: {e}")
        return []

async def update_row(sheet_name: str, row_index: int, row: List[Any]) -> bool:
    """Update specific row"""
    try:
        ws = get_worksheet(sheet_name)
        padded = pad_row(row, sheet_name)
        headers = SHEET_DEFINITIONS.get(sheet_name, [])
        range_name = f"A{row_index}:{chr(65 + len(headers) - 1)}{row_index}"
        ws.update(range_name, [padded])
        return True
    except Exception as e:
        logger.exception(f"Failed to update row {row_index} in {sheet_name}: {e}")
        return False

async def find_user(telegram_id: int) -> Optional[Tuple[int, List[str]]]:
    """Find user row by telegram_id"""
    rows = await get_all_rows("Users")
    for idx, row in enumerate(rows[1:], start=2):
        if row and str(row[0]) == str(telegram_id):
            return idx, row
    return None

# ============================================
# BOT INITIALIZATION
# ============================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

user_states = {}
_last_bot_messages = {}

# ============================================
# UTILITY FUNCTIONS
# ============================================
def now_iso() -> str:
    """Get current time in ISO format"""
    return datetime.utcnow().replace(microsecond=0).isoformat()

def parse_iso(date_str: str) -> Optional[datetime]:
    """Parse ISO date string"""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except:
        return None

def generate_referral_code(length: int = 6) -> str:
    """Generate unique referral code"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def generate_purchase_id() -> str:
    """Generate unique purchase ID"""
    return f"PUR{int(time.time())}{random.randint(1000, 9999)}"

def generate_ticket_id() -> str:
    """Generate unique ticket ID"""
    return f"TKT{uuid.uuid4().hex[:8].upper()}"

def generate_withdrawal_id() -> str:
    """Generate unique withdrawal ID"""
    return f"WDR{int(time.time())}{random.randint(1000, 9999)}"

def is_valid_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    try:
        return str(user_id) == str(ADMIN_TELEGRAM_ID)
    except:
        return False

# ============================================
# NOBITEX API FOR IRR PRICE
# ============================================
async def get_usdt_price_irr() -> float:
    """Get USDT price in IRR from Nobitex"""
    try:
        async with ClientSession() as session:
            async with session.get("https://api.nobitex.ir/v2/orderbook/USDTIRT") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    asks = data.get("asks", [])
                    if asks and len(asks) > 0:
                        price = float(asks[0][0])
                        logger.info(f"💱 USDT: {price:,.0f} IRR")
                        return price
    except Exception as e:
        logger.exception(f"Failed to get USDT price: {e}")
    
    return 68000.0

# ============================================
# TELEGRAM HELPERS
# ============================================
async def safe_delete_message(chat_id: int, message_id: int):
    """Safely delete message"""
    try:
        await bot.delete_message(chat_id, message_id)
    except (MessageToDeleteNotFound, MessageCantBeDeleted):
        pass
    except Exception:
        pass

async def send_and_record(user_id: int, text: str, **kwargs):
    """Send message and record for later deletion"""
    try:
        prev_msg_id = _last_bot_messages.get(user_id)
        if prev_msg_id:
            await safe_delete_message(user_id, prev_msg_id)
        
        msg = await bot.send_message(user_id, text, **kwargs)
        _last_bot_messages[user_id] = msg.message_id
        return msg
    except Exception as e:
        logger.exception(f"Failed to send message to {user_id}: {e}")
        return None

async def is_member_of_channel(channel_id: str, user_id: int) -> bool:
    """Check if user is member of channel"""
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False

async def check_required_channels(user_id: int) -> Tuple[bool, List[str]]:
    """Check if user is member of all required channels"""
    if not REQUIRED_CHANNELS_LIST:
        return True, []
    
    missing = []
    for channel in REQUIRED_CHANNELS_LIST:
        if not await is_member_of_channel(channel, user_id):
            missing.append(channel)
    
    return len(missing) == 0, missing

async def create_invite_link(channel_id: str, expire_minutes: int = 60) -> Optional[str]:
    """Create temporary invite link"""
    try:
        expire_date = int((datetime.utcnow() + timedelta(minutes=expire_minutes)).timestamp())
        link = await bot.create_chat_invite_link(
            chat_id=channel_id,
            expire_date=expire_date,
            member_limit=1
        )
        return link.invite_link
    except Exception as e:
        logger.exception(f"Failed to create invite link: {e}")
        return None

async def remove_from_channel(channel_id: str, user_id: int) -> bool:
    """Remove user from channel"""
    try:
        await bot.ban_chat_member(chat_id=channel_id, user_id=user_id)
        await asyncio.sleep(0.5)
        await bot.unban_chat_member(chat_id=channel_id, user_id=user_id)
        logger.info(f"✅ Removed user {user_id} from {channel_id}")
        return True
    except Exception as e:
        logger.exception(f"Failed to remove: {e}")
        return False

# ============================================
# USER MANAGEMENT
# ============================================
async def create_or_update_user(user: types.User, email: str = None) -> Tuple[int, List[str]]:
    """Create or update user"""
    result = await find_user(user.id)
    
    if result:
        row_idx, row_data = result
        row_data[1] = user.username or ""
        row_data[2] = user.full_name or ""
        row_data[9] = now_iso()
        
        if email and not row_data[3]:
            row_data[3] = email
        
        await update_row("Users", row_idx, row_data)
        return row_idx, row_data
    else:
        new_row = [
            str(user.id),
            user.username or "",
            user.full_name or "",
            email or "",
            generate_referral_code(),
            "",
            "0",
            "active",
            now_iso(),
            now_iso()
        ]
        
        await append_row("Users", new_row)
        rows = await get_all_rows("Users")
        return len(rows), new_row

async def get_user_balance(telegram_id: int) -> float:
    """Get user wallet balance"""
    result = await find_user(telegram_id)
    if result:
        _, row = result
        try:
            return float(row[6]) if len(row) > 6 else 0.0
        except:
            return 0.0
    return 0.0

async def update_user_balance(telegram_id: int, amount: float, add: bool = True):
    """Update user wallet balance"""
    result = await find_user(telegram_id)
    if result:
        row_idx, row = result
        try:
            current = float(row[6]) if len(row) > 6 else 0.0
        except:
            current = 0.0
        
        if add:
            current += amount
        else:
            current -= amount
        
        row[6] = str(max(0, current))
        await update_row("Users", row_idx, row)

async def get_active_subscription(telegram_id: int) -> Optional[List[str]]:
    """Get user's active subscription"""
    rows = await get_all_rows("Subscriptions")
    now = datetime.utcnow()
    
    for row in rows[1:]:
        if row and str(row[0]) == str(telegram_id):
            status = row[3] if len(row) > 3 else ""
            expires_str = row[5] if len(row) > 5 else ""
            
            if status == "active":
                expires = parse_iso(expires_str)
                if expires and expires > now:
                    return row
    
    return None

# ============================================
# PART 1 COMPLETE - Continue to Part 2
# ============================================
"""
Telegram Subscription Bot - Part 2/3
Keyboards, Command Handlers, and Payment Processing

⚠️ این فایل ادامه بخش 1 است - در انتهای فایل main.py قرار دهید
"""

# ============================================
# KEYBOARDS
# ============================================
def main_menu_keyboard():
    """Main menu keyboard"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(
        KeyboardButton("🆓 تست کانال"),
        KeyboardButton("💎 خرید اشتراک")
    )
    kb.row(
        KeyboardButton("💰 کیف پول"),
        KeyboardButton("🎁 دعوت دوستان")
    )
    kb.row(
        KeyboardButton("💬 پشتیبانی"),
        KeyboardButton("📚 راهنما")
    )
    return kb

def subscription_keyboard():
    """Subscription purchase keyboard"""
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(
            f"⭐️ اشتراک معمولی - ${NORMAL_PRICE}",
            callback_data="buy_normal"
        ),
        InlineKeyboardButton(
            f"💎 اشتراک ویژه - ${PREMIUM_PRICE}",
            callback_data="buy_premium"
        ),
        InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_menu")
    )
    return kb

def payment_method_keyboard(product: str):
    """Payment method selection"""
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("💳 کارت بانکی", callback_data=f"pay_card_{product}"),
        InlineKeyboardButton("🪙 تتر USDT", callback_data=f"pay_usdt_{product}"),
        InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_buy")
    )
    return kb

def wallet_keyboard(balance: float):
    """Wallet keyboard"""
    kb = InlineKeyboardMarkup(row_width=1)
    if balance >= 10:
        kb.add(InlineKeyboardButton("💸 برداشت پورسانت", callback_data="withdraw"))
    kb.add(
        InlineKeyboardButton("📊 تاریخچه", callback_data="wallet_history"),
        InlineKeyboardButton("🔙 بازگشت", callback_data="back_to_menu")
    )
    return kb

def withdrawal_method_keyboard():
    """Withdrawal method selection"""
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("💳 کارت بانکی", callback_data="withdraw_card"),
        InlineKeyboardButton("🪙 تتر USDT", callback_data="withdraw_usdt"),
        InlineKeyboardButton("🔙 بازگشت", callback_data="wallet")
    )
    return kb

def channel_membership_keyboard(missing_channels: List[str]):
    """Keyboard for joining channels"""
    kb = InlineKeyboardMarkup(row_width=1)
    
    for channel in missing_channels:
        channel_name = channel.replace("@", "")
        kb.add(InlineKeyboardButton(
            f"📢 عضویت در {channel}",
            url=f"https://t.me/{channel_name}"
        ))
    
    kb.add(InlineKeyboardButton("✅ بررسی عضویت", callback_data="check_membership"))
    return kb

def admin_purchase_keyboard(purchase_id: str, user_id: int):
    """Admin keyboard for purchase approval"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ تایید", callback_data=f"approve_{purchase_id}_{user_id}"),
        InlineKeyboardButton("❌ رد", callback_data=f"reject_{purchase_id}_{user_id}")
    )
    return kb

def admin_withdrawal_keyboard(withdrawal_id: str, user_id: int):
    """Admin keyboard for withdrawal approval"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ پرداخت شد", callback_data=f"approve_wd_{withdrawal_id}_{user_id}"),
        InlineKeyboardButton("❌ رد", callback_data=f"reject_wd_{withdrawal_id}_{user_id}")
    )
    return kb

# ============================================
# REFERRAL SYSTEM
# ============================================
async def process_referral_commission(purchase_id: str, buyer_id: int, amount_usd: float):
    """Process referral commissions"""
    buyer_result = await find_user(buyer_id)
    if not buyer_result:
        return
    
    _, buyer_row = buyer_result
    referrer_id = buyer_row[5] if len(buyer_row) > 5 else ""
    
    if not referrer_id:
        return
    
    # Level 1: 8%
    level1_commission = amount_usd * 0.08
    await update_user_balance(int(referrer_id), level1_commission, add=True)
    
    await append_row("Referrals", [
        str(referrer_id),
        str(buyer_id),
        "1",
        str(level1_commission),
        "paid",
        purchase_id,
        now_iso(),
        now_iso()
    ])
    
    # Notify level 1
    try:
        await bot.send_message(
            int(referrer_id),
            f"🎉 <b>پورسانت جدید!</b>\n\n"
            f"💰 مبلغ: <b>${level1_commission:.2f}</b>\n"
            f"👤 از: <code>{buyer_id}</code>\n\n"
            f"💎 موجودی شما افزایش یافت!",
            parse_mode="HTML"
        )
    except:
        pass
    
    # Level 2: 12%
    referrer_result = await find_user(int(referrer_id))
    if referrer_result:
        _, referrer_row = referrer_result
        level2_referrer_id = referrer_row[5] if len(referrer_row) > 5 else ""
        
        if level2_referrer_id and level2_referrer_id != str(buyer_id):
            level2_commission = amount_usd * 0.12
            await update_user_balance(int(level2_referrer_id), level2_commission, add=True)
            
            await append_row("Referrals", [
                str(level2_referrer_id),
                str(buyer_id),
                "2",
                str(level2_commission),
                "paid",
                purchase_id,
                now_iso(),
                now_iso()
            ])
            
            try:
                await bot.send_message(
                    int(level2_referrer_id),
                    f"🎉 <b>پورسانت سطح 2!</b>\n\n"
                    f"💰 مبلغ: <b>${level2_commission:.2f}</b>\n"
                    f"👤 از: <code>{buyer_id}</code>\n\n"
                    f"💎 موجودی شما افزایش یافت!",
                    parse_mode="HTML"
                )
            except:
                pass

# ============================================
# SUBSCRIPTION MANAGEMENT
# ============================================
async def activate_subscription(telegram_id: int, username: str, product: str, payment_method: str):
    """Activate subscription"""
    now = now_iso()
    expires = datetime.utcnow() + timedelta(days=180)
    expires_iso = expires.replace(microsecond=0).isoformat()
    
    rows = await get_all_rows("Subscriptions")
    found = False
    
    for idx, row in enumerate(rows[1:], start=2):
        if row and str(row[0]) == str(telegram_id):
            row[1] = username
            row[2] = product
            row[3] = "active"
            row[4] = now
            row[5] = expires_iso
            row[6] = payment_method
            
            await update_row("Subscriptions", idx, row)
            found = True
            break
    
    if not found:
        await append_row("Subscriptions", [
            str(telegram_id),
            username,
            product,
            "active",
            now,
            expires_iso,
            payment_method
        ])
    
    result = await find_user(telegram_id)
    if result:
        row_idx, row = result
        row[7] = "active"
        await update_row("Users", row_idx, row)
    
    channels = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if product == "premium" else [NORMAL_CHANNEL_ID]
    
    for channel in channels:
        if channel:
            link = await create_invite_link(channel, expire_minutes=1440)
            if link:
                try:
                    await bot.send_message(
                        telegram_id,
                        f"🎊 <b>لینک عضویت کانال:</b>\n\n"
                        f"{link}\n\n"
                        f"⏰ این لینک ۲۴ ساعت معتبر است.",
                        parse_mode="HTML"
                    )
                except:
                    pass
    
    delay = (expires - datetime.utcnow()).total_seconds()
    asyncio.create_task(schedule_expiry(telegram_id, channels, delay))

async def schedule_expiry(telegram_id: int, channels: List[str], delay: float):
    """Schedule subscription expiry"""
    try:
        await asyncio.sleep(delay)
        
        for channel in channels:
            if channel:
                await remove_from_channel(channel, telegram_id)
        
        rows = await get_all_rows("Subscriptions")
        for idx, row in enumerate(rows[1:], start=2):
            if row and str(row[0]) == str(telegram_id):
                row[3] = "expired"
                await update_row("Subscriptions", idx, row)
                break
        
        try:
            await bot.send_message(
                telegram_id,
                "⏰ <b>اشتراک شما به پایان رسید!</b>\n\n"
                "برای تمدید از منوی خرید استفاده کنید.\n\n"
                "💡 با دعوت دوستان پورسانت کسب کنید!",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard()
            )
        except:
            pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"Error in expiry: {e}")

# ============================================
# COMMAND HANDLERS
# ============================================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """Start command"""
    user = message.from_user
    args = message.get_args()
    
    is_member, missing = await check_required_channels(user.id)
    
    if not is_member:
        kb = channel_membership_keyboard(missing)
        await send_and_record(
            user.id,
            "🔐 <b>برای استفاده از ربات ابتدا باید در کانال‌های زیر عضو شوید:</b>\n\n"
            "پس از عضویت روی <b>✅ بررسی عضویت</b> کلیک کنید.",
            parse_mode="HTML",
            reply_markup=kb
        )
        return
    
    result = await find_user(user.id)
    
    if result:
        row_idx, row = result
        email = row[3] if len(row) > 3 else ""
        
        if not email:
            user_states[user.id] = {"state": "awaiting_email", "attempt": 1}
            await send_and_record(
                user.id,
                "📧 <b>لطفاً ایمیل خود را وارد کنید:</b>\n\n"
                "مثال: <code>example@gmail.com</code>",
                parse_mode="HTML"
            )
            return
    else:
        referred_by = ""
        if args:
            rows = await get_all_rows("Users")
            for r in rows[1:]:
                if len(r) > 4 and r[4].upper() == args.upper():
                    referred_by = r[0]
                    break
        
        new_row = [
            str(user.id),
            user.username or "",
            user.full_name or "",
            "",
            generate_referral_code(),
            referred_by,
            "0",
            "active",
            now_iso(),
            now_iso()
        ]
        
        await append_row("Users", new_row)
        
        user_states[user.id] = {"state": "awaiting_email", "attempt": 1}
        await send_and_record(
            user.id,
            "👋 <b>خوش آمدید!</b>\n\n"
            "📧 لطفاً ایمیل خود را وارد کنید:\n\n"
            "مثال: <code>example@gmail.com</code>",
            parse_mode="HTML"
        )
        return
    
    subscription = await get_active_subscription(user.id)
    
    if subscription:
        expires = parse_iso(subscription[5])
        expires_str = expires.strftime("%Y/%m/%d") if expires else "نامشخص"
        sub_type = subscription[2] if len(subscription) > 2 else "unknown"
        sub_name = "ویژه 💎" if sub_type == "premium" else "معمولی ⭐️"
        
        await send_and_record(
            user.id,
            f"👋 <b>سلام {user.full_name}!</b>\n\n"
            f"✅ اشتراک: {sub_name}\n"
            f"📅 انقضا: <code>{expires_str}</code>\n\n"
            f"از منوی زیر استفاده کنید:",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
    else:
        await send_and_record(
            user.id,
            f"👋 <b>سلام {user.full_name}!</b>\n\n"
            f"شما اشتراک فعالی ندارید.\n\n"
            f"🆓 تست رایگان یا 💎 خرید اشتراک",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )

@dp.callback_query_handler(lambda c: c.data == "check_membership")
async def callback_check_membership(callback: types.CallbackQuery):
    """Check membership"""
    user = callback.from_user
    is_member, missing = await check_required_channels(user.id)
    
    if is_member:
        await callback.answer("✅ عضویت تایید شد!", show_alert=True)
        await create_or_update_user(user)
        
        await callback.message.edit_text(
            "✅ <b>عضویت شما تایید شد!</b>\n\n"
            "اکنون می‌توانید از ربات استفاده کنید.",
            parse_mode="HTML"
        )
        
        await bot.send_message(
            user.id,
            "از منوی زیر استفاده کنید:",
            reply_markup=main_menu_keyboard()
        )
    else:
        await callback.answer("❌ هنوز عضو نشده‌اید!", show_alert=True)
        kb = channel_membership_keyboard(missing)
        await callback.message.edit_reply_markup(reply_markup=kb)

# ============================================
# EMAIL HANDLERS
# ============================================
@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state") == "awaiting_email")
async def handle_email_input(message: types.Message):
    """Handle email input"""
    user = message.from_user
    email = message.text.strip().lower()
    state = user_states.get(user.id, {})
    attempt = state.get("attempt", 1)
    
    if not is_valid_email(email):
        await message.reply(
            "❌ ایمیل نامعتبر!\n\n"
            "مثال صحیح: <code>example@gmail.com</code>",
            parse_mode="HTML"
        )
        return
    
    if attempt == 1:
        user_states[user.id] = {
            "state": "awaiting_email_confirm",
            "email": email,
            "attempt": 2
        }
        
        await message.reply(
            f"📧 ایمیل: <code>{email}</code>\n\n"
            "⚠️ برای تایید دوباره وارد کنید:",
            parse_mode="HTML"
        )

@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state") == "awaiting_email_confirm")
async def handle_email_confirmation(message: types.Message):
    """Handle email confirmation"""
    user = message.from_user
    email_confirm = message.text.strip().lower()
    state = user_states.get(user.id, {})
    original_email = state.get("email", "")
    
    if email_confirm != original_email:
        user_states[user.id] = {"state": "awaiting_email", "attempt": 1}
        await message.reply(
            "❌ <b>ایمیل‌ها مطابقت ندارند!</b>\n\n"
            "دوباره وارد کنید:",
            parse_mode="HTML"
        )
        return
    
    result = await find_user(user.id)
    if result:
        row_idx, row = result
        row[3] = original_email
        await update_row("Users", row_idx, row)
    else:
        await create_or_update_user(user, email=original_email)
    
    user_states.pop(user.id, None)
    
    await message.reply("✅ <b>ایمیل ثبت شد!</b>", parse_mode="HTML")
    await send_and_record(user.id, "از منوی زیر استفاده کنید:", reply_markup=main_menu_keyboard())

# ============================================
# MENU HANDLERS
# ============================================
@dp.message_handler(lambda msg: msg.text == "🆓 تست کانال")
async def handle_test_channel(message: types.Message):
    """Test channel handler"""
    user = message.from_user
    
    if not TEST_CHANNEL_ID:
        await message.reply("❌ کانال تست در دسترس نیست.")
        return
    
    rows = await get_all_rows("Purchases")
    for row in rows[1:]:
        if row and str(row[1]) == str(user.id) and row[3] == "test":
            await message.reply("⚠️ شما قبلاً از تست استفاده کرده‌اید.")
            return
    
    link = await create_invite_link(TEST_CHANNEL_ID, expire_minutes=5)
    
    if not link:
        await message.reply("❌ خطا در ایجاد لینک.")
        return
    
    purchase_id = generate_purchase_id()
    await append_row("Purchases", [
        purchase_id, str(user.id), user.username or "",
        "test", "0", "0", "test", "test",
        "approved", now_iso(), now_iso(), "system", "5min test"
    ])
    
    await message.reply(
        "🎉 <b>لینک تست (۵ دقیقه):</b>\n\n"
        f"{link}\n\n"
        "⏰ بعد از ۵ دقیقه حذف می‌شوید.",
        parse_mode="HTML"
    )
    
    asyncio.create_task(schedule_test_removal(user.id, TEST_CHANNEL_ID))

async def schedule_test_removal(user_id: int, channel_id: str):
    """Schedule test removal"""
    try:
        await asyncio.sleep(300)
        await remove_from_channel(channel_id, user_id)
        try:
            await bot.send_message(
                user_id,
                "⏰ تست به پایان رسید.",
                reply_markup=main_menu_keyboard()
            )
        except:
            pass
    except Exception as e:
        logger.exception(f"Test removal error: {e}")

@dp.message_handler(lambda msg: msg.text == "💎 خرید اشتراک")
async def handle_buy_subscription(message: types.Message):
    """Buy subscription"""
    kb = subscription_keyboard()
    await send_and_record(
        message.from_user.id,
        "💎 <b>خرید اشتراک</b>\n\n"
        f"⭐️ معمولی: <b>${NORMAL_PRICE}</b>\n"
        f"   • کانال معمولی\n"
        f"   • ۶ ماه\n\n"
        f"💎 ویژه: <b>${PREMIUM_PRICE}</b>\n"
        f"   • هر دو کانال\n"
        f"   • ۶ ماه\n\n"
        f"یک گزینه انتخاب کنید:",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data in ["buy_normal", "buy_premium"])
async def callback_buy(callback: types.CallbackQuery):
    """Buy callback"""
    product = "normal" if callback.data == "buy_normal" else "premium"
    price = NORMAL_PRICE if product == "normal" else PREMIUM_PRICE
    
    kb = payment_method_keyboard(product)
    
    await callback.message.edit_text(
        f"💳 <b>پرداخت {'معمولی' if product == 'normal' else 'ویژه'}</b>\n\n"
        f"💰 مبلغ: <b>${price}</b>\n\n"
        f"روش پرداخت را انتخاب کنید:",
        parse_mode="HTML",
        reply_markup=kb
    )
    await callback.answer()

# ============================================
# PART 2 COMPLETE - Continue to Part 3
# ============================================
"""
Telegram Subscription Bot - Part 3A
Payment Processing & Wallet System
"""

# ============================================
# PAYMENT PROCESSING
# ============================================
@dp.callback_query_handler(lambda c: c.data.startswith("pay_"))
async def callback_payment_method(callback: types.CallbackQuery):
    """Payment method selection"""
    parts = callback.data.split("_")
    method = parts[1]
    product = parts[2]
    
    price_usd = NORMAL_PRICE if product == "normal" else PREMIUM_PRICE
    user = callback.from_user
    
    if method == "card":
        usdt_rate = await get_usdt_price_irr()
        price_irr = price_usd * usdt_rate
        purchase_id = generate_purchase_id()
        
        await append_row("Purchases", [
            purchase_id, str(user.id), user.username or "", product,
            str(price_usd), str(price_irr), "card", "", "pending",
            now_iso(), "", "", ""
        ])
        
        user_states[user.id] = {
            "state": "awaiting_card_receipt",
            "purchase_id": purchase_id,
            "product": product,
            "amount_usd": price_usd,
            "amount_irr": price_irr
        }
        
        support_username = os.getenv("SUPPORT_USERNAME", "@YourSupportAccount")
        
        await callback.message.edit_text(
            f"💳 <b>پرداخت با کارت بانکی</b>\n\n"
            f"📦 محصول: اشتراک {'معمولی' if product == 'normal' else 'ویژه'}\n"
            f"💵 مبلغ: <b>{price_irr:,.0f}</b> تومان\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>شماره کارت:</b>\n<code>{CARD_NUMBER}</code>\n\n"
            f"👤 <b>به نام:</b> {CARD_HOLDER}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ پس از واریز:\n"
            f"۱. عکس رسید را بگیرید\n"
            f"۲. به {support_username} ارسال کنید\n"
            f"۳. همراه عکس این شناسه را بفرستید:\n"
            f"<code>{purchase_id}</code>\n\n"
            f"⏰ پس از تایید، اشتراک فعال می‌شود.",
            parse_mode="HTML"
        )
    
    elif method == "usdt":
        purchase_id = generate_purchase_id()
        
        await append_row("Purchases", [
            purchase_id, str(user.id), user.username or "", product,
            str(price_usd), "0", "usdt", "", "pending",
            now_iso(), "", "", ""
        ])
        
        user_states[user.id] = {
            "state": "awaiting_usdt_txid",
            "purchase_id": purchase_id,
            "product": product,
            "amount_usd": price_usd
        }
        
        await callback.message.edit_text(
            f"🪙 <b>پرداخت با تتر (USDT)</b>\n\n"
            f"📦 محصول: اشتراک {'معمولی' if product == 'normal' else 'ویژه'}\n"
            f"💵 مبلغ: <b>${price_usd} USDT</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 <b>شبکه:</b> BEP20 (BSC)\n\n"
            f"📋 <b>آدرس:</b>\n<code>{TETHER_WALLET}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ پس از واریز، TXID را ارسال کنید.\n\n"
            f"🔢 شناسه: <code>{purchase_id}</code>",
            parse_mode="HTML"
        )
    
    await callback.answer()

@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state") == "awaiting_card_receipt",
                   content_types=types.ContentType.PHOTO)
async def handle_card_receipt(message: types.Message):
    """Handle card receipt"""
    user = message.from_user
    state = user_states.get(user.id, {})
    purchase_id = state.get("purchase_id")
    product = state.get("product")
    amount_usd = state.get("amount_usd")
    amount_irr = state.get("amount_irr")
    
    if not purchase_id:
        await message.reply("❌ خطا: سفارش یافت نشد.")
        return
    
    rows = await get_all_rows("Purchases")
    for idx, row in enumerate(rows[1:], start=2):
        if row and row[0] == purchase_id:
            row[7] = f"photo:{message.photo[-1].file_id}"
            row[8] = "pending"
            await update_row("Purchases", idx, row)
            break
    
    user_states.pop(user.id, None)
    
    await message.reply(
        f"✅ <b>رسید دریافت شد!</b>\n\n"
        f"🔢 شناسه: <code>{purchase_id}</code>\n\n"
        f"⏳ در حال بررسی...",
        parse_mode="HTML"
    )
    
    if ADMIN_TELEGRAM_ID:
        try:
            kb = admin_purchase_keyboard(purchase_id, user.id)
            await bot.send_photo(
                int(ADMIN_TELEGRAM_ID),
                message.photo[-1].file_id,
                caption=f"🔔 <b>سفارش جدید</b>\n\n"
                        f"👤 {user.full_name}\n"
                        f"🆔 <code>{user.id}</code>\n"
                        f"📦 {product}\n"
                        f"💰 ${amount_usd} (≈ {amount_irr:,.0f} تومان)\n"
                        f"💳 کارت بانکی\n"
                        f"🔢 <code>{purchase_id}</code>",
                parse_mode="HTML",
                reply_markup=kb
            )
        except Exception as e:
            logger.exception(f"Admin notify failed: {e}")

@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state") == "awaiting_usdt_txid")
async def handle_usdt_txid(message: types.Message):
    """Handle USDT TXID"""
    user = message.from_user
    state = user_states.get(user.id, {})
    purchase_id = state.get("purchase_id")
    product = state.get("product")
    amount_usd = state.get("amount_usd")
    txid = message.text.strip()
    
    if not purchase_id:
        await message.reply("❌ سفارش یافت نشد.")
        return
    
    if len(txid) < 20:
        await message.reply("❌ TXID نامعتبر!")
        return
    
    rows = await get_all_rows("Purchases")
    for idx, row in enumerate(rows[1:], start=2):
        if row and row[0] == purchase_id:
            row[7] = txid
            row[8] = "pending"
            await update_row("Purchases", idx, row)
            break
    
    user_states.pop(user.id, None)
    
    await message.reply(
        f"✅ <b>TXID دریافت شد!</b>\n\n"
        f"🔢 <code>{purchase_id}</code>\n\n"
        f"⏳ در حال بررسی...",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


    if ADMIN_TELEGRAM_ID:
        try:
            kb = admin_purchase_keyboard(purchase_id, user.id)
            await bot.send_message(
                int(ADMIN_TELEGRAM_ID),
                f"🔔 <b>سفارش جدید</b>\n\n"
                f"👤 {user.full_name}\n"
                f"🆔 <code>{user.id}</code>\n"
                f"📦 {product}\n"
                f"💰 ${amount_usd} USDT\n"
                f"🪙 تتر BEP20\n"
                f"🔗 <code>{txid}</code>\n"
                f"🔢 <code>{purchase_id}</code>",
                parse_mode="HTML",
                reply_markup=kb
            )
        except Exception as e:
            logger.exception(f"Admin notify failed: {e}")

# ============================================
# ADMIN APPROVAL
# ============================================
@dp.callback_query_handler(lambda c: c.data.startswith("approve_") or c.data.startswith("reject_"))
async def callback_admin_purchase(callback: types.CallbackQuery):
    """Admin purchase approval"""
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔️ شما ادمین نیستید!", show_alert=True)
        return
    
    parts = callback.data.split("_")
    action = parts[0]
    purchase_id = parts[1]
    user_id = int(parts[2])
    
    rows = await get_all_rows("Purchases")
    purchase_row = None
    purchase_idx = None
    
    for idx, row in enumerate(rows[1:], start=2):
        if row and row[0] == purchase_id:
            purchase_row = row
            purchase_idx = idx
            break
    
    if not purchase_row:
        await callback.answer("❌ سفارش یافت نشد!", show_alert=True)
        return
    
    product = purchase_row[3]
    amount_usd = float(purchase_row[4])
    payment_method = purchase_row[6]
    
    if action == "approve":
        purchase_row[8] = "approved"
        purchase_row[10] = now_iso()
        purchase_row[11] = str(callback.from_user.id)
        await update_row("Purchases", purchase_idx, purchase_row)
        
        user_result = await find_user(user_id)
        username = user_result[1][1] if user_result else ""
        
        await activate_subscription(user_id, username, product, payment_method)
        await process_referral_commission(purchase_id, user_id, amount_usd)
        
        try:
            result = await find_user(user_id)
            if result:
                _, row = result
                referral_code = row[4] if len(row) > 4 else ""
                
                await bot.send_message(
                    user_id,
                    f"🎉 <b>پرداخت تایید شد!</b>\n\n"
                    f"✅ اشتراک فعال شد\n"
                    f"📅 مدت: ۶ ماه\n\n"
                    f"🎁 کد معرف:\n<code>{referral_code}</code>\n\n"
                    f"💡 با دعوت دوستان پورسانت کسب کنید!",
                    parse_mode="HTML",
                    reply_markup=main_menu_keyboard()
                )
        except:
            pass
        
        try:
            await callback.message.edit_caption(
                caption=callback.message.caption + "\n\n✅ <b>تایید شد</b>",
                parse_mode="HTML"
            )
        except:
            try:
                await callback.message.edit_text(
                    callback.message.text + "\n\n✅ <b>تایید شد</b>",
                    parse_mode="HTML"
                )
            except:
                pass
        
        await callback.answer("✅ تایید شد")
    
    else:
        purchase_row[8] = "rejected"
        purchase_row[10] = now_iso()
        purchase_row[11] = str(callback.from_user.id)
        await update_row("Purchases", purchase_idx, purchase_row)
        
        try:
            await bot.send_message(
                user_id,
                "❌ <b>سفارش رد شد</b>\n\n"
                "با پشتیبانی تماس بگیرید.",
                parse_mode="HTML"
            )
        except:
            pass
        
        try:
            await callback.message.edit_caption(
                caption=callback.message.caption + "\n\n❌ <b>رد شد</b>",
                parse_mode="HTML"
            )
        except:
            try:
                await callback.message.edit_text(
                    callback.message.text + "\n\n❌ <b>رد شد</b>",
                    parse_mode="HTML"
                )
            except:
                pass
        
        await callback.answer("❌ رد شد")

# ============================================
# WALLET SYSTEM
# ============================================
@dp.message_handler(lambda msg: msg.text == "💰 کیف پول")
async def handle_wallet(message: types.Message):
    """Wallet handler"""
    user = message.from_user
    balance = await get_user_balance(user.id)
    
    rows = await get_all_rows("Referrals")
    total_referrals = sum(1 for row in rows[1:] if row and str(row[0]) == str(user.id))
    
    kb = wallet_keyboard(balance)
    
    await send_and_record(
        user.id,
        f"💰 <b>کیف پول</b>\n\n"
        f"💵 موجودی: <b>${balance:.2f}</b>\n"
        f"👥 معرفی: <b>{total_referrals}</b>\n\n"
        f"{'💡 حداقل برداشت: $10' if balance < 10 else '✅ می‌توانید برداشت کنید'}",
        parse_mode="HTML",
        reply_markup=kb
    )

@dp.callback_query_handler(lambda c: c.data == "wallet")
async def callback_wallet(callback: types.CallbackQuery):
    """Wallet callback"""
    user = callback.from_user
    balance = await get_user_balance(user.id)
    rows = await get_all_rows("Referrals")
    total_referrals = sum(1 for row in rows[1:] if row and str(row[0]) == str(user.id))
    kb = wallet_keyboard(balance)
    
    await callback.message.edit_text(
        f"💰 <b>کیف پول</b>\n\n"
        f"💵 موجودی: <b>${balance:.2f}</b>\n"
        f"👥 معرفی: <b>{total_referrals}</b>\n\n"
        f"{'💡 حداقل: $10' if balance < 10 else '✅ برداشت کنید'}",
        parse_mode="HTML",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "withdraw")
async def callback_withdraw(callback: types.CallbackQuery):
    """Withdraw"""
    user = callback.from_user
    balance = await get_user_balance(user.id)
    
    if balance < 10:
        await callback.answer("❌ حداقل $10!", show_alert=True)
        return
    
    kb = withdrawal_method_keyboard()
    await callback.message.edit_text(
        f"💸 <b>برداشت</b>\n\n"
        f"💵 موجودی: <b>${balance:.2f}</b>\n"
        f"💡 حداقل: <b>$10</b>\n\n"
        f"روش را انتخاب کنید:",
        parse_mode="HTML",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "wallet_history")
async def callback_wallet_history(callback: types.CallbackQuery):
    """History"""
    user = callback.from_user
    rows = await get_all_rows("Referrals")
    user_referrals = [row for row in rows[1:] if row and str(row[0]) == str(user.id)]
    
    if not user_referrals:
        await callback.answer("هنوز پورسانتی ندارید.", show_alert=True)
        return
    
    history_text = "📊 <b>تاریخچه</b>\n\n"
    for row in user_referrals[-10:]:
        level = row[2] if len(row) > 2 else ""
        amount = row[3] if len(row) > 3 else "0"
        date = row[6] if len(row) > 6 else ""
        try:
            date_obj = parse_iso(date)
            date_str = date_obj.strftime("%Y/%m/%d") if date_obj else date
        except:
            date_str = date
        history_text += f"• ${amount} (سطح {level}) - {date_str}\n"
    
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔙 بازگشت", callback_data="wallet"))
    
    await callback.message.edit_text(history_text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("withdraw_"))
async def callback_withdraw_method(callback: types.CallbackQuery):
    """Withdraw method"""
    user = callback.from_user
    method = callback.data.split("_")[1]
    balance = await get_user_balance(user.id)
    
    if balance < 10:
        await callback.answer("❌ موجودی کم!", show_alert=True)
        return
    
    user_states[user.id] = {
        "state": f"awaiting_withdraw_{method}_info",
        "method": method,
        "balance": balance
    }
    
    if method == "card":
        await callback.message.edit_text(
            f"💳 <b>برداشت به کارت</b>\n\n"
            f"💵 موجودی: <b>${balance:.2f}</b>\n\n"
            f"فرمت:\n<code>مبلغ شماره_کارت</code>\n\n"
            f"مثال:\n<code>15 6037991234567890</code>",
            parse_mode="HTML"
        )
    else:
        await callback.message.edit_text(
            f"🪙 <b>برداشت به تتر</b>\n\n"
            f"💵 موجودی: <b>${balance:.2f}</b>\n\n"
            f"فرمت:\n<code>مبلغ آدرس_کیف_پول</code>\n\n"
            f"مثال:\n<code>20 0x1234...5678</code>",
            parse_mode="HTML"
        )
    
    await callback.answer()

@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state", "").startswith("awaiting_withdraw_"))
async def handle_withdrawal_request(message: types.Message):
    """Withdrawal request"""
    user = message.from_user
    state = user_states.get(user.id, {})
    method = state.get("method")
    balance = state.get("balance", 0)
    
    parts = message.text.strip().split(maxsplit=1)
    
    if len(parts) < 2:
        await message.reply("❌ فرمت نادرست!")
        return
    
    try:
        amount = float(parts[0])
    except:
        await message.reply("❌ مبلغ نامعتبر!")
        return
    
    if amount < 10:
        await message.reply("❌ حداقل $10!")
        return
    
    if amount > balance:
        await message.reply(f"❌ موجودی کم! موجودی: ${balance:.2f}")
        return
    
    destination = parts[1]
    withdrawal_id = generate_withdrawal_id()
    
    if method == "card":
        await append_row("Withdrawals", [
            withdrawal_id, str(user.id), str(amount), "card",
            "", destination, "pending", now_iso(), "", "", ""
        ])
    else:
        await append_row("Withdrawals", [
            withdrawal_id, str(user.id), str(amount), "usdt",
            destination, "", "pending", now_iso(), "", "", ""
        ])
    
    user_states.pop(user.id, None)
    
    await message.reply(
        f"✅ <b>درخواست ثبت شد!</b>\n\n"
        f"🔢 <code>{withdrawal_id}</code>\n"
        f"💰 ${amount}\n"
        f"🔄 {'کارت' if method == 'card' else 'تتر'}\n\n"
        f"⏳ پس از بررسی واریز می‌شود.",
        parse_mode="HTML"
    )
    
    if ADMIN_TELEGRAM_ID:
        try:
            kb = admin_withdrawal_keyboard(withdrawal_id, user.id)
            await bot.send_message(
                int(ADMIN_TELEGRAM_ID),
                f"💸 <b>درخواست برداشت</b>\n\n"
                f"👤 {user.full_name}\n"
                f"🆔 <code>{user.id}</code>\n"
                f"💰 ${amount}\n"
                f"🔄 {'کارت' if method == 'card' else 'تتر'}\n"
                f"📋 <code>{destination}</code>\n"
                f"🔢 <code>{withdrawal_id}</code>",
                parse_mode="HTML",
                reply_markup=kb
            )
        except:
            pass
"""
Telegram Subscription Bot - Part 3B (FINAL)
Admin Commands, Support, Referral & Startup
"""

# ============================================
# ADMIN WITHDRAWAL APPROVAL
# ============================================
@dp.callback_query_handler(lambda c: c.data.startswith("approve_wd_") or c.data.startswith("reject_wd_"))
async def callback_admin_withdrawal(callback: types.CallbackQuery):
    """Admin withdrawal approval"""
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔️ شما ادمین نیستید!", show_alert=True)
        return
    
    parts = callback.data.split("_")
    action = parts[0]
    withdrawal_id = parts[2]
    user_id = int(parts[3])
    
    rows = await get_all_rows("Withdrawals")
    withdrawal_row = None
    withdrawal_idx = None
    
    for idx, row in enumerate(rows[1:], start=2):
        if row and row[0] == withdrawal_id:
            withdrawal_row = row
            withdrawal_idx = idx
            break
    
    if not withdrawal_row:
        await callback.answer("❌ درخواست یافت نشد!", show_alert=True)
        return
    
    amount = float(withdrawal_row[2])
    
    if action == "approve":
        withdrawal_row[6] = "completed"
        withdrawal_row[8] = now_iso()
        withdrawal_row[9] = str(callback.from_user.id)
        await update_row("Withdrawals", withdrawal_idx, withdrawal_row)
        
        await update_user_balance(user_id, amount, add=False)
        
        try:
            await bot.send_message(
                user_id,
                f"✅ <b>برداشت انجام شد!</b>\n\n"
                f"💰 ${amount}\n"
                f"🔢 <code>{withdrawal_id}</code>\n\n"
                f"مبلغ واریز شد.",
                parse_mode="HTML"
            )
        except:
            pass
        
        await callback.message.edit_text(
            callback.message.text + "\n\n✅ <b>پرداخت شد</b>",
            parse_mode="HTML"
        )
        await callback.answer("✅ پرداخت شد")
    
    else:
        withdrawal_row[6] = "rejected"
        withdrawal_row[8] = now_iso()
        withdrawal_row[9] = str(callback.from_user.id)
        await update_row("Withdrawals", withdrawal_idx, withdrawal_row)
        
        try:
            await bot.send_message(
                user_id,
                f"❌ <b>درخواست رد شد</b>\n\n"
                f"🔢 <code>{withdrawal_id}</code>\n\n"
                f"با پشتیبانی تماس بگیرید.",
                parse_mode="HTML"
            )
        except:
            pass
        
        await callback.message.edit_text(
            callback.message.text + "\n\n❌ <b>رد شد</b>",
            parse_mode="HTML"
        )
        await callback.answer("❌ رد شد")

# ============================================
# REFERRAL SYSTEM
# ============================================
@dp.message_handler(lambda msg: msg.text == "🎁 دعوت دوستان")
async def handle_referral(message: types.Message):
    """Referral handler"""
    user = message.from_user
    
    # Check if user has active subscription
    subscription = await get_active_subscription(user.id)
    
    if not subscription:
        await message.reply(
            "⚠️ <b>برای استفاده از سیستم معرفی، ابتدا باید اشتراک خریداری کنید.</b>\n\n"
            "پس از خرید اشتراک، کد معرف شما فعال می‌شود.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        return
    
    result = await find_user(user.id)
    
    if not result:
        await message.reply("❌ خطا در بارگذاری اطلاعات.", reply_markup=main_menu_keyboard())
        return
    
    _, row = result
    referral_code = row[4] if len(row) > 4 else ""
    
    rows = await get_all_rows("Referrals")
    level1_count = sum(1 for r in rows[1:] if r and str(r[0]) == str(user.id) and r[2] == "1")
    level2_count = sum(1 for r in rows[1:] if r and str(r[0]) == str(user.id) and r[2] == "2")
    
    total_earned = 0
    for r in rows[1:]:
        if r and str(r[0]) == str(user.id) and r[4] == "paid":
            try:
                total_earned += float(r[3])
            except:
                pass
    
    bot_username = (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={referral_code}"
    
    await message.reply(
        f"🎁 <b>دعوت دوستان</b>\n\n"
        f"🔗 <b>لینک:</b>\n<code>{referral_link}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>آمار:</b>\n"
        f"👥 سطح 1: {level1_count} نفر (8%)\n"
        f"👥 سطح 2: {level2_count} نفر (12%)\n"
        f"💰 کل درآمد: <b>${total_earned:.2f}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <b>کسب درآمد:</b>\n"
        f"• از لینک بالا دعوت کنید\n"
        f"• هر خرید = پورسانت\n"
        f"• سطح 1: 8%\n"
        f"• سطح 2: 12%",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


# ============================================
# SUPPORT SYSTEM
# ============================================
@dp.message_handler(lambda msg: msg.text == "💬 پشتیبانی")
async def handle_support(message: types.Message):
    """Support handler"""
    user_states[message.from_user.id] = {"state": "awaiting_support_message"}
    
    await message.reply(
        "💬 <b>پشتیبانی</b>\n\n"
        "پیام خود را ارسال کنید.\n"
        "به زودی پاسخ داده می‌شود.",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


@dp.message_handler(lambda msg: user_states.get(msg.from_user.id, {}).get("state") == "awaiting_support_message")
async def handle_support_message(message: types.Message):
    """Handle support message"""
    user = message.from_user
    ticket_id = generate_ticket_id()
    
    await append_row("Tickets", [
        ticket_id, str(user.id), user.username or "",
        "پشتیبانی", message.text, "open",
        now_iso(), "", ""
    ])
    
    user_states.pop(user.id, None)
    
    await message.reply(
        f"✅ <b>تیکت ثبت شد!</b>\n\n"
        f"🔢 <code>{ticket_id}</code>\n\n"
        f"⏳ به زودی پاسخ می‌دهیم.",
        parse_mode="HTML"
    )
    
    if ADMIN_TELEGRAM_ID:
        try:
            await bot.send_message(
                int(ADMIN_TELEGRAM_ID),
                f"🎫 <b>تیکت جدید</b>\n\n"
                f"👤 {user.full_name} (@{user.username or 'ندارد'})\n"
                f"🆔 <code>{user.id}</code>\n"
                f"🔢 <code>{ticket_id}</code>\n\n"
                f"📝 {message.text}\n\n"
                f"پاسخ:\n<code>/reply {ticket_id} متن_پاسخ</code>",
                parse_mode="HTML"
            )
        except:
            pass

@dp.message_handler(lambda msg: msg.text == "📚 راهنما")
async def handle_help(message: types.Message):
    """Help handler"""
    await message.reply(
        "📚 <b>راهنما</b>\n\n"
        "🆓 <b>تست کانال:</b>\n"
        "• ۵ دقیقه رایگان\n"
        "• فقط یکبار\n\n"
        "💎 <b>خرید:</b>\n"
        "• معمولی: $5 (۶ ماه)\n"
        "• ویژه: $20 (۶ ماه)\n\n"
        "💰 <b>کیف پول:</b>\n"
        "• موجودی و برداشت\n"
        "• حداقل: $10\n\n"
        "🎁 <b>دعوت:</b>\n"
        "• سطح 1: 8%\n"
        "• سطح 2: 12%\n"
        "• نامحدود!\n\n"
        "💬 <b>پشتیبانی:</b>\n"
        "• ثبت تیکت\n"
        "• پاسخ سریع",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


# ============================================
# ADMIN COMMANDS
# ============================================
@dp.message_handler(commands=["reply"])
async def cmd_admin_reply(message: types.Message):
    """Admin reply to ticket"""
    if not is_admin(message.from_user.id):
        return
    
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply("استفاده: /reply TICKET_ID پاسخ")
        return
    
    ticket_id = parts[1]
    response = parts[2]
    
    rows = await get_all_rows("Tickets")
    for idx, row in enumerate(rows[1:], start=2):
        if row and row[0] == ticket_id:
            user_id = int(row[1])
            row[7] = response
            row[8] = now_iso()
            row[5] = "closed"
            await update_row("Tickets", idx, row)
            
            try:
                await bot.send_message(
                    user_id,
                    f"📬 <b>پاسخ پشتیبانی</b>\n\n"
                    f"🔢 <code>{ticket_id}</code>\n\n"
                    f"💬 {response}",
                    parse_mode="HTML"
                )
                await message.reply("✅ پاسخ ارسال شد.")
            except Exception as e:
                await message.reply(f"❌ خطا: {e}")
            return
    
    await message.reply("❌ تیکت یافت نشد.")

@dp.message_handler(commands=["stats"])
async def cmd_admin_stats(message: types.Message):
    """Admin statistics"""
    if not is_admin(message.from_user.id):
        return
    
    users = await get_all_rows("Users")
    subs = await get_all_rows("Subscriptions")
    purchases = await get_all_rows("Purchases")
    
    total_users = len(users) - 1
    active_subs = sum(1 for row in subs[1:] if row and len(row) > 3 and row[3] == "active")
    total_revenue = sum(float(row[4]) for row in purchases[1:] if row and len(row) > 8 and row[8] == "approved")
    
    await message.reply(
        f"📊 <b>آمار</b>\n\n"
        f"👥 کاربران: {total_users}\n"
        f"✅ اشتراک فعال: {active_subs}\n"
        f"💰 درآمد: ${total_revenue:.2f}\n"
        f"🛒 خرید: {len(purchases) - 1}",
        parse_mode="HTML"
    )

@dp.message_handler(commands=["broadcast"])
async def cmd_admin_broadcast(message: types.Message):
    """Admin broadcast"""
    if not is_admin(message.from_user.id):
        return
    
    text = message.text.replace("/broadcast", "").strip()
    if not text:
        await message.reply("استفاده: /broadcast پیام شما")
        return
    
    users = await get_all_rows("Users")
    success = 0
    failed = 0
    
    for row in users[1:]:
        if row:
            try:
                await bot.send_message(int(row[0]), text, parse_mode="HTML")
                success += 1
                await asyncio.sleep(0.05)
            except:
                failed += 1
    
    await message.reply(f"✅ ارسال شد: {success}\n❌ خطا: {failed}")

# ============================================
# CALLBACK HANDLERS
# ============================================
@dp.callback_query_handler(lambda c: c.data == "back_to_menu")
async def callback_back_to_menu(callback: types.CallbackQuery):
    """Back to menu"""
    await callback.message.delete()
    await bot.send_message(
        callback.from_user.id,
        "منوی اصلی:",
        reply_markup=main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_buy")
async def callback_back_to_buy(callback: types.CallbackQuery):
    """Back to buy"""
    kb = subscription_keyboard()
    await callback.message.edit_text(
        "💎 <b>خرید اشتراک</b>\n\n"
        f"⭐️ معمولی: <b>${NORMAL_PRICE}</b>\n"
        f"💎 ویژه: <b>${PREMIUM_PRICE}</b>\n\n"
        f"انتخاب کنید:",
        parse_mode="HTML",
        reply_markup=kb
    )
    await callback.answer()

# ============================================
# AUTO-PROCESS PURCHASES & TICKETS
# ============================================
async def poll_sheets_auto_process():
    """Check Purchases and Tickets sheets every 30 seconds"""
    await asyncio.sleep(10)
    
    while True:
        try:
            # Process Purchases
            rows = await get_all_rows("Purchases")
            
            for idx, row in enumerate(rows[1:], start=2):
                if not row or len(row) < 9:
                    continue
                
                purchase_id = row[0]
                telegram_id = int(row[1]) if row[1] else 0
                username = row[2] if len(row) > 2 else ""
                product = row[3] if len(row) > 3 else ""
                amount_usd = float(row[4]) if row[4] else 0
                payment_method = row[6] if len(row) > 6 else ""
                status = row[8] if len(row) > 8 else ""
                approved_at = row[10] if len(row) > 10 else ""
                notes = row[12] if len(row) > 12 else ""
                
                if status == "approved" and approved_at and "processed" not in notes:
                    logger.info(f"Auto-approving {purchase_id}")
                    
                    await activate_subscription(telegram_id, username, product, payment_method)
                    await process_referral_commission(purchase_id, telegram_id, amount_usd)
                    
                    try:
                        result = await find_user(telegram_id)
                        if result:
                            _, user_row = result
                            referral_code = user_row[4] if len(user_row) > 4 else ""
                            
                            await bot.send_message(
                                telegram_id,
                                f"🎉 <b>پرداخت تایید شد!</b>\n\n"
                                f"✅ اشتراک فعال شد\n"
                                f"📅 مدت: ۶ ماه\n\n"
                                f"🎁 کد معرف:\n<code>{referral_code}</code>\n\n"
                                f"💡 با دعوت دوستان پورسانت کسب کنید!",
                                parse_mode="HTML",
                                reply_markup=main_menu_keyboard()
                            )
                    except Exception as e:
                        logger.exception(f"Failed to notify: {e}")
                    
                    row[12] = "auto_processed"
                    await update_row("Purchases", idx, row)
                
                elif status == "rejected" and approved_at and "processed" not in notes:
                    logger.info(f"Auto-rejecting {purchase_id}")
                    
                    try:
                        await bot.send_message(
                            telegram_id,
                            "❌ <b>سفارش رد شد</b>\n\n"
                            "با پشتیبانی تماس بگیرید.",
                            parse_mode="HTML",
                            reply_markup=main_menu_keyboard()
                        )
                    except:
                        pass
                    
                    row[12] = "auto_processed"
                    await update_row("Purchases", idx, row)
            
            # Process Tickets
            ticket_rows = await get_all_rows("Tickets")
            
            for idx, row in enumerate(ticket_rows[1:], start=2):
                if not row or len(row) < 9:
                    continue
                
                ticket_id = row[0]
                telegram_id = int(row[1]) if row[1] else 0
                response = row[7] if len(row) > 7 else ""
                responded_at = row[8] if len(row) > 8 else ""
                status = row[5] if len(row) > 5 else ""
                
                if response and responded_at and status == "closed":
                    # Check if already sent
                    if "sent" not in response:
                        try:
                            await bot.send_message(
                                telegram_id,
                                f"📬 <b>پاسخ پشتیبانی</b>\n\n"
                                f"🔢 <code>{ticket_id}</code>\n\n"
                                f"💬 {response}",
                                parse_mode="HTML",
                                reply_markup=main_menu_keyboard()
                            )
                            
                            row[7] = response + " [sent]"
                            await update_row("Tickets", idx, row)
                        except Exception as e:
                            logger.exception(f"Failed to send ticket response: {e}")
            
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.exception(f"poll_sheets error: {e}")
            await asyncio.sleep(60)


# ============================================
# STARTUP & MAIN
# ============================================
async def on_startup(dp):
    """On startup"""
    logger.info("🚀 Bot starting...")
    
    for sheet_name in SHEET_DEFINITIONS.keys():
        try:
            get_worksheet(sheet_name)
            logger.info(f"✅ Sheet: {sheet_name}")
        except Exception as e:
            logger.error(f"❌ Sheet {sheet_name}: {e}")
    
    asyncio.create_task(rebuild_subscription_schedules())
    
    logger.info("✅ Bot started!")

async def rebuild_subscription_schedules():
    """Rebuild subscription schedules"""
    try:
        await asyncio.sleep(5)
        rows = await get_all_rows("Subscriptions")
        now = datetime.utcnow()
        
        for row in rows[1:]:
            if not row or len(row) < 6:
                continue
            
            telegram_id = int(row[0])
            product = row[2] if len(row) > 2 else ""
            status = row[3] if len(row) > 3 else ""
            expires_str = row[5] if len(row) > 5 else ""
            
            if status != "active":
                continue
            
            expires = parse_iso(expires_str)
            if not expires:
                continue
            
            if expires <= now:
                channels = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if product == "premium" else [NORMAL_CHANNEL_ID]
                for channel in channels:
                    if channel:
                        await remove_from_channel(channel, telegram_id)
                
                idx = rows.index(row) + 1
                row[3] = "expired"
                await update_row("Subscriptions", idx, row)
            else:
                delay = (expires - now).total_seconds()
                channels = [PREMIUM_CHANNEL_ID, NORMAL_CHANNEL_ID] if product == "premium" else [NORMAL_CHANNEL_ID]
                asyncio.create_task(schedule_expiry(telegram_id, channels, delay))
                logger.info(f"✅ Scheduled expiry for {telegram_id} in {delay/3600:.1f}h")
    except Exception as e:
        logger.exception(f"Rebuild schedules failed: {e}")

async def on_shutdown(dp):
    """On shutdown"""
    logger.info("🛑 Shutting down...")
    await bot.close()

async def start_health_server():
    """Start health check server"""
    app = web.Application()
    
    async def health(request):
        return web.Response(text="OK")
    
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"✅ Health server on port {PORT}")

# ============================================
# MAIN ENTRY POINT
# ============================================
if __name__ == "__main__":
    try:
        logger.info("=" * 50)
        logger.info("🤖 TELEGRAM SUBSCRIPTION BOT")
        logger.info("=" * 50)
        
        loop = asyncio.get_event_loop()
        loop.create_task(start_health_server())
        
        executor.start_polling(
            dp,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown
        )
    except KeyboardInterrupt:
        logger.info("⛔️ Stopped by user")
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}")









