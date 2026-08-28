import os
import time
import asyncio
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    MessageHandler, 
    CommandHandler, 
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    ContextTypes, 
    filters
)
from pymongo import MongoClient

# Environment Credentials
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "") 

# Admin IDs Setup
ADMIN_IDS_RAW = os.getenv("ADMIN_ID", "0")
ADMIN_IDS = [int(aid.strip()) for aid in ADMIN_IDS_RAW.split(",") if aid.strip().isdigit()]

CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK", "") 
PRIVATE_STORE_ID = int(os.getenv("PRIVATE_STORE_ID", "0"))  

# MongoDB Setup
client = MongoClient(MONGO_URI)

# Primary Database Configuration
primary_db = client['bot_primary_db']
user_col = primary_db['users']
delete_col = primary_db['delete_queue'] 
history_col = primary_db['user_history']  
registry_col = primary_db['batch_registry']
config_col = primary_db['bot_config']

# Dynamic Multi Force Join Collections
fsub_col = primary_db['force_sub_channels']
join_req_col = primary_db['join_requests_data']

user_queues = {}
backup_queues = {}
cancel_status = {}
processing_tasks = {}

# --- Dynamic File Database Selector ---
def get_active_file_db():
    config = config_col.find_one({"_id": "file_db_config"})
    idx = config.get("index", 0) if config else 0
    
    db_name = f"bot_file_db_{idx}"
    current_db = client[db_name]
    
    try:
        stats_data = current_db.command("dbStats")
        storage_size_mb = stats_data.get("storageSize", 0) / (1024 * 1024)
        
        if storage_size_mb >= 450.0:
            idx += 1
            config_col.update_one(
                {"_id": "file_db_config"},
                {"$set": {"index": idx}},
                upsert=True
            )
            db_name = f"bot_file_db_{idx}"
            current_db = client[db_name]
            print(f"⚠️ Database full! Switched to new database: {db_name}")
    except Exception as e:
        print(f"File DB check error: {e}")
        
    return current_db, db_name

# --- File Size Formatter ---
def get_readable_size(size_in_bytes):
    if not size_in_bytes:
        return "Unknown Size"
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

# --- Dynamic Multi Force Sub Checker ---
async def get_fsub_buttons(context, user_id, start_param):
    channels = list(fsub_col.find())
    if not channels:
        return True, [] 

    unjoined_buttons = []
    has_unjoined = False

    for ch in channels:
        ch_id = ch["channel_id"]
        ch_link = ch["invite_link"]
        ch_title = ch.get("title", "Join Channel")

        is_member = False
        
        # Real-time Status Check
        try:
            member = await context.bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                is_member = True
            elif member.status in ['left', 'kicked']:
                # Agar user remove kar diya gaya hai to record saf karein
                join_req_col.delete_one({"user_id": user_id, "channel_id": ch_id})
                is_member = False
        except Exception as e:
            print(f"Chat Member Check Error ({ch_id}): {e}")

        if is_member:
            continue

        # Check Active Join Request
        has_requested = join_req_col.find_one({"user_id": user_id, "channel_id": ch_id})
        if has_requested:
            continue

        # Target Channel Unjoined
        has_unjoined = True
        unjoined_buttons.append([InlineKeyboardButton(f"📢 Request {ch_title}", url=ch_link)])

    if has_unjoined:
        # Dynamic Inline Callback for Try Again instead of URL
        unjoined_buttons.append([InlineKeyboardButton("🔄 Try Again", callback_data=f"try_again_{start_param}")])
        return False, unjoined_buttons
    else:
        return True, []

# --- Background Services ---
async def auto_delete_monitor(app):
    while True:
        try:
            current_time = time.time()
            all_pending = delete_col.find({"delete_at": {"$lte": current_time}})
            for task in all_pending:
                chat_id = task['chat_id']
                for msg_id in task['message_ids']:
                    try:
                        await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except Exception:
                        pass
                    await asyncio.sleep(0.1) 
                delete_col.delete_one({"_id": task['_id']})
        except Exception as e: 
            print(f"Auto-Delete Error: {e}")
        await asyncio.sleep(15)

async def run_post_init(application):
    asyncio.create_task(auto_delete_monitor(application))

# --- Send Files Logic ---
async def send_files_logic(update, context, batch_key):
    user = update.effective_user
    chat_id = update.effective_chat.id
    cancel_status[user.id] = False 
    
    reg_record = registry_col.find_one({"batch_key": batch_key})
    batch = None
    if reg_record:
        target_db_name = reg_record["db_name"]
        batch = client[target_db_name]['file_batches'].find_one({"batch_key": batch_key})
    else:
        batch = client['bot_database']['file_batches'].find_one({"batch_key": batch_key})
    
    if not batch:
        await context.bot.send_message(chat_id=chat_id, text="❌ Link invalid ya expire ho chuka hai.")
        return

    info_msg = await context.bot.send_message(
        chat_id=chat_id,
        text="⏳ Sending files...", 
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("• Cancel", callback_data="cancel_action")],
            [InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]
        ])
    )
    
    sent_message_ids = [info_msg.message_id]
    is_cancelled = False
    
    for file in batch["files"]:
        if cancel_status.get(user.id): 
            is_cancelled = True
            break 

        try:
            sent_msg = None
            file_bytes = file.get('file_size', 0)
            readable_size = get_readable_size(file_bytes)
            file_type = file.get('file_type')
            original_caption = file.get('caption', '')
            
            if file_type == 'video' and original_caption:
                custom_caption = f"{original_caption}\n\n👉 FILE SIZE :- {readable_size} 👑\n>> JOIN > @AllstoryFM2 🔥"
            else:
                custom_caption = f">> JOIN > @AllstoryFM2 🔥\n✅✨\n\n👉 FILE SIZE :- {readable_size} 👑\n🔥"

            if file_type == 'document': 
                sent_msg = await context.bot.send_document(chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file_type == 'video': 
                sent_msg = await context.bot.send_video(chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file_type == 'photo': 
                sent_msg = await context.bot.send_photo(chat_id, file['file_id'], protect_content=True, caption=custom_caption)
            elif file_type == 'audio': 
                sent_msg = await context.bot.send_audio(chat_id, file['file_id'], protect_content=True, caption=custom_caption)

            if sent_msg: 
                sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.5) 
        except Exception as e:
            print(f"File send error: {e}")
            break

    if len(sent_message_ids) > 0:
        try:
            delete_col.insert_one({"chat_id": chat_id, "message_ids": sent_message_ids, "delete_at": time.time() + 14400})
        except Exception:
            pass

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=info_msg.message_id)
    except Exception:
        pass

    alert_text = "🧹 **IMPORTANT NOTICE - Auto Deletion!** 🧹\n\nसभी फ़ाइलें **4 घंटे** में डिलीट हो जाएँगी! ⏳\n\nफ़ाइलों को अपने **Saved Messages** में फॉरवर्ड कर लें।
    """
    await context.bot.send_message(chat_id=chat_id, text=alert_text)

# --- Command & Callbacks Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    # User Logging
    user_col.update_one(
        {"_id": user.id},
        {"$set": {"username": user.username, "first_name": user.first_name, "last_seen": time.time()}},
        upsert=True
    )

    args = context.args
    start_param = args[0] if args else ""

    # Check Force Sub Status
    is_approved, buttons = await get_fsub_buttons(context, user.id, start_param)
    
    if not is_approved:
        text = "Access Restricted!\n\nFiles receive karne ke liye niche diye gaye channels ko join/request karein:"
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # If Force Sub Passed
    if start_param.startswith("batch_"):
        batch_key = start_param.replace("batch_", "")
        await send_files_logic(update, context, batch_key)
    else:
        await context.bot.send_message(chat_id=chat_id, text="Welcome! Send a valid link to get files.")

async def try_again_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    chat_id = query.message.chat_id
    
    # Callback param extract (e.g., try_again_batch_xyz)
    data = query.data
    start_param = data.replace("try_again_", "") if "try_again_" in data else ""

    is_approved, buttons = await get_fsub_buttons(context, user.id, start_param)
    
    if not is_approved:
        text = "Access Restricted!\n\nFiles receive karne ke liye niche diye gaye channels ko join/request karein:"
        try:
            await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            pass
        return

    # Verified successfully
    try:
        await query.delete_message()
    except Exception:
        pass

    if start_param.startswith("batch_"):
        batch_key = start_param.replace("batch_", "")
        await send_files_logic(update, context, batch_key)
    else:
        await context.bot.send_message(chat_id=chat_id, text="✅ Verification complete! Aap ab bot use kar sakte hain.")

# Event listener to capture Channel Join Requests
async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    if req:
        user_id = req.from_user.id
        channel_id = req.chat.id
        
        join_req_col.update_one(
            {"user_id": user_id, "channel_id": channel_id},
            {"$set": {"timestamp": time.time()}},
            upsert=True
        )

# Event listener to monitor when user leaves or gets kicked from channels
async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if result:
        user_id = result.from_user.id
        channel_id = result.chat.id
        new_status = result.new_chat_member.status

        if new_status in ['left', 'kicked']:
            # Channel se nikaale jaane par record hata do taaki phir se join karna pade
            join_req_col.delete_one({"user_id": user_id, "channel_id": channel_id})

async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    cancel_status[user_id] = True
    await query.edit_message_text("❌ Process Cancelled!")

# Bot Setup
def main():
    req = HTTPXRequest(connection_pool_size=8, read_timeout=20, write_timeout=20, connect_timeout=20)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(req).post_init(run_post_init).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(try_again_handler, pattern="^try_again"))
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_action$"))
    
    # Join Request & Member Update Listeners
    app.add_handler(ChatJoinRequestHandler(join_request_handler))
    app.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))

    print("Bot operational with Fixed Try Again, Dynamic Keyboard & Excluded Member Auto-check...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
    
