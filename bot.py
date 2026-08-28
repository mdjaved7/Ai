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
        
        # Real-time check: Check if user is currently in the channel
        try:
            member = await context.bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                is_member = True
            elif member.status in ['left', 'kicked']:
                # Agar user ko nikal diya gaya hai, to Join Request DB se hata do
                join_req_col.delete_one({"user_id": user_id, "channel_id": ch_id})
                is_member = False
        except Exception as e:
            print(f"Chat Member Check Error ({ch_id}): {e}")

        if is_member:
            continue

        # Agar user member nahi hai, check karo ki usne Request bhej rakhi hai ya nahi
        has_requested = join_req_col.find_one({"user_id": user_id, "channel_id": ch_id})
        if has_requested:
            continue

        # Agar na join hai aur na active request hai
        has_unjoined = True
        unjoined_buttons.append([InlineKeyboardButton(f"📢 Request {ch_title}", url=ch_link)])

    if has_unjoined:
        bot_info = await context.bot.get_me()
        try_again_link = f"https://t.me/{bot_info.username}?start={start_param}"
        unjoined_buttons.append([InlineKeyboardButton("🔄 Try Again", url=try_again_link)])
        return False, unjoined_buttons
    else:
        return True, []

# --- Background Services ---
async def database_storage_checker(app):
    while True:
        try:
            active_db, active_name = get_active_file_db()
            stats_data = active_db.command("dbStats")
            storage_size_mb = stats_data.get("storageSize", 0) / (1024 * 1024)
            
            if storage_size_mb >= 450.0:
                alert_text = (
                    f"⚠️ <b>MONGODB STORAGE WARNING!</b> ⚠️\n\n"
                    f"Active DB ({active_name}) full hone wala hai!\n"
                    f"<b>Current Usage:</b> {storage_size_mb:.2f} MB / 512 MB"
                )
                for adm in ADMIN_IDS:
                    try:
                        await app.bot.send_message(chat_id=adm, text=alert_text, parse_mode="HTML")
                    except Exception:
                        pass
        except Exception as e:
            print(f"Database check error: {e}")
        await asyncio.sleep(3600)

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
    asyncio.create_task(database_storage_checker(application))

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

    try:
        history_col.insert_one({
            "user_id": user.id, 
            "first_name": user.first_name, 
            "username": user.username, 
            "action": "requested_files", 
            "batch_key": batch_key, 
            "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception:
        pass
    
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
                custom_caption = (
                    f">> JOIN > @AllstoryFM2 🔥\n"
                    f"✅✨\n\n"
                    f"👉 FILE SIZE :- {readable_size} 👑\n"
                    f"🔥"
                )

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
            delete_col.insert_one({
                "chat_id": chat_id, 
                "message_ids": sent_message_ids, 
                "delete_at": time.time() + 14400 
            })
        except Exception:
            pass

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=info_msg.message_id)
    except Exception:
        pass

    alert_text = "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳 \n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 \n𝙰𝙵𝚃𝙴𝚁 [ 4 𝙷𝙾𝚄𝚁𝚂 ] 𝙿𝙻𝙴𝙰𝚂𝙴 \n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
    if is_cancelled:
        alert_text += "\n\n⚠️ *Process was cancelled by user.*"

    try:
        final_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=alert_text, 
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]])
        )
        delete_col.insert_one({
            "chat_id": chat_id, 
            "message_ids": [final_msg.message_id], 
            "delete_at": time.time() + 14400
        })
    except Exception:
        pass

# --- Command Handler: /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        if not user_col.find_one({"user_id": user.id}):
            user_col.insert_one({"user_id": user.id, "username": user.username, "first_name": user.first_name})
    except Exception:
        pass
        
    args = context.args
    if not args:
        await update.message.reply_text("🗄️ Your automation scripts are securely archived 🛡️, fully optimized ⚙️, and ready for instant deployment 🚀💻⚡.")
        return

    target_batch = args[0]

    # Force Sub Check (Join Request / Joining Verified)
    has_joined_all, fsub_buttons = await get_fsub_buttons(context, user.id, target_batch)
    if not has_joined_all:
        await update.message.reply_text(
            "⚠️ <b>Access Restricted!</b>\n\nFiles receive karne ke liye niche दिए गए channels ko join/request karein:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(fsub_buttons)
        )
        return

    # User Checked & Verified -> Deliver Files
    asyncio.create_task(send_files_logic(update, context, target_batch))

# --- Admin Commands ---
async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    if len(context.args) < 3:
        await update.message.reply_text("❌ <b>Format:</b> `/addchannel <Channel_ID> <Invite_Link> <Channel_Title>`", parse_mode="Markdown")
        return
    
    try:
        ch_id = int(context.args[0])
        ch_link = context.args[1]
        ch_title = " ".join(context.args[2:])
        
        fsub_col.update_one(
            {"channel_id": ch_id},
            {"$set": {"invite_link": ch_link, "title": ch_title}},
            upsert=True
        )
        await update.message.reply_text(f"✅ Channel Added Successfully!\n\n📌 <b>Title:</b> {ch_title}\n🆔 <b>ID:</b> <code>{ch_id}</code>\n🔗 <b>Link:</b> {ch_link}", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Error adding channel: {e}")

async def del_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    if not context.args:
        await update.message.reply_text("❌ <b>Format:</b> `/delchannel <Channel_ID>`", parse_mode="Markdown")
        return
    try:
        ch_id = int(context.args[0])
        res = fsub_col.delete_one({"channel_id": ch_id})
        if res.deleted_count > 0:
            await update.message.reply_text(f"✅ Channel <code>{ch_id}</code> removed from Force-Sub list.", parse_mode="HTML")
        else:
            await update.message.reply_text("❌ Channel ID not found.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error removing channel: {e}")

async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    channels = list(fsub_col.find())
    if not channels:
        await update.message.reply_text("📁 Force Join list is empty.")
        return
    
    msg = "📢 <b>Active Force Join Channels:</b>\n\n"
    for idx, ch in enumerate(channels, 1):
        msg += f"{idx}. <b>{ch.get('title')}</b>\n🆔 <code>{ch.get('channel_id')}</code>\n🔗 {ch.get('invite_link')}\n\n"
    await update.message.reply_text(msg, parse_mode="HTML")

# --- Callbacks ---
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    cancel_status[user_id] = True 
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.answer("❌ Files bhejna rok diya gaya hai.")

# --- Batch Storage & Forwarding ---
async def process_batch_queue(user_id, context, message):
    await asyncio.sleep(15)
    if user_id not in user_queues: return
    raw_files = user_queues.pop(user_id)
    saved_files = []
    
    for msg in raw_files:
        if not msg: continue
        file_obj = msg.document or msg.video or (msg.photo[-1] if msg.photo else None) or msg.audio
        file_id = file_obj.file_id if file_obj else None
        file_size = file_obj.file_size if file_obj and hasattr(file_obj, 'file_size') else 0
        file_caption = msg.caption or "" 
        
        if file_id:
            while True:  
                try:
                    if PRIVATE_STORE_ID != 0:
                        await context.bot.forward_message(PRIVATE_STORE_ID, msg.chat_id, msg.message_id)
                    saved_files.append({
                        "file_id": file_id, 
                        "file_size": file_size,
                        "file_type": 'document' if msg.document else ('video' if msg.video else ('photo' if msg.photo else 'audio')),
                        "caption": file_caption 
                    })
                    await asyncio.sleep(0.2)
                    break
                except Exception as e:
                    error_str = str(e)
                    if "FloodWait" in error_str:
                        seconds = int(re.search(r'\d+', error_str).group()) if re.search(r'\d+', error_str) else 5
                        await asyncio.sleep(seconds + 1)
                    else:
                        break

    backup_queues[user_id] = saved_files
    await message.reply_text("✅ Batch stored! /getlink command bhejkar link generate karein.")

async def handle_incoming_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS: return

    if user_id not in user_queues:
        user_queues[user_id] = []
    
    user_queues[user_id].append(update.message)
    
    if user_id in processing_tasks:
        processing_tasks[user_id].cancel()

    processing_tasks[user_id] = asyncio.create_task(process_batch_queue(user_id, context, update.message))

async def get_link_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS: return
    
    if user_id not in backup_queues or not backup_queues[user_id]: 
        await update.message.reply_text("❌ Queue khali hai! Pehle files bhejein.")
        return
        
    batch_key = f"batch_{int(time.time())}"
    try:
        active_db, active_name = get_active_file_db()
        file_batch_col = active_db['file_batches']
        
        file_batch_col.insert_one({"batch_key": batch_key, "files": backup_queues[user_id], "timestamp": time.time()})
        registry_col.insert_one({"batch_key": batch_key, "db_name": active_name})
        
        backup_queues.pop(user_id, None)
        bot_info = await context.bot.get_me()
        await update.message.reply_text(f"🔗 Link: https://t.me/{bot_info.username}?start={batch_key}\n📂 Stored in: {active_name}")
    except Exception as e:
        await update.message.reply_text(f"❌ Link generation error: {e}")

# --- Channel Event Listeners ---
async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        result = update.chat_member
        if result:
            user_id = result.new_chat_member.user.id
            chat_id = result.chat.id
            new_status = result.new_chat_member.status
            
            # Agar user chhod deta hai ya admin use nikal deta hai, to DB se remove kar do
            if new_status in ['left', 'kicked']:
                join_req_col.delete_one({"user_id": user_id, "channel_id": chat_id})
            elif new_status in ['member', 'administrator', 'creator']:
                join_req_col.delete_one({"user_id": user_id, "channel_id": chat_id})
    except Exception as e:
        print(f"Chat Member Update Error: {e}")

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.chat_join_request.from_user
        chat = update.chat_join_request.chat
        
        # Join Request DB me store karein
        join_req_col.update_one(
            {"user_id": user.id, "channel_id": chat.id},
            {"$set": {"status": "requested", "time": time.time()}},
            upsert=True
        )
    except Exception as e:
        print(f"Join Request Handling Error: {e}")

# --- App Startup ---
def main():
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN missing!")
        return

    request_kwargs = HTTPXRequest(connect_timeout=20.0, read_timeout=20.0)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(request_kwargs).post_init(run_post_init).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("getlink", get_link_manually))
    app.add_handler(CommandHandler("addchannel", add_channel))
    app.add_handler(CommandHandler("delchannel", del_channel))
    app.add_handler(CommandHandler("channels", list_channels))
    
    # Callbacks
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_action$"))
    
    # Files
    app.add_handler(MessageHandler(filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.AUDIO, handle_incoming_files))

    # Realtime Event Handlers for Channels
    app.add_handler(ChatJoinRequestHandler(handle_join_request))
    app.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))

    print("🤖 Bot Running (Pure Force-Sub Mode)...")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
