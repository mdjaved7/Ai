import os
import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, TelegramError
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler, 
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    ContextTypes
)
from pymongo import MongoClient

# --- CONFIGURATION (सब कुछ यहाँ सेट करें) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
MONGO_URI = os.getenv("MONGO_URI", "YOUR_MONGO_URI_HERE")
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK", "https://t.me/AllstoryFM2") 

# Hardcoded Force Join Channels (यहाँ अपने सभी Channels की Details डालें)
FORCE_JOIN_CHANNELS = [
    {
        "channel_id": -1003982333880,
        "title": "Backup Channel 1",
        "link": "https://t.me/+Yn1F0Pju33QzY2Nl"
    },
    {
        "channel_id": -1004333260005,
        "title": "JOIN CHANNEL 2",
        "link": "https://t.me/+BPsh855KDVBhYzQ1"
    },
    {
        "channel_id": -1003955164011,
        "title": "All story FM 3",
        "link": "https://t.me/AllstoryFM2"
    },
    {
        "channel_id": -1003984093378,
        "title": "Channel 4",
        "link": "https://t.me/+EFvk-wHAJC1lNTM1"
]

# MongoDB Setup (केवल File Storage और Requests ट्रैकिंग के लिए)
client = MongoClient(MONGO_URI)
primary_db = client['bot_primary_db']

user_col = primary_db['users']
delete_col = primary_db['delete_queue'] 
registry_col = primary_db['batch_registry']
join_req_col = primary_db['join_requests_data']

cancel_status = {}

def get_readable_size(size_in_bytes):
    if not size_in_bytes:
        return "Unknown Size"
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

# --- Force Join Verification Logic ---
async def get_fsub_buttons(context, user_id, start_param):
    if not FORCE_JOIN_CHANNELS:
        return True, [] 

    unjoined_buttons = []
    has_unjoined = False

    for ch in FORCE_JOIN_CHANNELS:
        ch_id = ch["channel_id"]
        ch_link = ch["link"]
        ch_title = ch["title"]

        is_member = False
        
        # 1. Real-time Status Check
        try:
            member = await context.bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                is_member = True
            elif member.status in ['left', 'kicked']:
                # यूजर को निकाल दिया गया है तो पुराना Join Request रिकॉर्ड डिलीट करें
                join_req_col.delete_one({"user_id": user_id, "channel_id": ch_id})
                is_member = False
        except TelegramError:
            pass

        if is_member:
            continue

        # 2. Check Active Join Request (क्या यूज़र ने Request to Join बटन दबाया है)
        has_requested = join_req_col.find_one({"user_id": user_id, "channel_id": ch_id})
        if has_requested:
            continue

        has_unjoined = True
        unjoined_buttons.append([InlineKeyboardButton(f"📢 Request {ch_title}", url=ch_link)])

    if has_unjoined:
        cb_data = f"try_again_{start_param}" if start_param else "try_again_check"
        unjoined_buttons.append([InlineKeyboardButton("🔄 Try Again", callback_data=cb_data)])
        return False, unjoined_buttons
    else:
        return True, []

# --- Auto Delete Service ---
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

# --- File Sending Logic ---
async def send_files_logic(update, context, batch_key):
    user = update.effective_user
    chat_id = update.effective_chat.id
    cancel_status[user.id] = False 
    
    reg_record = registry_col.find_one({"batch_key": batch_key})
    batch = None
    if reg_record:
        target_db_name = reg_record.get("db_name", "bot_file_db_0")
        batch = client[target_db_name]['file_batches'].find_one({"batch_key": batch_key})
    else:
        batch = client['bot_database']['file_batches'].find_one({"batch_key": batch_key})
    
    if not batch:
        await context.bot.send_message(chat_id=chat_id, text="❌ Link invalid या expire हो गया है।")
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
    
    for file in batch.get("files", []):
        if cancel_status.get(user.id): 
            break 

        try:
            sent_msg = None
            readable_size = get_readable_size(file.get('file_size', 0))
            file_type = file.get('file_type')
            custom_caption = f">> JOIN > @AllstoryFM2 🔥\n✅✨\n\n👉 FILE SIZE :- {readable_size} 👑"

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
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception as e:
            print(f"File send error: {e}")
            break

    if sent_message_ids:
        delete_col.insert_one({"chat_id": chat_id, "message_ids": sent_message_ids, "delete_at": time.time() + 14400})

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=info_msg.message_id)
    except Exception:
        pass

    alert_text = (
        "𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳 \n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 \n𝙰𝙵𝚃𝙴𝚁 [ 𝟾 𝙷𝙾𝚄𝚁𝚂 ] 𝙿𝙻𝙴𝙰𝚂𝙴 \n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴."
    )
    
    final_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=alert_text, 
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📟 UPDATE CHANNEL", url=CHANNEL_INVITE_LINK)]])
    )
    delete_col.insert_one({"chat_id": chat_id, "message_ids": [final_msg.message_id], "delete_at": time.time() + 14400})

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    user_col.update_one(
        {"_id": user.id},
        {"$set": {"username": user.username, "first_name": user.first_name, "last_seen": time.time()}},
        upsert=True
    )

    args = context.args
    start_param = args[0] if args else ""

    is_approved, buttons = await get_fsub_buttons(context, user.id, start_param)
    
    if not is_approved:
        text = "Access Restricted!\n\nFiles receive karne ke liye niche दिए गए channels ko join/request karein:"
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    if start_param.startswith("batch_"):
        await send_files_logic(update, context, start_param.replace("batch_", ""))
    elif start_param:
        await send_files_logic(update, context, start_param)
    else:
        await context.bot.send_message(chat_id=chat_id, text="Welcome! Send a valid link to get files.")

async def try_again_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    try:
        await query.answer()
    except Exception:
        pass
    
    user = query.from_user
    chat_id = query.message.chat_id
    data = query.data
    
    start_param = data.replace("try_again_", "") if "try_again_" in data else ""
    if start_param == "check":
        start_param = ""

    is_approved, buttons = await get_fsub_buttons(context, user.id, start_param)
    
    if not is_approved:
        text = "Access Restricted!\n\nFiles receive karne ke liye niche दिए गए channels ko join/request karein:"
        try:
            await query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(buttons))
        except TelegramError:
            pass
        return

    try:
        await query.delete_message()
    except Exception:
        pass

    if start_param.startswith("batch_"):
        await send_files_logic(update, context, start_param.replace("batch_", ""))
    elif start_param:
        await send_files_logic(update, context, start_param)
    else:
        await context.bot.send_message(chat_id=chat_id, text="✅ Verification complete! Aap ab files le sakte hain.")

# Join Request Event Listener (जब यूज़र 'Request to join' बटन दबाएगा)
async def join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    if req:
        join_req_col.update_one(
            {"user_id": req.from_user.id, "channel_id": req.chat.id},
            {"$set": {"timestamp": time.time()}},
            upsert=True
        )

# Member Status Listener (यूज़र के लीव करने या रिमूव होने पर)
async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if result and result.new_chat_member.status in ['left', 'kicked']:
        join_req_col.delete_one({"user_id": result.from_user.id, "channel_id": result.chat.id})

async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cancel_status[query.from_user.id] = True
    try:
        await query.edit_message_text("❌ Process Cancelled!")
    except Exception:
        pass

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN missing!")
        return

    req = HTTPXRequest(connection_pool_size=10, read_timeout=30, write_timeout=30, connect_timeout=30)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).request(req).post_init(run_post_init).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(try_again_handler, pattern="^try_again"))
    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_action$"))
    
    app.add_handler(ChatJoinRequestHandler(join_request_handler))
    app.add_handler(ChatMemberHandler(chat_member_handler, ChatMemberHandler.CHAT_MEMBER))

    print("🤖 Bot Ready (All Hardcoded FSub Activated)...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
                                                        
