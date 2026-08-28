import os
import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, ContextTypes, filters
from pymongo import MongoClient
from bson.objectid import ObjectId

# 🔑 अपनी डिटेल्स यहाँ भरें
TELEGRAM_BOT_TOKEN = "8933588495:AAGI5TLmfV8wob7GY8aUHY4k8gflDRxb0dY"
MONGO_URI = "mongodb+srv://mybot7:mdjaved11@cluster0.kz1njzu.mongodb.net/?appName=Cluster0"
ADMIN_ID = 6598432032        
PRIVATE_STORE_ID = -1004319812230  

# 📢 4 चैनल्स के डिटेल्स (Username और Invite Links)
FORCE_SUB_CHANNELS = [
    {"username": "@AllstoryFM2", "link": "https://t.me/AllstoryFM2", "name": "Channel 1"},
    {"username": "@JOINCHANNELl  1", "link": "https://t.me/+Yn1F0Pju33QzY2Nl", "name": "Channel 2"},
    {"username": "@JOINCHANNELl  2", "link": "https://t.me/+BPsh855KDVBhYzQ1", "name": "Channel 3"},
    {"username": "@JOINCHANNELl  3", "link": "https://t.me/+EFvk-wHAJC1lNTM1", "name": "Channel 4"}
]

# MongoDB सेटअप
client = MongoClient(MONGO_URI)
db = client['bot_database']
batch_col = db['file_batches']
user_col = db['users']
delete_col = db['delete_queue'] 
history_col = db['user_history']  

user_queues = {}
backup_queues = {}

async def auto_delete_monitor(app):
    while True:
        try:
            current_time = time.time()
            all_pending = delete_col.find({"delete_at": {"$lte": current_time}})
            for task in all_pending:
                chat_id = task['chat_id']
                for msg_id in task['message_ids']:
                    try: await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except: pass
                    await asyncio.sleep(0.1) 
                delete_col.delete_one({"_id": task['_id']})
        except Exception as e: print(f"ऑटो-डिलीट मॉनिटर एरर: {e}")
        await asyncio.sleep(15)

async def run_post_init(application):
    asyncio.create_task(auto_delete_monitor(application))

# 🔍 चारों चैनलों के लिए सब्सक्रिप्शन चेक फ़ंक्शन
async def check_user_joined(context, user_id):
    unjoined_channels = []
    for ch in FORCE_SUB_CHANNELS:
        try:
            member = await context.bot.get_chat_member(chat_id=ch["username"], user_id=user_id)
            if member.status not in ['member', 'administrator', 'creator']:
                unjoined_channels.append(ch)
        except:
            unjoined_channels.append(ch)
    return unjoined_channels

async def send_files_logic(update, context, batch_key):
    user = update.effective_user
    batch = batch_col.find_one({"batch_key": batch_key})
    
    if not batch:
        await update.message.reply_text("❌ यह लिंक अमान्य है।")
        return

    history_col.insert_one({"user_id": user.id, "first_name": user.first_name, "username": user.username, "action": "requested_files", "batch_key": batch_key, "time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')})
    
    info_msg = await update.message.reply_text("⏳ Sending files...")
    sent_message_ids = [info_msg.message_id]
    delete_at_time = time.time() + 28800 
    
    for file in batch["files"]:
        try:
            sent_msg = None
            if file['file_type'] == 'document': sent_msg = await update.message.reply_document(file['file_id'], protect_content=True)
            elif file['file_type'] == 'video': sent_msg = await update.message.reply_video(file['file_id'], protect_content=True)
            elif file['file_type'] == 'photo': sent_msg = await update.message.reply_photo(file['file_id'], protect_content=True)
            elif file['file_type'] == 'audio': sent_msg = await update.message.reply_audio(file['file_id'], protect_content=True)
            
            if sent_msg: sent_message_ids.append(sent_msg.message_id)
            await asyncio.sleep(0.4) 
        except: break

    delete_col.insert_one({"chat_id": update.message.chat_id, "message_ids": sent_message_ids, "delete_at": delete_at_time})
    try: await context.bot.delete_message(chat_id=update.message.chat_id, message_id=info_msg.message_id)
    except: pass
    await update.message.reply_text("𝙷𝙸𝙽𝙳𝙸 𝚂𝚃𝙾𝚁𝚈\n❤️ 𝙷𝙴𝚈 𝙱𝚁𝙾 🇮🇳 \n\n📂 𝙵𝙸𝙻𝙴𝚂 𝚆𝙸𝙻𝙻 𝙱𝙴 𝙳𝙴𝙻𝙴𝚃𝙴𝙳 \n𝙰𝙵𝚃𝙴𝚁 [ 𝟾 𝙷𝙾𝚄𝚁𝚂 ] 𝙿𝙻𝙴𝙰𝚂𝙴 \n𝚂𝙰𝚅𝙴 𝚃𝙷𝙴𝙼 𝚂𝙾𝙼𝙴𝚆𝙷𝙴𝚁𝙴 𝚂𝙰𝙵𝙴.", parse_mode="Markdown")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user_col.find_one({"user_id": user.id}):
        user_col.insert_one({"user_id": user.id, "username": user.username, "first_name": user.first_name})

    args = context.args
    if args:
        batch_key = args[0]
        unjoined = await check_user_joined(context, user.id)
        
        # यदि यूज़र ने कोई भी चैनल जॉइन नहीं किया है
        if unjoined:
            buttons = []
            for ch in unjoined:
                buttons.append([InlineKeyboardButton(f"📢 Join {ch['name']}", url=ch['link'])])
            
            # पुनः प्रयास (Try Again) का बटन
            buttons.append([InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{(await context.bot.get_me()).username}?start={batch_key}")])
            
            await update.message.reply_text(
                "⚠️ फाइल्स प्राप्त करने के लिए कृपया नीचे दिए गए सभी चैनलों को जॉइन करें:",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return
            
        asyncio.create_task(send_files_logic(update, context, batch_key))
        return
    await update.message.reply_text("👋 Hello! I am a permanent batch file store bot.")

async def check_logs(update, context):
    if update.effective_user.id != ADMIN_ID: return
    logs = list(history_col.find().sort("_id", -1).limit(15))
    log_text = "📊 Recent Logs:\n\n" + "".join([f"👤 {e.get('first_name')}\n📥 {e.get('batch_key')}\n⏰ {e.get('time')}\n\n" for e in logs])
    await update.message.reply_text(log_text)

async def stats(update, context):
    if update.effective_user.id == ADMIN_ID:
        await update.message.reply_text(f"👥 Total Users: {user_col.count_documents({})}\n📥 Total Requests: {history_col.count_documents({})}")

async def broadcast(update, context):
    if update.effective_user.id != ADMIN_ID: return
    all_users = user_col.find()
    for user in all_users:
        try:
            if update.message.reply_to_message:
                await context.bot.copy_message(user['user_id'], update.message.chat_id, update.message.reply_to_message.message_id)
            else:
                await context.bot.send_message(user['user_id'], " ".join(context.args))
            await asyncio.sleep(0.05)
        except: pass
    await update.message.reply_text("✅ Broadcast complete.")

async def get_link_manually(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if ADMIN_ID not in backup_queues: return
    batch_key = f"batch_{int(time.time())}"
    batch_col.insert_one({"batch_key": batch_key, "files": backup_queues[ADMIN_ID], "timestamp": time.time()})
    await update.message.reply_text(f"🔗 Link: https://t.me/{(await context.bot.get_me()).username}?start={batch_key}")

async def process_batch_queue(user_id, context, message):
    await asyncio.sleep(60)
    if user_id not in user_queues: return
    raw_files = user_queues.pop(user_id)
    saved_files = []
    for msg in raw_files:
        file_id = msg.document.file_id if msg.document else (msg.video.file_id if msg.video else (msg.photo[-1].file_id if msg.photo else (msg.audio.file_id if msg.audio else None)))
        if file_id:
            try:
                await context.bot.forward_message(PRIVATE_STORE_ID, msg.chat_id, msg.message_id)
                saved_files.append({"file_id": file_id, "file_type": 'document' if msg.document else ('video' if msg.video else ('photo' if msg.photo else 'audio'))})
                await asyncio.sleep(0.5)
            except: pass
    backup_queues[user_id] = saved_files
    await message.reply_text("✅ Batch stored!")

async def store_file(update, context):
    if not update.message or not update.message.from_user or update.message.from_user.id != ADMIN_ID: return
    if update.message.from_user.id not in user_queues:
        user_queues[update.message.from_user.id] = []
        asyncio.create_task(process_batch_queue(update.message.from_user.id, context, update.message))
    user_queues[update.message.from_user.id].append(update.message)

if __name__ == "__main__":
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).post_init(run_post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("logs", check_logs))
    app.add_handler(CommandHandler("getlink", get_link_manually))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.ALL & ~filters.COMMAND, store_file))
    print("🤖 Bot is running on MongoDB!")
    app.run_polling(drop_pending_updates=True)
    
