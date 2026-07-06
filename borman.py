from __future__ import annotations
import os
import asyncio
import logging
import shutil
import sqlite3
import random
from datetime import datetime, timedelta, timezone
from collections import namedtuple
from typing import Optional, List, Any
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# بارگذاری متغیرهای محیطی از فایل .env
load_dotenv()

# =====================================================================
# 1. CONFIGURATION (LOADED SECURELY FROM ENVIRONMENT)
# =================================================A====================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("CRITICAL ERROR: TELEGRAM_BOT_TOKEN is not set in the environment or .env file!")

try:
    SOURCE_GROUP_ID = int(os.getenv("SOURCE_GROUP_ID", "0"))
    DESTINATION_GROUP_ID = int(os.getenv("DESTINATION_GROUP_ID", "0"))
except ValueError:
    raise ValueError("CRITICAL ERROR: SOURCE_GROUP_ID and DESTINATION_GROUP_ID must be valid integers!")

if SOURCE_GROUP_ID == 0 or DESTINATION_GROUP_ID == 0:
    raise ValueError("CRITICAL ERROR: SOURCE_GROUP_ID and DESTINATION_GROUP_ID must be configured!")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
INTERVAL_HOURS = int(os.getenv("INTERVAL_HOURS", "2"))

MIN_DELAY_PER_FILE_SEC = int(os.getenv("MIN_DELAY_PER_FILE_SEC", "300"))
MAX_DELAY_PER_FILE_SEC = int(os.getenv("MAX_DELAY_PER_FILE_SEC", "900"))

DB_FILE = os.getenv("DB_FILE", "scheduler.db")
BACKUP_FILE = os.getenv("BACKUP_FILE", "scheduler_backup.db")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")
PROXY_URL = os.getenv("PROXY_URL", "").strip()

DEFAULT_CAPTION = "<b><i><a href='https://t.me/radio_shahrestan'>radio shahrestan</a></i></b>\n\n<b><i><a href='https://t.me/radio_eley'>radio la</a></i></b>"
CUSTOM_HTML_CAPTION = os.getenv("CUSTOM_HTML_CAPTION", DEFAULT_CAPTION).replace("\\n", "\n")

# =====================================================================
# 2. UTILS & LOGGER
# =====================================================================
logger = logging.getLogger("telegram_scheduler")

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def copy_db_backup() -> None:
    try:
        if os.path.exists(DB_FILE):
            shutil.copy2(DB_FILE, BACKUP_FILE)
            logger.info("Database backup created successfully.")
    except Exception as e:
        logger.error(f"Failed to create database backup: {e}")

def human_time(dt: Optional[datetime]) -> str:
    if not dt:
        return "none"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

# =====================================================================
# 3. DATABASE LAYER
# =====================================================================
class Database:
    def __init__(self, db_path: str = DB_FILE) -> None:
        self.db_path = db_path
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheduled_at TEXT NOT NULL,
                    sent_at TEXT
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    message_type TEXT,
                    file_id TEXT,
                    file_unique_id TEXT,
                    caption TEXT,
                    media_group_id TEXT,
                    batch_id INTEGER,
                    FOREIGN KEY(batch_id) REFERENCES batches(id),
                    UNIQUE(source_chat_id, source_message_id)
                );
            """)
            conn.commit()

    def add_media(self, source_chat_id: int, source_message_id: int, message_type: str,
                  file_id: str, file_unique_id: str, caption: Optional[str],
                  media_group_id: Optional[str]) -> Optional[int]:
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO media (source_chat_id, source_message_id, message_type, file_id,
                                       file_unique_id, caption, media_group_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (source_chat_id, source_message_id, message_type, file_id,
                      file_unique_id, caption, media_group_id))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def get_queue_count(self) -> int:
        with self._get_conn() as conn:
            res = conn.execute("SELECT COUNT(*) FROM media WHERE batch_id IS NULL").fetchone()
            return res[0] if res else 0

    def get_scheduled_batches_count(self) -> int:
        with self._get_conn() as conn:
            res = conn.execute("SELECT COUNT(*) FROM batches WHERE sent_at IS NULL").fetchone()
            return res[0] if res else 0

    def get_next_send_time(self) -> Optional[str]:
        with self._get_conn() as conn:
            res = conn.execute("SELECT MIN(scheduled_at) FROM batches WHERE sent_at IS NULL").fetchone()
            return res[0] if res and res[0] else None

    def get_last_scheduled_time(self) -> Optional[str]:
        with self._get_conn() as conn:
            res = conn.execute("SELECT MAX(scheduled_at) FROM batches").fetchone()
            return res[0] if res and res[0] else None

    def get_random_unscheduled_media_ids(self, limit: int) -> List[int]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT id FROM media WHERE batch_id IS NULL ORDER BY RANDOM() LIMIT ?", (limit,)).fetchall()
            return [row['id'] for row in rows]

    def create_batch(self, scheduled_at_str: str, media_ids: List[int]) -> int:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO batches (scheduled_at) VALUES (?)", (scheduled_at_str,))
            batch_id = cursor.lastrowid
            placeholders = ",".join("?" for _ in media_ids)
            conn.execute(f"UPDATE media SET batch_id = ? WHERE id IN ({placeholders})", [batch_id] + list(media_ids))
            conn.commit()
            return batch_id

    def get_next_due_batch(self, now_str: str) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT * FROM batches WHERE sent_at IS NULL AND scheduled_at <= ? ORDER BY scheduled_at ASC LIMIT 1",
                (now_str,)
            ).fetchone()

    def get_batch_media(self, batch_id: int) -> List[sqlite3.Row]:
        with self._get_conn() as conn:
            return conn.execute("SELECT * FROM media WHERE batch_id = ?", (batch_id,)).fetchall()

    def mark_batch_sent(self, batch_id: int) -> None:
        with self._get_conn() as conn:
            now_str = datetime.now(timezone.utc).isoformat()
            conn.execute("UPDATE batches SET sent_at = ? WHERE id = ?", (now_str, batch_id))
            conn.commit()

# =====================================================================
# 4. QUEUE MANAGER
# =====================================================================
Stats = namedtuple("Stats", ["queue_count", "scheduled_batches", "next_send_at"])

class QueueManager:
    def __init__(self, db: Database) -> None:
        self.db = db

    def schedule_if_possible(self) -> int:
        batches_created = 0
        while True:
            queue_count = self.db.get_queue_count()
            if queue_count < BATCH_SIZE:
                break

            last_scheduled = self.db.get_last_scheduled_time()
            now_dt = datetime.now(timezone.utc)

            if last_scheduled:
                last_dt = datetime.fromisoformat(last_scheduled)
                next_dt = last_dt + timedelta(hours=INTERVAL_HOURS)
                if next_dt < now_dt:
                    next_dt = now_dt
            else:
                next_dt = now_dt

            media_ids = self.db.get_random_unscheduled_media_ids(BATCH_SIZE)
            if len(media_ids) < BATCH_SIZE:
                break

            self.db.create_batch(next_dt.isoformat(), media_ids)
            batches_created += 1

        return batches_created

    def get_stats(self) -> Stats:
        queue_count = self.db.get_queue_count()
        scheduled_batches = self.db.get_scheduled_batches_count()
        next_send = self.db.get_next_send_time()

        next_send_dt = datetime.fromisoformat(next_send) if next_send else None
        return Stats(queue_count=queue_count, scheduled_batches=scheduled_batches, next_send_at=next_send_dt)

    def reschedule_after_restart(self) -> None:
        self.schedule_if_possible()

# =====================================================================
# 5. SENDER LAYER
# =====================================================================
class Sender:
    def __init__(self, bot: Any, db: Database) -> None:
        self.bot = bot
        self.db = db

    async def send_batch(self, batch_id: int) -> None:
        media_items = self.db.get_batch_media(batch_id)
        if not media_items:
            return

        media_list = list(media_items)
        random.shuffle(media_list)

        total_items = len(media_list)
        logger.info(f"🚀 Processing Batch {batch_id} - {total_items} files (RANDOM ORDER)")

        for index, item in enumerate(media_list):
            try:
                m_type = item['message_type']
                f_id = item['file_id']
                caption = CUSTOM_HTML_CAPTION

                if m_type == "video_note":
                    await self.bot.send_video_note(chat_id=DESTINATION_GROUP_ID, video_note=f_id)
                    if caption:
                        await self.bot.send_message(chat_id=DESTINATION_GROUP_ID, text=caption, parse_mode="HTML")
                elif m_type == "photo":
                    await self.bot.send_photo(chat_id=DESTINATION_GROUP_ID, photo=f_id, caption=caption, parse_mode="HTML")
                elif m_type == "video":
                    await self.bot.send_video(chat_id=DESTINATION_GROUP_ID, video=f_id, caption=caption, parse_mode="HTML")
                elif m_type == "animation":
                    await self.bot.send_animation(chat_id=DESTINATION_GROUP_ID, animation=f_id, caption=caption, parse_mode="HTML")
                elif m_type == "document":
                    await self.bot.send_document(chat_id=DESTINATION_GROUP_ID, document=f_id, caption=caption, parse_mode="HTML")
                elif m_type == "audio":
                    await self.bot.send_audio(chat_id=DESTINATION_GROUP_ID, audio=f_id, caption=caption, parse_mode="HTML")
                elif m_type == "voice":
                    await self.bot.send_voice(chat_id=DESTINATION_GROUP_ID, voice=f_id, caption=caption, parse_mode="HTML")

                logger.info(f"✅ Sent {index+1}/{total_items} from batch {batch_id}")

                if index < total_items - 1:
                    delay = random.randint(MIN_DELAY_PER_FILE_SEC, MAX_DELAY_PER_FILE_SEC)
                    logger.info(f"⏳ Waiting {delay} seconds...")
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"Failed to send media id={item['id']} type={m_type}: {e}")

        self.db.mark_batch_sent(batch_id)
        logger.info(f"🎉 Batch {batch_id} completed successfully.")

# =====================================================================
# 6. SCHEDULER ENGINE
# =====================================================================
class SchedulerEngine:
    def __init__(self, db: Database, sender: Sender, queue_manager: QueueManager) -> None:
        self.db = db
        self.sender = sender
        self.queue_manager = queue_manager
        self.task: Optional[asyncio.Task] = None
        self.is_running = False

    def start(self) -> None:
        self.is_running = True
        self.task = asyncio.create_task(self._loop())
        logger.info("Scheduler background loop started.")

    async def stop(self) -> None:
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Scheduler background loop stopped.")

    async def _loop(self) -> None:
        while self.is_running:
            try:
                now_str = datetime.now(timezone.utc).isoformat()
                batch = self.db.get_next_due_batch(now_str)
                if batch:
                    batch_id = batch['id']
                    logger.info(f"Processing batch ID: {batch_id}")
                    await self.sender.send_batch(batch_id)
            except Exception as e:
                logger.error(f"Error in scheduler core engine loop: {e}", exc_info=True)

            await asyncio.sleep(30)

# =====================================================================
# 7. HANDLERS
# =====================================================================
def _extract_media(message: Any):
    if message.photo:
        return "photo", message.photo[-1].file_id, message.photo[-1].file_unique_id
    if message.video:
        return "video", message.video.file_id, message.video.file_unique_id
    if message.animation:
        return "animation", message.animation.file_id, message.animation.file_unique_id
    if message.document:
        return "document", message.document.file_id, message.document.file_unique_id
    if message.audio:
        return "audio", message.audio.file_id, message.audio.file_unique_id
    if message.voice:
        return "voice", message.voice.file_id, message.voice.file_unique_id
    if message.video_note:
        return "video_note", message.video_note.file_id, message.video_note.file_unique_id
    return None, None, None

async def handle_source_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message:
        return
    chat = update.effective_chat
    if not chat or chat.id != SOURCE_GROUP_ID:
        return

    message_type, file_id, file_unique_id = _extract_media(message)
    if not message_type:
        return

    db: Database = context.application.bot_data["db"]
    queue_manager: QueueManager = context.application.bot_data["queue_manager"]

    media_group_id = str(message.media_group_id) if message.media_group_id else None
    caption = message.caption if message.caption else None

    stored_id = db.add_media(
        source_chat_id=chat.id,
        source_message_id=message.message_id,
        message_type=message_type,
        file_id=file_id,
        file_unique_id=file_unique_id,
        caption=caption,
        media_group_id=media_group_id,
    )

    if stored_id is None:
        logger.info("Skipped duplicate media")
        return

    logger.info(f"Stored media id={stored_id} type={message_type}")
    created = queue_manager.schedule_if_possible()
    if created:
        logger.info(f"Scheduled {created} new batch(es)")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    queue_manager: QueueManager = context.application.bot_data["queue_manager"]
    stats = queue_manager.get_stats()
    next_send = human_time(stats.next_send_at)
    text = f"Queue items: {stats.queue_count}\nScheduled batches: {stats.scheduled_batches}\nNext send: {next_send}"
    await update.effective_message.reply_text(text)

async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db: Database = context.application.bot_data["db"]
    count = db.get_queue_count()
    await update.effective_message.reply_text(f"Queued items: {count}")

def register_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("queue", cmd_queue))
    application.add_handler(
        MessageHandler(filters.Chat(SOURCE_GROUP_ID) & ~filters.COMMAND, handle_source_message)
    )

# =====================================================================
# 8. LIFECYCLE
# =====================================================================
async def post_init(application: Application) -> None:
    queue_manager: QueueManager = application.bot_data["queue_manager"]
    scheduler: SchedulerEngine = application.bot_data["scheduler"]
    logger.info("Bot components post_init started.")
    copy_db_backup()
    queue_manager.reschedule_after_restart()
    scheduler.start()

async def post_shutdown(application: Application) -> None:
    scheduler: SchedulerEngine = application.bot_data["scheduler"]
    await scheduler.stop()
    copy_db_backup()
    logger.info("Bot application successfully shutdown.")

# =====================================================================
# 9. MAIN
# =====================================================================
import asyncio
from telegram.ext import Application

# =====================================================================
# 9. MAIN (جدید و درست)
# =====================================================================
def main() -> None:
    setup_logging()
    logger.info("Starting Telegram Random Scheduler Unification Engine...")

    # ایجاد دیتابیس و مدیریت‌کننده‌ها
    db = Database()
    queue_manager = QueueManager(db)

    # ==================== ساخت Application ====================
    application = Application.builder().token(TOKEN).build()

    # ایجاد Sender و Scheduler
    sender = Sender(application.bot, db)
    scheduler = SchedulerEngine(
        db=db,
        sender=sender,
        queue_manager=queue_manager
    )

    # ذخیره در bot_data
    application.bot_data["db"] = db
    application.bot_data["queue_manager"] = queue_manager
    application.bot_data["sender"] = sender
    application.bot_data["scheduler"] = scheduler

    # ثبت هندلرها
    register_handlers(application)

    logger.info("Bot initialized successfully.")

    # ==================== اجرای async ====================
    async def run_bot():
        try:
            # post_init دستی
            await post_init(application)

            await application.start()

            await application.updater.start_polling(
                allowed_updates=None,
                drop_pending_updates=True,
                poll_interval=1.0,
            )

            await asyncio.Event().wait()

        except asyncio.CancelledError:
            logger.info("Bot is shutting down...")
        except Exception as e:
            logger.error(f"Error in bot: {e}", exc_info=True)
        finally:
            await post_shutdown(application)
            await application.stop()
            await application.shutdown()

    asyncio.run(run_bot())


if __name__ == "__main__":
    main()    setup_logging()
    logger.info("Starting Telegram Random Scheduler Unification Engine...")

    # ایجاد دیتابیس و مدیریت‌کننده‌ها
    db = Database()
    queue_manager = QueueManager(db)

    # ==================== ساخت Application ====================
    builder = Application.builder().token(TOKEN)
    
    # بدون پروکسی (حذف کامل بخش پروکسی)
    application = (
        builder
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # ایجاد Sender و Scheduler
    sender = Sender(application.bot, db)
    scheduler = SchedulerEngine(
        db=db, 
        sender=sender, 
        queue_manager=queue_manager
    )

    # ذخیره کردن اشیاء در bot_data
    application.bot_data["db"] = db
    application.bot_data["queue_manager"] = queue_manager
    application.bot_data["sender"] = sender
    application.bot_data["scheduler"] = scheduler

    # ثبت هندلرها
    register_handlers(application)

    logger.info("Polling activated. Press Ctrl+C to terminate.")

    # ==================== اجرای صحیح async ====================
    async def run_bot():
        try:
            await application.initialize()
            await application.start()
            
            # شروع polling
            await application.updater.start_polling(
                allowed_updates=None,
                drop_pending_updates=True,
                # تنظیمات مناسب برای سرور
                poll_interval=1.0,
                timeout=30
            )
            
            # نگه داشتن برنامه تا وقتی که Ctrl+C بزنیم
            await asyncio.Event().wait()
            
        except asyncio.CancelledError:
            logger.info("Bot is shutting down...")
        except Exception as e:
            logger.error(f"Error in bot: {e}", exc_info=True)
        finally:
            # تمیز کردن در زمان خروج
            await application.stop()
            await application.shutdown()

    # اجرای اصلی
    asyncio.run(run_bot())

if __name__ == "__main__":
    main()