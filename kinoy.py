#!/usr/bin/env python3
# coding: utf-8
# kino_serial_bot_full_part1.py
# Aiogram 3.22 ga mos. Termux-friendly.
# PART 1 (PART 2 STARTS HERE yozuvi bilan davom etadi)

import os
import re
import asyncio
import logging
import datetime
import random
import html
import urllib.parse
from typing import Optional, Dict, Any, List, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.client.default import DefaultBotProperties

# ----------------- LOGGING -----------------

logging.basicConfig(
    level=logging.ERROR,  # faqat xatolar
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("kino_serial_bot")

# aiogram va aiohttp loglarini ham ERROR darajaga tushiramiz
logging.getLogger("aiogram").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.ERROR)

# ----------------- KONFIG -----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN kerak. Termuxda: export BOT_TOKEN=\"<token>\"")

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
except Exception:
    ADMIN_ID = 0
    logger.warning("ADMIN_ID noto'g'ri yoki o'rnatilmagan.")

DB_FILE = os.getenv("DB_FILE", "kino_serial_bot.db")
VALIDATION_TTL = int(os.getenv("VALIDATION_TTL", "3600"))  # tekshiruv TTL (sekund)
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "")  # agar bitta asosiy kanal bo'lsa

# Aiogram ob'ektlari
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()

# Oddiy admin state (in-memory)
admin_states: Dict[int, Dict[str, Any]] = {}

# ----------------- YORDAMCHI FUNKSIYALAR -----------------
def make_tg_url(val: Optional[str]) -> Optional[str]:
    """@username yoki t.me linkdan to'liq https://t.me/... hosil qiladi."""
    if not val:
        return None
    v = val.strip()
    if v.startswith("http://") or v.startswith("https://"):
        return v
    if v.startswith("t.me/") or v.startswith("telegram.me/"):
        return "https://" + v if not v.startswith("http") else v
    if v.startswith("@"):
        return "https://t.me/" + v.lstrip("@")
    return None

def normalize_invite_for_compare(invite: Optional[str]) -> Optional[str]:
    """URLni tekshirish uchun normalizatsiya qiladi."""
    if not invite:
        return None
    u = invite.strip()
    u = re.sub(r"^https?://(www\.)?", "", u, flags=re.I)
    u = u.rstrip("/")
    return u.lower()

# ----------------- DB INIT VA MIGRATION -----------------
async def init_db():
    """
    DB jadvallarini yaratadi va kerakli ustunlarni (quality) qo'shadi.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        # groups (majburiy kanallar)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                chat_id TEXT PRIMARY KEY,
                username TEXT,
                title TEXT,
                invite TEXT
            );
        """)
        # join_monitored
        await db.execute("""
            CREATE TABLE IF NOT EXISTS join_monitored (
                chat_id TEXT PRIMARY KEY,
                invite TEXT
            );
        """)
        # pending join requests
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pending_join_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                user_id INTEGER,
                username TEXT,
                full_name TEXT,
                requested_at TEXT
            );
        """)
        # movies jadvali (quality ustuni qo'shildi)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                code TEXT PRIMARY KEY,
                title TEXT,
                file_id TEXT,
                file_type TEXT,
                year TEXT,
                genre TEXT,
                quality TEXT,
                language TEXT,
                description TEXT,
                country TEXT,
                downloads INTEGER DEFAULT 0
            );
        """)
        # series va episodes
        await db.execute("""
            CREATE TABLE IF NOT EXISTS series (
                series_code TEXT PRIMARY KEY,
                title TEXT,
                language TEXT,
                description TEXT,
                created_at TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS episodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_code TEXT,
                episode_number INTEGER,
                file_id TEXT,
                file_type TEXT,
                episode_title TEXT,
                downloads INTEGER DEFAULT 0
            );
        """)
        # users va settings
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                subscribed INTEGER DEFAULT 0,
                last_validated_at TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        await db.commit()

        # migration: agar quality ustuni yo'q bo'lsa qo'shamiz
        try:
            cur = await db.execute("PRAGMA table_info(movies)")
            cols = await cur.fetchall()
            col_names = [c[1] for c in cols]
            if "downloads" not in col_names:
                await db.execute("ALTER TABLE movies ADD COLUMN downloads INTEGER DEFAULT 0")
                await db.commit()
            if "country" not in col_names:
                await db.execute("ALTER TABLE movies ADD COLUMN country TEXT")
                await db.commit()
            if "quality" not in col_names:
                await db.execute("ALTER TABLE movies ADD COLUMN quality TEXT")
                await db.commit()
        except Exception:
            logger.exception("Migration xatosi (davom etadi)")

# ----------------- SETTINGS HELPERS -----------------
async def settings_get(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (key,))
        r = await cur.fetchone()
    return r[0] if r else None

async def settings_set(key: str, value: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))
        await db.commit()

# ----------------- USERS HELPERS -----------------
async def add_user_db(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id, subscribed, last_validated_at) VALUES (?, 0, NULL)", (int(user_id),))
        await db.commit()

async def update_user_last_validated(user_id: int, validated_at: datetime.datetime):
    ts = validated_at.isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET last_validated_at = ?, subscribed = 1 WHERE user_id = ?", (ts, int(user_id)))
        await db.commit()

async def invalidate_user_subscription(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET subscribed = 0 WHERE user_id = ?", (int(user_id),))
        await db.commit()

async def get_user_record_db(user_id: int) -> Tuple[int, Optional[datetime.datetime]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT subscribed, last_validated_at FROM users WHERE user_id = ?", (int(user_id),))
        r = await cur.fetchone()
    if not r:
        return 0, None
    subscribed = int(r[0]) if r[0] is not None else 0
    last_validated_at = None
    if r[1]:
        try:
            last_validated_at = datetime.datetime.fromisoformat(r[1])
        except Exception:
            last_validated_at = None
    return subscribed, last_validated_at

# ----------------- GROUPS & JOIN MON HELPERS -----------------
async def add_group_db(chat_id: str, username: Optional[str], title: Optional[str], invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO groups(chat_id, username, title, invite) VALUES (?, ?, ?, ?)",
                         (str(chat_id), username, title, invite))
        await db.commit()

async def remove_group_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM groups WHERE chat_id = ?", (str(chat_id),))
        await db.commit()

async def list_groups_db() -> List[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, username, title, invite FROM groups ORDER BY chat_id")
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]

async def add_join_monitored_db(chat_id: str, invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO join_monitored(chat_id, invite) VALUES (?, ?)", (str(chat_id), invite))
        await db.commit()

async def remove_join_monitored_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM join_monitored WHERE chat_id = ?", (chat_id,))
        await db.commit()

async def list_join_monitored_db() -> List[Tuple[str, Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, invite FROM join_monitored ORDER BY chat_id")
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]

async def is_join_monitored_db(chat_id: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM join_monitored WHERE chat_id = ? LIMIT 1", (str(chat_id),))
        r = await cur.fetchone()
        return bool(r)

async def add_pending_join_request_db(chat_id: str, user_id: int, username: Optional[str], full_name: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO pending_join_requests(chat_id, user_id, username, full_name, requested_at) VALUES (?, ?, ?, ?, ?)",
                         (str(chat_id), int(user_id), username, full_name, datetime.datetime.now(datetime.timezone.utc).isoformat()))
        await db.commit()

async def list_pending_for_user_db(user_id: int) -> List[Tuple[int, str, int, Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT id, chat_id, user_id, username, full_name FROM pending_join_requests WHERE user_id = ?", (int(user_id),))
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
        #!/usr/bin/env python3
# PART 2 (davomi)

# ----------------- MOVIES HELPERS (quality qo'shildi) -----------------
async def add_movie_db(code: str, title: str, file_id: str, file_type: str,
                       year: Optional[str]=None, genre: Optional[str]=None, quality: Optional[str]=None,
                       language: Optional[str]=None, description: Optional[str]=None, country: Optional[str]=None):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT downloads FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
        downloads = int(r[0]) if r and r[0] is not None else 0
        await db.execute("""
            INSERT OR REPLACE INTO movies(code, title, file_id, file_type, year, genre, quality, language, description, country, downloads)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (code, title, file_id, file_type, year, genre, quality, language, description, country, downloads))
        await db.commit()

async def remove_movie_db(code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("DELETE FROM movies WHERE code = ?", (code,))
        await db.commit()
        return cur.rowcount > 0

async def get_movie_db(code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT title, file_id, file_type, year, genre, quality, language, description, country, COALESCE(downloads,0) FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
    return r

async def increment_movie_downloads(code: str) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE movies SET downloads = COALESCE(downloads,0) + 1 WHERE code = ?", (code,))
        await db.commit()
        cur = await db.execute("SELECT downloads FROM movies WHERE code = ?", (code,))
        r = await cur.fetchone()
    return int(r[0]) if r else 0

# ----------------- SERIES & EPISODES HELPERS -----------------
async def add_series_db(series_code: str, title: str, language: Optional[str], description: Optional[str]):
    created = datetime.datetime.now(datetime.timezone.utc).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO series(series_code, title, language, description, created_at) VALUES (?, ?, ?, ?, ?)",
                         (series_code, title, language, description, created))
        await db.commit()

async def add_episode_db(series_code: str, episode_number: int, file_id: str, file_type: str, episode_title: Optional[str]=None):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO episodes(series_code, episode_number, file_id, file_type, episode_title, downloads) VALUES (?, ?, ?, ?, ?, 0)",
                         (series_code, episode_number, file_id, file_type, episode_title))
        await db.commit()

async def get_series_meta(series_code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT title, language, description FROM series WHERE series_code = ? LIMIT 1", (series_code,))
        r = await cur.fetchone()
    return r

async def get_series_episodes(series_code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT episode_number, file_id, file_type, episode_title, COALESCE(downloads,0) FROM episodes WHERE series_code = ? ORDER BY episode_number", (series_code,))
        rows = await cur.fetchall()
    return rows

async def get_episode_db(series_code: str, episode_number: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT file_id, file_type, episode_title, COALESCE(downloads,0) FROM episodes WHERE series_code = ? AND episode_number = ? LIMIT 1", (series_code, episode_number))
        r = await cur.fetchone()
    return r

async def increment_episode_downloads(series_code: str, episode_number: int) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE episodes SET downloads = COALESCE(downloads,0) + 1 WHERE series_code = ? AND episode_number = ?", (series_code, episode_number))
        await db.commit()
        cur = await db.execute("SELECT downloads FROM episodes WHERE series_code = ? AND episode_number = ?", (series_code, episode_number))
        r = await cur.fetchone()
    return int(r[0]) if r else 0

async def remove_series_db(series_code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM episodes WHERE series_code = ?", (series_code,))
        cur = await db.execute("DELETE FROM series WHERE series_code = ?", (series_code,))
        await db.commit()
        try:
            return cur.rowcount > 0
        except Exception:
            cur2 = await db.execute("SELECT 1 FROM series WHERE series_code = ? LIMIT 1", (series_code,))
            r = await cur2.fetchone()
            return r is None

# ----------------- UI / KEYBOARDS -----------------
def admin_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Kino qo'shish 🎬"), KeyboardButton(text="Serial qo'shish 📺")],
            [KeyboardButton(text="Guruh qo'shish ➕"), KeyboardButton(text="Guruh o'chirish ➖")],
            [KeyboardButton(text="JoinRequest qo'shish"), KeyboardButton(text="JoinRequest o'chirish")],
            [KeyboardButton(text="List Groups"), KeyboardButton(text="List Monitored")],
            [KeyboardButton(text="Set Share Link"), KeyboardButton(text="Remove Share Link")],
            [KeyboardButton(text="Foydalanuvchilar"), KeyboardButton(text="Cancel")]
        ], resize_keyboard=True
    )

def admin_flow_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Cancel")]], resize_keyboard=True)

def collect_episodes_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Tugatish ✅"), KeyboardButton(text="Cancel")]], resize_keyboard=True)

def build_episodes_inline_kb(series_code: str, episodes: List[Tuple[int,str,str,Optional[str],int]]) -> InlineKeyboardMarkup:
    """
    Tugma matnlari: "1-qism", "2-qism", ... (callback play:series:ep)
    """
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for ep in episodes:
        ep_num = ep[0]
        btn = InlineKeyboardButton(text=f"{ep_num}-qism", callback_data=f"play:{series_code}:{ep_num}")
        current_row.append(btn)
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def build_movie_kb(code: str, title: str) -> InlineKeyboardMarkup:
    codes_link = await settings_get("codes_link")
    rows: List[List[InlineKeyboardButton]] = []
    share_text = f"Kod: {code} — {title}"
    if codes_link:
        url = "https://t.me/share/url?url=&text=" + urllib.parse.quote_plus(share_text) + "&to=" + urllib.parse.quote_plus(codes_link)
    else:
        url = "https://t.me/share/url?url=&text=" + urllib.parse.quote_plus(share_text)
    rows.append([InlineKeyboardButton(text="🔁 Ulashish", url=url), InlineKeyboardButton(text="❌ Yashirish", callback_data=f"movie:hide:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def groups_inline_kb(missing: List[Tuple[str, Optional[str]]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for cid, invite in missing:
        invite_url = make_tg_url(invite)
        if invite_url:
            rows.append([InlineKeyboardButton(text="Qo'shilish", url=invite_url)])
        else:
            rows.append([InlineKeyboardButton(text="Qo'shilish", callback_data=f"dummy:{cid}")])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ----------------- SAFE SEND -----------------
async def safe_send(user_id: int, text: str, reply_markup=None):
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup)
        return True
    except Exception as e:
        logger.warning("safe_send failed %s: %s", user_id, e)
        return False

# ----------------- CHECK SUBSCRIPTIONS -----------------
async def check_user_all(user_id: int) -> Tuple[bool, List[Tuple[str, Optional[str]]]]:
    missing: List[Tuple[str, Optional[str]]] = []
    monitored = await list_join_monitored_db()
    pendings = await list_pending_for_user_db(user_id)
    for chat_id, invite in monitored:
        found_pending = False
        for p in pendings:
            _, p_chat_id, _, _, _ = p
            if str(p_chat_id) == str(chat_id):
                found_pending = True
                break
        if found_pending:
            continue
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            status = getattr(member, "status", None)
            if status in ("member", "administrator", "creator"):
                continue
            else:
                missing.append((chat_id, invite))
        except Exception:
            missing.append((chat_id, invite))
    groups = await list_groups_db()
    for chat_id, username, title, invite in groups:
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            status = getattr(member, "status", None)
            if status in ("member", "administrator", "creator"):
                continue
            else:
                missing.append((chat_id, invite))
        except Exception:
            missing.append((chat_id, invite))
    return (len(missing) == 0), missing

# ----------------- CALLBACK HANDLERS -----------------
@dp.callback_query(lambda c: c.data and c.data.startswith("movie:hide:"))
async def cb_movie_hide(cq: CallbackQuery):
    try:
        await bot.delete_message(cq.message.chat.id, cq.message.message_id)
    except Exception:
        pass
    try:
        await cq.answer()
    except Exception:
        pass

@dp.callback_query(lambda c: c.data == "check_sub")
async def cb_check_sub(cq: CallbackQuery):
    user_id = cq.from_user.id
    ok, missing = await check_user_all(user_id)
    if ok:
        await update_user_last_validated(user_id, datetime.datetime.now(datetime.timezone.utc))
        try:
            await cq.message.edit_text("✅ Tekshiruv muvaffaqiyatli. Endi kod yuboring.", reply_markup=None)
        except Exception:
            pass
        await safe_send(user_id, "✅ Tekshiruv muvaffaqiyatli. Endi kod yuboring.")
        try:
            await cq.answer()
        except Exception:
            pass
        return
    kb = await groups_inline_kb(missing)
    try:
        await cq.message.edit_text("❌ Siz hali quyidagilarga a'zo emassiz yoki join-request yubormagansiz:", reply_markup=kb)
    except Exception:
        await safe_send(user_id, "❌ Siz hali quyidagilarga a'zo emassiz yoki join-request yubormagansiz:", reply_markup=kb)
    try:
        await cq.answer()
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith("dummy:"))
async def cb_dummy(cq: CallbackQuery):
    try:
        await cq.answer("Iltimos havola yoki guruh admini bilan bog'laning.", show_alert=True)
    except Exception:
        pass

@dp.callback_query(lambda c: c.data and c.data.startswith("play:"))
async def callback_play_episode(callback: CallbackQuery):
    data = callback.data
    parts = data.split(":")
    if len(parts) != 3:
        await callback.answer("Noto'g'ri so'rov.", show_alert=True)
        return
    _, series_code, ep_str = parts
    try:
        ep_num = int(ep_str)
    except ValueError:
        await callback.answer("Noto'g'ri epizod raqami.", show_alert=True)
        return
    ep = await get_episode_db(series_code, ep_num)
    if not ep:
        await callback.answer("❌ Epizod topilmadi.", show_alert=True)
        return
    file_id, file_type, ep_title, downloads = ep
    caption = f"{html.escape(ep_title or f'Qism {ep_num}')}\n\nKod: {series_code}-{ep_num}"
    try:
        await callback.answer()
        user_id = callback.from_user.id
        if file_type == "video":
            await bot.send_video(chat_id=user_id, video=file_id, caption=caption)
        else:
            await bot.send_document(chat_id=user_id, document=file_id, caption=caption)
        await increment_episode_downloads(series_code, ep_num)
    except Exception as e:
        logger.exception("Callback orqali epizodni jo'natishda xato: %s", e)
        try:
            await callback.answer("❌ Epizodni yuborishda xato yuz berdi.", show_alert=True)
        except Exception:
            pass

# ----------------- CHAT JOIN REQUEST HANDLER -----------------
@dp.chat_join_request()
async def on_chat_join_request(chat_join_request: ChatJoinRequest):
    try:
        chat = chat_join_request.chat
        user = chat_join_request.from_user
        monitored = False
        stored_invite = None
        try:
            if await is_join_monitored_db(str(chat.id)):
                monitored = True
                async with aiosqlite.connect(DB_FILE) as db:
                    cur = await db.execute("SELECT invite FROM join_monitored WHERE chat_id = ?", (str(chat.id),))
                    r = await cur.fetchone()
                    stored_invite = r[0] if r else None
        except Exception:
            monitored = False

        if not monitored:
            inv_link = chat_join_request.invite_link or ""
            inv_norm = normalize_invite_for_compare(inv_link)
            async with aiosqlite.connect(DB_FILE) as db:
                cur = await db.execute("SELECT chat_id, invite FROM join_monitored")
                rows = await cur.fetchall()
            for cid, inv in rows:
                if not inv:
                    continue
                try:
                    inv_norm2 = normalize_invite_for_compare(inv)
                    if inv_norm and inv_norm2 and inv_norm2 in inv_norm:
                        monitored = True
                        stored_invite = inv
                        break
                except Exception:
                    continue
        if not monitored:
            logger.info("Join-request ignored for unmonitored chat %s", getattr(chat, "id", None))
            return

        username = getattr(user, "username", None)
        full_name = getattr(user, "full_name", None)
        await add_pending_join_request_db(str(chat.id), int(user.id), username, full_name)

        admin_msg = (
            f"🔔 Join-request (monitored):\n"
            f"Chat: {chat.title or getattr(chat, 'username', None) or chat.id} (id: {chat.id})\n"
            f"User: {full_name} (id: {user.id})\n"
        )
        if username:
            admin_msg += f"Username: @{username}\nLink: https://t.me/{username}\n"
        if stored_invite:
            admin_msg += f"Invite used: {stored_invite}\n"
        admin_msg += "\nEslatma: BOT tasdiqlamaydi — adminlar kanal/guruhda qo'lda tasdiqlasin."
        await safe_send(ADMIN_ID, admin_msg)
        try:
            await safe_send(user.id, f"Siz {chat.title or 'kanal/guruh'} ga qo'shilish uchun ariza yubordingiz. Adminlar arizangizni ko'rib chiqadi.")
        except Exception:
            pass
    except Exception:
        logger.exception("join_request handler error")

# ----------------- BACKGROUND CHECK -----------------
async def background_sub_check():
    await asyncio.sleep(5)
    while True:
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                cur = await db.execute("SELECT user_id FROM users")
                rows = await cur.fetchall()
            for (uid,) in rows:
                try:
                    uid = int(uid)
                    ok, _ = await check_user_all(uid)
                    if not ok and REQUIRED_CHANNEL:
                        await safe_send(uid, f"📢 Iltimos kanalga obuna bo'ling: {REQUIRED_CHANNEL}")
                        await invalidate_user_subscription(uid)
                    else:
                        await update_user_last_validated(uid, datetime.datetime.now(datetime.timezone.utc))
                except Exception:
                    continue
        except Exception:
            logger.exception("Background subscription check failed")
        await asyncio.sleep(3600)

# ----------------- ADMIN HANDLER -----------------
@dp.message(lambda m: m.from_user is not None and m.from_user.id == ADMIN_ID)
async def admin_text_handler(message: Message):
    text = (message.text or "").strip()
    st = admin_states.get(ADMIN_ID)

    # Cancel
    if text == "Cancel":
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, "❌ Operatsiya bekor qilindi.", reply_markup=admin_main_kb())
        return

    # Agar state mavjud bo'lsa -> davom ettirish
    if st:
        action = st.get("action")
        step = st.get("step")

        # GROUP qo'shish oqimi
        if action == "add_group" and step == "wait_link":
            ident = text
            invite_norm = make_tg_url(ident) or ident
            admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_chatid", "invite": invite_norm}
            await safe_send(ADMIN_ID, f"Invite qabul qilindi: {invite_norm}\nEndi chat_id yuboring (misol: -1001234567890) yoki Cancel.", reply_markup=admin_flow_kb())
            return

        if action == "add_group" and step == "wait_chatid":
            chat_id_text = text
            st2 = admin_states.pop(ADMIN_ID, None)
            invite = st2.get("invite") if st2 else None
            try:
                chat_id_to_save = str(int(chat_id_text))
            except Exception:
                m = re.fullmatch(r"-?\d{5,}", chat_id_text)
                if m:
                    chat_id_to_save = chat_id_text
                else:
                    await safe_send(ADMIN_ID, "Chat id noto'g'ri.", reply_markup=admin_main_kb())
                    return
            try:
                try:
                    ch = await bot.get_chat(chat_id_to_save)
                except Exception:
                    ch = None
                if ch:
                    await add_group_db(str(ch.id), getattr(ch, "username", None), getattr(ch, "title", None), invite)
                    await safe_send(ADMIN_ID, f"Guruh qo'shildi: {ch.id}", reply_markup=admin_main_kb())
                else:
                    await add_group_db(chat_id_to_save, None, None, invite)
                    await safe_send(ADMIN_ID, f"Guruh saqlandi: {chat_id_to_save}", reply_markup=admin_main_kb())
            except Exception:
                logger.exception("add_group error")
                await safe_send(ADMIN_ID, "DB xatolik.", reply_markup=admin_main_kb())
            return

        if action == "remove_group" and step == "wait_chatid":
            cid = text.strip()
            admin_states.pop(ADMIN_ID, None)
            await remove_group_db(cid)
            await safe_send(ADMIN_ID, f"Guruh olib tashlandi: {cid}", reply_markup=admin_main_kb())
            return

        # JOIN monitoring qo'shish
        if action == "add_join" and step == "wait_link":
            ident = text
            invite_norm = make_tg_url(ident) or ident
            admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_chatid", "invite": invite_norm}
            await safe_send(ADMIN_ID, f"Invite qabul qilindi: {invite_norm}\nEndi chat_id yuboring:", reply_markup=admin_flow_kb())
            return

        if action == "add_join" and step == "wait_chatid":
            chat_id_text = text
            st2 = admin_states.pop(ADMIN_ID, None)
            invite = st2.get("invite") if st2 else None
            try:
                chat_id_to_save = str(int(chat_id_text))
            except Exception:
                m = re.fullmatch(r"-?\d{5,}", chat_id_text)
                if m:
                    chat_id_to_save = chat_id_text
                else:
                    await safe_send(ADMIN_ID, "Chat id noto'g'ri.", reply_markup=admin_main_kb())
                    return
            try:
                await add_join_monitored_db(chat_id_to_save, invite)
                await safe_send(ADMIN_ID, f"JoinRequest monitoring qo'shildi: {chat_id_to_save}", reply_markup=admin_main_kb())
            except Exception:
                logger.exception("add_join error")
                await safe_send(ADMIN_ID, "DB xatolik.", reply_markup=admin_main_kb())
            return

        if action == "remove_join" and step == "wait_chatid":
            cid = text.strip()
            admin_states.pop(ADMIN_ID, None)
            await remove_join_monitored_db(cid)
            await safe_send(ADMIN_ID, f"Join monitoring olib tashlandi: {cid}", reply_markup=admin_main_kb())
            return

        # SET / REMOVE share link
        if action == "set_share" and step == "wait_share":
            link = text.strip()
            await settings_set("codes_link", link)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Share link saqlandi: {link}", reply_markup=admin_main_kb())
            return

        if action == "remove_share" and step == "confirm":
            admin_states.pop(ADMIN_ID, None)
            await settings_set("codes_link", "")
            await safe_send(ADMIN_ID, "✅ Share link o'chirildi.", reply_markup=admin_main_kb())
            return

        # ======= MOVIE qo'shish (media-first) =======
        if action == "add_movie" and step == "wait_media":
            file_id = None; ftype = None
            if message.video:
                file_id = message.video.file_id; ftype = "video"
            elif message.document:
                file_id = message.document.file_id; ftype = "document"
            elif message.animation:
                file_id = message.animation.file_id; ftype = "animation"
            else:
                await safe_send(ADMIN_ID, "Iltimos video yoki fayl yuboring.", reply_markup=admin_flow_kb())
                return
            admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_title_kino", "file_id": file_id, "file_type": ftype}
            await safe_send(ADMIN_ID, "✅ Fayl qabul qilindi. Endi KINO NOMINI kiriting (majburiy):", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_title_kino":
            title = (message.text or "").strip()
            if not title:
                await safe_send(ADMIN_ID, "Nom majburiy. Iltimos kiriting:", reply_markup=admin_flow_kb())
                return
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_language_kino", "title": title})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "🌐 Endi KINO TILINI kiriting (majburiy):", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_language_kino":
            language = (message.text or "").strip()
            if not language:
                await safe_send(ADMIN_ID, "Til majburiy. Iltimos kiriting:", reply_markup=admin_flow_kb())
                return
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_genre_kino", "language": language})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "🎞 Janr kiriting yoki '-' :", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_genre_kino":
            genre = (message.text or "").strip()
            genre = None if genre in ("", "-") else genre
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_quality_kino", "genre": genre})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "📹 Sifatni kiriting (majburiy). Masalan: 720p, 1080p:", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_quality_kino":
            quality = (message.text or "").strip()
            if not quality:
                await safe_send(ADMIN_ID, "Sifat majburiy. Iltimos kiriting (masalan 720p):", reply_markup=admin_flow_kb())
                return
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_country_kino", "quality": quality})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "🏳️ Davlat kiriting yoki '-' :", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_country_kino":
            country = (message.text or "").strip()
            country = None if country in ("", "-") else country
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_year_kino", "country": country})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "📅 Yil kiriting yoki '-' :", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_year_kino":
            year = (message.text or "").strip()
            year = None if year in ("", "-") else year
            st = admin_states.get(ADMIN_ID, {})
            st.update({"step": "wait_description_kino", "year": year})
            admin_states[ADMIN_ID] = st
            await safe_send(ADMIN_ID, "✍️ Endi qisqacha tavsif yuboring yoki '-' :", reply_markup=admin_flow_kb())
            return

        if action == "add_movie" and step == "wait_description_kino":
            description = (message.text or "").strip()
            description = None if description in ("", "-") else description
            st = admin_states.pop(ADMIN_ID, None)
            if not st:
                await safe_send(ADMIN_ID, "Xatolik: holat topilmadi.", reply_markup=admin_main_kb())
                return
            file_id = st.get("file_id"); ftype = st.get("file_type")
            title = st.get("title"); language = st.get("language"); genre = st.get("genre"); quality = st.get("quality"); country = st.get("country"); year = st.get("year")
            nxt = await settings_get("next_code")
            try:
                ni = int(nxt) if nxt else 1
            except Exception:
                ni = 1
            code = str(ni)
            await settings_set("next_code", str(ni + 1))
            await add_movie_db(code, title, file_id, ftype, year, genre, quality, language, description, country)
            await safe_send(ADMIN_ID, f"✅ Kino saqlandi! Kod: {code}", reply_markup=admin_main_kb())
            return

        # ======= SERIAL qo'shish (media-first: epizodlarni to'plash) =======
        if action == "add_movie" and step == "collect_episodes":
            if text == "Tugatish ✅":
                st = admin_states.get(ADMIN_ID)
                if not st:
                    await safe_send(ADMIN_ID, "Xatolik: holat topilmadi.", reply_markup=admin_main_kb()); return
                admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_series_meta", "temp_eps": st.get("episodes", [])}
                await safe_send(ADMIN_ID, "✅ Hammasi qabul qilindi. Endi SERIAL NOMI va TILINI yuboring (misol: Nom — o'zbekcha).", reply_markup=admin_flow_kb())
                return
            file_id = None; ftype = None
            if message.video:
                file_id = message.video.file_id; ftype = "video"
            elif message.document:
                file_id = message.document.file_id; ftype = "document"
            elif message.animation:
                file_id = message.animation.file_id; ftype = "animation"
            else:
                await safe_send(ADMIN_ID, "Iltimos epizod faylini yuboring yoki 'Tugatish ✅' ni bosing.", reply_markup=collect_episodes_kb()); return
            st = admin_states.get(ADMIN_ID, {})
            eps = st.get("episodes", [])
            eps.append((file_id, ftype))
            st["episodes"] = eps
            admin_states[ADMIN_ID] = st
            epnum = len(eps)
            await safe_send(ADMIN_ID, f"✅ {epnum}-qism saqlandi. Yana fayl yuboring yoki 'Tugatish ✅'ni bosing.", reply_markup=collect_episodes_kb())
            return

        if action == "add_movie" and step == "wait_series_meta":
            meta = (message.text or "").strip()
            if not meta:
                await safe_send(ADMIN_ID, "Iltimos serial nomi va tilini yuboring (Nom — til).", reply_markup=admin_flow_kb()); return
            st = admin_states.pop(ADMIN_ID, None)
            if not st:
                await safe_send(ADMIN_ID, "Xatolik: holat topilmadi.", reply_markup=admin_main_kb()); return
            title = meta; language = None
            if "—" in meta:
                parts = [p.strip() for p in meta.split("—", 1)]
                title = parts[0]; language = parts[1] if len(parts) > 1 else None
            elif "-" in meta:
                parts = [p.strip() for p in meta.split("-", 1)]
                title = parts[0]; language = parts[1] if len(parts) > 1 else None
            series_code = await settings_get("next_series_code")
            try:
                scn = int(series_code) if series_code else 1000
            except Exception:
                scn = 1000
            series_code = str(scn)
            await settings_set("next_series_code", str(scn + 1))
            await add_series_db(series_code, title, language, None)
            eps = st.get("temp_eps", []) or st.get("episodes", []) or []
            if not eps:
                await safe_send(ADMIN_ID, "Hech epizod topilmadi.", reply_markup=admin_main_kb()); return
            for idx, (fid, ftype) in enumerate(eps, start=1):
                await add_episode_db(series_code, idx, fid, ftype, None)
            await safe_send(ADMIN_ID, f"🎉 Serial saqlandi: {title} (kod: {series_code}). Jami {len(eps)} qism qo'shildi.", reply_markup=admin_main_kb())
            return

        # ======= O'chirish (movie/series/episode) =======
        if action == "remove_movie" and step == "wait_code":
            code = text.strip()
            admin_states.pop(ADMIN_ID, None)
            m = re.fullmatch(r"(\d+)-(\d+)", code)
            if m:
                sc = m.group(1); epn = int(m.group(2))
                async with aiosqlite.connect(DB_FILE) as db:
                    await db.execute("DELETE FROM episodes WHERE series_code = ? AND episode_number = ?", (sc, epn))
                    await db.commit()
                await safe_send(ADMIN_ID, f"🗑️ Episode {code} o'chirildi.", reply_markup=admin_main_kb())
                return
            if re.fullmatch(r"\d+", code):
                mv = await get_movie_db(code)
                if mv:
                    ok = await remove_movie_db(code)
                    if ok:
                        await safe_send(ADMIN_ID, f"🗑️ Kino {code} o'chirildi.", reply_markup=admin_main_kb())
                    else:
                        await safe_send(ADMIN_ID, "❌ Kino o'chirilmadi.", reply_markup=admin_main_kb())
                    return
                meta = await get_series_meta(code)
                if meta:
                    ok = await remove_series_db(code)
                    if ok:
                        await safe_send(ADMIN_ID, f"🗑️ Serial va uning barcha qismlari o'chirildi (kod: {code}).", reply_markup=admin_main_kb())
                    else:
                        await safe_send(ADMIN_ID, "❌ Serialni o'chirishda muammo.", reply_markup=admin_main_kb())
                    return
                await safe_send(ADMIN_ID, "❌ Bunday kod topilmadi.", reply_markup=admin_main_kb())
                return

    # ---------- NO ACTIVE STATE: ADMIN MAIN MENU ----------
    if text == "Kino qo'shish 🎬":
        admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_media"}
        await safe_send(ADMIN_ID, "📤 Iltimos VIDEO yoki fayl yuboring (kino uchun):", reply_markup=admin_flow_kb())
        return

    if text == "Serial qo'shish 📺":
        admin_states[ADMIN_ID] = {"action": "add_movie", "step": "collect_episodes", "episodes": []}
        await safe_send(ADMIN_ID, "📤 Serial qo'shish: Endi EPIZOD fayllarini yuboring. Hammasini yuborgach 'Tugatish ✅' ni bosing. (Keyin nom/til so'raladi)", reply_markup=collect_episodes_kb())
        return

    if text == "Guruh qo'shish ➕":
        admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_link"}
        await safe_send(ADMIN_ID, "Guruh ssilkasi yoki @username yoki invite yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "Guruh o'chirish ➖":
        admin_states[ADMIN_ID] = {"action": "remove_group", "step": "wait_chatid"}
        await safe_send(ADMIN_ID, "O'chirish uchun chat_id yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "JoinRequest qo'shish":
        admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_link"}
        await safe_send(ADMIN_ID, "JoinRequest invite yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "JoinRequest o'chirish":
        admin_states[ADMIN_ID] = {"action": "remove_join", "step": "wait_chatid"}
        await safe_send(ADMIN_ID, "JoinRequest monitoringni olib tashlash uchun chat_id yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "List Groups":
        groups = await list_groups_db()
        lines = [f"- {c} ({u or t or 'no title'}) invite:{inv or '-'}" for c, u, t, inv in groups]
        await safe_send(ADMIN_ID, "Guruhlar:\n" + ("\n".join(lines) if lines else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return

    if text == "List Monitored":
        rows = await list_join_monitored_db()
        lines = [f"- {c} invite:{inv or '-'}" for c, inv in rows]
        await safe_send(ADMIN_ID, "Monitored join requests:\n" + ("\n".join(lines) if rows else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return

    if text == "Set Share Link":
        admin_states[ADMIN_ID] = {"action": "set_share", "step": "wait_share"}
        await safe_send(ADMIN_ID, "Share linkni yuboring (misol: https://t.me/yourchannel):", reply_markup=admin_flow_kb())
        return

    if text == "Remove Share Link":
        admin_states[ADMIN_ID] = {"action": "remove_share", "step": "confirm"}
        await safe_send(ADMIN_ID, "Share linkni o'chirishni tasdiqlang (Cancel bilan bekor qilish mumkin).", reply_markup=admin_flow_kb())
        return

    if text == "Foydalanuvchilar":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 200")
            rows = await cur.fetchall()
            cur2 = await db.execute("SELECT COUNT(*) FROM users")
            total = (await cur2.fetchone())[0]
        users = [str(r[0]) for r in rows]
        await safe_send(ADMIN_ID, f"Foydalanuvchilar soni: {total}\nBirinchi {len(users)} ID:\n" + ("\n".join(users) if users else "Hech narsa topilmadi."), reply_markup=admin_main_kb())
        return

    await safe_send(ADMIN_ID, "🔧 Admin: menyudan buyruq tanlang.", reply_markup=admin_main_kb())

# ----------------- PUBLIC (USER) HANDLERS -----------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await add_user_db(message.from_user.id)
    ok, missing = await check_user_all(message.from_user.id)
    if ok:
        await update_user_last_validated(message.from_user.id, datetime.datetime.now(datetime.timezone.utc))
        await safe_send(message.from_user.id, "👋 Assalomu alaykum! Tekshiruv muvaffaqiyatli. Kodni yuboring (misol: 100 yoki 1000-1).")
    else:
        await invalidate_user_subscription(message.from_user.id)
        if REQUIRED_CHANNEL:
            await safe_send(message.from_user.id, f"👋 Assalomu alaykum! Iltimos quyidagi kanal/guruhlarga qo'shiling: {REQUIRED_CHANNEL}")
        else:
            kb = await groups_inline_kb(missing)
            await safe_send(message.from_user.id, "👋 Assalomu alaykum! Kodni olish uchun avvalo quyidagilarga a'zo bo'ling yoki join-request yuboring:", reply_markup=kb)
    if message.from_user.id == ADMIN_ID:
        await safe_send(ADMIN_ID, "🔧 Admin panelga xush kelibsiz.", reply_markup=admin_main_kb())

@dp.message(Command("help"))
async def cmd_help(message: Message):
    txt = ("ℹ️ Yordam:\n"
           "/start — bosh sahifa va tekshiruv\n"
           "/help — yordam\n"
           "Kodni yuboring: 123 yoki 1000-1 (serial epizod)\n")
    await safe_send(message.from_user.id, txt)

@dp.message(Command("settings"))
async def cmd_settings(message: Message):
    await safe_send(message.from_user.id, "⚙️ Sozlamalar: hozircha minimal.")

@dp.message(lambda m: m.from_user is not None and m.from_user.id != ADMIN_ID)
async def user_message_handler(message: Message):
    await add_user_db(message.from_user.id)
    txt = (message.text or "").strip()
    # serial ep: 1234-1
    m = re.fullmatch(r"(\d+)-(\d+)", txt)
    if m:
        sc, epn = m.group(1), int(m.group(2))
        subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
        now = datetime.datetime.now(datetime.timezone.utc)
        need_validation = True
        if subscribed and last_validated_at:
            elapsed = (now - last_validated_at).total_seconds()
            if elapsed < VALIDATION_TTL:
                need_validation = False
        if need_validation:
            ok, missing = await check_user_all(message.from_user.id)
            if not ok:
                kb = await groups_inline_kb(missing)
                await safe_send(message.from_user.id, "Kodni olish uchun avvalo quyidagilarga a'zo bo'ling yoki join-request yuboring:", reply_markup=kb)
                await invalidate_user_subscription(message.from_user.id)
                return
            await update_user_last_validated(message.from_user.id, now)
        ep = await get_episode_db(sc, epn)
        if not ep:
            await safe_send(message.from_user.id, "❌ Bunday epizod topilmadi.")
            return
        file_id, file_type, ep_title, downloads = ep
        meta = await get_series_meta(sc)
        title = meta[0] if meta else "Serial"
        caption = f"{html.escape(title)}\n\n{html.escape(ep_title or '')}\n\nKod: {txt}"
        kb = await build_movie_kb(txt, title or "Serial")
        try:
            if file_type == "video":
                await bot.send_video(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            else:
                await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
        except Exception:
            logger.exception("Failed to send episode media")
            await safe_send(message.from_user.id, "❌ Media jo'natishda xatolik yuz berdi.")
            return
        await increment_episode_downloads(sc, epn)
        return

    # single number: movie yoki series listing
    m2 = re.fullmatch(r"(\d+)", txt)
    if m2:
        code = m2.group(1)
        mv = await get_movie_db(code)
        if mv:
            subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
            now = datetime.datetime.now(datetime.timezone.utc)
            need_validation = True
            if subscribed and last_validated_at:
                elapsed = (now - last_validated_at).total_seconds()
                if elapsed < VALIDATION_TTL:
                    need_validation = False
            if need_validation:
                ok, missing = await check_user_all(message.from_user.id)
                if not ok:
                    kb = await groups_inline_kb(missing)
                    await safe_send(message.from_user.id, "Kodni olish uchun avvalo quyidagilarga a'zo bo'ling yoki join-request yuboring:", reply_markup=kb)
                    await invalidate_user_subscription(message.from_user.id)
                    return
                await update_user_last_validated(message.from_user.id, datetime.datetime.now(datetime.timezone.utc))
            title, file_id, file_type, year, genre, quality, language, desc, country, downloads = mv
            caption_parts = []
            caption_parts.append(f"🎥 Nomi: {title or 'Film'}")
            caption_parts.append(f"📹 Sifati: {quality or '-'}")
            caption_parts.append(f"🎞 Janr: {genre or '-'}")
            caption_parts.append(f"🌐 Davlat: {country or '-'}")
            caption_parts.append(f"🇺🇿 Tarjima: {language or '-'}")
            caption_parts.append("")
            caption_parts.append(f"Bizning sahifa👉 {REQUIRED_CHANNEL or (await settings_get('codes_link') or 'https://t.me/your_channel')}")
            caption = "\n".join(caption_parts)
            kb = await build_movie_kb(code, title or "Film")
            try:
                if file_type == "video":
                    await bot.send_video(message.from_user.id, file_id, caption=caption, reply_markup=kb)
                else:
                    await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            except Exception:
                logger.exception("Failed to send movie media")
                await safe_send(message.from_user.id, "❌ Media jo'natishda xatolik yuz berdi.")
                return
            await increment_movie_downloads(code)
            return
        meta = await get_series_meta(code)
        if not meta:
            await safe_send(message.from_user.id, "❌ Bunday kod topilmadi.")
            return
        ok, missing = await check_user_all(message.from_user.id)
        if not ok:
            kb = await groups_inline_kb(missing)
            await safe_send(message.from_user.id, "Kodni ko'rishdan oldin quyidagilarga a'zo bo'ling:", reply_markup=kb)
            await invalidate_user_subscription(message.from_user.id)
            return
        await update_user_last_validated(message.from_user.id, datetime.datetime.now(datetime.timezone.utc))
        title, language, desc = meta
        eps = await get_series_episodes(code)
        if not eps:
            await safe_send(message.from_user.id, "❌ Bu serialda epizodlar mavjud emas.")
            return

        # Birinchi yuborilgan epizodni preview sifatida yuboramiz
        first_ep = eps[0]
        first_ep_num, first_file_id, first_file_type, first_ep_title, _ = first_ep
        preview_caption = (
            f"📽️ {title}\n"
            f"🇺🇿 {language or '-'}\n"
            f"📚 Jami: {len(eps)} ta qism\n"
            f"Kod: {code}"
        )
        try:
            if first_file_type == "video":
                await bot.send_video(message.from_user.id, first_file_id, caption=preview_caption)
            else:
                await bot.send_document(message.from_user.id, first_file_id, caption=preview_caption)
        except Exception:
            logger.exception("Preview epizodni yuborishda xato")

        # Inline tugmalar — "1-qism", "2-qism", ...
        kb = build_episodes_inline_kb(code, eps)
        try:
            await bot.send_message(message.from_user.id, "Epizodlardan birini tanlang:", reply_markup=kb)
        except Exception:
            await safe_send(message.from_user.id, "Epizodlar ro'yxati:", reply_markup=kb)
        return

    await safe_send(message.from_user.id, "ℹ️ Iltimos kino yoki serial kodini yuboring (misol: 123 yoki 1000-1).")

# ----------------- STARTUP / RUNNER -----------------
async def main():
    await init_db()
    if not await settings_get("next_code"):
        await settings_set("next_code", "100")
    if not await settings_get("next_series_code"):
        await settings_set("next_series_code", "1000")
    asyncio.create_task(background_sub_check())
    logger.info("Bot ishga tushmoqda (VALIDATION_TTL=%s seconds)...", VALIDATION_TTL)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot to'xtatildi.")
    except Exception:
        logger.exception("Kutilmagan xatolik yuz berdi.")
