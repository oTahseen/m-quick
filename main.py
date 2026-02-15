import asyncio
import aiohttp
import random
import os
import uuid
import sqlite3
from datetime import datetime
from pathlib import Path
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

SQLITE_PATH = os.environ.get("SQLITE_PATH", "mquick.db")

user_tokens = {}
matching_tasks = {}
user_stats = {}
task_meta = {}

sql_db = None

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

HEADERS_TEMPLATE = {
    "User-Agent": "okhttp/5.1.0 (Linux; Android 13; Pixel 6 Build/TQ3A.230901.001)",
    "Accept-Encoding": "gzip",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "api.meeff.com",
}

ANSWER_URL = "https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"

async def init_db():
    global sql_db
    sql_db = await aiosqlite.connect(SQLITE_PATH, timeout=30)
    await sql_db.execute("PRAGMA journal_mode=WAL;")
    await sql_db.execute("PRAGMA synchronous=NORMAL;")
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS exclude (
            chat_id INTEGER,
            country TEXT,
            PRIMARY KEY(chat_id, country)
        );
        """
    )
    await sql_db.execute(
        """
        CREATE TABLE IF NOT EXISTS history (
            user_id TEXT PRIMARY KEY,
            first_added_at TEXT,
            added_by TEXT,
            reserved INTEGER DEFAULT 0
        );
        """
    )
    await sql_db.commit()

async def get_config_value(key):
    async with sql_db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None

async def set_config_value(key, value):
    await sql_db.execute(
        "INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    await sql_db.commit()

async def list_excluded_countries(chat_id):
    async with sql_db.execute("SELECT country FROM exclude WHERE chat_id = ?", (chat_id,)) as cur:
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def add_excluded_countries(chat_id, countries):
    async with sql_db.execute("BEGIN"):
        for c in countries:
            await sql_db.execute(
                "INSERT OR IGNORE INTO exclude(chat_id, country) VALUES(?, ?)",
                (chat_id, c),
            )
    await sql_db.commit()

async def reserve_user(user_id, chat_id):
    now = datetime.utcnow().isoformat()
    try:
        await sql_db.execute(
            "INSERT INTO history(user_id, first_added_at, added_by, reserved) VALUES(?, ?, ?, 1)",
            (user_id, now, f",{chat_id},"),
        )
        await sql_db.commit()
        return True
    except sqlite3.IntegrityError:
        return False

async def mark_user_added(user_id, chat_id):
    async with sql_db.execute("SELECT added_by FROM history WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
        if not row:
            now = datetime.utcnow().isoformat()
            await sql_db.execute(
                "INSERT OR REPLACE INTO history(user_id, first_added_at, added_by, reserved) VALUES(?, ?, ?, 0)",
                (user_id, now, f",{chat_id},"),
            )
            await sql_db.commit()
            return
        added_by = row[0] or ""
        token = f",{chat_id},"
        if token not in added_by:
            new_added_by = added_by + str(chat_id) + "," if added_by else token
            await sql_db.execute(
                "UPDATE history SET added_by = ?, reserved = 0 WHERE user_id = ?",
                (new_added_by, user_id),
            )
        else:
            await sql_db.execute("UPDATE history SET reserved = 0 WHERE user_id = ?", (user_id,))
        await sql_db.commit()

async def unreserve_user_on_failure(user_id):
    await sql_db.execute("DELETE FROM history WHERE user_id = ? AND reserved = 1", (user_id,))
    await sql_db.commit()

async def history_for_chat(chat_id, limit=20):
    token = f",{chat_id},"
    async with sql_db.execute(
        "SELECT user_id, first_added_at FROM history WHERE added_by LIKE ? ORDER BY first_added_at DESC LIMIT ?",
        (f"%{token}%", limit),
    ) as cur:
        rows = await cur.fetchall()
        return rows

async def history_count_for_chat(chat_id):
    token = f",{chat_id},"
    async with sql_db.execute("SELECT COUNT(*) FROM history WHERE added_by LIKE ?", (f"%{token}%",)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 0

async def clear_history_for_chat(chat_id):
    token = f",{chat_id},"
    await sql_db.execute(
        "UPDATE history SET added_by = REPLACE(added_by, ?, '') WHERE added_by LIKE ?",
        (token, f"%{token}%"),
    )
    await sql_db.execute("DELETE FROM history WHERE added_by IS NULL OR added_by = ''")
    await sql_db.commit()

async def fetch_users(session, explore_url):
    async with session.get(explore_url) as res:
        status = res.status
        text = await res.text()
        if status != 200:
            return status, text, None
        try:
            data = await res.json(content_type=None)
        except:
            return status, text, None
        return status, text, data

async def start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard):
    key = f"{chat_id}:{token}"
    headers = HEADERS_TEMPLATE.copy()
    headers["meeff-access-token"] = token
    stats = {"requests": 0, "cycles": 0, "errors": 0}
    user_stats[key] = stats
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(ssl=False, limit_per_host=10)
    empty_count = 0
    stop_reason = None
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
            async def answer_user(user_id):
                nonlocal stop_reason
                try:
                    async with session.get(ANSWER_URL.format(user_id=user_id)) as res:
                        text = await res.text()
                        if res.status == 429 or "LikeExceeded" in text:
                            stop_reason = "LIMIT EXCEEDED"
                            await unreserve_user_on_failure(user_id)
                            return False
                        if res.status == 401 or "AuthRequired" in text:
                            stop_reason = "TOKEN EXPIRED"
                            await unreserve_user_on_failure(user_id)
                            return False
                        if res.status == 200:
                            await mark_user_added(user_id, chat_id)
                        else:
                            await unreserve_user_on_failure(user_id)
                        return True
                except Exception:
                    stats["errors"] += 1
                    try:
                        await unreserve_user_on_failure(user_id)
                    except:
                        pass
                    return True

            while task_meta.get(task_id) and task_meta[task_id].get("running", True):
                try:
                    excluded_countries = set([c.upper() for c in await list_excluded_countries(chat_id)])
                except Exception:
                    excluded_countries = set()
                status, raw_text, data = await fetch_users(session, explore_url)
                if status == 401 or "AuthRequired" in str(raw_text):
                    stop_reason = "TOKEN EXPIRED"
                    break
                if data is None or not data.get("users"):
                    empty_count += 1
                    if empty_count >= 6:
                        stop_reason = "NO USERS FOUND"
                        break
                    await asyncio.sleep(1)
                    continue
                empty_count = 0
                users = data.get("users", [])
                tasks = []
                results = []
                for user in users:
                    user_id = user.get("_id")
                    if not user_id:
                        continue
                    nat = user.get("nationalityCode") or user.get("locale")
                    if nat:
                        nat_code = nat.upper()
                        if "-" in nat_code:
                            nat_code = nat_code.split("-")[-1]
                        if nat_code in excluded_countries:
                            continue
                    reserved = await reserve_user(user_id, chat_id)
                    if not reserved:
                        continue
                    task = asyncio.create_task(answer_user(user_id))
                    tasks.append(task)
                    stats["requests"] += 1
                    await asyncio.sleep(random.uniform(0.05, 0.2))
                    if len(tasks) >= 10:
                        batch_results = await asyncio.gather(*tasks)
                        results.extend(batch_results)
                        tasks.clear()
                        if False in batch_results:
                            break
                if tasks:
                    batch_results = await asyncio.gather(*tasks)
                    results.extend(batch_results)
                if False in results:
                    break
                stats["cycles"] += 1
                final_text = (
                    f"Live Stats:\n"
                    f"Requests: {stats['requests']}\n"
                    f"Cycles: {stats['cycles']}\n"
                    f"Errors: {stats['errors']}"
                )
                if stop_reason:
                    final_text += f"\n\n⚠️ {stop_reason}"
                try:
                    await stat_msg.edit_text(final_text, reply_markup=keyboard)
                except:
                    pass
                await asyncio.sleep(random.uniform(1, 2))
    except asyncio.CancelledError:
        try:
            await stat_msg.edit_text(
                f"Stopped.\n\nRequests: {stats['requests']}\nCycles: {stats['cycles']}\nErrors: {stats['errors']}"
            )
        except:
            pass
        raise
    except Exception as e:
        try:
            await stat_msg.edit_text(f"Error: {e}", reply_markup=keyboard)
        except:
            pass
    if stop_reason:
        try:
            await stat_msg.edit_text(
                f"Live Stats:\n"
                f"Requests: {stats['requests']}\n"
                f"Cycles: {stats['cycles']}\n"
                f"Errors: {stats['errors']}\n\n"
                f"⚠️ {stop_reason}"
            )
        except:
            pass
    matching_tasks.pop(key, None)
    user_stats.pop(key, None)
    task_meta.pop(task_id, None)
    lst = user_tokens.get(chat_id, [])
    try:
        if token in lst:
            lst.remove(token)
            if lst:
                user_tokens[chat_id] = lst
            else:
                user_tokens.pop(chat_id, None)
    except Exception:
        pass

@dp.callback_query(F.data.startswith("stop_task:"))
async def _stop_task(callback: CallbackQuery):
    task_id = callback.data.split(":", 1)[1]
    meta = task_meta.get(task_id)
    if not meta:
        await callback.answer("Already stopped.", show_alert=False)
        return
    meta["running"] = False
    key = meta["key"]
    t = matching_tasks.pop(key, None)
    if t:
        t.cancel()
    try:
        await meta["stat_msg"].edit_text("Stopping...")
    except:
        pass
    await callback.answer("Stopping task.", show_alert=False)

@dp.message(F.text == "meeff")
async def meeff_auto(message):
    chat_id = message.chat.id
    tokens = user_tokens.get(chat_id)
    if not tokens:
        return await message.answer("Send token first.")
    explore_url = await get_config_value("explore_url")
    if not explore_url:
        return await message.answer("Use /seturl first.")
    for token in list(tokens):
        key = f"{chat_id}:{token}"
        if key in matching_tasks:
            continue
        task_id = uuid.uuid4().hex
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Stop", callback_data=f"stop_task:{task_id}")]
        ])
        stat_msg = await bot.send_message(
            chat_id,
            "Live Stats:\nRequests: 0\nCycles: 0\nErrors: 0",
            reply_markup=keyboard,
        )
        task = asyncio.create_task(start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard))
        matching_tasks[key] = task
        task_meta[task_id] = {"key": key, "stat_msg": stat_msg, "running": True}

@dp.message(Command("start"))
async def start(message):
    await message.answer("Send Meeff Token.")

@dp.message(Command("seturl"))
async def set_url(message):
    url = message.text.replace("/seturl", "").strip()
    if not url.startswith("https://"):
        return await message.answer("Invalid URL.")
    await set_config_value("explore_url", url)
    await message.answer("✔️ URL saved.")

@dp.message(F.text.startswith("ex"))
async def exclude_countries(message):
    text = message.text.strip()
    chat_id = message.chat.id
    parts = text.split()
    if len(parts) == 1:
        countries = await list_excluded_countries(chat_id)
        if not countries:
            await message.answer("No excluded countries set.")
        else:
            await message.answer("Excluded countries: " + ", ".join(countries))
        return
    codes = [p.upper() for p in parts[1:] if p.strip()]
    if not codes:
        await message.answer("No country codes provided.")
        return
    await add_excluded_countries(chat_id, codes)
    await message.answer("Added to exclude: " + ", ".join(codes))

@dp.message(F.text.startswith("history"))
async def history_cmd(message):
    text = message.text.strip()
    chat_id = message.chat.id
    if text == "history":
        try:
            items = await history_for_chat(chat_id, limit=20)
            total = await history_count_for_chat(chat_id)
            if total == 0:
                await message.answer("No history for this chat.")
                return
            ids = [item[0] for item in items]
            resp = f"History count: {total}\nLast {len(ids)} ids:\n" + "\n".join(ids)
            await message.answer(resp)
        except Exception as e:
            await message.answer(f"Error fetching history: {e}")
        return
    if text == "history clear":
        try:
            await clear_history_for_chat(chat_id)
            await message.answer("History cleared for this chat.")
        except Exception as e:
            await message.answer(f"Error clearing history: {e}")
        return

@dp.message(F.text)
async def receive_token(message):
    if not message.text:
        return
    if message.text.startswith("/"):
        return
    chat_id = message.chat.id
    token = message.text.strip()
    lst = user_tokens.get(chat_id, [])
    if token not in lst:
        lst.append(token)
        user_tokens[chat_id] = lst
    explore_url = await get_config_value("explore_url")
    if not explore_url:
        return await message.answer("Use /seturl first.")
    key = f"{chat_id}:{token}"
    if key in matching_tasks:
        return
    task_id = uuid.uuid4().hex
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Stop", callback_data=f"stop_task:{task_id}")]
    ])
    stat_msg = await bot.send_message(
        chat_id,
        "Live Stats:\nRequests: 0\nCycles: 0\nErrors: 0",
        reply_markup=keyboard,
    )
    task = asyncio.create_task(start_matching(chat_id, token, explore_url, stat_msg, task_id, keyboard))
    matching_tasks[key] = task
    task_meta[task_id] = {"key": key, "stat_msg": stat_msg, "running": True}

async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
