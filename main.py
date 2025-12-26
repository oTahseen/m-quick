import asyncio
import aiohttp
import random
import os
import uuid
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
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is required")

user_tokens = {}
matching_tasks = {}
user_stats = {}
task_meta = {}

mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo["meeff_db"]
config = db["config"]

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
                            return False
                        if res.status == 401 or "AuthRequired" in text:
                            stop_reason = "TOKEN EXPIRED"
                            return False
                        return True
                except:
                    stats["errors"] += 1
                    return True

            while task_meta.get(task_id) and task_meta[task_id].get("running", True):
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
    data = await config.find_one({"_id": "explore_url"})
    if not data:
        return await message.answer("Use /seturl first.")
    explore_url = data["url"]
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
    await config.update_one({"_id": "explore_url"}, {"$set": {"url": url}}, upsert=True)
    await message.answer("✔️ URL saved.")


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

    data = await config.find_one({"_id": "explore_url"})
    if not data:
        return await message.answer("Use /seturl first.")
    explore_url = data["url"]

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
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
