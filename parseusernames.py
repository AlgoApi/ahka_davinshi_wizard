import os
import json
import asyncio
import re
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest, GetHistoryRequest
from telethon.tl.types import InputPeerEmpty
import socks


def load_sessions(folder):
    sessions = []
    for file in os.listdir(folder):
        if file.endswith(".json"):
            with open(os.path.join(folder, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                if "session" in data and "proxy" in data:
                    sessions.append(data)
    return sessions


async def scrape_users(session_data, output_file):
    session_name = session_data["session"].replace(".session", "")
    proxy = session_data["proxy"]
    api_id, api_hash = session_data["api_id"], session_data["api_hash"]
    proxy_settings = (proxy["host"], proxy["port"], True, proxy["username"], proxy["password"])

    client = TelegramClient(session_name, api_id, api_hash, proxy=(socks.SOCKS5, *proxy_settings))
    await client.connect()

    if not await client.is_user_authorized():
        print(f"Session {session_name} not authorized. Skipping...")
        return

    dialogs = await client(
        GetDialogsRequest(offset_date=None, offset_id=0, offset_peer=InputPeerEmpty(), limit=200, hash=0))
    users = set()

    for dialog in dialogs.chats:
        if dialog.id > 0:  # Личный чат
            user = await client.get_entity(dialog.id)
            if user.username:
                users.add(f"https://t.me/{user.username}")
            else:
                users.add(f"https://t.me/+{user.id}")

    # Парсинг сообщений в избранном
    me = await client.get_me()
    messages = await client(
        GetHistoryRequest(peer=me, limit=100, offset_date=None, offset_id=0, max_id=0, min_id=0, add_offset=0, hash=0))

    for message in messages.messages:
        if message.message:
            links = re.findall(r"t\.me/([a-zA-Z0-9_]+)", message.message)
            users.update(f"https://t.me/{link}" for link in links)

    await client.disconnect()

    with open(output_file, "a", encoding="utf-8") as f:
        for user in users:
            f.write(user + "\n")


async def main():
    session_folder = r"C:\Users\AlgoApi\Desktop\GIGATEST\CONCURENT\autolike\sessions"
    output_file = r"C:\Users\AlgoApi\Desktop\GIGATEST\CONCURENT\autolike\telegram_users.txt"


    sessions = load_sessions(session_folder)
    tasks = [scrape_users(session, output_file) for session in sessions]
    await asyncio.gather(*tasks)
    print("Парсинг завершен. Результаты в telegram_users.txt")


if __name__ == "__main__":
    asyncio.run(main())
