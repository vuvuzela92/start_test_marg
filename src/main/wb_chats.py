import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import json
import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, Any

from utils.utils import load_api_tokens
from utils.logger import setup_logger
from utils.my_db_functions import create_connection_w_env

logger = setup_logger('wb_chats.log')

SEM = asyncio.Semaphore(3)

async def fetch_events_page(
    session: aiohttp.ClientSession,
    token: str,
    next_timestamp: int
) -> Dict[str, Any]:
    """Выполняет один запрос к API Wildberries и возвращает события."""
    url = f"https://buyer-chat-api.wildberries.ru/api/v1/seller/events?next={next_timestamp}"
    headers = {"Authorization": token, "Content-Type": "application/json"}

    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
    except aiohttp.ClientError as e:
        print(f"Ошибка HTTP-запроса (next={next_timestamp}): {e}")
        return {"events": [], "next_ts": None, "total": 0}

    result = data.get("result", {})
    events = result.get("events", [])
    next_ts = result.get("next")
    total = result.get("totalEvents", 0)

    oldest_time = result.get("oldestEventTime")

    if oldest_time:
        try:
            oldest_date = datetime.fromisoformat(oldest_time).date()
        except ValueError:
            oldest_date = None
    else:
        oldest_date = None

    print(f"Получено событий: {total}, next={next_ts}, oldest date: {oldest_date}")
    return {
        "events": events,
        "next_ts": next_ts,
        "total": total
    }


def insert_events(conn, events, client):
    """Insert a list of events into the wb_chats table with client info."""
    if not events:
        return
    with conn.cursor() as cur:
        for e in events:
            message = e.get("message")
            attachments = message.get("attachments") if message else None
            cur.execute(
                """
                INSERT INTO wb_chats (
                    chat_id, event_id, event_type, is_new_chat, add_timestamp, add_time,
                    sender, client_id, client_name, message, attachments, client, created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    e.get("chatID"),
                    e.get("eventID"),
                    e.get("eventType"),
                    e.get("isNewChat"),
                    e.get("addTimestamp"),
                    datetime.fromisoformat(e.get("addTime").replace("Z", "+00:00")) if e.get("addTime") else None,
                    e.get("sender"),
                    e.get("clientID"),
                    e.get("clientName"),
                    json.dumps(message) if message else None,
                    json.dumps(attachments) if attachments else None,
                    client
                )
            )
        conn.commit()
    logger.info(f"Inserted {len(events)} events into wb_chats for client: {client}")


async def fetch_all_for_client(session, conn, acc_name, token):
    async with SEM:
        logger.info(f"Starting fetch for client: {acc_name}")
        next_timestamp = 0

        while next_timestamp is not None:
            data = await fetch_events_page(session, token, next_timestamp)

            if data["total"] == 0:
                logger.info(f"No events for client {acc_name}. Stopping.")
                break

            insert_events(conn, data["events"], acc_name)
            next_timestamp = data["next_ts"]

            await asyncio.sleep(1)  # per-client rate limit

        logger.info(f"{acc_name}: finished fetching")


async def test_call_one_client():
    tokens = load_api_tokens()
    # Take the first client only
    acc_name, token = next(iter(tokens.items()))
    print(f"Testing fetch for client: {acc_name}")

    async with aiohttp.ClientSession() as session:
        next_timestamp = 0  # start from beginning
        data = await fetch_events_page(session, token, next_timestamp)

    print(f"Data for {acc_name}:")
    print(data)


async def upload_all_data():
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_all_for_client(session, conn, acc_name, token)
            for acc_name, token in tokens.items()
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(upload_all_data())
    except KeyboardInterrupt:
        print("Script stopped by user.")