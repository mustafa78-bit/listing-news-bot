import asyncio
import logging
import os
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import psycopg2
import psycopg2.extras
from telethon import TelegramClient
from telethon.sessions import StringSession


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("spot-listing-bot")


SOURCE_CHANNELS = [
    item.strip()
    for item in os.getenv(
        "SOURCE_CHANNELS",
        ",".join(
            [
                "CoinMarketCapAnnouncements",
                "CoinDeskGlobal",
                "coinlistofficialchannel",
                "OKXAnnouncements",
                "dwflabs",
                "crypto_fundraising",
                "Bloomberg",
                "the_block_crypto",
                "binance_announcements",
            ]
        ),
    ).split(",")
    if item.strip()
]

ERROR_RETRY_SECONDS = int(os.getenv("ERROR_RETRY_SECONDS", "20"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))
BACKFILL_LIMIT = int(os.getenv("BACKFILL_LIMIT", "50"))

SPOT_TERMS = [
    "spot trading",
    "spot trading pair",
    "spot trading pairs",
    "open trading for",
    "opens trading for",
    "will list",
    "lists",
    "listed on",
    "adds",
    "new listing",
    "spot listing",
]

BLOCK_TERMS = [
    "futures",
    "perpetual",
    "perp",
    "pre-market",
    "premarket",
    "margin",
    "loan",
    "earn",
    "staking",
    "launchpool",
    "launchpad",
    "airdrop",
    "campaign",
    "competition",
    "options",
    "binary options",
    "convert",
    "vip loan",
    "delist",
    "delisting",
]


@dataclass
class ListingHit:
    source: str
    coin: str
    pair: str
    message_url: str
    raw_text: str


class StateStore:
    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL", "").strip()
        self.sqlite_path = os.getenv("SQLITE_PATH", "listing_bot.db")
        self.is_postgres = self.database_url.startswith(("postgres://", "postgresql://"))
        self._init_db()

    def _connect(self):
        if self.is_postgres:
            return psycopg2.connect(self.database_url)
        return sqlite3.connect(self.sqlite_path)

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            cur = conn.cursor()
            if self.is_postgres:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS channel_state (
                        channel TEXT PRIMARY KEY,
                        last_message_id BIGINT NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sent_alerts (
                        source_channel TEXT NOT NULL,
                        message_id BIGINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (source_channel, message_id)
                    )
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS channel_state (
                        channel TEXT PRIMARY KEY,
                        last_message_id INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sent_alerts (
                        source_channel TEXT NOT NULL,
                        message_id INTEGER NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (source_channel, message_id)
                    )
                    """
                )
            conn.commit()

    def get_last_id(self, channel: str) -> int:
        with closing(self._connect()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT last_message_id FROM channel_state WHERE channel = %s" if self.is_postgres else "SELECT last_message_id FROM channel_state WHERE channel = ?", (channel,))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def set_last_id(self, channel: str, message_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._connect()) as conn:
            cur = conn.cursor()
            if self.is_postgres:
                cur.execute(
                    """
                    INSERT INTO channel_state(channel, last_message_id, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (channel)
                    DO UPDATE SET last_message_id = EXCLUDED.last_message_id, updated_at = NOW()
                    """,
                    (channel, message_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO channel_state(channel, last_message_id, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(channel)
                    DO UPDATE SET last_message_id = excluded.last_message_id, updated_at = excluded.updated_at
                    """,
                    (channel, message_id, now),
                )
            conn.commit()

    def alert_was_sent(self, channel: str, message_id: int) -> bool:
        with closing(self._connect()) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM sent_alerts WHERE source_channel = %s AND message_id = %s"
                if self.is_postgres
                else "SELECT 1 FROM sent_alerts WHERE source_channel = ? AND message_id = ?",
                (channel, message_id),
            )
            return cur.fetchone() is not None

    def mark_alert_sent(self, channel: str, message_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with closing(self._connect()) as conn:
            cur = conn.cursor()
            if self.is_postgres:
                cur.execute(
                    """
                    INSERT INTO sent_alerts(source_channel, message_id, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    (channel, message_id),
                )
            else:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO sent_alerts(source_channel, message_id, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (channel, message_id, now),
                )
            conn.commit()


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def normalize_channel(channel: str) -> str:
    return channel.strip().removeprefix("@")


def message_link(channel: str, message_id: int) -> str:
    return f"https://t.me/{normalize_channel(channel)}/{message_id}"


def contains_any(text: str, terms: Iterable[str]) -> bool:
    low = text.lower()
    return any(term in low for term in terms)


def extract_coin(text: str) -> Optional[str]:
    patterns = [
        r"\(([A-Z0-9]{2,12})\)",
        r"\$([A-Z0-9]{2,12})\b",
        r"\b([A-Z0-9]{2,12})/USDT\b",
        r"\b([A-Z0-9]{2,12})/USDC\b",
        r"\b([A-Z0-9]{2,12})/FDUSD\b",
        r"\blist(?:s|ed|ing)?\s+([A-Z0-9]{2,12})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return "BELIRSIZ"


def extract_pair(text: str, coin: str) -> str:
    pair_match = re.search(r"\b([A-Z0-9]{2,12}/(?:USDT|USDC|FDUSD|TRY|BTC|ETH))\b", text)
    if pair_match:
        return pair_match.group(1).upper()
    if coin and coin != "BELIRSIZ":
        return f"{coin}/USDT"
    return "BELIRSIZ"


def detect_spot_listing(source: str, message_id: int, text: str) -> Optional[ListingHit]:
    if not text:
        return None

    if not contains_any(text, SPOT_TERMS):
        return None

    if contains_any(text, BLOCK_TERMS):
        return None

    low = text.lower()
    if "spot" not in low and normalize_channel(source).lower() not in {
        "coinmarketcapannouncements",
        "coinlistofficialchannel",
    }:
        return None

    coin = extract_coin(text)
    pair = extract_pair(text, coin)
    return ListingHit(
        source=source,
        coin=coin,
        pair=pair,
        message_url=message_link(source, message_id),
        raw_text=" ".join(text.split())[:700],
    )


def build_turkish_alert(hit: ListingHit) -> str:
    return (
        "🚨 SPOT LISTING ALARMI\n\n"
        f"Kaynak: {hit.source}\n"
        f"Coin: {hit.coin}\n"
        f"Parite: {hit.pair}\n"
        f"Link: {hit.message_url}\n\n"
        "Kısa yorum:\n"
        "Bu haber spot listing filtresinden geçti. İlk dakikalarda sert volatilite olabilir; "
        "tepeden market kovalamadan grafik ve likidite kontrolü yap.\n\n"
        "Haber özeti:\n"
        f"{hit.raw_text}"
    )


async def send_alert(bot: TelegramClient, chat_id: str, text: str) -> None:
    await bot.send_message(chat_id, text, link_preview=False)


async def process_channel(
    reader: TelegramClient,
    sender: TelegramClient,
    store: StateStore,
    alert_chat_id: str,
    channel: str,
) -> None:
    last_id = store.get_last_id(channel)
    newest_seen = last_id
    entity = await reader.get_entity(channel)

    messages = []
    async for message in reader.iter_messages(entity, min_id=last_id, reverse=True, limit=BACKFILL_LIMIT):
        messages.append(message)

    for message in messages:
        newest_seen = max(newest_seen, int(message.id))
        text = message.message or ""
        hit = detect_spot_listing(channel, int(message.id), text)
        if hit and not store.alert_was_sent(channel, int(message.id)):
            await send_alert(sender, alert_chat_id, build_turkish_alert(hit))
            store.mark_alert_sent(channel, int(message.id))
            log.info("Spot listing alert sent: %s message=%s", channel, message.id)

        store.set_last_id(channel, int(message.id))

    if messages:
        log.info("Processed %s messages from %s; last_id=%s", len(messages), channel, newest_seen)


async def run_once(reader: TelegramClient, sender: TelegramClient, store: StateStore, alert_chat_id: str) -> None:
    for channel in SOURCE_CHANNELS:
        try:
            await process_channel(reader, sender, store, alert_chat_id, normalize_channel(channel))
        except Exception:
            log.exception("Channel failed: %s", channel)
            raise


async def main() -> None:
    api_id = int(env_required("TELEGRAM_API_ID"))
    api_hash = env_required("TELEGRAM_API_HASH")
    session_string = env_required("TELEGRAM_SESSION_STRING")
    alert_bot_token = env_required("ALERT_BOT_TOKEN")
    alert_chat_id = env_required("ALERT_CHAT_ID")

    store = StateStore()
    reader = TelegramClient(StringSession(session_string), api_id, api_hash)
    sender = TelegramClient("alert_bot", api_id, api_hash)

    await reader.start()
    await sender.start(bot_token=alert_bot_token)

    log.info("Bot started. Channels=%s", ", ".join(SOURCE_CHANNELS))

    while True:
        try:
            await run_once(reader, sender, store, alert_chat_id)
            await asyncio.sleep(POLL_SECONDS)
        except Exception:
            log.exception("Main loop error. Restarting from saved state in %s seconds.", ERROR_RETRY_SECONDS)
            await asyncio.sleep(ERROR_RETRY_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
