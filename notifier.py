"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self._bot = Bot(token=token)
        self._chat_id = chat_id

    async def send_alert(
        self,
        match_name: str,
        tournament: str,
        opening: float,
        live: float,
        direction: str,   # "ALT" (under) or "ÜST" (over)
        diff: float,
        status: str,
        score: str = "",
    ) -> bool:
        """
        Sends a single alert notification.
        Returns True on success, False on error.
        """
        if direction == "ALT":
            emoji = "🔻"
            tip = "Barem yükseldi → ALT oyna (Under)"
        else:
            emoji = "🔺"
            tip = "Barem düştü → ÜST oyna (Over)"

        score_line = f"📊 Skor: <b>{score}</b>\n" if score else ""

        text = (
            f"{emoji} <b>Bahis Fırsatı: {direction}</b>\n\n"
            f"🏀 <b>{match_name}</b>\n"
            f"🏆 {tournament} | {status}\n"
            f"{score_line}\n"
            f"Açılış Baremi: <b>{opening:.1f}</b>\n"
            f"Güncel Barem:  <b>{live:.1f}</b>\n"
            f"Fark: <b>{diff:+.1f}</b> puan\n\n"
            f"💡 <i>{tip}</i>"
        )

        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            logger.info(f"Alert sent: {match_name} [{direction}]")
            return True
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return False

    async def send_startup(self):
        """Sends an info message when the bot starts."""
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=(
                    "🤖 <b>Basket Tahmin Botu başlatıldı.</b>\n"
                    "Canlı basketbol maçlarındaki barem hareketleri izleniyor..."
                ),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError as e:
            logger.error(f"Failed to send startup message: {e}")

    async def send_error(self, message: str):
        """Sends a critical error notification."""
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=f"⚠️ Bot hatası: {message}",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
