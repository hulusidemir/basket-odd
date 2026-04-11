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
        # Support multiple chat IDs (comma-separated)
        self._chat_ids = [cid.strip() for cid in chat_id.split(",") if cid.strip()]

    async def _send_to_all(self, text: str, reply_to: dict | None = None) -> dict:
        """Send a message to all configured chat IDs.
        Returns {chat_id: message_id} for sent messages."""
        msg_ids = {}
        for cid in self._chat_ids:
            try:
                reply_id = reply_to.get(cid) if reply_to else None
                msg = await self._bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=reply_id,
                )
                msg_ids[cid] = msg.message_id
            except TelegramError as e:
                logger.error(f"Telegram error (chat_id={cid}): {e}")
        return msg_ids

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
        signal_count: int = 1,
    ) -> dict:
        """
        Sends a single alert notification.
        Returns {chat_id: message_id} dict for reply threading.
        """
        if direction == "ALT":
            emoji = "🔻"
            tip = "Canlı barem açılışa göre yükseldi"
        else:
            emoji = "🔺"
            tip = "Canlı barem açılışa göre düştü"

        score_line = f"📊 Skor: <b>{score}</b>\n" if score else ""
        signal_line = f"🔁 <b>{signal_count}. sinyal</b>\n" if signal_count > 1 else ""

        text = (
            f"{emoji} <b>Sinyal: {direction}</b>\n"
            f"{signal_line}\n"
            f"🏀 <b>{match_name}</b>\n"
            f"🏆 {tournament} | {status}\n"
            f"{score_line}\n"
            f"Açılış Baremi: <b>{opening:.1f}</b>\n"
            f"Güncel Barem:  <b>{live:.1f}</b>\n"
            f"Fark: <b>{diff:+.1f}</b> puan\n\n"
            f"💡 <i>{tip}</i>"
            f"\n"
        )

        try:
            msg_ids = await self._send_to_all(text)
            logger.info(f"Alert sent: {match_name} [{direction}]")
            return msg_ids
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return {}

    async def send_analysis(self, analysis: str, match_name: str, reply_to: dict | None = None) -> bool:
        """Send AI analysis as a reply to the original alert message."""
        # Truncate if too long for Telegram (4096 char limit)
        if len(analysis) > 3800:
            analysis = analysis[:3800] + "\n\n<i>... (kırpıldı)</i>"

        text = f"🤖 <b>AI Analiz: {match_name}</b>\n\n{analysis}"

        try:
            await self._send_to_all(text, reply_to=reply_to)
            logger.info(f"Analysis sent: {match_name}")
            return True
        except TelegramError as e:
            logger.error(f"Analysis send error: {e}")
            return False

    async def send_startup(self):
        """Sends an info message when the bot starts."""
        try:
            await self._send_to_all(
                "🤖 <b>Basket Tahmin Botu başlatıldı.</b>\n"
                "Canlı basketbol maçlarındaki barem hareketleri izleniyor..."
            )
        except TelegramError as e:
            logger.error(f"Failed to send startup message: {e}")

    async def send_error(self, message: str):
        """Sends a critical error notification."""
        try:
            await self._send_to_all(f"⚠️ Bot hatası: {message}")
        except TelegramError:
            pass
