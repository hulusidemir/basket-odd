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
        analysis: dict | None = None,
        prematch: float | None = None,
        baseline: float | None = None,
        baseline_label: str = "Açılış",
        threshold: float = 10.0,
    ) -> dict:
        """
        Sends a single alert notification.
        Returns {chat_id: message_id} dict for reply threading.
        """
        if direction == "ALT":
            emoji = "🔻"
            tip = f"Canlı barem {baseline_label.lower()} referansına göre yükseldi"
        else:
            emoji = "🔺"
            tip = f"Canlı barem {baseline_label.lower()} referansına göre düştü"

        analysis = analysis or {}
        fair_line = analysis.get("fair_line")
        fair_edge = analysis.get("fair_edge")
        projected = analysis.get("projected_total")
        market_total = analysis.get("market_total")
        team_recent_total = analysis.get("team_recent_total")
        h2h_total = analysis.get("h2h_total")
        history_total = analysis.get("history_total")
        weights = analysis.get("weights") or {}
        recommendation = analysis.get("recommendation") or "Tavsiye üretilemedi."
        warnings = analysis.get("warnings") or []

        score_line = f"📊 Skor: <b>{score}</b>\n" if score else ""
        signal_line = f"🔁 <b>{signal_count}. sinyal</b>\n" if signal_count > 1 else ""
        fair_line_text = f"{float(fair_line):.1f}" if fair_line is not None else "Hesaplanamadı"
        fair_edge_line = f"Canlıya Göre:  <b>{float(fair_edge):+.1f}</b>\n" if fair_edge is not None else ""
        projected_line = f"Projeksiyon:   <b>{float(projected):.1f}</b>\n" if projected is not None else ""
        market_line = f"Piyasa Bazı:   <b>{float(market_total):.1f}</b>\n" if market_total is not None else ""
        team_line = f"Son Maç:       <b>{float(team_recent_total):.1f}</b>\n" if team_recent_total is not None else ""
        h2h_line = f"H2H:           <b>{float(h2h_total):.1f}</b>\n" if h2h_total is not None else ""
        history_line = (
            f"H2H/Son Maç:   <b>{float(history_total):.1f}</b>\n"
            if history_total is not None and team_recent_total is None and h2h_total is None else ""
        )
        weight_labels = {
            "projection": "projeksiyon",
            "market": "piyasa",
            "team_recent": "son maç",
            "h2h": "H2H",
        }
        weight_parts = [
            f"%{int(value)} {weight_labels[key]}"
            for key, value in weights.items()
            if int(value or 0) > 0 and key in weight_labels
        ]
        weights_line = f"Ağırlık:       <b>{' / '.join(weight_parts)}</b>\n" if weight_parts else ""
        warning_line = "\n".join(f"❔ {item}" for item in warnings[:6])
        if warning_line:
            warning_line += "\n"

        prematch_line = f"Maç Öncesi:    <b>{prematch:.1f}</b>\n" if prematch is not None else ""
        reference_value = baseline if baseline is not None else opening
        reference_line = f"Referans:      <b>{baseline_label} {reference_value:.1f}</b>\n"
        text = (
            f"🎯 <b>Sinyal: {direction}</b>\n"
            f"{signal_line}\n"
            f"🏀 <b>{match_name}</b>\n"
            f"🏆 {tournament} | {status}\n"
            f"{score_line}\n"
            f"Açılış Baremi: <b>{opening:.1f}</b>\n"
            f"{prematch_line}"
            f"{reference_line}"
            f"Güncel Barem:  <b>{live:.1f}</b>\n"
            f"{projected_line}"
            f"{market_line}"
            f"{team_line}"
            f"{h2h_line}"
            f"{history_line}"
            f"Adil Barem:    <b>{fair_line_text}</b>\n"
            f"{fair_edge_line}"
            f"{weights_line}"
            f"Fark: <b>{diff:+.1f}</b> puan\n\n"
            f"💡 <b>Tavsiye:</b> {recommendation}\n"
            f"{warning_line}"
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
