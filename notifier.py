"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging
from html import escape

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


def _quality_text(analysis: dict) -> str:
    label = str(analysis.get("quality_label") or "").strip()
    title = str(analysis.get("quality_title") or "").strip()
    if not label or label == "-":
        return "-"
    if title and title != "Kalite kuralı uygulanmadı.":
        return f"{label} — {title}"
    return label


def _list_text(analysis: dict) -> str:
    markers = analysis.get("list_markers") if isinstance(analysis, dict) else []
    if not isinstance(markers, list) or not markers:
        return "-"
    parts = []
    for marker in markers:
        if not isinstance(marker, dict):
            continue
        prefix = "🟥" if marker.get("type") == "black" else "🟩"
        title = str(marker.get("title") or "").strip()
        if title:
            parts.append(f"{prefix} {title}")
    return " | ".join(parts) if parts else "-"


def _build_alert_text(
    *,
    match_name: str,
    tournament: str,
    opening: float,
    live: float,
    direction: str,
    diff: float,
    status: str,
    score: str,
    signal_count: int,
    prematch: float | None,
    analysis: dict,
    period: int | None,
) -> str:
    fair_line = analysis.get("fair_line")
    fair_edge = analysis.get("fair_edge")
    projected = analysis.get("projected_total")
    h2h_total = analysis.get("h2h_total")

    status_text = (status or "").strip()
    if status_text and period and not status_text.upper().startswith(f"Q{period}"):
        when = f"Q{period} {status_text}"
    elif status_text:
        when = status_text
    elif period:
        when = f"Q{period}"
    else:
        when = "-"

    repeat = f" · {signal_count}. sinyal" if signal_count > 1 else ""

    fair_text = "-"
    if fair_line is not None:
        fair_text = f"{float(fair_line):.1f}"
        if fair_edge is not None:
            fair_text += f" (canlıya göre {float(fair_edge):+.1f})"

    proj_text = f"{float(projected):.1f}" if projected is not None else "-"
    h2h_text = f"{float(h2h_total):.1f}" if h2h_total is not None else "-"
    quality_text = _quality_text(analysis)
    list_text = _list_text(analysis)

    return (
        f"<b>{direction} Sinyali</b>{repeat}\n"
        f"🏀 <b>{escape(match_name)}</b>\n"
        f"🏆 {escape(tournament or '-')}\n\n"
        f"<b>Skor:</b> {escape(score or '-')}\n"
        f"<b>Sinyal Zamanı:</b> {when}\n"
        f"<b>Barem:</b> {opening:.1f} → {live:.1f} ({diff:+.1f})\n"
        f"<b>KALİTE:</b> {escape(quality_text)}\n"
        f"<b>LİSTE:</b> {escape(list_text)}\n"
        f"<b>H2H:</b> {h2h_text}\n"
        f"<b>ADİL BAREM:</b> {fair_text}\n"
        f"<b>PROJEKSİYON:</b> {proj_text}"
    )


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self._bot = Bot(token=token)
        self._chat_ids = [cid.strip() for cid in chat_id.split(",") if cid.strip()]

    async def _send_to_all(self, text: str) -> dict:
        msg_ids = {}
        for cid in self._chat_ids:
            try:
                msg = await self._bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
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
        direction: str,
        diff: float,
        status: str,
        score: str = "",
        signal_count: int = 1,
        prematch: float | None = None,
        analysis: dict | None = None,
        period: int | None = None,
    ) -> dict:
        analysis = analysis or {}
        text = _build_alert_text(
            match_name=match_name,
            tournament=tournament,
            opening=opening,
            live=live,
            direction=direction,
            diff=diff,
            status=status,
            score=score,
            signal_count=signal_count,
            prematch=prematch,
            analysis=analysis,
            period=period,
        )
        try:
            msg_ids = await self._send_to_all(text)
            logger.info(f"Alert sent: {match_name} [{direction}]")
            return msg_ids
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return {}

    async def send_startup(self):
        try:
            await self._send_to_all(
                "🤖 <b>Basket Tahmin Botu başlatıldı.</b>\n"
                "Canlı basketbol maçlarındaki barem hareketleri izleniyor..."
            )
        except TelegramError as e:
            logger.error(f"Failed to send startup message: {e}")

    async def send_error(self, message: str):
        try:
            await self._send_to_all(f"⚠️ Bot hatası: {message}")
        except TelegramError:
            pass
