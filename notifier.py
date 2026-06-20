"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging
from html import escape

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


def _float_text(value, digits: int = 2) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "-"


def _quarter_score_text(analysis: dict) -> str:
    scores = analysis.get("quarter_scores") if isinstance(analysis, dict) else {}
    if not isinstance(scores, dict):
        return "-"
    home = scores.get("home") if isinstance(scores.get("home"), list) else []
    away = scores.get("away") if isinstance(scores.get("away"), list) else []
    rows = []
    for index, (h, a) in enumerate(zip(home, away), start=1):
        try:
            rows.append(f"Q{index} {int(h)}-{int(a)}")
        except (TypeError, ValueError):
            continue
        if len(rows) >= 4:
            break
    return " | ".join(rows) if rows else "-"


def _quarter_ppm_text(analysis: dict) -> str:
    ppm_values = analysis.get("quarter_ppm") if isinstance(analysis, dict) else []
    if not isinstance(ppm_values, list) or not ppm_values:
        return "-"
    rows = []
    for index, value in enumerate(ppm_values[:4], start=1):
        text = _float_text(value)
        if text != "-":
            rows.append(f"Q{index} {text}")
    return " | ".join(rows) if rows else "-"


def _match_ppm_text(analysis: dict) -> str:
    components = analysis.get("projection_components") if isinstance(analysis, dict) else {}
    if not isinstance(components, dict):
        components = {}
    value = analysis.get("match_ppm") if isinstance(analysis, dict) else None
    if value is None:
        value = components.get("current_pace_per_min")
    return _float_text(value)


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
    quarter_score_text = _quarter_score_text(analysis)
    match_ppm_text = _match_ppm_text(analysis)
    quarter_ppm_text = _quarter_ppm_text(analysis)
    final_direction = str(
        analysis.get("final_direction")
        or analysis.get("direction")
        or direction
    ).strip().upper().replace("UST", "ÜST")
    if final_direction not in {"ALT", "ÜST"}:
        final_direction = direction

    signal_headline = f"✅ <b>{final_direction} oyna</b>{repeat}"

    reason_text = str(analysis.get("selection_reason") or "").strip()
    if not reason_text:
        reason_text = "Nihai sinyal yönü, canlı barem hareketi ve adil barem/projeksiyon kontrolüyle seçildi."

    league_line = ""

    return (
        f"{signal_headline}\n"
        f"🏀 <b>{escape(match_name)}</b>\n"
        f"🏆 {escape(tournament or '-')}\n\n"
        f"<b>Gerekçe:</b> {escape(reason_text)}\n"
        f"<b>Skor:</b> {escape(score or '-')}\n"
        f"<b>Ne zaman geldi:</b> {when}\n"
        f"<b>Çeyrek Skorları:</b> {escape(quarter_score_text)}\n"
        f"<b>Maç hızı:</b> {escape(match_ppm_text)} sayı/dakika\n"
        f"<b>Çeyrek hızları:</b> {escape(quarter_ppm_text)}\n"
        f"<b>Barem değişimi:</b> {opening:.1f} → {live:.1f} ({diff:+.1f})\n"
        f"<b>Adil barem:</b> {fair_text}\n"
        f"<b>Maç sonu tahmini:</b> {proj_text}\n"
        f"<b>Geçmiş maç ortalaması:</b> {h2h_text}"
        f"{league_line}"
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
        followed_upcoming: bool = False,
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
        if followed_upcoming:
            text = "<b>TAKİP EDİLEN MAÇA AİT SİNYAL GELDİ</b>\n" + text
        try:
            msg_ids = await self._send_to_all(text)
            logger.info(f"Alert sent: {match_name} [{direction}]")
            return msg_ids
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return {}

    async def send_followed_match_alert(
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
        return await self.send_alert(
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
            followed_upcoming=True,
        )

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
