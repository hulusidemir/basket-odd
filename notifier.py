"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


_LABEL_ICON = {
    "Güçlü": "🔥",
    "Oynanabilir": "✅",
    "İzle": "👀",
    "Pas": "⚠️",
}


def _parse_score_gap(score: str) -> int | None:
    if not score or "-" not in score:
        return None
    try:
        a, b = score.split("-", 1)
        return abs(int(a.strip()) - int(b.strip()))
    except (ValueError, AttributeError):
        return None


def _build_warnings(
    *,
    direction: str,
    diff: float,
    opening: float,
    period: int | None,
    score: str,
    signal_count: int,
    analysis: dict,
) -> list[str]:
    """Bağlamsal uyarılar — sadece o sinyale özel olanlar, en fazla 4 madde."""
    warnings: list[str] = []
    abs_diff = abs(float(diff or 0))
    opening_val = float(opening or 0)
    gap = _parse_score_gap(score)
    fair_edge = analysis.get("fair_edge")
    pace_dir = analysis.get("pace_anomaly_direction")
    flip_reason = analysis.get("flip_reason") or ""

    if direction == "ÜST":
        warnings.append("📉 ÜST sinyali geçmişte %44 kazandı; ters (ALT) %56 kazandı")
        if 0 < opening_val < 155:
            warnings.append("Düşük totalli lig (<155) — ÜST için en zayıf segment")
        if period == 4:
            warnings.append("Q4 ÜST tarihsel olarak %40 — risk yüksek")
        return warnings[:4]

    # ALT için
    if gap is not None:
        if gap <= 6:
            warnings.append("Yakın maç → taktik faul / OT riski olabilir")
        elif gap >= 18:
            warnings.append("Kopuk maç → garbage time, ALT için avantaj")

    if period == 4 and gap is not None and gap <= 4:
        warnings.append("Q4 + çok yakın skor → OT/foul kaosu, ALT riskli")

    if opening_val >= 190:
        warnings.append("Yüksek tempolu maç (≥190) → ALT geçmişte %39")
    elif opening_val >= 175:
        warnings.append("Yüksek total (175-190) → ALT zorlaşır")
    elif 0 < opening_val < 145:
        warnings.append("Düşük totalli lig (<145) → ALT için avantaj")

    if pace_dir == "ÜST":
        warnings.append("⚡ Tempo hızlanıyor → ALT için risk")
    elif pace_dir == "ALT":
        warnings.append("✓ Tempo yavaşlıyor → ALT için destek")

    if abs_diff >= 20:
        warnings.append(f"Geç tepki (fark {abs_diff:.0f}); değer azalmış olabilir")
    elif 13 <= abs_diff <= 17:
        warnings.append(f"Sweet-spot fark ({abs_diff:.0f}) → ALT için verimli bant")

    if int(signal_count or 1) >= 3:
        warnings.append(f"{signal_count}. tekrar sinyal → değer azalıyor")

    if fair_edge is not None:
        fe = float(fair_edge)
        if fe <= -8:
            warnings.append(f"Adil barem net düşük ({fe:+.1f}) → ALT'ı güçlü destekliyor")
        elif fe >= 3:
            warnings.append(f"⚠️ Adil barem yönle ters ({fe:+.1f}) → güven düşük")

    if flip_reason:
        warnings.append(f"Yön çevrildi → {flip_reason}")

    return warnings[:4]


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
    label = analysis.get("signal_quality_label") or "İzle"
    score_val = analysis.get("signal_quality_score")
    icon = _LABEL_ICON.get(label, "")
    fair_line = analysis.get("fair_line")
    fair_edge = analysis.get("fair_edge")
    projected = analysis.get("projected_total")
    team_recent = analysis.get("team_recent_total")
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

    score_text = score or "-"
    repeat = f" · {signal_count}. sinyal" if signal_count > 1 else ""
    score_line = f"<b>Skor:</b> {score_text}"
    if score_val is not None:
        score_line += f" · Puan {int(score_val)}/100"

    fair_text = "-"
    if fair_line is not None:
        fair_text = f"{float(fair_line):.1f}"
        if fair_edge is not None:
            fair_text += f" (canlıya göre {float(fair_edge):+.1f})"

    proj_text = f"{float(projected):.1f}" if projected is not None else "-"
    sf_text = f"{float(team_recent):.1f}" if team_recent is not None else "-"
    h2h_text = f"{float(h2h_total):.1f}" if h2h_total is not None else "-"

    warnings = _build_warnings(
        direction=direction,
        diff=diff,
        opening=opening,
        period=period,
        score=score,
        signal_count=signal_count,
        analysis=analysis,
    )
    warnings_block = ""
    if warnings:
        items = "\n".join(f"• {w}" for w in warnings)
        warnings_block = f"\n\n<b>UYARILAR:</b>\n{items}"

    header = f"{icon} <b>{direction} Sinyali</b> ({label}){repeat}"
    body = (
        f"🏀 <b>{match_name}</b>\n"
        f"🏆 {tournament}\n\n"
        f"{score_line}\n"
        f"<b>Sinyal Zamanı:</b> {when}\n"
        f"<b>Barem:</b> {opening:.1f} → {live:.1f} ({diff:+.1f})\n"
        f"<b>H2H:</b> {h2h_text}\n"
        f"<b>SF:</b> {sf_text}\n"
        f"<b>ADİL BAREM:</b> {fair_text}\n"
        f"<b>PROJEKSİYON:</b> {proj_text}"
    )
    return f"{header}\n{body}{warnings_block}"


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
