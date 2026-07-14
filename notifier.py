"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging
import hashlib
import hmac
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
    quality = analysis.get("signal_quality") if isinstance(analysis.get("signal_quality"), dict) else {}
    confidence_value = quality.get("quality_score")
    try:
        confidence_text = f"{int(round(float(confidence_value)))}/100"
    except (TypeError, ValueError):
        confidence_text = "-"
    confidence_label = str(quality.get("quality_label") or "").strip()
    if confidence_label and confidence_text != "-":
        confidence_text += f" · {confidence_label}"
    quarter_score_text = _quarter_score_text(analysis)
    match_ppm_text = _match_ppm_text(analysis)
    quarter_ppm_text = _quarter_ppm_text(analysis)
    prematch_text = f" → {float(prematch):.1f}" if prematch is not None else ""
    final_direction = str(
        analysis.get("final_direction")
        or analysis.get("direction")
        or direction
    ).strip().upper().replace("UST", "ÜST")
    if final_direction not in {"ALT", "ÜST"}:
        final_direction = direction

    gate = analysis.get("signal_gate") if isinstance(analysis.get("signal_gate"), dict) else {}
    evidence = gate.get("evidence") if isinstance(gate.get("evidence"), dict) else {}
    confidence_floor = evidence.get("wilson_low_95")
    evidence_text = (
        f"%95 alt güven sınırı %{float(confidence_floor):.1f} · "
        f"{int(evidence.get('resolved_unique') or 0)} benzersiz maç"
        if confidence_floor is not None
        else "İleri tarihli güven kanıtı yok"
    )
    gate_state = str(gate.get("state") or "LEGACY_UNVERIFIED").upper()
    if gate_state == "TRUSTED":
        signal_headline = f"✅ <b>ONAYLI · {final_direction} oynanabilir</b>{repeat}"
    elif gate_state == "SHADOW":
        signal_headline = f"🧪 <b>TEST · {final_direction} araştırma sinyali</b>{repeat}"
    elif gate_state == "BLOCKED":
        signal_headline = f"⛔ <b>PAS · {final_direction}</b>{repeat}"
    else:
        signal_headline = f"⚠️ <b>SİNYAL · {final_direction}</b>{repeat}"

    reason_text = str(analysis.get("selection_reason") or "").strip()
    if not reason_text:
        reason_text = "Nihai sinyal yönü, canlı barem hareketi ve adil barem/projeksiyon kontrolüyle seçildi."

    return (
        f"{signal_headline}\n"
        f"🏀 <b>{escape(match_name)}</b>\n"
        f"🏆 {escape(tournament or '-')}\n\n"
        f"<b>Gerekçe:</b> {escape(reason_text)}\n"
        f"<b>Kanıt:</b> {escape(evidence_text)}\n"
        f"<b>Güven skoru:</b> {escape(confidence_text)}\n"
        f"<b>Skor:</b> {escape(score or '-')}\n"
        f"<b>Ne zaman geldi:</b> {when}\n"
        f"<b>Çeyrek Skorları:</b> {escape(quarter_score_text)}\n"
        f"<b>Maç hızı:</b> {escape(match_ppm_text)} sayı/dakika\n"
        f"<b>Çeyrek hızları:</b> {escape(quarter_ppm_text)}\n"
        f"<b>Barem değişimi:</b> {opening:.1f}{prematch_text} → {live:.1f} ({diff:+.1f})\n"
        f"<b>Adil barem:</b> {fair_text}\n"
        f"<b>Maç sonu tahmini:</b> {proj_text}\n"
        f"<b>Geçmiş maç ortalaması:</b> {h2h_text}"
    )


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self._bot = Bot(token=token)
        self._chat_ids = [cid.strip() for cid in chat_id.split(",") if cid.strip()]
        secret = str(token or "local-recipient-key").encode("utf-8")
        self._recipients = {
            hmac.new(secret, cid.encode("utf-8"), hashlib.sha256).hexdigest()[:20]: cid
            for cid in self._chat_ids
        }

    @property
    def recipient_keys(self) -> set[str]:
        return set(self._recipients)

    def delivery_complete(self, message_ids: dict) -> bool:
        return bool(self._recipients) and self.recipient_keys.issubset(message_ids or {})

    async def _send_to_all(
        self,
        text: str,
        pending_recipient_keys: set[str] | None = None,
    ) -> dict:
        msg_ids = {}
        targets = self._recipients.items()
        if pending_recipient_keys is not None:
            targets = [
                (key, cid)
                for key, cid in targets
                if key in pending_recipient_keys
            ]
        for chat_index, (recipient_key, cid) in enumerate(targets, start=1):
            try:
                msg = await self._bot.send_message(
                    chat_id=cid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
                msg_ids[recipient_key] = msg.message_id
            except TelegramError as e:
                logger.error("Telegram error (chat #%s): %s", chat_index, e)
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
        pending_recipient_keys: set[str] | None = None,
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
            if pending_recipient_keys is None:
                msg_ids = await self._send_to_all(text)
            else:
                msg_ids = await self._send_to_all(
                    text,
                    pending_recipient_keys=pending_recipient_keys,
                )
            if msg_ids:
                logger.info(f"Alert sent: {match_name} [{direction}]")
            else:
                logger.warning("Alert could not be delivered to any configured chat: %s", match_name)
            return msg_ids
        except TelegramError as e:
            logger.error(f"Telegram error: {e}")
            return {}

    async def send_startup(self):
        try:
            await self._send_to_all(
                "🤖 <b>Basket Tahmin Botu başlatıldı.</b>\n"
                "Canlı barem hareketleri izleniyor. Eşiği geçen tüm sinyaller "
                "PAS, TEST veya ONAY etiketiyle gönderilir."
            )
        except TelegramError as e:
            logger.error(f"Failed to send startup message: {e}")

    async def send_error(self, message: str):
        try:
            await self._send_to_all(f"⚠️ Bot hatası: {message}")
        except TelegramError as exc:
            logger.warning("Could not deliver bot error notification: %s", exc)
