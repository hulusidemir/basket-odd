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
    period: int | None,
    reference_used: str | None = None,
    reference_total: float | None = None,
    effective_threshold: float | None = None,
    pregame_ppm: float | None = None,
    market_future_pace: float | None = None,
    future_pace_lower: float | None = None,
    future_pace_upper: float | None = None,
    fair_total: float | None = None,
) -> str:
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

    prematch_text = f" → {float(prematch):.1f}" if prematch is not None else ""
    signal_headline = f"📊 <b>{escape(direction)}</b>{repeat}"
    reference_text = ""
    if reference_used in {"opening", "prematch"} and reference_total is not None:
        label = "Maç önü" if reference_used == "prematch" else "Açılış"
        reference_text = f"\n<b>Referans:</b> {label} {reference_total:.1f} · fark {diff:+.1f}"
        if effective_threshold is not None:
            reference_text += f" · eşik {effective_threshold:.2f}"
    change_suffix = "" if reference_text else f" ({diff:+.1f})"

    future_text = ""
    if fair_total is not None:
        future_text = f"\n\n<b>Adil Barem:</b> {fair_total:.1f}"
    return (
        f"{signal_headline}\n"
        f"🏀 <b>{escape(match_name)}</b>\n"
        f"🏆 {escape(tournament or '-')}\n\n"
        f"<b>Skor:</b> {escape(score or '-')}\n"
        f"<b>Ne zaman geldi:</b> {when}\n"
        f"<b>Barem değişimi:</b> {opening:.1f}{prematch_text} → {live:.1f}{change_suffix}"
        f"{reference_text}"
        f"{future_text}"
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
        period: int | None = None,
        followed_upcoming: bool = False,
        pending_recipient_keys: set[str] | None = None,
        reference_used: str | None = None,
        reference_total: float | None = None,
        effective_threshold: float | None = None,
        pregame_ppm: float | None = None,
        market_future_pace: float | None = None,
        future_pace_lower: float | None = None,
        future_pace_upper: float | None = None,
        fair_total: float | None = None,
    ) -> dict:
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
            period=period,
            reference_used=reference_used,
            reference_total=reference_total,
            effective_threshold=effective_threshold,
            pregame_ppm=pregame_ppm,
            market_future_pace=market_future_pace,
            future_pace_lower=future_pace_lower,
            future_pace_upper=future_pace_upper,
            fair_total=fair_total,
        )
        if followed_upcoming:
            first_line, separator, remainder = text.partition("\n")
            text = (
                first_line
                + "\n<b>TAKİP EDİLEN MAÇA AİT SİNYAL GELDİ</b>"
                + (separator + remainder if separator else "")
            )
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
                "Canlı baremler maç önü (yoksa açılış) referansıyla izleniyor. "
                "Dinamik eşik ve periyot filtrelerinden geçen sinyaller gönderilir."
            )
        except TelegramError as e:
            logger.error(f"Failed to send startup message: {e}")

    async def send_error(self, message: str):
        try:
            await self._send_to_all(f"⚠️ Bot hatası: {message}")
        except TelegramError as exc:
            logger.warning("Could not deliver bot error notification: %s", exc)
