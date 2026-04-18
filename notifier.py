"""
notifier.py — Sends betting alerts via Telegram.
"""

import logging

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from signal_reliability import alert_reliability

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
        quality: dict | None = None,
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

        reliability = alert_reliability(
            direction=direction,
            quality_grade=(quality or {}).get("grade", ""),
            status=status,
            diff=diff,
            threshold=threshold,
        )

        score_line = f"📊 Skor: <b>{score}</b>\n" if score else ""
        signal_line = f"🔁 <b>{signal_count}. sinyal</b>\n" if signal_count > 1 else ""
        summary_line = ""
        reasons_line = ""
        team_line = ""
        quality_grade = (quality or {}).get("grade", "-")
        reliability_label = reliability["label"]
        if quality:
            if quality.get("setup"):
                summary_line += f"🧩 <b>Setup:</b> {quality['setup']}\n"
            supporting = quality.get("supporting_signals") or []
            opposing = quality.get("opposing_signals") or []
            neutral = quality.get("neutral_signals") or []
            support_names = ", ".join(item.get("name", "-") for item in supporting) or "-"
            opposing_names = ", ".join(item.get("name", "-") for item in opposing) or "-"
            reasons_line += f"✅ <b>Destekleyen ({len(supporting)}):</b> {support_names}\n"
            reasons_line += f"⚠️ <b>Karşı ({len(opposing)}):</b> {opposing_names}\n"
            if neutral:
                neutral_names = ", ".join(item.get("name", "-") for item in neutral)
                reasons_line += f"➖ <b>Nötr ({len(neutral)}):</b> {neutral_names}\n"
            if quality.get("reverse_risk"):
                reasons_line += "🟥 <b>TERS RİSKİ VAR</b>\n"
            if quality.get("script_note"):
                summary_line += f"📝 {quality['script_note']}\n"
            elif quality.get("summary"):
                summary_line += f"📝 {quality['summary']}\n"
            ctx = quality.get("team_context") or {}
            if ctx:
                lines = ["📚 <b>Takım Profili</b>"]
                align_code = ctx.get("alignment_code")
                align_icon = {"support": "✅", "against": "⚠️", "mixed": "🔀", "neutral": "➖"}.get(align_code, "➖")
                if ctx.get("alignment"):
                    lines.append(f"{align_icon} {ctx['alignment']}")
                if ctx.get("regression_note"):
                    lines.append(f"📉 {ctx['regression_note']}")
                home = ctx.get("home_profile") or {}
                away = ctx.get("away_profile") or {}
                if home.get("avg_total") is not None:
                    lines.append(
                        f"🏠 <b>{home.get('team','Ev')}</b> · son 5 ort "
                        f"<b>{home['avg_total']:.1f}</b> ({home.get('ppg',0):.0f}+{home.get('oppg',0):.0f})"
                        f" · over %{int(home.get('over_pct') or 0)} · <i>{home.get('label','')}</i>"
                    )
                if away.get("avg_total") is not None:
                    lines.append(
                        f"🚌 <b>{away.get('team','Dep')}</b> · son 5 ort "
                        f"<b>{away['avg_total']:.1f}</b> ({away.get('ppg',0):.0f}+{away.get('oppg',0):.0f})"
                        f" · over %{int(away.get('over_pct') or 0)} · <i>{away.get('label','')}</i>"
                    )
                if ctx.get("h2h_note"):
                    lines.append(f"🤝 {ctx['h2h_note']}")
                team_line = "\n".join(lines) + "\n"

        prematch_line = f"Maç Öncesi:    <b>{prematch:.1f}</b>\n" if prematch is not None else ""
        reference_value = baseline if baseline is not None else opening
        reference_line = f"Referans:      <b>{baseline_label} {reference_value:.1f}</b>\n"
        text = (
            f"🎯 <b>Sinyal: {direction} ({quality_grade}) — {reliability_label}</b>\n"
            f"{signal_line}\n"
            f"🏀 <b>{match_name}</b>\n"
            f"🏆 {tournament} | {status}\n"
            f"{score_line}\n"
            f"Açılış Baremi: <b>{opening:.1f}</b>\n"
            f"{prematch_line}"
            f"{reference_line}"
            f"Güncel Barem:  <b>{live:.1f}</b>\n"
            f"Fark: <b>{diff:+.1f}</b> puan\n\n"
            f"{summary_line}"
            f"{reasons_line}"
            f"{team_line}"
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
