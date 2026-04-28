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
        emoji = "🔻" if direction == "ALT" else "🔺"
        tip = (
            "Canlı barem açılışa göre yükseldi"
            if direction == "ALT"
            else "Canlı barem açılışa göre düştü"
        )

        _period_names = {1: "1. Çeyrek (Q1)", 2: "2. Çeyrek (Q2)", 3: "3. Çeyrek (Q3)", 4: "4. Çeyrek (Q4)"}
        period_line = f"⏱ Periyot: <b>{_period_names.get(period, f'Q{period}')}</b>\n" if period else ""
        score_line = f"📊 Skor: <b>{score}</b>\n" if score else ""
        signal_line = f"🔁 <b>{signal_count}. sinyal</b>\n" if signal_count > 1 else ""
        prematch_line = f"Maç Öncesi: <b>{prematch:.1f}</b>\n" if prematch is not None else ""
        analysis = analysis or {}
        fair_line = analysis.get("fair_line")
        fair_edge = analysis.get("fair_edge")
        projected = analysis.get("projected_total")
        market_total = analysis.get("market_total")
        team_recent_total = analysis.get("team_recent_total")
        h2h_total = analysis.get("h2h_total")
        weights = analysis.get("weights") or {}
        recommendation = analysis.get("recommendation") or ""
        warnings = analysis.get("warnings") or []
        pace_anomaly_note = analysis.get("pace_anomaly_note") or ""
        quarter_paces = analysis.get("quarter_paces") or {}

        projected_line = f"Projeksiyon: <b>{float(projected):.1f}</b>\n" if projected is not None else ""
        market_line = f"Piyasa Bazı: <b>{float(market_total):.1f}</b>\n" if market_total is not None else ""
        team_line = f"Son Maç: <b>{float(team_recent_total):.1f}</b>\n" if team_recent_total is not None else ""
        h2h_line = f"H2H: <b>{float(h2h_total):.1f}</b>\n" if h2h_total is not None else ""
        fair_line_text = f"{float(fair_line):.1f}" if fair_line is not None else "Hesaplanamadı"
        fair_edge_line = f"Canlıya Göre: <b>{float(fair_edge):+.1f}</b>\n" if fair_edge is not None else ""
        weight_labels = {
            "projection": "projeksiyon",
            "market": "piyasa",
            "team_recent": "son maç",
            "h2h": "H2H",
        }
        weight_parts = [
            f"%{int(value)} {weight_labels[key]}"
            for key, value in weights.items()
            if key in weight_labels and int(value or 0) > 0
        ]
        weights_line = f"Ağırlık: <b>{' / '.join(weight_parts)}</b>\n" if weight_parts else ""
        fair_warning_line = ""
        fair_alert_line = ""
        if fair_edge is not None:
            fair_edge_value = float(fair_edge)
            fair_edge_abs = abs(fair_edge_value)
            fair_icon = "🔴 ❔" if fair_edge_abs > 10 else "🟡 ❔"
            fair_warning_line = f"{fair_icon} Adil barem canlı farkı: <b>{fair_edge_value:+.1f}</b>\n"
            if 5 <= fair_edge_abs <= 20:
                fair_alert_line = (
                    f"⚠️ <b>UYARI!!!</b> Canlı ile Adil barem arasında "
                    f"<b>{fair_edge_abs:.1f}</b> puan fark var.\n"
                )
        recommendation_line = f"💡 <b>Tavsiye:</b> {recommendation}\n" if recommendation else ""

        # Çeyrek hız anomali bloğu
        pace_anomaly_line = ""
        if pace_anomaly_note:
            pace_anomaly_line = f"⚡ <b>Hız Anomalisi:</b> {pace_anomaly_note}\n"
        quarter_pace_line = ""
        if quarter_paces:
            qp_parts = [f"Q{q}:{v:.0f}" for q, v in sorted(quarter_paces.items())]
            quarter_pace_line = f"📈 Çeyrek hız (puan/10dk): <b>{' | '.join(qp_parts)}</b>\n"

        warning_line = "\n".join(f"❔ {item}" for item in warnings[:6])
        if warning_line:
            warning_line += "\n"

        # ---- Bölümler ----------------------------------------------------------
        header = (
            f"{emoji} <b>Sinyal: {direction}</b>\n"
            f"{signal_line}"
            f"🏀 <b>{match_name}</b>\n"
            f"🏆 {tournament} | {status}\n"
            f"{period_line}"
            f"{score_line}"
        )

        prematch_inline = f"• Maç Öncesi: <b>{prematch:.1f}</b>\n" if prematch is not None else ""
        odds_section = (
            f"\n<b>📐 Barem</b>\n"
            f"• Açılış: <b>{opening:.1f}</b>\n"
            f"{prematch_inline}"
            f"• Canlı: <b>{live:.1f}</b>\n"
            f"• Fark: <b>{diff:+.1f}</b> puan\n"
        )

        projection_lines = "".join([
            f"• Projeksiyon: <b>{float(projected):.1f}</b>\n" if projected is not None else "",
            f"• Piyasa: <b>{float(market_total):.1f}</b>\n" if market_total is not None else "",
            f"• Son Maç: <b>{float(team_recent_total):.1f}</b>\n" if team_recent_total is not None else "",
            f"• H2H: <b>{float(h2h_total):.1f}</b>\n" if h2h_total is not None else "",
        ])
        weight_inline = f"  <i>({' / '.join(weight_parts)})</i>\n" if weight_parts else ""
        fair_edge_inline = f" (Canlıya göre <b>{float(fair_edge):+.1f}</b>)" if fair_edge is not None else ""
        fair_block = f"• Adil Barem: <b>{fair_line_text}</b>{fair_edge_inline}\n"
        projection_section = ""
        if projection_lines or fair_line is not None:
            projection_section = (
                f"\n<b>🧮 Analiz</b>\n"
                f"{projection_lines}"
                f"{weight_inline}"
                f"{fair_block}"
            )

        pace_section = ""
        if pace_anomaly_line or quarter_pace_line:
            pace_section = (
                f"\n<b>⚡ Tempo</b>\n"
                f"{quarter_pace_line}"
                f"{pace_anomaly_line}"
            )

        warnings_block = ""
        if fair_warning_line or fair_alert_line or warning_line:
            warnings_block = (
                f"\n<b>⚠️ Uyarılar</b>\n"
                f"{fair_warning_line}"
                f"{fair_alert_line}"
                f"{warning_line}"
            )

        footer = ""
        if recommendation_line or tip:
            footer = (
                f"\n{recommendation_line}"
                f"💡 <i>{tip}</i>\n"
            )

        text = (
            f"{header}"
            f"{odds_section}"
            f"{projection_section}"
            f"{pace_section}"
            f"{warnings_block}"
            f"{footer}"
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
