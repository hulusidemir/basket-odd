"""
main.py — Basketball odds monitoring bot main loop.

Usage:
    python main.py
"""

import asyncio
import logging
import random
import re
import sys

from aiscore_opera_scraper import AiscoreOperaScraper
from config import Config
from db import Database
from notifier import TelegramNotifier


def _calculate_base_risk(direction: str, inplay_total: float, opening_total: float, score: str, status: str, match_name: str, tournament: str) -> dict:
    """
    Calculate base risk assessment based on score tempo vs live total line.
    - Uses league specific quarter lengths (NBA: 12, NCAA: 20, FIBA: 10)
    - Applies crunch-time multipliers to expected totals
    - Disables analysis if time is unknown
    Returns dict with level, emoji, text, icon keys.
    """
    # ── Parse score ──
    if not score or not score.strip():
        return {
            "level": "unknown", "emoji": "❓", "icon": "?",
            "text": "Skor verisi alınamadı, risk değerlendirmesi yapılamadı",
        }

    score_match = re.match(r'(\d+)\s*[-–]\s*(\d+)', score.strip())
    if not score_match:
        return {
            "level": "unknown", "emoji": "❓", "icon": "?",
            "text": "Skor verisi okunamadı, risk değerlendirmesi yapılamadı",
        }

    home_score = int(score_match.group(1))
    away_score = int(score_match.group(2))
    total_score = home_score + away_score

    if total_score == 0:
        return {
            "level": "unknown", "emoji": "❓", "icon": "?",
            "text": f"Skor 0-0, tempo hesaplanamıyor ({status or 'Durum bilinmiyor'})",
        }

    # ── Determine league-specific time settings ──
    text_to_check = f"{match_name} {tournament}".upper()
    if "NBA" in text_to_check:
        quarter_length = 12
        total_game_min = 48
    elif "NCAA" in text_to_check:
        quarter_length = 20
        total_game_min = 40
    else:
        quarter_length = 10
        total_game_min = 40

    # ── Try to parse period from status ──
    period = None
    remaining_min = None
    status_clean = (status or "").strip()

    if status_clean:
        # OT — can't project in overtime
        if re.match(r'^OT', status_clean, re.IGNORECASE):
            return {
                "level": "unknown", "emoji": "❓", "icon": "?",
                "text": f"Oyun uzatmada, tempo hesaplanamıyor ({status_clean})",
                "recommendation": "PAS GEÇ",
                "rec_emoji": "⛔"
            }

        # HT (halftime) — period 2, remaining 0
        if re.match(r'^HT$', status_clean, re.IGNORECASE):
            period = 2
            remaining_min = 0.0

        # Q1-Q4 with time: "Q2 05:30", "Q4-09:29", "3Q 04:11"
        if period is None:
            q_match = re.match(
                r'(?:Q(\d)|(\d)Q)\s*[-\s]?\s*(\d{1,2}):(\d{2})',
                status_clean, re.IGNORECASE,
            )
            if q_match:
                period = int(q_match.group(1) or q_match.group(2))
                remaining_min = int(q_match.group(3)) + int(q_match.group(4)) / 60.0

        # "1st", "2nd", "3rd", "4th" with optional time
        if period is None:
            ord_match = re.match(
                r'(\d)(?:st|nd|rd|th)\s*[-\s]?\s*(?:(\d{1,2}):(\d{2}))?',
                status_clean, re.IGNORECASE,
            )
            if ord_match:
                period = int(ord_match.group(1))
                if ord_match.group(2) and ord_match.group(3):
                    remaining_min = int(ord_match.group(2)) + int(ord_match.group(3)) / 60.0
                else:
                    remaining_min = 5.0

        # Just period label: "Q2", "3Q", "1H", "2H"
        if period is None:
            q_only = re.match(r'^(?:Q(\d)|(\d)Q)$', status_clean, re.IGNORECASE)
            if q_only:
                period = int(q_only.group(1) or q_only.group(2))
                remaining_min = 5.0

        # Half labels: "1H" → mid first half (~Q1-Q2 boundary), "2H" → mid second half (~Q3-Q4 boundary)
        if period is None:
            h_match = re.match(r'^(\d)H$', status_clean, re.IGNORECASE)
            if h_match:
                half = int(h_match.group(1))
                period = 2 if half == 1 else 4
                remaining_min = 5.0  # assume mid-half

        # Standalone digit: "1", "2", "3", "4" (period number)
        if period is None:
            d_match = re.match(r'^([1-4])$', status_clean)
            if d_match:
                period = int(d_match.group(1))
                remaining_min = 5.0

        # Numeric period-time: "01-05:32", "03-02:38", "2-08:00"
        if period is None:
            nt_match = re.match(
                r'(\d{1,2})[-:](\d{1,2}):(\d{2})',
                status_clean,
            )
            if nt_match:
                p = int(nt_match.group(1))
                if 1 <= p <= 4:
                    period = p
                    remaining_min = int(nt_match.group(2)) + int(nt_match.group(3)) / 60.0

    # ── Period-based projection (preferred method) ──
    if period is not None and remaining_min is not None:
        if quarter_length == 20:
            elapsed_min = (period - 1) * 20 + (20 - remaining_min)
        else:
            elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)

        if elapsed_min > 1:
            projected_total = (total_score / elapsed_min) * total_game_min
            
            # --- YORGUNLUK / PACE DECAY ÇARPANI (Dinamik Ağırlıklandırma) ---
            if period <= 2:
                decay_factor = 0.94  # 1. ve 2. çeyreklerdeki tempolar sonradan %6 civarı düşer
                projected_total *= decay_factor
            
            # --- Crunch Time & Garbage Time Logic ---
            score_diff = abs(home_score - away_score)
            is_crunch_time = False
            is_garbage_time = False
            last_period = 4 if quarter_length != 20 else 2
            
            if period == last_period and score_diff >= 20:
                is_garbage_time = True
            elif period == last_period and remaining_min <= 3.0 and score_diff <= 12:
                is_crunch_time = True
                projected_total *= 1.15  # %15 faul/kriz anı artışı
                
            pace_ratio = projected_total / inplay_total if inplay_total > 0 else 1.0

            detail = (
                f"Projeksiyon: {projected_total:.0f}, "
                f"Canlı barem: {inplay_total:.0f}, "
                f"Skor: {score}, {status_clean}"
            )
            if is_garbage_time:
                detail += " [Garbage Time: Fark çok yüksek, tempo düşecektir!]"
            elif is_crunch_time:
                detail += " [Taktik Faul Çarpanı Aktif!]"
                
            return _evaluate_pace(direction, pace_ratio, detail, is_crunch_time, is_garbage_time, opening_total, projected_total)

    # ── Fallback: Time unknown ──
    return {
        "level": "unknown", "emoji": "❓", "icon": "?",
        "text": f"Süre verisi net değil, risk/tempo matematiksel olarak hesaplanamıyor. (Skor: {score}, Durum: {status_clean})",
        "recommendation": "PAS GEÇ",
        "rec_emoji": "⛔"
    }


def _evaluate_pace(direction: str, pace_ratio: float, detail: str, is_crunch_time: bool = False, is_garbage_time: bool = False, opening_total: float = 0.0, projected_total: float = 0.0) -> dict:
    """Evaluate risk based on pace_ratio (projected_total / inplay_total)."""
    
    trust_score = ""
    # "ALT" oynayacağız (Canlı barem açılıştan yükseğe çıktı, düşmesini bekliyoruz)
    # Eğer Projeksiyon hala Açılış bareminden küçükse (Bookmaker haklı çıkacaksa), güven yüksektir!
    if direction == "ALT":
        if opening_total > 0:
            if projected_total <= opening_total * 1.03:
                trust_score = " 🎯 [Güven: YÜKSEK - Bookmaker Regresyonu]"
            elif projected_total > opening_total * 1.10:
                trust_score = " ⚠️ [Güven: DÜŞÜK - Projeksiyon çok fırladı]"
                
        if is_garbage_time:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"GÜVENLİ LİMAN: Garbage Time (Maç Koptu), takımlar süreyi eritecek. OYNA ({detail})"}
                    
        if is_crunch_time:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Taktik faul tehlikesi! ALT oynamayın ({detail})"}
                    
        if pace_ratio > 1.12:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Tempo çok yüksek ({detail}){trust_score}"}
        elif pace_ratio > 1.05:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Tempo ortalamanın üstünde ({detail}){trust_score}"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Tempo uyumlu ({detail}){trust_score}"}
    else:  # ÜST oynayacağız (Canlı barem düştü)
        if opening_total > 0:
            if projected_total >= opening_total * 0.97:
                trust_score = " 🎯 [Güven: YÜKSEK - Bookmaker Regresyonu]"
            elif projected_total < opening_total * 0.90:
                trust_score = " ⚠️ [Güven: DÜŞÜK - Projeksiyon çok geride]"
                
        if pace_ratio < 0.88:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Tempo çok düşük ({detail}){trust_score}"}
        elif pace_ratio < 0.95:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Tempo ortalamanın altında ({detail}){trust_score}"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Tempo uyumlu ({detail}){trust_score}"}


def calculate_risk(direction: str, inplay_total: float, opening_total: float, score: str, status: str, match_name: str, tournament: str) -> dict:
    """
    Full risk assessment: base risk + blowout detection + early signal + recommendation.
    Returns dict with: level, emoji, text, icon, recommendation, rec_emoji, warnings, is_blowout, is_early.
    """
    result = _calculate_base_risk(direction, inplay_total, opening_total, score, status, match_name, tournament)

    # ── Blowout detection (score diff >= 25) ──
    is_blowout = False
    if score and score.strip():
        m = re.match(r'(\d+)\s*[-–]\s*(\d+)', score.strip())
        if m:
            is_blowout = abs(int(m.group(1)) - int(m.group(2))) >= 25

    # ── Early signal detection (1st quarter) ──
    is_early = False
    s = (status or "").strip()
    if s:
        q = re.match(r'(?:Q(\d)|(\d)Q)', s, re.IGNORECASE)
        if q:
            is_early = int(q.group(1) or q.group(2)) == 1
        elif re.match(r'^1(?:st)?\b', s, re.IGNORECASE) or re.match(r'^01[-:]', s):
            is_early = True

    # ── Build warnings ──
    warnings = []
    is_garbage_time_opportunity = "GÜVENLİ LİMAN" in result.get("text", "")

    if is_blowout and not is_garbage_time_opportunity:
        warnings.append("💥 Blowout: Skor farkı 25+ puan, çöp sayılar riski var (ÜST için tehlikeli)")
    if is_early:
        warnings.append("⏰ Erken sinyal: 1. çeyrek, güvenilirlik düşük")

    # ── Determine recommendation ──
    level = result["level"]
    if "recommendation" not in result:
        if (is_blowout and not is_garbage_time_opportunity) or level == "high" or level == "unknown":
            rec, rec_emoji = "PAS GEÇ", "⛔"
        elif is_early or level == "medium":
            rec, rec_emoji = "DİKKAT", "⚠️"
        else:
            rec, rec_emoji = "OYNA", "✅"

        result["recommendation"] = rec
        result["rec_emoji"] = rec_emoji

    result["warnings"] = warnings
    result["is_blowout"] = is_blowout
    result["is_early"] = is_early

    return result


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def process_match(
    match: dict,
    db: Database,
    notifier: TelegramNotifier,
    config: Config,
) -> None:
    """
    Processes a single match:
    - Calculates the difference between Opening odds and In-play odds total lines.
    - Sends a Telegram notification if the difference exceeds the threshold.
    - Spawns background AI analysis task.
    """
    match_id = match["match_id"]
    match_name = match["match_name"]
    tournament = match.get("tournament", "")
    opening_total = match["opening_total"]
    inplay_total = match["inplay_total"]
    status = match.get("status", "Canlı")
    url = match.get("url", "")
    score = match.get("score", "")

    log = logging.getLogger("main")

    # Uzatmaya giden (OT) maçları yoksay
    if re.search(r'\bOT\b|Uzatma', status, re.IGNORECASE):
        log.debug("Skipped (Overtime): %s", match_name)
        return

    # Blacklist check
    if config.BLACKLIST:
        check_text = f"{match_name} {tournament} {url}".lower()
        for term in config.BLACKLIST:
            if term in check_text:
                log.debug("Blacklisted (%s): %s", term, match_name)
                return

    diff = inplay_total - opening_total  # positive → line went up, negative → line went down
    abs_diff = abs(diff)
    log.info(
        "📊 %s | Açılış: %.1f | Canlı: %.1f | Fark: %+.1f | Skor: %s | Durum: %s",
        match_name,
        opening_total,
        inplay_total,
        diff,
        score or "-",
        status or "-",
    )
    if abs_diff < config.THRESHOLD:
        return

    if diff >= 0:
        direction = "ALT"
    else:
        direction = "ÜST"

    if db.was_alerted_recently(match_id, direction, config.ALERT_COOLDOWN_MINUTES):
        log.debug(
            "Skipped (cooldown active): id=%s | direction=%s | cooldown=%s min",
            match_id, direction, config.ALERT_COOLDOWN_MINUTES,
        )
        return

    # 1) Count previous alerts for this match (signal number)
    signal_count = db.count_match_alerts(match_id) + 1

    # 1.5) Risk assessment based on score tempo
    risk = calculate_risk(direction, inplay_total, opening_total, score, status, match_name, tournament)
    risk_note = risk["text"]
    if risk.get("warnings"):
        risk_note += "\n" + "\n".join(risk["warnings"])
    recommendation = risk.get("recommendation", "")
    log.info("Risk: %s | Öneri: %s | %s | %s", risk["level"], recommendation, match_name, risk_note)

    # 2) Send instant Telegram alert
    msg_ids = await notifier.send_alert(
        match_name, tournament, opening_total, inplay_total, direction, abs_diff, status,
        score=score, signal_count=signal_count, risk=risk,
    )

    # 3) Save to database
    alert_id = db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score, signal_count=signal_count,
        risk_note=risk_note, recommendation=recommendation,
    )

    log.info(
        "Alert sent: id=%s | name=%s | direction=%s | diff=%.2f",
        match_id, match_name, direction, abs_diff,
    )

    # Yapay zeka analizi devredışı bırakıldı


async def run():
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    setup_logging(config.LOG_LEVEL)
    log = logging.getLogger("main")

    db = Database(config.DB_PATH)
    db.init()

    notifier = TelegramNotifier(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)
    scraper = AiscoreOperaScraper(
        cdp_url=config.OPERA_CDP_URL,
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=config.MAX_MATCHES_PER_CYCLE,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        browser_mode=config.BROWSER_MODE,
        opera_binary=config.OPERA_BINARY,
        cdp_port=config.OPERA_CDP_PORT,
    )

    await notifier.send_startup()
    log.info(
        "Bot started. Threshold: %s pts | Poll interval: %s-%ss | CDP: %s",
        config.THRESHOLD,
        config.POLL_INTERVAL_MIN,
        config.POLL_INTERVAL_MAX,
        config.OPERA_CDP_URL,
    )

    consecutive_errors = 0

    while True:
        try:
            matches = await scraper.get_live_basketball_totals()
            log.info("Captured opening/in-play totals for %s matches.", len(matches))

            for match in matches:
                await process_match(match, db, notifier, config)

            consecutive_errors = 0

        except KeyboardInterrupt:
            log.info("Bot stopped.")
            break
        except Exception as e:
            consecutive_errors += 1
            log.error(f"Loop error (#{consecutive_errors}): {e}", exc_info=True)
            if consecutive_errors >= 5:
                await notifier.send_error(f"{consecutive_errors} consecutive errors: {e}")
                consecutive_errors = 0  # Reset and retry

        delay = random.uniform(config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX)
        log.debug(f"Next check in {delay:.0f}s")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
