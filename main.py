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

from cachetools import TTLCache

from aiscore_opera_scraper import AiscoreOperaScraper
from analyzer import get_match_analysis
from config import Config
from db import Database
from notifier import TelegramNotifier

# In-memory cache: one Gemini call per match. Key = match_id, TTL = 4 hours.
_analysis_cache: TTLCache = TTLCache(maxsize=1024, ttl=4 * 3600)


def calculate_risk(direction: str, inplay_total: float, score: str, status: str) -> dict:
    """
    Calculate risk assessment based on score tempo vs live total line.
    Two methods:
      1. Period-based: if period + time can be parsed → project full-game total from pace
      2. Score-ratio fallback: compare total_score / inplay_total regardless of period

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

    # ── Try to parse period from status ──
    period = None
    remaining_min = None
    status_clean = (status or "").strip()

    if status_clean:
        # OT — can't project in overtime
        if re.match(r'^OT', status_clean, re.IGNORECASE):
            return _score_ratio_risk(direction, inplay_total, total_score, score,
                                     status, note="Uzatma (OT)")

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
        quarter_length = 10  # FIBA standard
        elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)
        total_game_min = 4 * quarter_length  # 40 min

        if elapsed_min > 1:
            projected_total = (total_score / elapsed_min) * total_game_min
            pace_ratio = projected_total / inplay_total if inplay_total > 0 else 1.0

            detail = (
                f"Projeksiyon: {projected_total:.0f}, "
                f"Canlı barem: {inplay_total:.0f}, "
                f"Skor: {score}, {status_clean}"
            )
            return _evaluate_pace(direction, pace_ratio, detail)

    # ── Fallback: score-ratio method (no period info) ──
    return _score_ratio_risk(direction, inplay_total, total_score, score, status)


def _evaluate_pace(direction: str, pace_ratio: float, detail: str) -> dict:
    """Evaluate risk based on pace_ratio (projected_total / inplay_total)."""
    if direction == "ALT":
        if pace_ratio > 1.12:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Tempo çok yüksek ({detail})"}
        elif pace_ratio > 1.05:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Tempo ortalamanın üstünde ({detail})"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Tempo uyumlu ({detail})"}
    else:
        if pace_ratio < 0.88:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Tempo çok düşük ({detail})"}
        elif pace_ratio < 0.95:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Tempo ortalamanın altında ({detail})"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Tempo uyumlu ({detail})"}


def _score_ratio_risk(
    direction: str, inplay_total: float, total_score: int,
    score: str, status: str, note: str = "",
) -> dict:
    """
    Fallback risk assessment when period info is unavailable.
    Uses total_score / inplay_total ratio.

    The live barem (inplay_total) is the bookmaker's projection for the full game.
    If a large fraction of it is already scored → pace is high.
    If a small fraction is scored → pace is low.
    """
    if inplay_total <= 0:
        return {"level": "unknown", "emoji": "❓", "icon": "?",
                "text": "Canlı barem verisi geçersiz"}

    score_ratio = total_score / inplay_total
    score_pct = score_ratio * 100

    prefix = f"{note}, " if note else ""
    detail = (
        f"{prefix}Skor toplamı: {total_score}, "
        f"Canlı barem: {inplay_total:.0f}, "
        f"Oran: %{score_pct:.0f}, "
        f"Skor: {score}, "
        f"Durum: {status or '?'}"
    )

    if direction == "ALT":
        # ALT = under. Risky if score ratio is high (pace genuinely fast)
        if score_ratio > 0.60:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Toplam skor baremin %{score_pct:.0f}'ine ulaşmış, tempo yüksek ({detail})"}
        elif score_ratio > 0.50:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Toplam skor baremin yarısını aşmış ({detail})"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Skor/barem oranı makul ({detail})"}
    else:
        # ÜST = over. Risky if score ratio is low (pace genuinely slow)
        if score_ratio < 0.30:
            return {"level": "high", "emoji": "⚠️", "icon": "!",
                    "text": f"Yüksek risk: Toplam skor baremin sadece %{score_pct:.0f}'i, tempo düşük ({detail})"}
        elif score_ratio < 0.40:
            return {"level": "medium", "emoji": "⚠️", "icon": "!",
                    "text": f"Orta risk: Skor/barem oranı düşük ({detail})"}
        else:
            return {"level": "low", "emoji": "✅", "icon": "",
                    "text": f"Düşük risk: Skor/barem oranı makul ({detail})"}


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
    risk = calculate_risk(direction, inplay_total, score, status)
    risk_note = risk["text"]
    log.info("Risk: %s | %s | %s", risk["level"], match_name, risk_note)

    # 2) Send instant Telegram alert
    msg_ids = await notifier.send_alert(
        match_name, tournament, opening_total, inplay_total, direction, abs_diff, status,
        score=score, signal_count=signal_count, risk=risk,
    )

    # 3) Save to database
    alert_id = db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score, signal_count=signal_count,
        risk_note=risk_note,
    )

    log.info(
        "Alert sent: id=%s | name=%s | direction=%s | diff=%.2f",
        match_id, match_name, direction, abs_diff,
    )

    # 3) Spawn background AI analysis (non-blocking) — one per match
    if config.GEMINI_API_KEY:
        if match_id in _analysis_cache:
            log.debug("AI skipped (already analyzed): match_id=%s | %s", match_id, match_name)
        else:
            _analysis_cache[match_id] = True
            log.info("AI analysis queued (first time): match_id=%s | %s", match_id, match_name)
            asyncio.create_task(
                _run_analysis(
                    config, db, notifier, alert_id, msg_ids,
                    match_name, tournament, score, opening_total, inplay_total, diff, direction, status,
                )
            )


async def _run_analysis(
    config: Config,
    db: Database,
    notifier: TelegramNotifier,
    alert_id: int,
    msg_ids: dict,
    match_name: str,
    tournament: str,
    score: str,
    opening: float,
    inplay: float,
    diff: float,
    direction: str,
    status: str,
):
    """Background task: get Gemini analysis, send as Telegram reply, save to DB."""
    log = logging.getLogger("main")
    try:
        analysis = await get_match_analysis(
            api_key=config.GEMINI_API_KEY,
            model=config.GEMINI_MODEL,
            match_name=match_name,
            tournament=tournament,
            score=score,
            opening=opening,
            inplay=inplay,
            diff=diff,
            direction=direction,
            status=status,
        )
        if analysis:
            await notifier.send_analysis(analysis, match_name, reply_to=msg_ids)
            db.update_analysis(alert_id, analysis)
            log.info("AI analysis saved for alert #%s: %s", alert_id, match_name)
        else:
            log.warning("Empty AI analysis for: %s", match_name)
    except Exception as e:
        log.error("Background analysis error for %s: %s", match_name, e)


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
