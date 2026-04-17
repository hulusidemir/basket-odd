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

from aiscore_scraper import AiscoreScraper
from config import Config
from db import Database
from notifier import TelegramNotifier
from signal_quality import assess_signal_quality


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
    scraper: AiscoreScraper,
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
    prematch_total = match.get("prematch_total")  # None olabilir
    status = match.get("status", "Canlı")
    url = match.get("url", "")
    score = match.get("score", "")

    log = logging.getLogger("main")

    if db.is_match_deleted(match_id):
        log.debug("Skipped (deleted match): %s", match_name)
        return

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

    # PRIMARY comparison: inplay vs prematch (fallback: inplay vs opening)
    baseline = prematch_total if prematch_total is not None else opening_total
    diff = inplay_total - baseline
    abs_diff = abs(diff)
    log.info(
        "📊 %s | Açılış: %.1f | Maç Öncesi: %s | Canlı: %.1f | Fark: %+.1f | Skor: %s | Durum: %s",
        match_name,
        opening_total,
        f"{prematch_total:.1f}" if prematch_total else "-",
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

    try:
        insights = await scraper.get_match_insights(url)
    except Exception as exc:
        log.debug("Could not fetch AIScore insights for %s: %s", match_name, exc)
        insights = {}

    quality = assess_signal_quality(
        {
            **match,
            "direction": direction,
            "baseline": baseline,
        },
        insights,
        config.THRESHOLD,
    )

    # 2) Send instant Telegram alert
    await notifier.send_alert(
        match_name, tournament, opening_total, inplay_total, direction, abs_diff, status,
        score=score, signal_count=signal_count, quality=quality,
        prematch=prematch_total,
        threshold=config.THRESHOLD,
    )

    # 3) Save to database
    db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score, signal_count=signal_count,
        quality_grade=quality["grade"],
        quality_score=quality["score"],
        quality_setup=quality["setup"],
        quality_summary=quality["summary"],
        quality_reasons=quality["reasons_text"],
        counter_direction=quality["counter_direction"],
        counter_level=quality["counter_level"],
        counter_score=quality["counter_score"],
        counter_note=quality["counter_note"],
        counter_reasons=quality["counter_reasons_text"],
        prematch=prematch_total,
    )

    log.info(
        "Alert sent: id=%s | name=%s | direction=%s | diff=%.2f | kalite=%s %.1f",
        match_id, match_name, direction, abs_diff, quality["grade"], quality["score"],
    )


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
    scraper = AiscoreScraper(
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=config.MAX_MATCHES_PER_CYCLE,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
    )

    await notifier.send_startup()
    log.info(
        "Bot started. Threshold: %s pts | Poll interval: %s-%ss",
        config.THRESHOLD,
        config.POLL_INTERVAL_MIN,
        config.POLL_INTERVAL_MAX,
    )

    consecutive_errors = 0

    while True:
        try:
            matches = await scraper.get_live_basketball_totals()
            log.info("Captured opening/in-play totals for %s matches.", len(matches))

            for match in matches:
                await process_match(match, db, notifier, scraper, config)

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
