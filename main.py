"""
main.py — Basketball odds monitoring bot main loop.

Usage:
    python main.py
"""

import asyncio
import json
import logging
import random
import re
import sys

from aiscore_scraper import AiscoreScraper
from config import Config
from db import Database
from notifier import TelegramNotifier
from projection import game_clock
from signal_analysis import build_signal_analysis


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
    match_id = match["match_id"]
    match_name = match["match_name"]
    tournament = match.get("tournament", "")
    opening_total = match["opening_total"]
    inplay_total = match["inplay_total"]
    prematch_total = match.get("prematch_total")
    status = match.get("status", "Canlı")
    url = match.get("url", "")
    score = match.get("score", "")

    log = logging.getLogger("main")

    if db.is_match_deleted(match_id):
        log.debug("Skipped (deleted match): %s", match_name)
        return

    if re.search(r'\bOT\b|Uzatma', status, re.IGNORECASE):
        log.debug("Skipped (Overtime): %s", match_name)
        return

    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    quarter_length = clock["quarter_length"]

    if period == 4 and remaining_min is not None and remaining_min < 5:
        log.debug("Skipped (Q4 under 5:00): %s", match_name)
        return

    if period == 1 and remaining_min is not None and (quarter_length - remaining_min) < 4:
        log.debug("Skipped (Q1 first 4:00 — pace unstable): %s", match_name)
        return

    if config.BLACKLIST:
        check_text = f"{match_name} {tournament} {url}".lower()
        for term in config.BLACKLIST:
            if term in check_text:
                log.debug("Blacklisted (%s): %s", term, match_name)
                return

    diff = inplay_total - opening_total
    abs_diff = abs(diff)

    log.info(
        "📊 %s | Açılış: %.1f | Canlı: %.1f | Fark: %+.1f | Skor: %s | Durum: %s",
        match_name, opening_total, inplay_total, diff, score or "-", status or "-",
    )

    if abs_diff < config.THRESHOLD:
        return

    direction = "ALT" if diff > 0 else "ÜST"

    total_alerts = db.count_match_alerts(match_id)
    if total_alerts >= config.MAX_SIGNALS_PER_MATCH:
        log.debug("Skipped (match cap %s reached): id=%s", config.MAX_SIGNALS_PER_MATCH, match_id)
        return

    if period is not None and db.was_alerted_in_period(match_id, period):
        log.debug("Skipped (period %s already alerted): id=%s", period, match_id)
        return

    signal_count = total_alerts + 1
    context = {"h2h": {"body_text": match.get("h2h_body_text", "")}}
    analysis = build_signal_analysis(
        {
            **match,
            "direction": direction,
            "opening_total": opening_total,
            "inplay_total": inplay_total,
            "prematch_total": prematch_total,
        },
        context,
        config.THRESHOLD,
    )

    await notifier.send_alert(
        match_name, tournament, opening_total, inplay_total, direction, abs_diff, status,
        score=score, signal_count=signal_count, prematch=prematch_total, analysis=analysis,
    )

    db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score,
        signal_count=signal_count, prematch=prematch_total,
        ai_analysis=json.dumps(analysis, ensure_ascii=False),
        alert_period=period,
        alert_moment=status,
    )

    log.info(
        "Alert sent: id=%s | %s | %s | diff=%.2f | fair_line=%s",
        match_id, match_name, direction, abs_diff, analysis.get("fair_line"),
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
        "Bot started. Threshold: %s pts | Poll: %s-%ss | Max/match: %s | 1 alert per period",
        config.THRESHOLD, config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX,
        config.MAX_SIGNALS_PER_MATCH,
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
                consecutive_errors = 0

        delay = random.uniform(config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX)
        log.debug(f"Next check in {delay:.0f}s")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
