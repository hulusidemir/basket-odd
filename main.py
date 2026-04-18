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
from signal_engine import decide_signal
from signal_features import build_signal_features
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
    - Calculates the difference between reference and in-play total lines.
    - Sends a Telegram notification if the difference exceeds the threshold.
    - Scores the signal with live context and market projection.
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

    clock = game_clock(status, match_name, tournament)
    if clock["period"] == 4 and clock["remaining_min"] is not None and clock["remaining_min"] < 5:
        log.debug(
            "Skipped (Q4 under 5:00): %s | status=%s | remaining=%.2f",
            match_name,
            status,
            clock["remaining_min"],
        )
        return

    # Blacklist check
    if config.BLACKLIST:
        check_text = f"{match_name} {tournament} {url}".lower()
        for term in config.BLACKLIST:
            if term in check_text:
                log.debug("Blacklisted (%s): %s", term, match_name)
                return

    # PRIMARY comparison: in-play vs prematch (fallback: in-play vs opening)
    baseline = prematch_total if prematch_total is not None else opening_total
    baseline_label = "Maç Öncesi" if prematch_total is not None else "Açılış"
    diff = inplay_total - baseline
    abs_diff = abs(diff)
    recent_snapshots = db.recent_match_snapshots(match_id, limit=8)
    features = build_signal_features(
        match,
        reference_total=baseline,
        reference_label=baseline_label,
        recent_snapshots=recent_snapshots,
        blowout_margin_threshold=config.BLOWOUT_MARGIN_THRESHOLD,
        late_game_minutes_threshold=config.LATE_GAME_MINUTES_THRESHOLD,
        foul_game_score_diff_threshold=config.FOUL_GAME_SCORE_DIFF_THRESHOLD,
    )
    db.save_match_snapshot(
        match_id=match_id,
        match_name=match_name,
        tournament=tournament,
        status=status,
        score=score,
        opening=opening_total,
        prematch=prematch_total,
        live=inplay_total,
        elapsed_minutes=features.elapsed_minutes,
        total_score=features.current_total_score,
    )
    log.info(
        "📊 %s | Açılış: %.1f | Maç Öncesi: %s | Referans: %s %.1f | Canlı: %.1f | Fark: %+.1f | Skor: %s | Durum: %s | Tempo: %s | Son tempo: %s",
        match_name,
        opening_total,
        f"{prematch_total:.1f}" if prematch_total is not None else "-",
        baseline_label,
        baseline,
        inplay_total,
        diff,
        score or "-",
        status or "-",
        f"{features.current_points_per_minute:.2f}" if features.current_points_per_minute is not None else "-",
        f"{features.recent_points_per_minute:.2f} ({features.recent_pace_trend})" if features.recent_points_per_minute is not None else "-",
    )
    if abs_diff < config.THRESHOLD:
        return

    try:
        insights = await scraper.get_match_insights(url)
    except Exception as exc:
        log.debug("Could not fetch AIScore insights for %s: %s", match_name, exc)
        insights = {}

    quality_by_direction = {}
    for candidate_direction in ("ALT", "ÜST"):
        quality_by_direction[candidate_direction] = assess_signal_quality(
            {
                **match,
                "direction": candidate_direction,
                "baseline": baseline,
                "baseline_label": baseline_label,
            },
            insights,
            config.THRESHOLD,
        )

    decision = decide_signal(features, quality_by_direction, config)
    if not decision.should_alert or not decision.direction:
        log.info(
            "PASS: %s | action=%s | confidence=%.1f | reason=%s",
            match_name,
            decision.action,
            decision.confidence,
            decision.reason,
        )
        return

    direction = decision.direction

    if db.was_alerted_recently(match_id, direction, config.ALERT_COOLDOWN_MINUTES):
        log.debug(
            "Skipped (cooldown active): id=%s | direction=%s | cooldown=%s min",
            match_id, direction, config.ALERT_COOLDOWN_MINUTES,
        )
        return

    # 1) Count previous alerts for this match (signal number)
    signal_count = db.count_match_alerts(match_id) + 1

    quality = quality_by_direction[direction]
    original_summary = quality.get("summary", "")
    setup_label = {
        "OVER": "Continuation ÜST",
        "UNDER": "Continuation ALT",
        "CONTRARIAN_OVER": "Contrarian ÜST",
        "CONTRARIAN_UNDER": "Contrarian ALT",
    }.get(decision.action, decision.action)
    quality["grade"] = decision.grade
    quality["score"] = decision.confidence
    quality["setup"] = setup_label
    quality["summary"] = f"{decision.reason} Risk: {decision.risk_note}" + (
        f" | {original_summary}" if original_summary else ""
    )
    quality["decision"] = decision.to_dict()
    quality["features"] = features.to_dict()

    # 2) Send Telegram alert
    await notifier.send_alert(
        match_name, tournament, opening_total, inplay_total, direction, abs_diff, status,
        score=score, signal_count=signal_count, quality=quality,
        prematch=prematch_total,
        baseline=baseline,
        baseline_label=baseline_label,
        threshold=config.THRESHOLD,
    )

    # 3) Save to database
    team_context_json = json.dumps(quality.get("team_context") or {}, ensure_ascii=False) if quality.get("team_context") else ""
    opposing_signals_json = json.dumps(quality.get("opposing_signals") or [], ensure_ascii=False)
    db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score, signal_count=signal_count,
        quality_grade=quality["grade"],
        quality_score=quality["score"],
        quality_setup=quality["setup"],
        quality_summary=quality["summary"],
        quality_reasons=quality["reasons_text"],
        opposing_signals=opposing_signals_json,
        team_context=team_context_json,
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
