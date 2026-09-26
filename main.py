"""
main.py — Basketball odds monitoring bot main loop.

Usage:
    python main.py
"""

import asyncio
import json
import logging
import math
import random
import re
import sys
import time
from datetime import datetime, timezone

from aiscore_scraper import AiscoreScraper
from config import Config
from db import Database
from notifier import TelegramNotifier
from match_state import (
    confirmed_12_minute_quarters, first_confirmed_12_snapshot_index,
    game_clock, normalize_quarter_scores, parse_score, quarter_clock_seconds,
)
from signal_lists import build_signal_blacklist_matches, build_signal_list_profile
from signal_repeat import live_total_delta
from live_signals import SignalDecision, evaluate_live_signal, valid_total
from signal_quality import score_signal_quality


class _ConsecutiveFailureAlertLatch:
    """Send one alert per outage and re-arm only after a healthy cycle."""

    def __init__(self, threshold: int = 5):
        self.threshold = max(1, int(threshold))
        self.count = 0
        self.alert_sent = False

    def record_failure(self) -> tuple[int, bool]:
        self.count += 1
        should_alert = self.count >= self.threshold and not self.alert_sent
        if should_alert:
            self.alert_sent = True
        return self.count, should_alert

    def record_success(self) -> int:
        previous_count = self.count
        self.count = 0
        self.alert_sent = False
        return previous_count


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # python-telegram-bot uses httpx; its INFO request log contains the bot
    # token in the URL path. Keep transport internals below WARNING and rely on
    # our credential-safe notifier outcome logs instead.
    for logger_name in ("httpx", "httpcore", "telegram.request"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # Scrapling installs its own handler and also propagates to the root logger,
    # which duplicates every line. It also reports the normal "no challenge"
    # case as ERROR. Keep real Scrapling errors while removing that false alarm.
    scrapling_logger = logging.getLogger("scrapling")
    scrapling_logger.propagate = False

    class _ScraplingNoiseFilter(logging.Filter):
        def filter(self, record):
            return record.getMessage() != "No Cloudflare challenge found."

    for handler in scrapling_logger.handlers:
        handler.addFilter(_ScraplingNoiseFilter())


def _normalize_match_payload(match: dict) -> dict:
    """Validate one scraper record without letting it poison the whole cycle."""
    if not isinstance(match, dict):
        raise ValueError("match payload must be a dictionary")

    normalized = dict(match)
    for key in ("match_id", "match_name"):
        value = str(match.get(key) or "").strip()
        if not value:
            raise ValueError(f"missing required field: {key}")
        normalized[key] = value

    for key in ("tournament", "status", "url", "score"):
        normalized[key] = str(match.get(key) or "").strip()

    for key in ("opening_total", "inplay_total"):
        raw = match.get(key)
        if isinstance(raw, bool):
            raise ValueError(f"invalid numeric field: {key}")
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid numeric field: {key}") from exc
        if not math.isfinite(value) or value <= 0 or value > 1000:
            raise ValueError(f"out-of-range numeric field: {key}")
        normalized[key] = value

    normalized["prematch_total"] = valid_total(match.get("prematch_total"))

    normalized["quarter_scores"] = normalize_quarter_scores(
        match.get("quarter_scores"),
        normalized.get("score", ""),
    )

    return normalized


def _live_observation_age_seconds(match: dict) -> float | None:
    captured = match.get("_market_captured_monotonic")
    if captured is None or isinstance(captured, bool):
        return None
    try:
        return max(0.0, time.monotonic() - float(captured))
    except (TypeError, ValueError):
        return None


def _scraper_health_summary(scraper) -> dict | None:
    """Expose a bounded, credential-safe scraper report when supported."""
    report = getattr(scraper, "last_report", None)
    if report is None:
        return None
    report_type = type(report).__name__
    if not isinstance(report, dict):
        report = getattr(report, "__dict__", None)
    if not isinstance(report, dict):
        return {"available": True, "type": report_type}

    safe_fields = {
        "status",
        "links_found",
        "matches_found",
        "listing_attempts",
        "discovered_count",
        "reported_live_count",
        "unverified_count",
        "attempted_count",
        "unattempted_count",
        "parsed_count",
        "skipped_count",
        "failed_count",
        "coverage_pct",
        "parse_coverage_pct",
        "emitted_count",
        "effective_concurrency",
        "navigation_failure_count",
        "skip_reason_summary",
        "matches_checked",
        "matches_parsed",
        "matches_succeeded",
        "matches_skipped",
        "matches_failed",
        "success_count",
        "error_count",
        "duration_seconds",
        "elapsed_seconds",
    }
    summary = {
        key: value
        for key, value in report.items()
        if key in safe_fields and isinstance(value, (str, int, float, bool, type(None)))
    }
    return summary or {"available": True, "field_count": len(report)}


def _telegram_delivery_complete(notifier, message_ids: dict) -> bool:
    checker = getattr(notifier, "delivery_complete", None)
    if callable(checker):
        return bool(checker(message_ids or {}))
    # Lightweight test/custom notifiers historically represented a complete
    # delivery with any non-empty mapping.
    return bool(message_ids)


def _match_is_blocked(match, db, config, signal_list_profile) -> bool:
    match_id = match["match_id"]
    match_name = match["match_name"]
    tournament = match.get("tournament", "")
    url = match.get("url", "")

    log = logging.getLogger("main")

    if db.is_match_deleted(match_id):
        log.debug("Skipped (deleted match): %s", match_name)
        return True

    if signal_list_profile is None:
        list_entries = getattr(db, "list_signal_list_entries", None)
        signal_list_profile = build_signal_list_profile(
            list_entries() if callable(list_entries) else []
        )
    blacklist_matches = build_signal_blacklist_matches(match, signal_list_profile)
    if blacklist_matches:
        matched = ", ".join(
            f"{item.get('scope')}={item.get('value')}"
            for item in blacklist_matches
        )
        log.info("Skipped (dashboard blacklist: %s): %s", matched, match_name)
        return True

    if config.BLACKLIST:
        check_text = f"{match_name} {tournament} {url}".lower()
        for term in config.BLACKLIST:
            if term in check_text:
                log.debug("Blacklisted (%s): %s", term, match_name)
                return True
    return False


def _next_signal_count(match, decision, db, config) -> int | None:
    match_id = match["match_id"]
    match_name = match["match_name"]
    inplay_total = match["inplay_total"]
    direction = decision.direction
    period = decision.period
    log = logging.getLogger("main")

    total_alerts = db.count_match_alerts(match_id)
    if total_alerts >= config.MAX_SIGNALS_PER_MATCH:
        log.debug(
            "Skipped (max signals reached): id=%s match=%s total_alerts=%s max=%s",
            match_id, match_name, total_alerts, config.MAX_SIGNALS_PER_MATCH,
        )
        return

    period_has_any_alert = period is not None and db.was_alerted_in_period(match_id, period)
    if period_has_any_alert:
        log.debug("Skipped (period %s already alerted): id=%s", period, match_id)
        return

    signal_count = total_alerts + 1
    previous_same_direction = db.latest_match_alert_in_direction(match_id, direction)
    if previous_same_direction:
        previous_live = previous_same_direction.get("live")
        repeat_delta = live_total_delta(inplay_total, previous_live)
        if repeat_delta is None:
            log.debug(
                "Skipped (same-direction live delta unavailable): id=%s match=%s direction=%s current=%r previous=%r",
                match_id, match_name, direction, inplay_total, previous_live,
            )
            return

        if repeat_delta < config.SAME_DIRECTION_MIN_LIVE_DELTA:
            log.debug(
                "Skipped (same-direction live delta too small): id=%s match=%s direction=%s delta=%.1f min=%.1f",
                match_id, match_name, direction, repeat_delta, config.SAME_DIRECTION_MIN_LIVE_DELTA,
            )
            return

    return signal_count


def _save_signal(match: dict, decision: SignalDecision, signal_count: int, db: Database,
                 config: Config, observation_age_seconds: float | None = None) -> int:
    repeated = signal_count > 1 and db.latest_match_alert_in_direction(match["match_id"], decision.direction) is not None
    score, label, factors = score_signal_quality(
        match, decision, config, repeated=repeated, observation_age_seconds=observation_age_seconds,
    )
    return db.save_alert(
        match["match_id"], match["match_name"], match["opening_total"], match["inplay_total"],
        decision.direction, abs(decision.diff),
        tournament=match["tournament"], status=match["status"], url=match["url"], score=match["score"],
        signal_count=signal_count, prematch=match["prematch_total"],
        alert_period=decision.period,
        alert_moment=" | ".join(p for p in (match["status"], match["score"]) if p),
        telegram_required=True,
        quarter_scores=match["quarter_scores"],
        reference_used=decision.reference_used,
        reference_total=decision.reference_total,
        effective_threshold=decision.effective_threshold,
        fair_total=decision.sustainable_projection_center,
        quality_score=score, quality_label=label, quality_version="v1", quality_factors=factors,
    )


async def _deliver_signal(match, decision, signal_count, alert_id, db, notifier):
    followed_upcoming = db.is_upcoming_followed(match["match_id"])

    message_ids = {}
    try:
        delivered = await notifier.send_alert(
            match["match_name"], match["tournament"], match["opening_total"], match["inplay_total"],
            decision.direction, decision.diff, match["status"],
            score=match["score"], signal_count=signal_count, prematch=match["prematch_total"],
            period=decision.period,
            followed_upcoming=followed_upcoming,
            reference_used=decision.reference_used,
            reference_total=decision.reference_total,
            effective_threshold=decision.effective_threshold,
            market_future_pace=decision.market_implied_pace,
            future_pace_lower=decision.pace_lower_bound,
            future_pace_upper=decision.pace_upper_bound,
            fair_total=decision.sustainable_projection_center,
        )
    except Exception as exc:
        db.mark_telegram_delivery_failed(
            alert_id,
            f"{type(exc).__name__}: notifier delivery failed",
        )
        raise
    if isinstance(delivered, dict) and _telegram_delivery_complete(notifier, delivered):
        message_ids = delivered
        db.mark_telegram_delivery_sent(alert_id, message_ids)
    else:
        db.mark_telegram_delivery_failed(
            alert_id,
            "Notifier did not deliver to every configured recipient.",
            **({"message_ids": delivered} if isinstance(delivered, dict) and delivered else {}),
        )

    logging.getLogger("main").info(
        "Signal saved (telegram=%s%s): alert_id=%s match_id=%s | %s | %s | diff=%.2f",
        "sent" if message_ids else "not-sent",
        " · followed" if followed_upcoming else "",
        alert_id, match["match_id"], match["match_name"], decision.direction, abs(decision.diff),
    )


async def process_match(
    match: dict,
    db: Database,
    notifier: TelegramNotifier,
    config: Config,
    signal_list_profile: dict | None = None,
) -> None:
    observation_age = _live_observation_age_seconds(match)
    max_age = float(getattr(config, "MAX_LIVE_OBSERVATION_AGE_SECONDS", 20.0))
    if observation_age is not None and observation_age > max_age:
        logging.getLogger("main").warning(
            "Skipped stale market observation: match=%s age=%.1fs max=%.1fs",
            str(match.get("match_name") or match.get("match_id") or "unknown"),
            observation_age,
            max_age,
        )
        return
    match = _normalize_match_payload(match)
    if _match_is_blocked(match, db, config, signal_list_profile):
        return
    if match.get("_market_stale"):
        logging.getLogger("main").warning(
            "Skipped stale market observation after list filters: match=%s reason=%s",
            match["match_name"],
            str(match.get("_market_stale_reason") or "stale_inplay_total"),
        )
        return

    previous_snapshots = db.get_match_snapshots(match["match_id"])
    configured_clock = game_clock("", tournament=match["tournament"])
    four_quarters = configured_clock["period_count"] == 4
    observed_seconds = quarter_clock_seconds(match["status"]) if four_quarters else None
    first_confirmed_index = first_confirmed_12_snapshot_index(previous_snapshots, match["tournament"])
    frozen_12_minutes = first_confirmed_index is not None
    observed_12_minutes = observed_seconds is not None and observed_seconds > 10 * 60
    if observed_12_minutes and configured_clock["quarter_length"] == 10 and not frozen_12_minutes:
        logging.getLogger("main").warning(
            "DURATION_FORMAT_OVERRIDE match_id=%s tournament=%s configured=40 observed=48 clock=%02d:%02d",
            match["match_id"], match["tournament"], *divmod(observed_seconds, 60),
        )
    runtime_override = (
        configured_clock["quarter_length"] == 10
        and (frozen_12_minutes or observed_12_minutes)
    )
    with confirmed_12_minute_quarters(runtime_override):
        await _process_match_with_format(match, db, notifier, config, observation_age, runtime_override)


async def _process_match_with_format(match, db, notifier, config, observation_age, runtime_override):
    # Save snapshot
    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    period = clock["period"]
    home_score, away_score = parse_score(match.get("score", ""))
    elapsed_sec = 0
    remaining_min = 0.0
    if period and home_score is not None and clock.get("quarter_length") and clock.get("remaining_min") is not None:
        ql = clock["quarter_length"]
        pc = clock["period_count"]
        rem = clock["remaining_min"]
        elap_min = (period - 1) * ql + (ql - rem)
        elapsed_sec = int(elap_min * 60)
        remaining_min = (pc * ql) - elap_min

        snapshots = db.get_match_snapshots(match["match_id"])
        current_score = home_score + away_score
        if snapshots and elapsed_sec < max(row["elapsed_game_seconds"] for row in snapshots):
            logging.getLogger("main").warning(
                "Skipped regressed game clock: match=%s", match["match_name"]
            )
            return
        captured_at = match.get("market_captured_at")
        if snapshots and captured_at:
            try:
                captured = datetime.fromisoformat(str(captured_at).replace("Z", "+00:00"))
                recorded = datetime.fromisoformat(snapshots[-1]["recorded_at"]).replace(tzinfo=timezone.utc)
                if captured.tzinfo is not None and captured < recorded:
                    logging.getLogger("main").warning(
                        "Skipped out-of-order market observation: match=%s", match["match_name"]
                    )
                    return
            except (TypeError, ValueError):
                pass

        score_decreased = bool(snapshots and current_score < snapshots[-1]["total_score"])
        if len(snapshots) >= 2 and (
            snapshots[-1]["total_score"] < snapshots[-2]["total_score"]
            and elapsed_sec <= snapshots[-1]["elapsed_game_seconds"]
            and current_score < snapshots[-2]["total_score"]
        ):
            return

        db.save_snapshot_if_changed(
            match_id=match["match_id"],
            period=period,
            game_clock=(
                f"{divmod(seconds, 60)[0]:02d}:{divmod(seconds, 60)[1]:02d}"
                if (seconds := quarter_clock_seconds(match["status"])) is not None else ""
            ),
            elapsed_game_seconds=elapsed_sec,
            remaining_minutes=remaining_min,
            home_score=home_score,
            away_score=away_score,
            total_score=home_score + away_score,
            pregame_total=valid_total(match.get("prematch_total")),
            live_total=float(match["inplay_total"]),
            heartbeat_seconds=config.HEARTBEAT_SECONDS
        )
        if score_decreased:
            logging.getLogger("main").warning(
                "Skipped score correction pending confirmation: match=%s", match["match_name"]
            )
            return

    snapshots = db.get_match_snapshots(match["match_id"])
    confirmed_index = (
        first_confirmed_12_snapshot_index(snapshots, match["tournament"])
        if runtime_override else None
    )
    decision_snapshots = snapshots[confirmed_index:] if confirmed_index is not None else snapshots
    decision = evaluate_live_signal(match, decision_snapshots, config)
    logging.getLogger("main").info(
        "📊 %s | Referans (%s): %.1f | Canlı: %.1f | Fark: %+.1f | Eşik: %.2f | Durum: %s | Filtre: %s",
        match["match_name"], decision.reference_used, decision.reference_total,
        match["inplay_total"], decision.diff, decision.effective_threshold,
        match["status"], decision.skip_reason or "passed",
    )
    if decision.skip_reason:
        return
    signal_count = _next_signal_count(match, decision, db, config)
    if signal_count is None:
        return
    alert_id = _save_signal(match, decision, signal_count, db, config, observation_age)
    await _deliver_signal(match, decision, signal_count, alert_id, db, notifier)


async def process_match_batch(
    matches: list,
    db: Database,
    notifier: TelegramNotifier,
    config: Config,
    signal_list_profile: dict | None = None,
) -> dict:
    """Process every scraper item independently and return cycle health counts."""
    log = logging.getLogger("main")
    processed_count = 0
    failed_count = 0
    if signal_list_profile is None:
        list_entries = getattr(db, "list_signal_list_entries", None)
        signal_list_profile = build_signal_list_profile(
            list_entries() if callable(list_entries) else []
        )
    for index, match in enumerate(matches):
        try:
            await process_match(
                match,
                db,
                notifier,
                config,
                signal_list_profile,
            )
            processed_count += 1
        except Exception as exc:
            failed_count += 1
            match_id = match.get("match_id") if isinstance(match, dict) else None
            match_name = match.get("match_name") if isinstance(match, dict) else None
            log.exception(
                "Match processing failed; cycle continues: index=%s match_id=%r match_name=%r error=%s",
                index,
                str(match_id or "")[:120],
                str(match_name or "")[:160],
                exc,
            )

    return {
        "received": len(matches),
        "processed": processed_count,
        "failed": failed_count,
    }


async def retry_pending_telegram_deliveries(
    db: Database,
    notifier: TelegramNotifier,
    *,
    limit: int = 20,
) -> dict:
    """Retry durable alert deliveries without blocking other rows."""
    log = logging.getLogger("main")
    rows = db.pending_telegram_alerts(limit=limit)
    sent = 0
    failed = 0
    cancelled = 0
    list_entries = getattr(db, "list_signal_list_entries", None)
    signal_list_profile = build_signal_list_profile(
        list_entries() if callable(list_entries) else []
    )
    for row in rows:
        alert_id = int(row.get("id") or 0)
        blacklist_matches = build_signal_blacklist_matches(row, signal_list_profile)
        if blacklist_matches:
            matched = ", ".join(
                f"{item.get('scope')}={item.get('value')}"
                for item in blacklist_matches
            )
            db.cancel_telegram_delivery(
                alert_id,
                f"Dashboard blacklist matched: {matched}",
            )
            cancelled += 1
            log.info(
                "Pending Telegram delivery cancelled by dashboard blacklist: alert_id=%s match_id=%s matches=%s",
                alert_id,
                str(row.get("match_id") or "")[:120],
                matched,
            )
            continue
        try:
            stored_message_ids = json.loads(row.get("telegram_message_ids") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_message_ids = {}
        if not isinstance(stored_message_ids, dict):
            stored_message_ids = {}
        try:
            direction = str(row.get("direction") or "")
            clock = game_clock(
                str(row.get("status") or ""),
                str(row.get("match_name") or ""),
                str(row.get("tournament") or ""),
            )
            send_kwargs = {}
            if row.get("reference_used"):
                send_kwargs.update(
                    reference_used=row["reference_used"],
                    reference_total=row.get("reference_total"),
                    effective_threshold=row.get("effective_threshold"),
                )
            if row.get("fair_total") is not None:
                send_kwargs.update(fair_total=row.get("fair_total"))
            recipient_keys = getattr(notifier, "recipient_keys", None)
            if isinstance(recipient_keys, set):
                stored_message_ids = {
                    key: value
                    for key, value in stored_message_ids.items()
                    if key in recipient_keys
                }
                send_kwargs["pending_recipient_keys"] = recipient_keys - set(stored_message_ids)
            delivered = await notifier.send_alert(
                str(row.get("match_name") or ""),
                str(row.get("tournament") or ""),
                float(row.get("opening")),
                float(row.get("live")),
                direction,
                float(row["diff"]) * (1 if direction == "ALT" else -1),
                str(row.get("status") or ""),
                score=str(row.get("score") or ""),
                signal_count=int(row.get("signal_count") or 1),
                prematch=row.get("prematch"),
                period=clock.get("period"),
                followed_upcoming=db.is_upcoming_followed(str(row.get("match_id") or "")),
                **send_kwargs,
            )
            if not isinstance(delivered, dict):
                delivered = {}
            combined_message_ids = {**stored_message_ids, **delivered}
            if not _telegram_delivery_complete(notifier, combined_message_ids):
                db.mark_telegram_delivery_failed(
                    alert_id,
                    "delivery retry did not reach every configured recipient",
                    message_ids=combined_message_ids,
                )
                failed += 1
                continue
            db.mark_telegram_delivery_sent(alert_id, combined_message_ids)
            sent += 1
        except Exception as exc:
            db.mark_telegram_delivery_failed(
                alert_id,
                f"{type(exc).__name__}: delivery retry failed",
                message_ids=stored_message_ids,
            )
            failed += 1
            log.warning(
                "Pending Telegram delivery failed: alert_id=%s match_id=%s error_type=%s",
                alert_id,
                str(row.get("match_id") or "")[:120],
                type(exc).__name__,
            )
    return {
        "pending": len(rows),
        "sent": sent,
        "failed": failed,
        "cancelled": cancelled,
    }


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
        concurrency=config.AISCORE_CONCURRENCY,
        persistent_session=True,
        match_timeout_seconds=config.LIVE_MATCH_TIMEOUT_SECONDS,
        stale_line_seconds=config.LIVE_LINE_STALE_SECONDS,
        stale_score_delta=config.LIVE_LINE_STALE_SCORE_DELTA,
        stale_game_minutes=config.LIVE_LINE_STALE_GAME_MINUTES,
    )

    await notifier.send_startup()
    log.info("Healthy live poll: %.1fs; persistent browser and continuous detail queue enabled.", config.LIVE_POLL_SECONDS)
    log.info(
        "Bot started. Threshold: %s%% (%s, hybrid floor: %s pts) | Q4 disabled: %s | OT disabled | Poll: %s-%ss | Max/match: %s | Same direction: %s pts live-total gap | 1 alert per period",
        config.THRESHOLD_PERCENT, config.THRESHOLD_MODE, config.THRESHOLD, config.DISABLE_Q4_SIGNALS,
        config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX,
        config.MAX_SIGNALS_PER_MATCH, config.SAME_DIRECTION_MIN_LIVE_DELTA,
    )

    failure_alert = _ConsecutiveFailureAlertLatch(threshold=5)

    while True:
        try:
            delivery_summary = await retry_pending_telegram_deliveries(db, notifier)
            if delivery_summary["pending"]:
                log.info(
                    "Telegram outbox: pending=%s sent=%s failed=%s cancelled=%s",
                    delivery_summary["pending"],
                    delivery_summary["sent"],
                    delivery_summary["failed"],
                    delivery_summary["cancelled"],
                )
            cycle_started = time.monotonic()
            cycle_summary = {"received": 0, "processed": 0, "failed": 0}
            signal_list_profile = build_signal_list_profile(db.list_signal_list_entries())
            processing_tasks: list[asyncio.Task] = []

            async def process_captured_match_task(match: dict, index: int) -> None:
                try:
                    await process_match(
                        match,
                        db,
                        notifier,
                        config,
                        signal_list_profile,
                    )
                    cycle_summary["processed"] += 1
                except Exception as exc:
                    cycle_summary["failed"] += 1
                    log.exception(
                        "Match processing failed; live scrape continues: "
                        "index=%s match_id=%r match_name=%r error=%s",
                        index,
                        str(match.get("match_id") or "")[:120],
                        str(match.get("match_name") or "")[:160],
                        exc,
                    )

            async def schedule_captured_match(match: dict) -> None:
                index = cycle_summary["received"]
                cycle_summary["received"] += 1
                processing_tasks.append(asyncio.create_task(
                    process_captured_match_task(match, index)
                ))

            try:
                async with asyncio.timeout(config.LIVE_SCRAPE_TIMEOUT_SECONDS):
                    matches = await scraper.get_live_basketball_totals(
                        on_match=schedule_captured_match,
                    )
            except TimeoutError as exc:
                raise RuntimeError(
                    "AIScore canlı tarama çevrimi "
                    f"{config.LIVE_SCRAPE_TIMEOUT_SECONDS:.0f} saniyeyi aştı ve iptal edildi."
                ) from exc
            finally:
                if processing_tasks:
                    await asyncio.gather(*processing_tasks)
            scrape_seconds = time.monotonic() - cycle_started
            log.info(
                "Captured opening/in-play totals for %s matches in %.1fs.",
                len(matches),
                scrape_seconds,
            )
            health = _scraper_health_summary(scraper)
            if health is not None:
                log.info("Scraper health: %s", health)
            log.info(
                "Cycle processing summary: received=%s processed=%s failed=%s",
                cycle_summary["received"],
                cycle_summary["processed"],
                cycle_summary["failed"],
            )
            health_status = str((health or {}).get("status") or "").lower()
            degraded = (
                cycle_summary["failed"] > 0
                or health_status in {"partial", "error", "failed", "degraded"}
            )
            if degraded:
                consecutive_errors, should_alert = failure_alert.record_failure()
                log.warning(
                    "Degraded scrape cycle (#%s): health=%s failed_rows=%s/%s",
                    consecutive_errors,
                    health_status or "unknown",
                    cycle_summary["failed"],
                    cycle_summary["received"],
                )
                if should_alert:
                    await notifier.send_error(
                        "5 consecutive degraded scrape cycles; check scraper health logs."
                    )
            else:
                previous_failures = failure_alert.record_success()
                consecutive_errors = 0
                if previous_failures:
                    log.info(
                        "Scraper health recovered after %s degraded/error cycle(s).",
                        previous_failures,
                    )

        except KeyboardInterrupt:
            log.info("Bot stopped.")
            break
        except Exception as e:
            consecutive_errors, should_alert = failure_alert.record_failure()
            log.error(f"Loop error (#{consecutive_errors}): {e}", exc_info=True)
            if should_alert:
                await notifier.send_error(f"{consecutive_errors} consecutive errors: {e}")

        # Healthy cycles resume quickly; failures retain the configured backoff.
        delay = config.LIVE_POLL_SECONDS if consecutive_errors == 0 else random.uniform(
            config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX
        )
        log.debug(f"Next check in {delay:.0f}s")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
