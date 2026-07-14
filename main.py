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

from aiscore_scraper import AiscoreScraper
from config import Config
from db import Database
from notifier import TelegramNotifier
from pace_tracker import PaceTracker
from projection import game_clock, parse_score
from signal_analysis import build_signal_analysis
from signal_gate import DEFAULT_GATE_POLICY, build_gate_evidence, evaluate_signal_gate
from signal_quality import calculate_signal_quality
from signal_repeat import live_total_delta


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

    prematch = match.get("prematch_total")
    if prematch is not None and str(prematch).strip() != "":
        try:
            parsed_prematch = float(prematch)
        except (TypeError, ValueError):
            parsed_prematch = None
        normalized["prematch_total"] = (
            parsed_prematch
            if parsed_prematch is not None
            and math.isfinite(parsed_prematch)
            and 0 < parsed_prematch <= 1000
            else None
        )

    return normalized


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


async def process_match(
    match: dict,
    db: Database,
    notifier: TelegramNotifier,
    config: Config,
    pace_tracker: PaceTracker | None = None,
    backtest_profile: dict | None = None,
    gate_evidence: dict | None = None,
) -> None:
    match = _normalize_match_payload(match)
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

    if period is None:
        # Status boş ise yayın henüz canlı sayfaya gelmemiş demek (gürültü değil).
        # Status dolu ama parse edilemediyse format değişmiş, ilgilenmek lazım.
        if status:
            log.warning(
                "Status format tanınmadı (çeyrek bilgisi okunamadı): %s | status=%r | score=%r",
                match_name, status, score,
            )
        else:
            log.debug(
                "Skipped (henüz canlı status yok): %s | score=%r",
                match_name, score,
            )
        return

    if period == 4 and remaining_min is not None and remaining_min < 5:
        log.debug("Skipped (Q4 under 5:00): %s", match_name)
        return

    if period == 1 and remaining_min is not None and (quarter_length - remaining_min) < 4:
        log.debug("Skipped (Q1 first 4:00 — pace unstable): %s", match_name)
        return

    # Çeyrek hız takibini güncelle
    pace_data: dict | None = None
    if pace_tracker is not None and period is not None:
        home_score, away_score = parse_score(match.get("score", ""))
        if home_score is not None and away_score is not None:
            pace_data = pace_tracker.update(
                match_id,
                period,
                home_score + away_score,
                quarter_length,
                remaining_min=remaining_min,
            )

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

    legacy_direction = "ALT" if diff > 0 else "ÜST"

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
    context = {"h2h": {"body_text": match.get("h2h_body_text", "")}}
    analysis = build_signal_analysis(
        {
            **match,
            "direction": legacy_direction,
            "opening_total": opening_total,
            "inplay_total": inplay_total,
            "prematch_total": prematch_total,
            "signal_count": signal_count,
        },
        context,
        config.THRESHOLD,
        pace_data=pace_data,
        backtest_profile=backtest_profile,
    )
    direction = analysis.get("direction") or legacy_direction
    previous_directions = []
    active_alerts_for_match = getattr(db, "active_alerts_for_match", None)
    if callable(active_alerts_for_match):
        previous_directions = [
            row.get("direction")
            for row in active_alerts_for_match(match_id)
            if row.get("direction")
        ]
    quality = calculate_signal_quality(
        {
            **match,
            **analysis,
            "opening": opening_total,
            "prematch": prematch_total,
            "live": inplay_total,
            "direction": direction,
            "signal_count": signal_count,
            "previous_directions": previous_directions,
        }
    )
    gate = evaluate_signal_gate(
        {
            **match,
            "signal_count": signal_count,
        },
        analysis,
        quality,
        gate_evidence,
    )
    analysis = {
        **analysis,
        "direction": direction,
        "final_direction": direction,
        "signal_quality": quality,
        "signal_gate": gate,
    }

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

    alert_id = db.save_alert(
        match_id, match_name, opening_total, inplay_total, direction, abs_diff,
        tournament=tournament, status=status, url=url, score=score,
        signal_count=signal_count, prematch=prematch_total,
        ai_analysis=json.dumps(analysis, ensure_ascii=False),
        alert_period=period,
        alert_moment=" | ".join(p for p in (status, score) if p),
        telegram_required=True,
    )

    followed_upcoming = db.is_upcoming_followed(match_id)

    message_ids = {}
    try:
        delivered = await notifier.send_alert(
            match_name, tournament, opening_total, inplay_total, direction, diff, status,
            score=score, signal_count=signal_count, prematch=prematch_total, analysis=analysis,
            period=period,
            followed_upcoming=followed_upcoming,
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

    log.info(
        "Signal saved (gate=%s telegram=%s%s): alert_id=%s match_id=%s | %s | %s | diff=%.2f | fair_line=%s",
        gate.get("state"),
        "sent" if message_ids else "not-sent",
        " · followed" if followed_upcoming else "",
        alert_id, match_id, match_name, direction, abs_diff, analysis.get("fair_line"),
    )


async def process_match_batch(
    matches: list,
    db: Database,
    notifier: TelegramNotifier,
    config: Config,
    pace_tracker: PaceTracker,
    backtest_profile: dict | None = None,
    gate_evidence: dict | None = None,
) -> dict:
    """Process every scraper item independently and return cycle health counts."""
    log = logging.getLogger("main")
    processed_count = 0
    failed_count = 0
    for index, match in enumerate(matches):
        try:
            await process_match(
                match,
                db,
                notifier,
                config,
                pace_tracker,
                backtest_profile,
                gate_evidence,
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

    active_match_ids = {
        str(match.get("match_id") or "").strip()
        for match in matches
        if isinstance(match, dict) and str(match.get("match_id") or "").strip()
    }
    pruned_count = pace_tracker.prune(
        active_match_ids=active_match_ids if matches else None
    )
    return {
        "received": len(matches),
        "processed": processed_count,
        "failed": failed_count,
        "pace_states_pruned": pruned_count,
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
    for row in rows:
        alert_id = int(row.get("id") or 0)
        try:
            stored_message_ids = json.loads(row.get("telegram_message_ids") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            stored_message_ids = {}
        if not isinstance(stored_message_ids, dict):
            stored_message_ids = {}
        try:
            analysis = json.loads(row.get("ai_analysis") or "{}")
            if not isinstance(analysis, dict):
                raise ValueError("stored analysis is not an object")
            direction = str(
                analysis.get("final_direction")
                or analysis.get("direction")
                or row.get("direction")
                or ""
            )
            clock = game_clock(
                str(row.get("status") or ""),
                str(row.get("match_name") or ""),
                str(row.get("tournament") or ""),
            )
            send_kwargs = {}
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
                float(row.get("live")) - float(row.get("opening")),
                str(row.get("status") or ""),
                score=str(row.get("score") or ""),
                signal_count=int(row.get("signal_count") or 1),
                prematch=row.get("prematch"),
                analysis=analysis,
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
    return {"pending": len(rows), "sent": sent, "failed": failed}


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
    )
    pace_tracker = PaceTracker()

    await notifier.send_startup()
    log.info(
        "Bot started. Threshold: %s pts | Poll: %s-%ss | Max/match: %s | Same direction: %s pts live-total gap | 1 alert per period",
        config.THRESHOLD, config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX,
        config.MAX_SIGNALS_PER_MATCH, config.SAME_DIRECTION_MIN_LIVE_DELTA,
    )

    failure_alert = _ConsecutiveFailureAlertLatch(threshold=5)

    while True:
        try:
            delivery_summary = await retry_pending_telegram_deliveries(db, notifier)
            if delivery_summary["pending"]:
                log.info(
                    "Telegram outbox: pending=%s sent=%s failed=%s",
                    delivery_summary["pending"],
                    delivery_summary["sent"],
                    delivery_summary["failed"],
                )
            cycle_started = time.monotonic()
            matches = await scraper.get_live_basketball_totals()
            scrape_seconds = time.monotonic() - cycle_started
            log.info(
                "Captured opening/in-play totals for %s matches in %.1fs.",
                len(matches),
                scrape_seconds,
            )
            policy = DEFAULT_GATE_POLICY
            gate_evidence = build_gate_evidence(db.signal_trial_rows(
                policy_id=policy.policy_id,
                strategy_id=policy.strategy_id,
                strategy_version=policy.strategy_version,
                evidence_epoch=policy.evidence_epoch,
                limit=policy.evidence_window,
            ))

            cycle_summary = await process_match_batch(
                matches,
                db,
                notifier,
                config,
                pace_tracker,
                None,
                gate_evidence,
            )
            if cycle_summary["pace_states_pruned"]:
                log.info(
                    "Pruned %s stale pace-tracker state(s).",
                    cycle_summary["pace_states_pruned"],
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

        delay = random.uniform(config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX)
        log.debug(f"Next check in {delay:.0f}s")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
