"""
finished_match_service.py — Shared finished match checking service.
Used by both the background worker and manual UI-triggered checks.
"""

import logging
import threading

from aiscore_final_scraper import (
    AiscoreFinishedMatchChecker,
    FinishedCheckBusy,
    final_status_label,
    is_final_status,
    parse_score_total,
)


logger = logging.getLogger("finished_match_service")
_active_finished_scan_lock = threading.Lock()


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def evaluate_signal_result(direction: str, live_line: float, final_total: float) -> str:
    direction_key = (direction or "").strip().upper()
    if abs(final_total - live_line) < 0.0001:
        return "İade"
    if direction_key == "ALT":
        return "Başarılı" if final_total < live_line else "Başarısız"
    if direction_key in {"ÜST", "UST"}:
        return "Başarılı" if final_total > live_line else "Başarısız"
    return ""


def _float_or_none(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def canonical_alert_direction(alert: dict) -> str:
    """Return the stored playable direction for result settlement."""
    return _normalize_direction(alert.get("direction"))


def _empty_result_summary(tracked_count: int = 0) -> dict:
    return {
        "tracked_count": tracked_count,
        "checked_count": 0,
        "check_failed_count": 0,
        "retry_count": 0,
        "coverage_percent": 100.0 if tracked_count == 0 else 0.0,
        "failure_counts": {},
        "check_failures": [],
        "finished_match_count": 0,
        "updated_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "in_progress_count": 0,
        "details": [],
    }


def _finished_checker(config) -> "AiscoreFinishedMatchChecker":
    return AiscoreFinishedMatchChecker(
        page_timeout_ms=getattr(
            config,
            "FINISHED_PAGE_TIMEOUT_MS",
            min(int(config.PAGE_TIMEOUT_MS), 20000),
        ),
        # The proxy is substantially more reliable with one match page at a
        # time. Sequential checks prioritize complete coverage over raw speed.
        concurrency=1,
        retry_attempts=getattr(config, "FINISHED_RETRY_ATTEMPTS", 1),
    )


def _merge_checker_report(
    summary: dict,
    checker: "AiscoreFinishedMatchChecker",
    attempted_count: int,
    results: list[dict],
) -> None:
    report = checker.last_report
    if report.get("attempted_count") == attempted_count:
        checked_count = int(report.get("checked_count", len(results)))
        failed_count = int(
            report.get("check_failed_count", max(0, attempted_count - checked_count))
        )
        retry_count = int(report.get("retry_count", 0))
        failure_counts = dict(report.get("failure_counts") or {})
        failures = list(report.get("failures") or [])
    else:
        # Keep custom checker implementations and tests compatible without
        # describing missing results as successfully checked.
        checked_count = len(results)
        failed_count = max(0, attempted_count - checked_count)
        retry_count = 0
        failure_counts = {"unknown_error": failed_count} if failed_count else {}
        failures = []

    summary["checked_count"] += checked_count
    summary["check_failed_count"] += failed_count
    summary["retry_count"] += retry_count
    for error_code, count in failure_counts.items():
        summary["failure_counts"][error_code] = (
            summary["failure_counts"].get(error_code, 0) + int(count)
        )
    summary["check_failures"].extend(failures)
    summary["coverage_percent"] = (
        round(summary["checked_count"] / summary["tracked_count"] * 100, 1)
        if summary["tracked_count"]
        else 100.0
    )


def _settle_deleted_match_from_final_score(
    db,
    summary: dict,
    match_id: str,
    final_score: str,
    final_status: str,
    *,
    count_checked: bool = True,
    force: bool = False,
) -> bool:
    if not match_id or not is_final_status(final_status):
        return False
    final_total = parse_score_total(final_score)
    if final_total is None:
        return False

    alerts = (
        db.get_deleted_alerts_for_match(match_id)
        if force
        else db.get_deleted_alerts_for_result_check(match_id)
    )
    if not alerts:
        return False

    final_label = final_status_label(final_status)
    db.update_deleted_match_final_observation(
        match_id,
        final_score=final_score,
        final_status=final_label,
    )
    updated_any = False

    def mark_match_updated() -> None:
        nonlocal updated_any
        if updated_any:
            return
        if count_checked:
            summary["checked_count"] += 1
        summary["finished_match_count"] += 1
        updated_any = True

    for alert in alerts:
        playable_direction = canonical_alert_direction(alert)
        live_line = _float_or_none(alert.get("live"))
        if live_line is None:
            continue
        signal_result = evaluate_signal_result(playable_direction, live_line, final_total)
        if not signal_result:
            continue

        updated = db.update_deleted_alert_final_result(
            alert["id"],
            result=signal_result,
            final_score=final_score,
            final_status=final_label,
            force=force,
        )
        if not updated:
            continue

        mark_match_updated()
        summary["updated_count"] += 1
        if signal_result == "Başarılı":
            summary["successful_count"] += 1
        elif signal_result == "Başarısız":
            summary["failed_count"] += 1
        elif signal_result == "İade":
            summary["push_count"] += 1

        summary["details"].append({
            "id": alert["id"],
            "match_id": alert["match_id"],
            "match_name": alert["match_name"],
            "direction": playable_direction,
            "live_line": live_line,
            "final_score": final_score,
            "final_total": final_total,
            "result": signal_result,
        })

    return updated_any


def _deleted_result_message(summary: dict) -> str:
    summary["coverage_percent"] = (
        round(summary["checked_count"] / summary["tracked_count"] * 100, 1)
        if summary["tracked_count"]
        else 100.0
    )
    if summary["tracked_count"] == 0:
        return "Kontrol edilecek silinen maç bulunamadı."
    if summary["checked_count"] == 0:
        return (
            f"{summary['tracked_count']} maç kontrol listesinde, ancak maç sayfasına ulaşılamadı "
            "ve kayıtlı final skor bulunamadı. Biraz sonra tekrar deneyin."
        )
    return (
        f"{summary['checked_count']} maç kontrol edildi, "
        f"{summary['finished_match_count']} maç bitmiş bulundu, "
        f"{summary['updated_count']} sinyal güncellendi."
    )





async def run_active_match_finished_scan(db, config, before_delete=None, on_progress=None) -> dict:
    """Scan active (not-yet-deleted) alerts for matches that have ended, and
    soft-delete them so they flow into the finished-match result pipeline."""
    if not _active_finished_scan_lock.acquire(blocking=False):
        return {
            **_empty_result_summary(),
            "busy": True,
            "archive_failed_count": 0,
            "moved_count": 0,
            "message": (
                "Biten maç kontrolü zaten çalışıyor. "
                "Mevcut tarama tamamlanınca tekrar deneyin."
            ),
        }
    try:
        return await _run_active_match_finished_scan(db, config, before_delete, on_progress)
    finally:
        _active_finished_scan_lock.release()


async def _run_active_match_finished_scan(db, config, before_delete=None, on_progress=None) -> dict:
    tracked_matches = db.get_active_matches_with_urls()
    summary = {
        **_empty_result_summary(tracked_count=len(tracked_matches)),
        "finished_match_count": 0,
        "moved_count": 0,
        "archive_failed_count": 0,
    }
    if not tracked_matches:
        summary["message"] = "Taranacak aktif maç bulunamadı."
        return summary

    handled = set()

    def archive_result(result):
        match_id = result.get("match_id")
        if match_id in handled:
            return
        handled.add(match_id)
        if (not result.get("is_finished") or not is_final_status(result.get("status", ""))
                or parse_score_total(result.get("score", "")) is None):
            return
        summary["finished_match_count"] += 1
        match_id = result.get("match_id")
        if not match_id:
            return
        try:
            if before_delete is not None:
                # The dashboard callback freezes and archives rows atomically.
                affected = before_delete(match_id)
                if not isinstance(affected, int) or isinstance(affected, bool):
                    raise ValueError("archive callback must return the archived row count")
            else:
                raise RuntimeError("snapshot archive callback is required")
            if affected > 0:
                summary["moved_count"] += 1
                settlement = _empty_result_summary(tracked_count=1)
                _settle_deleted_match_from_final_score(
                    db,
                    settlement,
                    match_id,
                    result.get("score", ""),
                    result.get("status", ""),
                    count_checked=False,
                )
                for key in ("updated_count", "successful_count", "failed_count", "push_count"):
                    summary[key] += settlement[key]
                detail = {
                    "match_id": match_id,
                    "match_name": result.get("match_name", ""),
                    "status": result.get("status", ""),
                    "final_score": result.get("score", ""),
                    "affected_alerts": affected,
                    "settled_alerts": settlement["updated_count"],
                }
                if settlement["details"]:
                    detail["results"] = settlement["details"]
                summary["details"].append(detail)
        except Exception as exc:
            summary["archive_failed_count"] += 1
            summary["details"].append({
                "match_id": match_id,
                "match_name": result.get("match_name", ""),
                "error_code": "ARCHIVE_FAILED",
            })
            logger.exception(
                "Finished match archive failed; remaining matches continue: match_id=%s error=%s",
                match_id,
                exc,
            )

    def progress(data):
        if on_progress:
            on_progress({**data, "moved_count": summary["moved_count"],
                         "archive_failed_count": summary["archive_failed_count"]})

    checker = _finished_checker(config)
    results = await checker.check_matches(
        tracked_matches, on_result=archive_result, on_progress=progress,
    )
    _merge_checker_report(summary, checker, len(tracked_matches), results)
    # Also support custom checkers that only return their observations.
    for result in results:
        archive_result(result)

    coverage = f"{summary['checked_count']}/{summary['tracked_count']} maç doğrulandı"
    if summary["check_failed_count"]:
        coverage += (
            f"; {summary['check_failed_count']} maça ulaşılamadı ve aktif bırakıldı"
        )

    if summary["checked_count"] == 0 and summary["check_failed_count"]:
        summary["message"] = f"{coverage}. Maçların bitiş durumu doğrulanamadı; tekrar deneyin."
    elif summary["moved_count"] == 0 and summary["finished_match_count"] == 0:
        summary["message"] = (
            f"{coverage}. Biten maç bulunamadı."
        )
    else:
        summary["message"] = (
            f"{summary['moved_count']} biten maç Silinen Maçlar'a taşındı "
            f"({coverage})."
        )
    if summary["archive_failed_count"]:
        summary["message"] += (
            f" {summary['archive_failed_count']} maçın arşivleme/sonuçlandırma işlemi "
            "tamamlanamadı; tekrar deneyin."
        )
    return summary


async def run_deleted_match_result_cycle(db, config) -> dict:
    tracked_matches = db.get_deleted_matches_for_result_check(limit=None)
    logger.info("Checking %s deleted matches for final results.", len(tracked_matches))

    if not tracked_matches:
        return {**_empty_result_summary(), "message": "Kontrol edilecek silinen maç bulunamadı."}

    return await _run_deleted_match_result_check_for_matches(db, config, tracked_matches)


async def run_single_deleted_match_result_check(db, config, alert_id: int) -> dict:
    # Unlike the bulk cycle, a user-triggered single recheck must re-evaluate the
    # alert regardless of its current `result` — otherwise pre-fix records stuck
    # with absurd scores (e.g. "8-12") can never be corrected via the UI button.
    empty_summary = _empty_result_summary()

    alert = db.get_deleted_alert_by_id(alert_id)
    tracked_match = db.get_deleted_match_for_result_check_by_alert_id(alert_id)
    if not alert or not tracked_match:
        return {**empty_summary, "message": "Kontrol edilecek maç bulunamadı."}
    summary = _empty_result_summary(tracked_count=1)
    # An unresolved row may already carry a trustworthy final score. A
    # resolved row, however, was explicitly rechecked by the user and must be
    # fetched again; reusing its old final fields would only reproduce a stale
    # or previously misparsed result.
    if not str(alert.get("result") or "").strip():
        if _settle_deleted_match_from_final_score(
            db,
            summary,
            alert["match_id"],
            tracked_match.get("score", ""),
            tracked_match.get("status", ""),
        ):
            summary["message"] = _deleted_result_message(summary)
            return summary

    checker = _finished_checker(config)
    results = await checker.check_matches([tracked_match])
    _merge_checker_report(summary, checker, 1, results)

    if not results:
        summary["message"] = "Maç sayfasına ulaşılamadı."
        return summary

    result = results[0]
    if not result.get("is_finished"):
        summary["in_progress_count"] = db.mark_deleted_match_in_progress(alert["match_id"])
        summary["message"] = (
            "Maç sayfası yeniden kontrol edildi; Full Time görülmedi. "
            f"{summary['in_progress_count']} sinyal Devam Ediyor olarak güncellendi."
        )
        return summary

    final_score = result.get("score", "")
    final_total = parse_score_total(final_score)
    if final_total is None:
        summary["message"] = (
            "Maç bitmiş görünüyor ama final skoru güvenilir okunamadı. "
            "Birkaç dakika sonra tekrar deneyin."
        )
        return summary

    _settle_deleted_match_from_final_score(
        db,
        summary,
        alert["match_id"],
        final_score,
        result.get("status", ""),
        count_checked=False,
        force=True,
    )
    summary["message"] = _deleted_result_message(summary)
    return summary


async def _run_deleted_match_result_check_for_matches(db, config, tracked_matches: list[dict]) -> dict:
    summary = _empty_result_summary(tracked_count=len(tracked_matches))
    remaining_matches = []
    for match in tracked_matches:
        settled = _settle_deleted_match_from_final_score(
            db,
            summary,
            match.get("match_id", ""),
            match.get("score", ""),
            match.get("status", ""),
        )
        if not settled:
            remaining_matches.append(match)

    if not remaining_matches:
        summary["message"] = _deleted_result_message(summary)
        return summary

    checker = _finished_checker(config)
    results = await checker.check_matches(remaining_matches)
    _merge_checker_report(summary, checker, len(remaining_matches), results)

    for result in results:
        if not result.get("is_finished"):
            affected = db.mark_deleted_match_in_progress(result.get("match_id", ""))
            if affected:
                summary["in_progress_count"] += affected
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug("Deleted match is finished but score is not parseable: %s", result.get("match_id"))
            continue

        _settle_deleted_match_from_final_score(
            db,
            summary,
            result["match_id"],
            final_score,
            result.get("status", ""),
            count_checked=False,
        )

    summary["message"] = _deleted_result_message(summary)
    return summary
