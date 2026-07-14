"""
upcoming_app.py — "Gelecek Maçlar" Flask Blueprint.

Mounted on the main dashboard from dashboard.py:
    from upcoming_app import upcoming_bp
    app.register_blueprint(upcoming_bp)

Routes:
    GET  /upcoming           — renders templates/upcoming_matches.html
    POST /upcoming/api/fetch — runs the scraper and returns JSON
"""

import asyncio
import logging
import os
import threading
from datetime import datetime, timezone

from flask import Blueprint, jsonify, render_template, request

from config import Config
from db import Database
from upcoming_scraper import UpcomingScraper

log = logging.getLogger("upcoming_app")


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


FETCH_TIMEOUT_SECONDS = _env_int(
    "UPCOMING_FETCH_TIMEOUT_SECONDS",
    300,
    minimum=60,
    maximum=3600,
)
STALE_AFTER_SECONDS = _env_int(
    "UPCOMING_STALE_AFTER_SECONDS",
    1800,
    minimum=300,
    maximum=86400,
)

_fetch_lock = threading.RLock()
_fetch_state = {
    "job_id": 0,
    "running": False,
    "started_at": None,
    "finished_at": None,
    "matches": [],
    "count": 0,
    "saved_matches": 0,
    "saved_signals": 0,
    "report": None,
    "timeout_seconds": FETCH_TIMEOUT_SECONDS,
    "error": None,
}

upcoming_bp = Blueprint(
    "upcoming",
    __name__,
    url_prefix="/upcoming",
    template_folder="templates",
)


@upcoming_bp.route("/")
def upcoming_index():
    return render_template("upcoming_matches.html")


@upcoming_bp.route("/api/fetch", methods=["POST"])
def upcoming_api_fetch():
    config = Config()
    scraper = _build_scraper(config)
    timeout_seconds = max(
        FETCH_TIMEOUT_SECONDS,
        scraper.estimated_outer_timeout_seconds(scraper.max_matches),
    )
    with _fetch_lock:
        _expire_stale_fetch_locked()
        if _fetch_state["running"]:
            return jsonify(_public_state()), 202
        job_id = int(_fetch_state.get("job_id") or 0) + 1

        _fetch_state.update(
            {
                "job_id": job_id,
                "running": True,
                "started_at": _now_iso(),
                "finished_at": None,
                "saved_matches": 0,
                "saved_signals": 0,
                "report": {
                    "status": "running",
                    "started_at": _now_iso(),
                    "finished_at": None,
                },
                "timeout_seconds": timeout_seconds,
                "error": None,
            }
        )

    thread = threading.Thread(
        target=_run_fetch_job,
        args=(job_id, scraper, timeout_seconds, config.DB_PATH),
        daemon=True,
    )
    thread.start()
    return jsonify(_public_state()), 202


@upcoming_bp.route("/api/status")
def upcoming_api_status():
    with _fetch_lock:
        _expire_stale_fetch_locked()
    return jsonify(_public_state())


@upcoming_bp.route("/api/list")
def upcoming_api_list():
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    matches = db.list_upcoming_matches(limit=500)
    saved_signals = sum(1 for match in matches if match.get("signal_direction"))
    state = _public_state()
    freshness = _freshness_summary(matches)
    return jsonify(
        {
            "matches": matches,
            "count": len(matches),
            "saved_matches": len(matches),
            "saved_signals": saved_signals,
            "running": state["running"],
            "started_at": state["started_at"],
            "finished_at": state["finished_at"],
            "report": state["report"],
            "last_fetch_error": state["error"],
            "error": None,
            **freshness,
        }
    )


@upcoming_bp.route("/api/clear", methods=["POST"])
def upcoming_api_clear():
    with _fetch_lock:
        if _fetch_state.get("running"):
            return jsonify({"error": "Çekim sürerken gelecek maçlar temizlenemez."}), 409
        config = Config()
        db = Database(config.DB_PATH)
        db.init()
        summary = db.clear_upcoming_matches()
        _fetch_state.update(
            {
                "matches": [],
                "count": 0,
                "saved_matches": 0,
                "saved_signals": 0,
            }
        )
    return jsonify({"cleared": True, **summary})


@upcoming_bp.route("/api/matches/<path:match_id>/<action>", methods=["POST"])
def upcoming_api_match_action(match_id: str, action: str):
    match_key = str(match_id or "").strip()
    if not match_key:
        return jsonify({"error": "match_id is required"}), 400

    config = Config()
    db = Database(config.DB_PATH)
    db.init()

    if action == "delete":
        affected = db.delete_upcoming_match_data(match_key)
        return jsonify(
            {
                "match_id": match_key,
                "deleted": True,
                "affected": affected,
            }
        )

    current = db.get_upcoming_match_action_status(match_key)
    if action == "bet":
        new_val = not bool(current.get("bet_placed"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            bet_placed=new_val,
            ignored=False if new_val else None,
            followed=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "bet_placed": int(new_val),
                "ignored": 0 if new_val else None,
                "followed": 0 if new_val else None,
                "affected": affected,
            }
        )

    if action == "ignore":
        new_val = not bool(current.get("ignored"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            ignored=new_val,
            bet_placed=False if new_val else None,
            followed=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "ignored": int(new_val),
                "bet_placed": 0 if new_val else None,
                "followed": 0 if new_val else None,
                "affected": affected,
            }
        )

    if action == "follow":
        new_val = not bool(current.get("followed"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            followed=new_val,
            bet_placed=False if new_val else None,
            ignored=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "followed": int(new_val),
                "bet_placed": 0 if new_val else None,
                "ignored": 0 if new_val else None,
                "affected": affected,
            }
        )

    return jsonify({"error": "unknown action"}), 404


@upcoming_bp.route("/api/saved-lists", methods=["GET"])
def upcoming_api_list_saved_lists():
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    lists = db.list_saved_match_lists(limit=100)
    return jsonify({"lists": lists, "count": len(lists)})


@upcoming_bp.route("/api/saved-lists", methods=["POST"])
def upcoming_api_save_list():
    data = request.get_json(force=True, silent=True) or {}
    name = str(data.get("name") or "").strip()[:200] or "Liste"
    matches = data.get("matches")
    if not isinstance(matches, list):
        return jsonify({"error": "matches is required"}), 400
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    list_id = db.save_match_list(name, matches)
    return jsonify({"id": list_id, "name": name, "match_count": len(matches)}), 201


@upcoming_bp.route("/api/saved-lists/<int:list_id>", methods=["GET"])
def upcoming_api_get_saved_list(list_id: int):
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    item = db.get_saved_match_list(list_id)
    if not item:
        return jsonify({"error": "not found"}), 404
    return jsonify(item)


@upcoming_bp.route("/api/saved-lists/<int:list_id>", methods=["DELETE"])
def upcoming_api_delete_saved_list(list_id: int):
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    ok = db.delete_saved_match_list(list_id)
    if not ok:
        return jsonify({"error": "not found"}), 404
    return jsonify({"deleted": True, "id": list_id})


def _build_scraper(config: Config) -> UpcomingScraper:
    max_matches = _env_int(
        "UPCOMING_MAX_MATCHES",
        12,
        minimum=0,
        maximum=500,
    )
    match_timeout_seconds = _env_int(
        "UPCOMING_MATCH_TIMEOUT_SECONDS",
        max(45, min(90, int(config.PAGE_TIMEOUT_MS / 1000) * 2)),
        minimum=30,
        maximum=180,
    )
    return UpcomingScraper(
        aiscore_url=config.AISCORE_URL,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        max_matches=max_matches or None,
        days_ahead=config.UPCOMING_DAYS_AHEAD,
        timezone_id=config.AISCORE_TIMEZONE,
        concurrency=config.UPCOMING_CONCURRENCY,
        match_timeout_seconds=match_timeout_seconds,
    )


def _reconcile_allowed(report: dict | None) -> bool:
    report = report if isinstance(report, dict) else {}
    seen_ids = {
        str(match_id).strip()
        for match_id in report.get("discovered_match_ids") or []
        if str(match_id or "").strip()
    }
    return bool(
        seen_ids
        and report.get("status") == "complete"
        and report.get("listing_complete") is True
        and report.get("reconcile_safe") is True
        and not report.get("truncated")
        and int(report.get("failed") or 0) == 0
        and float(report.get("coverage") or 0) >= 1.0
    )


def _run_fetch_job(
    job_id: int,
    scraper: UpcomingScraper | None = None,
    timeout_seconds: int | None = None,
    db_path: str | None = None,
):
    config = Config()
    scraper = scraper or _build_scraper(config)
    timeout_seconds = max(
        FETCH_TIMEOUT_SECONDS,
        int(timeout_seconds or scraper.estimated_outer_timeout_seconds(scraper.max_matches)),
    )
    db_path = db_path or config.DB_PATH
    try:
        matches = asyncio.run(
            asyncio.wait_for(scraper.fetch(), timeout=timeout_seconds)
        )
        report = dict(scraper.last_report or {})
        seen_match_ids = report.get("discovered_match_ids") or []
        reconcile = _reconcile_allowed(report)
        db = Database(db_path)
        db.init()
        saved = db.save_upcoming_matches_and_signals(
            matches,
            seen_match_ids=seen_match_ids,
            reconcile=reconcile,
        )
        # A partial scrape must not make intact older rows disappear from the UI.
        matches = db.list_upcoming_matches(limit=500)
    except TimeoutError:
        message = f"Upcoming fetch timed out after {timeout_seconds} seconds."
        log.warning(message)
        report = dict(getattr(scraper, "last_report", {}) or {})
        report.update(
            {
                "status": "failed",
                "finished_at": _now_iso(),
                "reconcile_safe": False,
                "error": message,
            }
        )
        _finish_fetch_job(
            job_id,
            {
                "running": False,
                "finished_at": _now_iso(),
                "saved_matches": 0,
                "saved_signals": 0,
                "report": report,
                "error": message,
            },
        )
        return
    except Exception as exc:
        log.exception("Upcoming fetch failed: %s", exc)
        report = dict(getattr(scraper, "last_report", {}) or {})
        report.update(
            {
                "status": "failed",
                "finished_at": _now_iso(),
                "reconcile_safe": False,
                "error": str(exc)[:500],
            }
        )
        _finish_fetch_job(
            job_id,
            {
                "running": False,
                "finished_at": _now_iso(),
                "saved_matches": 0,
                "saved_signals": 0,
                "report": report,
                "error": str(exc),
            },
        )
        return

    _finish_fetch_job(
        job_id,
        {
            "running": False,
            "finished_at": _now_iso(),
            "matches": matches,
            "count": len(matches),
            "saved_matches": saved["saved_matches"],
            "saved_signals": saved["saved_signals"],
            "report": {
                **report,
                "reconciled": bool(saved.get("reconciled")),
                "removed_missing": int(saved.get("removed_missing") or 0),
                "removed_expired": int(saved.get("removed_expired") or 0),
            },
            "error": None,
        },
    )


def _finish_fetch_job(job_id: int, payload: dict) -> None:
    with _fetch_lock:
        if int(_fetch_state.get("job_id") or 0) != int(job_id):
            log.info("Ignoring stale upcoming fetch job result: job_id=%s", job_id)
            return
        _fetch_state.update(payload)


def _expire_stale_fetch_locked() -> None:
    if not _fetch_state.get("running"):
        return
    started_at = _fetch_state.get("started_at")
    if not started_at:
        return
    try:
        started = datetime.fromisoformat(str(started_at))
    except ValueError:
        return
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - started).total_seconds()
    timeout_seconds = max(
        FETCH_TIMEOUT_SECONDS,
        int(_fetch_state.get("timeout_seconds") or FETCH_TIMEOUT_SECONDS),
    )
    if age <= timeout_seconds + 15:
        return
    message = f"Onceki gelecek mac cekimi {int(age)} saniye sonra zaman asimina dustu."
    _fetch_state.update(
        {
            "job_id": int(_fetch_state.get("job_id") or 0) + 1,
            "running": False,
            "finished_at": _now_iso(),
            "saved_matches": 0,
            "saved_signals": 0,
            "report": {
                **(_fetch_state.get("report") or {}),
                "status": "failed",
                "finished_at": _now_iso(),
                "reconcile_safe": False,
                "error": message,
            },
            "error": message,
        }
    )


def _parse_timestamp(value) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _freshness_summary(matches: list[dict], now: datetime | None = None) -> dict:
    timestamps = [
        parsed
        for parsed in (_parse_timestamp(row.get("fetched_at")) for row in matches or [])
        if parsed is not None
    ]
    if not timestamps:
        return {
            "fetched_at": None,
            "oldest_fetched_at": None,
            "data_age_seconds": None,
            "oldest_data_age_seconds": None,
            "stale_row_count": len(matches or []),
            "stale": True,
            "stale_after_seconds": STALE_AFTER_SECONDS,
        }
    newest = max(timestamps)
    oldest = min(timestamps)
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = max(0, int((now - newest).total_seconds()))
    oldest_age = max(0, int((now - oldest).total_seconds()))
    stale_row_count = sum(
        1
        for row in matches or []
        if (parsed := _parse_timestamp(row.get("fetched_at"))) is None
        or (now - parsed).total_seconds() > STALE_AFTER_SECONDS
    )
    return {
        "fetched_at": newest.isoformat(),
        "oldest_fetched_at": oldest.isoformat(),
        "data_age_seconds": age,
        "oldest_data_age_seconds": oldest_age,
        "stale_row_count": stale_row_count,
        "stale": stale_row_count > 0,
        "stale_after_seconds": STALE_AFTER_SECONDS,
    }


def _public_state():
    with _fetch_lock:
        matches = list(_fetch_state["matches"])
        return {
            "running": _fetch_state["running"],
            "started_at": _fetch_state["started_at"],
            "finished_at": _fetch_state["finished_at"],
            "matches": matches,
            "count": _fetch_state["count"],
            "saved_matches": _fetch_state["saved_matches"],
            "saved_signals": _fetch_state["saved_signals"],
            "report": dict(_fetch_state.get("report") or {}) or None,
            "timeout_seconds": _fetch_state.get("timeout_seconds"),
            "error": _fetch_state["error"],
            **_freshness_summary(matches),
        }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()
