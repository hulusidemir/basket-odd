"""
live_matches_worker.py — AIScore anasayfasindaki tum canli maclari periyodik
olarak tarar ve her macin acilis/canli/adil/projeksiyon bilgilerini JSON
snapshot dosyasina yazar. Dashboard bu snapshot'i 5 saniyede bir okur.

Usage:
    python live_matches_worker.py
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from datetime import datetime

from aiscore_scraper import AiscoreScraper
from config import Config
from pace_tracker import PaceTracker
from projection import calculate_projected_total, game_clock, parse_score
from signal_analysis import build_signal_analysis


logger = logging.getLogger("live_matches_worker")

SNAPSHOT_PATH = os.getenv(
    "LIVE_MATCHES_SNAPSHOT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_matches_snapshot.json"),
)
MIN_CYCLE_SECONDS = int(os.getenv("LIVE_MATCHES_MIN_CYCLE_SECONDS", "5"))


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _period_label(status: str, match_name: str, tournament: str) -> str:
    clock = game_clock(status, match_name, tournament)
    period = clock.get("period")
    remaining = clock.get("remaining_min")
    if period is None:
        return status or "-"
    period_text = {1: "1. Çeyrek", 2: "2. Çeyrek", 3: "3. Çeyrek", 4: "4. Çeyrek"}.get(
        period, f"{period}. Çeyrek"
    )
    if remaining is None:
        return period_text
    minutes = int(remaining)
    seconds = int(round((remaining - minutes) * 60))
    if seconds == 60:
        minutes += 1
        seconds = 0
    return f"{period_text} · {minutes:02d}:{seconds:02d} kaldı"


def _clean_match_name(name: str) -> str:
    if not name:
        return ""
    cleaned = re.sub(r"\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*", "", name)
    cleaned = re.sub(r"\s*betting odds\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def build_live_row(match: dict, pace_tracker: PaceTracker | None = None) -> dict:
    match_name = _clean_match_name(match.get("match_name") or "")
    tournament = match.get("tournament") or ""
    status = match.get("status") or ""
    score = match.get("score") or ""
    opening = float(match.get("opening_total") or 0.0)
    live = float(match.get("inplay_total") or 0.0)

    # Çeyrek hız takibini güncelle
    pace_data: dict | None = None
    clock = game_clock(status, match_name, tournament)
    period_now = clock.get("period")
    if pace_tracker is not None and period_now is not None:
        home_sc, away_sc = parse_score(score)
        if home_sc is not None and away_sc is not None:
            pace_data = pace_tracker.update(
                match.get("match_id", ""),
                period_now,
                home_sc + away_sc,
                clock.get("quarter_length", 10),
            )

    analysis = build_signal_analysis(
        {
            **match,
            "opening_total": opening,
            "inplay_total": live,
            "direction": "ALT" if (live - opening) > 0 else "ÜST",
        },
        context={"h2h": {"body_text": ""}},
        threshold=0,
        pace_data=pace_data,
    )

    projected = calculate_projected_total(score, status, match_name, tournament)
    home_score, away_score = parse_score(score)

    return {
        "match_id": match.get("match_id"),
        "match_name": match_name or "-",
        "tournament": tournament or "-",
        "url": match.get("url") or "",
        "status_raw": status,
        "status_label": _period_label(status, match_name, tournament),
        "period": clock.get("period"),
        "remaining_min": clock.get("remaining_min"),
        "score": score,
        "home_score": home_score,
        "away_score": away_score,
        "opening_total": round(opening, 1),
        "prematch_total": (
            round(float(match["prematch_total"]), 1)
            if match.get("prematch_total") is not None
            else None
        ),
        "inplay_total": round(live, 1),
        "fair_line": analysis.get("fair_line"),
        "fair_edge": analysis.get("fair_edge"),
        "projected_total": round(projected, 1) if projected is not None else None,
        "recommendation": analysis.get("recommendation") or "",
        "market_locked": bool(match.get("market_locked")),
    }


def write_snapshot(path: str, payload: dict) -> None:
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".live_matches_snapshot_", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


async def run_single_cycle(
    scraper: AiscoreScraper,
    snapshot_path: str = SNAPSHOT_PATH,
    pace_tracker: PaceTracker | None = None,
) -> dict:
    cycle_started = datetime.utcnow()
    status = "ok"
    error_message = ""
    rows: list[dict] = []

    try:
        matches = await scraper.get_live_basketball_totals()
        for match in matches:
            try:
                rows.append(build_live_row(match, pace_tracker))
            except Exception as exc:
                logger.debug("Row build failed for %s: %s", match.get("match_id"), exc)

        rows.sort(
            key=lambda r: (
                -(r.get("period") or 0),
                (r.get("tournament") or ""),
                (r.get("match_name") or ""),
            )
        )
        logger.info("Live matches cycle complete. matches=%s", len(rows))
    except Exception as exc:
        status = "error"
        error_message = str(exc)
        logger.error("Live matches cycle failed: %s", exc, exc_info=True)

    cycle_finished = datetime.utcnow()
    payload = {
        "generated_at": cycle_finished.isoformat(timespec="seconds") + "Z",
        "cycle_started_at": cycle_started.isoformat(timespec="seconds") + "Z",
        "cycle_duration_seconds": round((cycle_finished - cycle_started).total_seconds(), 1),
        "status": status,
        "error": error_message,
        "count": len(rows),
        "matches": rows,
    }
    try:
        write_snapshot(snapshot_path, payload)
    except Exception as exc:
        logger.error("Snapshot write failed: %s", exc, exc_info=True)
        payload["status"] = "error"
        payload["error"] = (payload.get("error") or "") + f" | snapshot write: {exc}"
    return payload


def build_default_scraper() -> AiscoreScraper:
    config = Config()
    return AiscoreScraper(
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=config.MAX_MATCHES_PER_CYCLE,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        skip_h2h=True,
    )


async def run_manual_cycle(snapshot_path: str = SNAPSHOT_PATH) -> dict:
    """Dashboard'un manuel tetiklemesi icin tek seferlik cevrim."""
    return await run_single_cycle(build_default_scraper(), snapshot_path)


async def run_worker():
    config = Config()
    setup_logging(config.LOG_LEVEL)

    scraper = build_default_scraper()
    pace_tracker = PaceTracker()

    logger.info(
        "Live matches worker started. snapshot=%s min_cycle=%ss",
        SNAPSHOT_PATH, MIN_CYCLE_SECONDS,
    )

    while True:
        cycle_started = datetime.utcnow()
        try:
            await run_single_cycle(scraper, SNAPSHOT_PATH, pace_tracker)
        except KeyboardInterrupt:
            logger.info("Live matches worker stopped.")
            break

        elapsed = (datetime.utcnow() - cycle_started).total_seconds()
        sleep_for = max(MIN_CYCLE_SECONDS - elapsed, 0)
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(run_worker())
