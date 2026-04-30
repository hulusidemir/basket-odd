"""
live_matches_worker.py — Dashboard'daki "Manuel Çek" butonu için tek seferlik
canlı maç çekim fonksiyonu. Sadece scraper'dan dönen ham veriyi
(maç/lig/durum/skor/açılış/canlı) snapshot JSON'una yazar.
"""

import json
import logging
import os
import re
import tempfile
from datetime import datetime

from aiscore_scraper import AiscoreScraper
from config import Config


logger = logging.getLogger("live_matches_worker")

SNAPSHOT_PATH = os.getenv(
    "LIVE_MATCHES_SNAPSHOT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_matches_snapshot.json"),
)


def _clean_match_name(name: str) -> str:
    if not name:
        return ""
    cleaned = re.sub(r"\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*", "", name)
    cleaned = re.sub(r"\s*betting odds\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def build_live_row(match: dict) -> dict:
    return {
        "match_id": match.get("match_id"),
        "match_name": _clean_match_name(match.get("match_name") or "") or "-",
        "tournament": match.get("tournament") or "-",
        "url": match.get("url") or "",
        "status": match.get("status") or "",
        "score": match.get("score") or "",
        "opening_total": (
            round(float(match["opening_total"]), 1)
            if match.get("opening_total") is not None
            else None
        ),
        "inplay_total": (
            round(float(match["inplay_total"]), 1)
            if match.get("inplay_total") is not None
            else None
        ),
        "market_locked": bool(match.get("market_locked")),
        "quarter_scores": match.get("quarter_scores") or {},
        "odds_snapshot": match.get("odds_snapshot") or {},
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
) -> dict:
    cycle_started = datetime.utcnow()
    status = "ok"
    error_message = ""
    rows: list[dict] = []

    try:
        matches = await scraper.get_live_basketball_totals()
        for match in matches:
            try:
                rows.append(build_live_row(match))
            except Exception as exc:
                logger.debug("Row build failed for %s: %s", match.get("match_id"), exc)

        rows.sort(
            key=lambda r: (
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
    """Dashboard'un manuel tetiklemesi için tek seferlik çevrim."""
    return await run_single_cycle(build_default_scraper(), snapshot_path)
