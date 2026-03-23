"""
scraper.py — Fetches live basketball total (over/under) lines via the SofaScore
public API.

SofaScore has no official API, but public endpoints are used.
To avoid being banned:
  • Realistic User-Agent and Referer headers
  • Small random delay (0.5–1.5s) per request
  • Session object (reuses connections)
"""

import logging
import random
import re
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# SofaScore public endpoints
_BASE = "https://api.sofascore.com/api/v1"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.sofascore.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.sofascore.com",
}

# Known SofaScore market type IDs for basketball totals (over/under)
# 10: Totals (over/under), 18: May include Asian Handicap-like structures
_TOTAL_MARKET_IDS = {10, 18, 226}

# Reasonable range for basketball total lines
_MIN_TOTAL = 120.0
_MAX_TOTAL = 380.0


class SofaScoreScraper:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    def _get(self, url: str, timeout: int = 10) -> Optional[dict]:
        """Makes a single GET request; returns None on error."""
        try:
            time.sleep(random.uniform(0.5, 1.5))
            resp = self._session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                logger.warning("Rate limit hit! Waiting 60 seconds...")
                time.sleep(60)
            else:
                logger.debug(f"HTTP {resp.status_code} — {url}")
        except requests.RequestException as e:
            logger.warning(f"Request error: {e}")
        return None

    # ------------------------------------------------------------------ #
    #  List live events                                                    #
    # ------------------------------------------------------------------ #

    def get_live_events(self) -> list[dict]:
        """
        Returns currently live basketball events.
        Each element: {id, homeTeam, awayTeam, tournament, status, ...}
        """
        data = self._get(f"{_BASE}/sport/basketball/events/live")
        if not data:
            logger.warning("Failed to fetch live basketball events.")
            return []
        events = data.get("events", [])
        logger.debug(f"Found {len(events)} live basketball events.")
        return events

    # ------------------------------------------------------------------ #
    #  Match odds data                                                     #
    # ------------------------------------------------------------------ #

    def get_event_odds(self, event_id: int) -> Optional[dict]:
        """
        Fetches all odds data for an event.
        Tries multiple endpoints; uses whichever responds.
        """
        endpoints = [
            f"{_BASE}/event/{event_id}/odds/1/all/all",
            f"{_BASE}/event/{event_id}/odds/0/all/all",
        ]
        for url in endpoints:
            data = self._get(url)
            if data:
                return data
        return None

    # ------------------------------------------------------------------ #
    #  Parse total line                                                    #
    # ------------------------------------------------------------------ #

    def extract_total(self, odds_data: dict) -> Optional[float]:
        """
        Extracts the over/under line from odds response.
        Tries multiple paths based on SofaScore's response structure.
        """
        if not odds_data:
            return None

        # 1) markets → choices path
        for market in odds_data.get("markets", []):
            total = _parse_market(market)
            if total is not None:
                return total

        # 2) Flat list path (some responses return a direct list instead of markets)
        if isinstance(odds_data, list):
            for market in odds_data:
                total = _parse_market(market)
                if total is not None:
                    return total

        # 3) Legacy format: flat list under the "odds" key
        for item in odds_data.get("odds", []):
            total = _extract_total_from_flat_odds(item)
            if total is not None:
                return total

        return None

    # ------------------------------------------------------------------ #
    #  Main method: returns all live basketball matches with total lines  #
    # ------------------------------------------------------------------ #

    def get_live_basketball_totals(self) -> list[dict]:
        """
        Returns all currently live basketball matches with their total lines.

        Return format:
        [
          {
            "match_id": "12345678",
            "match_name": "Boston Celtics - LA Lakers",
            "tournament": "NBA",
            "live_total": 221.5,
            "status": "2nd Quarter"
          },
          ...
        ]
        """
        events = self.get_live_events()
        results = []

        for event in events:
            event_id = event.get("id")
            if not event_id:
                continue

            match_name = _build_match_name(event)
            tournament = _get_tournament_name(event)
            status = _get_status_text(event)

            # Fetch odds data
            odds_data = self.get_event_odds(event_id)
            live_total = self.extract_total(odds_data) if odds_data else None

            if live_total is None:
                logger.debug(f"Total line not found: {match_name}")
                continue

            results.append(
                {
                    "match_id": str(event_id),
                    "match_name": match_name,
                    "tournament": tournament,
                    "live_total": live_total,
                    "status": status,
                }
            )

        logger.info(
            f"Found total lines for {len(results)} out of {len(events)} live matches."
        )
        return results


# ------------------------------------------------------------------ #
#  Helper functions                                                    #
# ------------------------------------------------------------------ #


def _parse_market(market: dict) -> Optional[float]:
    """
    Parses the total line from a single market object.
    """
    market_id = market.get("marketId") or market.get("id")
    market_name = str(market.get("marketName") or market.get("name") or "").lower()

    is_totals = (
        market_id in _TOTAL_MARKET_IDS
        or "total" in market_name
        or "over" in market_name
        or "o/u" in market_name
    )

    if not is_totals:
        return None

    choices = market.get("choices") or market.get("outcomes") or []
    for choice in choices:
        choice_name = str(choice.get("name") or choice.get("choiceName") or "")
        # Try formats like "Over 221.5" or "221.5"
        total = _parse_total_from_string(choice_name)
        if total is not None:
            return total

    return None


def _extract_total_from_flat_odds(item: dict) -> Optional[float]:
    """
    Searches for a total line from legacy/flat odds objects.
    """
    name = str(item.get("name") or item.get("handicap") or "").lower()
    if any(kw in name for kw in ["total", "over", "under", "o/u"]):
        val = item.get("handicap") or item.get("value") or item.get("line")
        if val is not None:
            try:
                total = float(str(val).replace(",", "."))
                if _MIN_TOTAL <= total <= _MAX_TOTAL:
                    return total
            except ValueError:
                pass
    return None


def _parse_total_from_string(s: str) -> Optional[float]:
    """
    Extracts the first number from a string and checks if it's in basketball range.
    Example: "Over 221.5" → 221.5, "Under 218" → 218.0
    """
    match = re.search(r"\d{3}(?:[.,]\d)?", s)
    if match:
        try:
            val = float(match.group().replace(",", "."))
            if _MIN_TOTAL <= val <= _MAX_TOTAL:
                return val
        except ValueError:
            pass
    return None


def _build_match_name(event: dict) -> str:
    home = (event.get("homeTeam") or {}).get("name", "Home")
    away = (event.get("awayTeam") or {}).get("name", "Away")
    return f"{home} - {away}"


def _get_tournament_name(event: dict) -> str:
    t = event.get("tournament") or {}
    return t.get("name") or t.get("uniqueTournament", {}).get("name") or "Unknown"


def _get_status_text(event: dict) -> str:
    status = event.get("status") or {}
    desc = status.get("description") or ""
    period = status.get("period") or ""
    if period:
        return f"Period {period}"
    return desc or "Live"
