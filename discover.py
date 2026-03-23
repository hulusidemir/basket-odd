"""
discover.py — Inspects SofaScore API responses and saves them as JSON.

Run this tool once; examine the output to adjust scraper.py
for your match/odds structure.

Usage:
    python discover.py
"""

import json
import logging
import time
import random

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("discover")

BASE = "https://api.sofascore.com/api/v1"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.sofascore.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8",
}


def get(url: str) -> dict | None:
    try:
        time.sleep(random.uniform(1, 2))
        r = requests.get(url, headers=HEADERS, timeout=15)
        log.info(f"GET {url} → HTTP {r.status_code}")
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log.error(f"Error: {e}")
    return None


def main():
    output = {}

    # 1) Live basketball events
    log.info("=== Live basketball events ===")
    live = get(f"{BASE}/sport/basketball/events/live")
    output["live_events"] = live

    if not live or not live.get("events"):
        log.warning("No live basketball events right now. Checking today's scheduled events...")
        from datetime import date
        today = date.today().isoformat()
        scheduled = get(f"{BASE}/sport/basketball/scheduled-events/{today}")
        output["scheduled_events"] = scheduled
        events = (scheduled or {}).get("events", [])[:3]
    else:
        events = live["events"][:3]

    log.info(f"Fetching odds data for {len(events)} matches.")

    # 2) Fetch odds data for a few matches
    output["sample_odds"] = {}
    for event in events:
        eid = event.get("id")
        name = f"{event.get('homeTeam', {}).get('name', '?')} - {event.get('awayTeam', {}).get('name', '?')}"
        log.info(f"Fetching odds: {name} (ID: {eid})")

        for suffix in ["1/all/all", "0/all/all"]:
            url = f"{BASE}/event/{eid}/odds/{suffix}"
            odds = get(url)
            if odds:
                output["sample_odds"][f"{eid}_{suffix}"] = odds
                log.info(f"  ✓ Odds bulundu → {url}")
                break
        else:
            log.warning(f"  ✗ Could not fetch odds data for {name}.")

    # 3) Kaydet
    out_file = "discovery_output.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"\nResults saved to '{out_file}'.")
    log.info("Open this file and search for 'markets' or 'odds' keys.")
    log.info("Find the market structure containing Over/Under or Total and update scraper.py.")

    # 4) Print summary
    _print_summary(output)


def _print_summary(output: dict):
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    # Live match count
    live_events = (output.get("live_events") or {}).get("events", [])
    print(f"Live matches: {len(live_events)}")

    # Sample odds structure
    sample = output.get("sample_odds", {})
    if not sample:
        print("No odds data found.")
        return

    first_key = next(iter(sample))
    first_odds = sample[first_key]

    # Are there markets?
    markets = first_odds.get("markets", [])
    if markets:
        print(f"\nMarket sayısı (ilk maç): {len(markets)}")
        for m in markets[:8]:
            mid = m.get("marketId") or m.get("id")
            name = m.get("marketName") or m.get("name", "?")
            choices = m.get("choices") or m.get("outcomes") or []
            choice_names = [c.get("name", "?") for c in choices[:3]]
            print(f"  [{mid}] {name} → {choice_names}")
    else:
        print("'markets' key not found. Check discovery_output.json.")


if __name__ == "__main__":
    main()
