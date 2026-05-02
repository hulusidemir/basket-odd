import asyncio
import logging
from upcoming_scraper import UpcomingScraper

logging.basicConfig(level=logging.DEBUG)

async def main():
    scraper = UpcomingScraper(days_ahead=2, max_matches=10)
    matches = await scraper.fetch()
    print("\nRESULTS:")
    for m in matches:
        print(f"{m.get('match_name')} - Open: {m.get('opening_total')}, Pre: {m.get('prematch_total')} - URL: {m.get('url')}")

if __name__ == "__main__":
    asyncio.run(main())
