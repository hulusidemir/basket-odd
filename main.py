"""
main.py — Basketbol barem izleme botu ana döngüsü.

Çalıştırmak için:
    python main.py
"""

import asyncio
import logging
import random
import sys

from aiscore_opera_scraper import AiscoreOperaScraper
from config import Config
from db import Database
from notifier import TelegramNotifier


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
    config: Config,
) -> None:
    """
    Tek bir maçı işler:
    - AIScore ekranındaki Opening odds ve In-play odds total line farkını hesaplar.
    - Fark eşik üzerindeyse Telegram bildirimi gönderir.
    """
    match_id = match["match_id"]
    match_name = match["match_name"]
    tournament = match.get("tournament", "")
    opening_total = match["opening_total"]
    inplay_total = match["inplay_total"]
    status = match.get("status", "Canlı")

    diff = inplay_total - opening_total  # pozitif → barem yükseldi, negatif → düştü
    abs_diff = abs(diff)
    if abs_diff < config.THRESHOLD:
        return

    url = match.get("url", "")

    if diff >= 0:
        direction = "ALT"
        if not db.was_alerted_recently(match_id, direction, config.ALERT_COOLDOWN_HOURS):
            await notifier.send_alert(
                match_name, tournament, opening_total, inplay_total, direction, abs_diff, status
            )
            db.save_alert(match_id, match_name, opening_total, inplay_total, direction, abs_diff,
                          tournament=tournament, status=status, url=url)

    else:
        direction = "ÜST"
        if not db.was_alerted_recently(match_id, direction, config.ALERT_COOLDOWN_HOURS):
            await notifier.send_alert(
                match_name, tournament, opening_total, inplay_total, direction, abs_diff, status
            )
            db.save_alert(match_id, match_name, opening_total, inplay_total, direction, abs_diff,
                          tournament=tournament, status=status, url=url)


async def run():
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        print(f"HATA: {e}")
        sys.exit(1)

    setup_logging(config.LOG_LEVEL)
    log = logging.getLogger("main")

    db = Database(config.DB_PATH)
    db.init()

    notifier = TelegramNotifier(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)
    scraper = AiscoreOperaScraper(
        cdp_url=config.OPERA_CDP_URL,
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=config.MAX_MATCHES_PER_CYCLE,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        browser_mode=config.BROWSER_MODE,
        opera_binary=config.OPERA_BINARY,
        cdp_port=config.OPERA_CDP_PORT,
    )

    await notifier.send_startup()
    log.info(
        "Bot başlatıldı. Eşik: %s puan | Kontrol aralığı: %s-%ss | CDP: %s",
        config.THRESHOLD,
        config.POLL_INTERVAL_MIN,
        config.POLL_INTERVAL_MAX,
        config.OPERA_CDP_URL,
    )

    consecutive_errors = 0

    while True:
        try:
            matches = await scraper.get_live_basketball_totals()
            log.info("%s maçta opening/in-play total yakalandı.", len(matches))

            for match in matches:
                await process_match(match, db, notifier, config)

            consecutive_errors = 0

        except KeyboardInterrupt:
            log.info("Bot durduruldu.")
            break
        except Exception as e:
            consecutive_errors += 1
            log.error(f"Döngü hatası (#{consecutive_errors}): {e}", exc_info=True)
            if consecutive_errors >= 5:
                await notifier.send_error(f"{consecutive_errors} ardışık hata: {e}")
                consecutive_errors = 0  # Sıfırla, tekrar dene

        delay = random.uniform(config.POLL_INTERVAL_MIN, config.POLL_INTERVAL_MAX)
        log.debug(f"Sonraki kontrol: {delay:.0f}s sonra")
        await asyncio.sleep(delay)


if __name__ == "__main__":
    asyncio.run(run())
