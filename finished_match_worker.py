"""
finished_match_worker.py — Periodically checks whether tracked AIScore matches
have finished and archives signal outcomes into finished_matches.
"""

import asyncio
import logging

from config import Config
from db import Database
from finished_match_service import run_finished_match_cycle


logger = logging.getLogger("finished_match_worker")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def run_worker():
    config = Config()
    setup_logging(config.LOG_LEVEL)

    db = Database(config.DB_PATH)
    db.init()

    logger.info(
        "Finished match worker started. poll=%ss batch=%s",
        config.FINISHED_MATCH_POLL_SECONDS,
        config.FINISHED_MATCH_BATCH_SIZE,
    )

    while True:
        try:
            summary = await run_finished_match_cycle(db, config)
            logger.info(
                "Finished match cycle complete. tracked=%s checked=%s finished=%s archived=%s success=%s fail=%s push=%s",
                summary["tracked_count"],
                summary["checked_count"],
                summary["finished_match_count"],
                summary["archived_count"],
                summary["successful_count"],
                summary["failed_count"],
                summary["push_count"],
            )
        except KeyboardInterrupt:
            logger.info("Finished match worker stopped.")
            break
        except Exception as exc:
            logger.error("Finished match worker cycle failed: %s", exc, exc_info=True)

        await asyncio.sleep(config.FINISHED_MATCH_POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_worker())
