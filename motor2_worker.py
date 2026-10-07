"""Bounded M2 data collection on its own browser, outside M1 publication."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlsplit

from scrapling.fetchers import AsyncStealthySession

from aiscore_basketball_data import BASKETBALL_READY_JS, fetch_basketball_data
from aiscore_browser import session_options
from aiscore_match_page import mobile_match_url
from motor2 import MAX_CAPTURE_DELAY_SECONDS, evaluate_m2, m2_status


logger = logging.getLogger(__name__)
READ_BUDGET_SECONDS = 40


def _expired(context):
    try:
        observed = datetime.fromisoformat(str(context["observed_at"]).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - observed).total_seconds()
        return not 0 <= age <= MAX_CAPTURE_DELAY_SECONDS
    except (KeyError, TypeError, ValueError):
        return True


class M2Collector:
    def __init__(self):
        self.session = None

    async def close(self):
        session, self.session = self.session, None
        if session is None:
            return
        try:
            await asyncio.wait_for(session.__aexit__(None, None, None), 10)
        except Exception:
            driver = getattr(session, "playwright", None)
            if driver is not None:
                try:
                    await asyncio.wait_for(driver.stop(), 5)
                except Exception:
                    logger.warning("M2 browser cleanup failed")

    async def read(self, alert, context):
        try:
            async with asyncio.timeout(READ_BUDGET_SECONDS):
                if self.session is None:
                    self.session = AsyncStealthySession(**session_options("m2", 20000))
                    await self.session.__aenter__()
                url = mobile_match_url(alert.get("url", "")) + "/odds"
                return await self._fetch_statistics(url, alert["match_id"], context)
        except TimeoutError:
            await self.close()
            return {"available": False, "error": "statistics_fetch_timeout"}
        except Exception as exc:
            logger.warning("M2 statistics unavailable: error_type=%s", type(exc).__name__)
            await self.close()
            return {"available": False, "error": "statistics_fetch_failed"}

    async def _fetch_statistics(self, url, match_id, context):
        # Scrapling owns navigation and challenge solving, as in the final reader.
        # Observe its page concurrently: unrelated load resources must not consume
        # the whole signal-time budget after the correct match is already ready.
        ready = asyncio.Event()
        holder = {}

        async def setup(page):
            holder["page"] = page
            page.set_default_timeout(8000)
            ready.set()

        async def observe():
            await ready.wait()
            page = holder["page"]
            while True:
                if urlsplit(page.url).path == urlsplit(url).path:
                    try:
                        matched = await page.evaluate(BASKETBALL_READY_JS, str(match_id),
                                                      isolated_context=False)
                    except Exception:
                        matched = False  # Redirect or hydration replaced the context.
                    if matched:
                        return await fetch_basketball_data(
                            page, match_id, expected_score=context.get("score"))
                await asyncio.sleep(.2)

        observation = asyncio.create_task(observe())

        async def action(page):
            # Keep the pool page alive if navigation finishes before the API read.
            await observation

        fetch = asyncio.create_task(self.session.fetch(
            url, solve_cloudflare=True, page_setup=setup, page_action=action,
            timeout=90000,
        ))
        try:
            done, _ = await asyncio.wait({fetch, observation}, return_when=asyncio.FIRST_COMPLETED)
            if observation in done:
                return observation.result()
            await fetch
            return {"available": False, "error": "statistics_reader_failed"}
        finally:
            for task in (fetch, observation):
                if not task.done():
                    task.cancel()
            await asyncio.gather(fetch, observation, return_exceptions=True)


async def run_m2_worker(database, *, enabled=True, collector=None):
    """Only rows explicitly marked pending at M1 INSERT are eligible."""
    collector = collector or M2Collector()
    try:
        while True:
            try:
                for alert in database.pending_m2_alerts():
                    pending = json.loads(alert["m2_analysis_json"])
                    context = pending.get("context") or {}
                    if not enabled:
                        analysis = m2_status("disabled", "M2 kapalı", context=context)
                    elif _expired(context):
                        analysis = m2_status("unavailable", "Sinyal anında veri alınamadı; analiz süresi doldu",
                                             context=context, reason_code="capture_expired")
                    else:
                        data = await collector.read(alert, context)
                        analysis = evaluate_m2(context, data)
                    database.complete_m2_analysis(alert["id"], analysis)
            except Exception as exc:
                # Never propagate failures into live scan or Telegram tasks.
                logger.warning("M2 worker failed: error_type=%s", type(exc).__name__)
            await asyncio.sleep(1)
    finally:
        await collector.close()
