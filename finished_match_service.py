"""
finished_match_service.py — Shared finished match checking service.
Used by both the background worker and manual UI-triggered checks.
"""

import asyncio
import logging
import re

try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright
except ModuleNotFoundError:
    PlaywrightTimeoutError = TimeoutError
    async_playwright = None


logger = logging.getLogger("finished_match_service")


def parse_score_total(score: str) -> float | None:
    match = re.match(r"\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$", score or "")
    if not match:
        return None
    return float(int(match.group(1)) + int(match.group(2)))


def evaluate_signal_result(direction: str, live_line: float, final_total: float) -> str:
    direction_key = (direction or "").strip().upper()
    if abs(final_total - live_line) < 0.0001:
        return "İade"
    if direction_key == "ALT":
        return "Başarılı" if final_total < live_line else "Başarısız"
    if direction_key in {"ÜST", "UST"}:
        return "Başarılı" if final_total > live_line else "Başarısız"
    return ""



class AiscoreFinishedMatchChecker:
    def __init__(
        self,
        *,
        page_timeout_ms: int,
        concurrency: int = 4,
    ):
        self.page_timeout_ms = page_timeout_ms
        self.concurrency = concurrency

    async def _launch_headless_context(self, playwright):
        try:
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1600, "height": 1000},
                locale="en-US",
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            return browser, context, True
        except Exception as exc:
            raise RuntimeError(
                "Headless Chromium could not be started. "
                "Run 'playwright install chromium' on the server."
            ) from exc

    async def check_matches(self, tracked_matches: list[dict]) -> list[dict]:
        if not tracked_matches:
            return []
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Run 'pip install playwright' and 'playwright install chromium'.")

        results = []
        async with async_playwright() as playwright:
            browser, context, should_close_browser = await self._launch_headless_context(playwright)
            try:
                for index in range(0, len(tracked_matches), self.concurrency):
                    batch = tracked_matches[index:index + self.concurrency]
                    batch_results = await asyncio.gather(
                        *(self._check_single(context, match) for match in batch),
                        return_exceptions=True,
                    )
                    for item in batch_results:
                        if isinstance(item, dict):
                            results.append(item)
                        elif isinstance(item, Exception):
                            logger.debug("Finished check skipped due to error: %s", item)
            finally:
                if should_close_browser:
                    await browser.close()
        return results

    async def _check_single(self, context, match: dict) -> dict | None:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)

        try:
            await page.goto(match["url"], wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            parsed = await page.evaluate(
                r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();
                  const smallTexts = Array.from(document.querySelectorAll('span, div, strong, b'))
                    .map(el => text(el.innerText))
                    .filter(value => value.length > 0 && value.length < 50);

                  const exactFinished = smallTexts.find(value =>
                    /^(FT|Finished|Ended|Final|Full Time)$/i.test(value)
                  ) || '';

                  const liveStatus = smallTexts.find(value =>
                    /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(value)
                    || /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)$/i.test(value)
                    || /^\d{1,2}:\d{2}$/.test(value)
                  ) || '';

                  const status = exactFinished || liveStatus;

                  let score = '';
                  const directScore = smallTexts.find(value => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(value));
                  if (directScore) {
                    score = directScore;
                  }

                  if (!score) {
                    const topNumbers = Array.from(document.querySelectorAll('span, div, strong, b'))
                      .filter(el => {
                        const rect = el.getBoundingClientRect();
                        return rect.top < 220 && el.children.length === 0;
                      })
                      .map(el => ({
                        text: text(el.innerText),
                        size: parseFloat(window.getComputedStyle(el).fontSize) || 0
                      }))
                      .filter(item => /^\d{1,3}$/.test(item.text))
                      .sort((a, b) => b.size - a.size);

                    if (topNumbers.length >= 2 && topNumbers[0].size >= 16) {
                      score = `${topNumbers[0].text} - ${topNumbers[1].text}`;
                    }
                  }

                  const finishBadge = document.querySelector(
                    '[class*="finished"], [class*="Finished"], [class*="ended"], [class*="Ended"], [class*="final-score"], [class*="finalScore"]'
                  );

                  const title = text(document.title || '')
                    .replace(/\s*\|.*/, '')
                    .replace(/\s*-\s*AiScore.*/i, '')
                    .replace(/\s*live score.*/i, '')
                    .trim();

                  const isFinished = Boolean(exactFinished || finishBadge);
                  return { status, score, isFinished, title };
                }
                """
            )
            if not parsed:
                return None

            return {
                "match_id": match["match_id"],
                "match_name": parsed.get("title") or match.get("match_name", ""),
                "status": parsed.get("status") or "",
                "score": parsed.get("score") or "",
                "is_finished": bool(parsed.get("isFinished")),
            }
        except PlaywrightTimeoutError:
            logger.debug("Finished match check timeout: %s", match.get("url"))
            return None
        except Exception as exc:
            logger.debug("Could not check match %s: %s", match.get("match_id"), exc)
            return None
        finally:
            await page.close()


async def run_finished_match_cycle(db, config) -> dict:
    tracked_matches = db.get_tracked_deleted_matches(limit=config.FINISHED_MATCH_BATCH_SIZE)
    logger.info("Tracking %s deleted matches for finish checks.", len(tracked_matches))

    if not tracked_matches:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "archived_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
        }

    checker = AiscoreFinishedMatchChecker(
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=4,
    )

    results = await checker.check_matches(tracked_matches)
    summary = {
        "tracked_count": len(tracked_matches),
        "checked_count": len(results),
        "finished_match_count": 0,
        "archived_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "pending_count": 0,
        "details": [],
    }

    for result in results:
        if not result.get("is_finished"):
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug(
                "Match finished but final score could not be parsed yet: %s",
                result.get("match_id"),
            )
            continue

        summary["finished_match_count"] += 1
        pending_alerts = db.get_pending_deleted_alerts_for_match(result["match_id"])

        for alert in pending_alerts:
            signal_result = evaluate_signal_result(
                alert.get("direction", ""),
                float(alert.get("live") or 0),
                final_total,
            )
            inserted_id = db.archive_finished_alert(
                alert,
                final_status=result.get("status", ""),
                final_score=final_score,
                final_total=final_total,
                result=signal_result,
            )
            if not inserted_id:
                continue

            summary["archived_count"] += 1
            if signal_result == "Başarılı":
                summary["successful_count"] += 1
            elif signal_result == "Başarısız":
                summary["failed_count"] += 1
            elif signal_result == "İade":
                summary["push_count"] += 1
            else:
                summary["pending_count"] += 1

            summary["details"].append({
                "match_id": alert["match_id"],
                "match_name": alert["match_name"],
                "direction": alert["direction"],
                "live_line": float(alert["live"]),
                "final_score": final_score,
                "final_total": final_total,
                "result": signal_result,
            })

    return summary


async def run_deleted_match_result_cycle(db, config) -> dict:
    tracked_matches = db.get_deleted_matches_for_result_check(limit=None)
    logger.info("Checking %s deleted matches for final results.", len(tracked_matches))

    if not tracked_matches:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "updated_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
        }

    return await _run_deleted_match_result_check_for_matches(db, config, tracked_matches)


async def run_single_deleted_match_result_check(db, config, alert_id: int) -> dict:
    tracked_match = db.get_deleted_match_for_result_check_by_alert_id(alert_id)
    if not tracked_match:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "updated_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
            "message": "Kontrol edilecek maç bulunamadı.",
        }

    summary = await _run_deleted_match_result_check_for_matches(db, config, [tracked_match])
    if summary["finished_match_count"] == 0:
        summary["message"] = "Maç henüz bitmemiş veya final skoru alınamadı."
    elif summary["updated_count"] == 0:
        summary["message"] = "Maç bitmiş görünüyor ama güncellenecek boş sonuç bulunamadı."
    else:
        summary["message"] = f"{summary['updated_count']} sinyal güncellendi."
    return summary


async def _run_deleted_match_result_check_for_matches(db, config, tracked_matches: list[dict]) -> dict:
    checker = AiscoreFinishedMatchChecker(
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=4,
    )
    results = await checker.check_matches(tracked_matches)
    summary = {
        "tracked_count": len(tracked_matches),
        "checked_count": len(results),
        "finished_match_count": 0,
        "updated_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "details": [],
    }

    for result in results:
        if not result.get("is_finished"):
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug("Deleted match is finished but score is not parseable: %s", result.get("match_id"))
            continue

        summary["finished_match_count"] += 1
        alerts = db.get_deleted_alerts_for_result_check(result["match_id"])
        for alert in alerts:
            signal_result = evaluate_signal_result(
                alert.get("direction", ""),
                float(alert.get("live") or 0),
                final_total,
            )
            if not signal_result:
                continue

            updated = db.update_deleted_alert_final_result(
                alert["id"],
                result=signal_result,
                final_score=final_score,
                final_status=result.get("status", "") or "Full Time",
            )
            if not updated:
                continue

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
                "direction": alert["direction"],
                "live_line": float(alert["live"]),
                "final_score": final_score,
                "final_total": final_total,
                "result": signal_result,
            })

    return summary
