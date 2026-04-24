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
    total = int(match.group(1)) + int(match.group(2))
    # A real basketball match final rarely totals below ~100; anything under 60
    # is almost certainly a misread (single-quarter cell or team-name year digits).
    if total < 60 or total > 400:
        return None
    return float(total)


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
                  const leaf = el => el.children.length === 0;
                  const allLeafs = Array.from(document.querySelectorAll('span, div, strong, b')).filter(leaf);

                  const finishedRe = /^(Full Time|FT|Finished|Ended|Final)$/i;
                  const liveRe     = /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th|BT)(\s*[-\s]?\s*\d{1,2}:\d{2})?$/i;
                  const clockRe    = /^\d{1,2}:\d{2}$/;

                  let statusEl = null;
                  let status = '';
                  let isFinished = false;

                  for (const el of allLeafs) {
                    const t = text(el.innerText);
                    if (finishedRe.test(t)) { statusEl = el; status = t; isFinished = true; break; }
                  }
                  if (!statusEl) {
                    for (const el of allLeafs) {
                      const t = text(el.innerText);
                      if (liveRe.test(t) || clockRe.test(t)) { statusEl = el; status = t; break; }
                    }
                  }
                  // Only a narrow final-score class hook; [class*="score"] alone is too broad
                  // (the live-score container also has that substring and triggered false positives).
                  if (!isFinished) {
                    const badge = document.querySelector('[class*="final-score"], [class*="finalScore"]');
                    if (badge) isFinished = true;
                  }

                  const nums = allLeafs
                    .map(el => ({
                      el,
                      txt: text(el.innerText),
                      rect: el.getBoundingClientRect(),
                      size: parseFloat(window.getComputedStyle(el).fontSize) || 0,
                      cls: (el.className || '').toString(),
                    }))
                    .filter(o => /^\d{1,3}$/.test(o.txt));

                  let score = '';

                  // Strategy A: AiScore renders scores in <div class="score ...">.
                  // class~="score" is an exact token match (unlike [class*="score"] substring).
                  const scoreClassEls = nums.filter(n =>
                    n.cls.split(/\s+/).includes('score') && n.size >= 16
                  );
                  if (scoreClassEls.length >= 2) {
                    scoreClassEls.sort((a, b) => b.size - a.size || a.rect.left - b.rect.left);
                    const home = scoreClassEls[0];
                    let away = null;
                    for (let i = 1; i < scoreClassEls.length; i++) {
                      const c = scoreClassEls[i];
                      if (Math.abs(c.rect.top - home.rect.top) < 20 && c.rect.left !== home.rect.left) {
                        away = c; break;
                      }
                    }
                    if (!away) away = scoreClassEls[1];
                    const leftEl  = home.rect.left <= away.rect.left ? home : away;
                    const rightEl = home.rect.left <= away.rect.left ? away : home;
                    score = `${leftEl.txt} - ${rightEl.txt}`;
                  }

                  // Strategy B: anchor on the status label and pick one big number each side.
                  if (!score && statusEl) {
                    const sr = statusEl.getBoundingClientRect();
                    const sCenterX = (sr.left + sr.right) / 2;
                    const sCenterY = (sr.top + sr.bottom) / 2;
                    const aligned = nums.filter(n => {
                      const cy = (n.rect.top + n.rect.bottom) / 2;
                      return Math.abs(cy - sCenterY) < 60 && n.size >= 18;
                    }).sort((a, b) => b.size - a.size);

                    const lefts  = aligned.filter(n => n.rect.right <= sCenterX);
                    const rights = aligned.filter(n => n.rect.left  >= sCenterX);
                    if (lefts.length && rights.length) {
                      let pair = null;
                      for (const l of lefts) {
                        for (const r of rights) {
                          if (Math.abs(l.size - r.size) < 1) { pair = [l, r]; break; }
                        }
                        if (pair) break;
                      }
                      if (!pair) pair = [lefts[0], rights[0]];
                      score = `${pair[0].txt} - ${pair[1].txt}`;
                    }
                  }

                  // Strategy C: directly combined "93-62" pattern (rare AiScore variants).
                  if (!score) {
                    const combined = Array.from(document.querySelectorAll('span, div, strong, b'))
                      .map(el => text(el.innerText))
                      .find(t => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(t));
                    if (combined) score = combined;
                  }

                  // Strategy D (last resort): the old top-of-page biggest-two heuristic,
                  // but require both numbers to share font size and be clearly large.
                  if (!score) {
                    const top = nums.filter(n => n.rect.top < 220)
                      .sort((a, b) => b.size - a.size);
                    if (top.length >= 2 && top[0].size >= 20 && Math.abs(top[0].size - top[1].size) < 2) {
                      const a = top[0], b = top[1];
                      const leftEl  = a.rect.left <= b.rect.left ? a : b;
                      const rightEl = a.rect.left <= b.rect.left ? b : a;
                      score = `${leftEl.txt} - ${rightEl.txt}`;
                    }
                  }

                  // Basketball sanity guard: reject absurd totals before writing anywhere.
                  const m = score.match(/^\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$/);
                  if (m) {
                    const total = parseInt(m[1]) + parseInt(m[2]);
                    if (total < 60 || total > 400) score = '';
                  } else {
                    score = '';
                  }

                  const title = text(document.title || '')
                    .replace(/\s*\|.*/, '')
                    .replace(/\s*-\s*AiScore.*/i, '')
                    .replace(/\s*live score.*/i, '')
                    .replace(/\s*betting odds.*/i, '')
                    .trim();

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


async def run_active_match_finished_scan(db, config) -> dict:
    """Scan active (not-yet-deleted) alerts for matches that have ended, and
    soft-delete them so they flow into the finished-match result pipeline."""
    tracked_matches = db.get_active_matches_with_urls()
    summary = {
        "tracked_count": len(tracked_matches),
        "checked_count": 0,
        "finished_match_count": 0,
        "moved_count": 0,
        "details": [],
    }
    if not tracked_matches:
        summary["message"] = "Taranacak aktif maç bulunamadı."
        return summary

    checker = AiscoreFinishedMatchChecker(
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=4,
    )
    results = await checker.check_matches(tracked_matches)
    summary["checked_count"] = len(results)

    for result in results:
        if not result.get("is_finished"):
            continue
        summary["finished_match_count"] += 1
        match_id = result.get("match_id")
        if not match_id:
            continue
        affected = db.delete_match_data(match_id)
        if affected > 0:
            summary["moved_count"] += 1
            summary["details"].append({
                "match_id": match_id,
                "match_name": result.get("match_name", ""),
                "status": result.get("status", ""),
                "final_score": result.get("score", ""),
                "affected_alerts": affected,
            })

    if summary["moved_count"] == 0 and summary["finished_match_count"] == 0:
        summary["message"] = (
            f"{summary['tracked_count']} maç tarandı, biten maç bulunamadı."
        )
    else:
        summary["message"] = (
            f"{summary['moved_count']} biten maç Silinen Maçlar'a taşındı "
            f"({summary['tracked_count']} maç tarandı)."
        )
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
    # Unlike the bulk cycle, a user-triggered single recheck must re-evaluate the
    # alert regardless of its current `result` — otherwise pre-fix records stuck
    # with absurd scores (e.g. "8-12") can never be corrected via the UI button.
    empty_summary = {
        "tracked_count": 0,
        "checked_count": 0,
        "finished_match_count": 0,
        "updated_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "details": [],
    }

    alert = db.get_deleted_alert_by_id(alert_id)
    tracked_match = db.get_deleted_match_for_result_check_by_alert_id(alert_id)
    if not alert or not tracked_match:
        return {**empty_summary, "message": "Kontrol edilecek maç bulunamadı."}

    checker = AiscoreFinishedMatchChecker(
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=1,
    )
    results = await checker.check_matches([tracked_match])
    summary = {
        **empty_summary,
        "tracked_count": 1,
        "checked_count": len(results),
    }

    if not results:
        summary["message"] = "Maç sayfasına ulaşılamadı."
        return summary

    result = results[0]
    if not result.get("is_finished"):
        summary["message"] = "Maç henüz bitmemiş veya final skoru alınamadı."
        return summary

    final_score = result.get("score", "")
    final_total = parse_score_total(final_score)
    if final_total is None:
        summary["message"] = (
            "Maç bitmiş görünüyor ama final skoru güvenilir okunamadı. "
            "Birkaç dakika sonra tekrar deneyin."
        )
        return summary

    summary["finished_match_count"] = 1
    signal_result = evaluate_signal_result(
        alert.get("direction", ""),
        float(alert.get("live") or 0),
        final_total,
    )
    if not signal_result:
        summary["message"] = "Sinyal yönü tanınmadı."
        return summary

    updated = db.update_deleted_alert_final_result(
        alert_id,
        result=signal_result,
        final_score=final_score,
        final_status=result.get("status", "") or "Full Time",
    )
    if not updated:
        summary["message"] = "Güncelleme yapılamadı."
        return summary

    summary["updated_count"] = 1
    if signal_result == "Başarılı":
        summary["successful_count"] = 1
    elif signal_result == "Başarısız":
        summary["failed_count"] = 1
    elif signal_result == "İade":
        summary["push_count"] = 1
    summary["details"].append({
        "id": alert_id,
        "match_id": alert["match_id"],
        "match_name": alert["match_name"],
        "direction": alert["direction"],
        "live_line": float(alert["live"]),
        "final_score": final_score,
        "final_total": final_total,
        "result": signal_result,
    })
    summary["message"] = f"Sinyal güncellendi: {signal_result} ({final_score})"
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
