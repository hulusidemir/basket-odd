"""
finished_match_service.py — Shared finished match checking service.
Used by both the background worker and manual UI-triggered checks.
"""

import asyncio
import logging
import os
import re
import threading
import time

try:
    from camoufox.async_api import AsyncNewBrowser
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    from playwright.async_api import async_playwright
except ModuleNotFoundError:
    AsyncNewBrowser = None
    PlaywrightTimeoutError = TimeoutError
    async_playwright = None


logger = logging.getLogger("finished_match_service")
_active_finished_scan_lock = threading.Lock()


def _check_failure(match: dict, error_code: str, *, attempts: int = 1) -> dict:
    return {
        "match_id": str(match.get("match_id") or ""),
        "match_name": str(match.get("match_name") or ""),
        "_check_error": error_code,
        "_check_attempts": max(1, int(attempts)),
    }


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def is_final_status(status: str) -> bool:
    """Only explicit final labels are allowed to settle a match."""
    return bool(re.match(r"^\s*(Full Time|FT|Finished|Ended|Final)\s*$", status or "", re.IGNORECASE))


def final_status_label(status: str) -> str:
    return "Full Time" if is_final_status(status) else ""


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


def _float_or_none(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def canonical_alert_direction(alert: dict) -> str:
    """Return the stored playable direction for result settlement."""
    return _normalize_direction(alert.get("direction"))


def _empty_result_summary(tracked_count: int = 0) -> dict:
    return {
        "tracked_count": tracked_count,
        "checked_count": 0,
        "check_failed_count": 0,
        "retry_count": 0,
        "coverage_percent": 100.0 if tracked_count == 0 else 0.0,
        "failure_counts": {},
        "check_failures": [],
        "finished_match_count": 0,
        "updated_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "in_progress_count": 0,
        "details": [],
    }


def _finished_checker(config) -> "AiscoreFinishedMatchChecker":
    return AiscoreFinishedMatchChecker(
        page_timeout_ms=getattr(
            config,
            "FINISHED_PAGE_TIMEOUT_MS",
            min(int(config.PAGE_TIMEOUT_MS), 20000),
        ),
        # The proxy is substantially more reliable with one match page at a
        # time. Sequential checks prioritize complete coverage over raw speed.
        concurrency=1,
        retry_attempts=getattr(config, "FINISHED_RETRY_ATTEMPTS", 1),
    )


def _merge_checker_report(
    summary: dict,
    checker: "AiscoreFinishedMatchChecker",
    attempted_count: int,
    results: list[dict],
) -> None:
    report = checker.last_report
    if report.get("attempted_count") == attempted_count:
        checked_count = int(report.get("checked_count", len(results)))
        failed_count = int(
            report.get("check_failed_count", max(0, attempted_count - checked_count))
        )
        retry_count = int(report.get("retry_count", 0))
        failure_counts = dict(report.get("failure_counts") or {})
        failures = list(report.get("failures") or [])
    else:
        # Keep custom checker implementations and tests compatible without
        # describing missing results as successfully checked.
        checked_count = len(results)
        failed_count = max(0, attempted_count - checked_count)
        retry_count = 0
        failure_counts = {"unknown_error": failed_count} if failed_count else {}
        failures = []

    summary["checked_count"] += checked_count
    summary["check_failed_count"] += failed_count
    summary["retry_count"] += retry_count
    for error_code, count in failure_counts.items():
        summary["failure_counts"][error_code] = (
            summary["failure_counts"].get(error_code, 0) + int(count)
        )
    summary["check_failures"].extend(failures)
    summary["coverage_percent"] = (
        round(summary["checked_count"] / summary["tracked_count"] * 100, 1)
        if summary["tracked_count"]
        else 100.0
    )


def _settle_deleted_match_from_final_score(
    db,
    summary: dict,
    match_id: str,
    final_score: str,
    final_status: str,
    *,
    count_checked: bool = True,
    force: bool = False,
) -> bool:
    if not match_id or not is_final_status(final_status):
        return False
    final_total = parse_score_total(final_score)
    if final_total is None:
        return False

    alerts = (
        db.get_deleted_alerts_for_match(match_id)
        if force
        else db.get_deleted_alerts_for_result_check(match_id)
    )
    if not alerts:
        return False

    final_label = final_status_label(final_status)
    db.update_deleted_match_final_observation(
        match_id,
        final_score=final_score,
        final_status=final_label,
    )
    updated_any = False

    def mark_match_updated() -> None:
        nonlocal updated_any
        if updated_any:
            return
        if count_checked:
            summary["checked_count"] += 1
        summary["finished_match_count"] += 1
        updated_any = True

    for alert in alerts:
        playable_direction = canonical_alert_direction(alert)
        live_line = _float_or_none(alert.get("live"))
        if live_line is None:
            continue
        signal_result = evaluate_signal_result(playable_direction, live_line, final_total)
        if not signal_result:
            continue

        updated = db.update_deleted_alert_final_result(
            alert["id"],
            result=signal_result,
            final_score=final_score,
            final_status=final_label,
            force=force,
        )
        if not updated:
            continue

        mark_match_updated()
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
            "direction": playable_direction,
            "live_line": live_line,
            "final_score": final_score,
            "final_total": final_total,
            "result": signal_result,
        })

    return updated_any


def _deleted_result_message(summary: dict) -> str:
    summary["coverage_percent"] = (
        round(summary["checked_count"] / summary["tracked_count"] * 100, 1)
        if summary["tracked_count"]
        else 100.0
    )
    if summary["tracked_count"] == 0:
        return "Kontrol edilecek silinen maç bulunamadı."
    if summary["checked_count"] == 0:
        return (
            f"{summary['tracked_count']} maç kontrol listesinde, ancak maç sayfasına ulaşılamadı "
            "ve kayıtlı final skor bulunamadı. Biraz sonra tekrar deneyin."
        )
    return (
        f"{summary['checked_count']} maç kontrol edildi, "
        f"{summary['finished_match_count']} maç bitmiş bulundu, "
        f"{summary['updated_count']} sinyal güncellendi."
    )



class AiscoreFinishedMatchChecker:
    def __init__(
        self,
        *,
        page_timeout_ms: int,
        concurrency: int = 1,
        retry_attempts: int = 1,
    ):
        self.page_timeout_ms = page_timeout_ms
        self.concurrency = max(1, int(concurrency))
        self.retry_attempts = max(0, int(retry_attempts))
        self.last_report = {
            "attempted_count": 0,
            "checked_count": 0,
            "check_failed_count": 0,
            "retry_count": 0,
            "failure_counts": {},
            "failures": [],
        }

    async def _launch_headless_context(self, playwright):
        try:
            proxy_server = os.getenv("PLAYWRIGHT_PROXY")
            launch_kwargs: dict = {
                "headless": True,
                "humanize": True,
                "locale": "en-US",
            }
            if proxy_server:
                launch_kwargs["proxy"] = {"server": proxy_server}
                logger.info("Finished-match checker proxy enabled.")
            browser = await AsyncNewBrowser(playwright, **launch_kwargs)
            context = await browser.new_context(
                viewport={"width": 430, "height": 932},
                locale="en-US",
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            return browser, context, True
        except Exception as exc:
            raise RuntimeError(
                "Camoufox could not be started. Run 'python -m camoufox fetch'."
            ) from exc

    async def check_matches(self, tracked_matches: list[dict]) -> list[dict]:
        if not tracked_matches:
            self.last_report = {
                "attempted_count": 0,
                "checked_count": 0,
                "check_failed_count": 0,
                "retry_count": 0,
                "failure_counts": {},
                "failures": [],
            }
            return []
        if async_playwright is None or AsyncNewBrowser is None:
            raise RuntimeError("Camoufox is not installed. Run 'python -m camoufox fetch'.")

        results = []
        failures = []
        retry_count = 0
        async with async_playwright() as playwright:
            browser, context, should_close_browser = await self._launch_headless_context(playwright)
            try:
                for index in range(0, len(tracked_matches), self.concurrency):
                    batch = tracked_matches[index:index + self.concurrency]
                    batch_results = await asyncio.gather(
                        *(self._check_single_with_retry(context, match) for match in batch),
                        return_exceptions=True,
                    )
                    for match, item in zip(batch, batch_results):
                        if isinstance(item, dict) and not item.get("_check_error"):
                            results.append(item)
                            retry_count += max(0, int(item.pop("_check_attempts", 1)) - 1)
                        elif isinstance(item, dict):
                            failures.append(item)
                            retry_count += max(0, int(item.get("_check_attempts", 1)) - 1)
                        elif isinstance(item, Exception):
                            failures.append(_check_failure(match, "unexpected_error"))
                            logger.warning(
                                "Finished check failed unexpectedly: match_id=%s error=%s",
                                match.get("match_id"),
                                type(item).__name__,
                            )
            finally:
                if should_close_browser:
                    await browser.close()

        failure_counts: dict[str, int] = {}
        for failure in failures:
            error_code = str(failure.get("_check_error") or "unknown_error")
            failure_counts[error_code] = failure_counts.get(error_code, 0) + 1
        self.last_report = {
            "attempted_count": len(tracked_matches),
            "checked_count": len(results),
            "check_failed_count": len(failures),
            "retry_count": retry_count,
            "failure_counts": failure_counts,
            "failures": [
                {
                    "match_id": failure.get("match_id", ""),
                    "error_code": failure.get("_check_error", "unknown_error"),
                    "attempts": int(failure.get("_check_attempts", 1)),
                }
                for failure in failures[:20]
            ],
        }
        if failures:
            logger.warning(
                "Finished-match coverage incomplete: checked=%s attempted=%s failures=%s",
                len(results),
                len(tracked_matches),
                failure_counts,
            )
        return results

    async def _check_single_with_retry(self, context, match: dict) -> dict:
        last_failure = _check_failure(match, "unknown_error")
        total_attempts = self.retry_attempts + 1
        for attempt in range(1, total_attempts + 1):
            try:
                result = await self._check_single(context, match)
            except Exception:
                logger.exception(
                    "Unexpected finished-match check error: match_id=%s attempt=%s",
                    match.get("match_id"),
                    attempt,
                )
                result = _check_failure(match, "unexpected_error")
            if result and not result.get("_check_error"):
                result["_check_attempts"] = attempt
                return result
            if isinstance(result, dict):
                last_failure = result
            if attempt < total_attempts:
                await asyncio.sleep(0.35 * attempt)
        last_failure["_check_attempts"] = total_attempts
        return last_failure

    async def _check_single(self, context, match: dict) -> dict:
        page = None

        try:
            page = await context.new_page()
            page.set_default_timeout(self.page_timeout_ms)
            await page.set_extra_http_headers({
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            })
            target_url = re.sub(
                r"^https?://(?:www\.)?aiscore\.com",
                "https://m.aiscore.com",
                str(match["url"]),
                flags=re.IGNORECASE,
            )
            sep = "&" if "?" in target_url else "?"
            await page.goto(f"{target_url}{sep}_fresh_check={int(time.time() * 1000)}", wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            parsed = await page.evaluate(
                r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();
                  const leaf = el => el.children.length === 0;
                  const allLeafs = Array.from(document.querySelectorAll('span, div, strong, b'))
                    .filter(leaf)
                    .map(el => {
                      const rect = el.getBoundingClientRect();
                      return {
                        el,
                        txt: text(el.innerText),
                        rect,
                        size: parseFloat(window.getComputedStyle(el).fontSize) || 0,
                        cls: (el.className || '').toString(),
                      };
                    })
                    .filter(o => o.txt && o.rect.width > 0 && o.rect.height > 0);
                  const mainLeafs = allLeafs.filter(o => o.rect.top >= 0 && o.rect.top < 460);

                  const finishedRe = /^(Full Time|FT|Finished|Ended|Final)$/i;
                  const liveRe     = /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th|BT)(\s*[-\s]?\s*\d{1,2}:\d{2})?$/i;
                  const clockRe    = /^\d{1,2}:\d{2}$/;

                  let statusEl = null;
                  let status = '';
                  let isFinished = false;

                  // Do not infer "finished" from score-looking elements or CSS classes.
                  // Some live AiScore pages expose final-score-ish containers before the
                  // match ends; only an explicit Full Time/FT label may settle results.

                  const nums = mainLeafs.filter(o => /^\d{1,3}$/.test(o.txt));

                  let score = '';
                  let scorePair = null;

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
                    scorePair = [leftEl, rightEl];
                  }

                  // Strategy B: directly combined "93-62" pattern near the main scoreboard.
                  if (!score) {
                    const combined = mainLeafs
                      .find(o => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(o.txt) && o.size >= 16);
                    if (combined) {
                      score = combined.txt;
                      scorePair = [combined, combined];
                    }
                  }

                  // Strategy C (last resort): top-of-page biggest-two heuristic,
                  // but require both numbers to share font size and be clearly large.
                  if (!score) {
                    const top = nums.filter(n => n.rect.top < 300)
                      .sort((a, b) => b.size - a.size);
                    if (top.length >= 2 && top[0].size >= 20 && Math.abs(top[0].size - top[1].size) < 2) {
                      const a = top[0], b = top[1];
                      const leftEl  = a.rect.left <= b.rect.left ? a : b;
                      const rightEl = a.rect.left <= b.rect.left ? b : a;
                      score = `${leftEl.txt} - ${rightEl.txt}`;
                      scorePair = [leftEl, rightEl];
                    }
                  }

                  if (scorePair) {
                    const left = scorePair[0], right = scorePair[1];
                    const scoreCenterY = (
                      (left.rect.top + left.rect.bottom) / 2 +
                      (right.rect.top + right.rect.bottom) / 2
                    ) / 2;
                    const minX = Math.min(left.rect.left, right.rect.left) - 220;
                    const maxX = Math.max(left.rect.right, right.rect.right) + 220;
                    const nearScoreStatus = mainLeafs.filter(o => {
                      const cx = (o.rect.left + o.rect.right) / 2;
                      const cy = (o.rect.top + o.rect.bottom) / 2;
                      return cy >= scoreCenterY - 95
                        && cy <= scoreCenterY + 95
                        && cx >= minX
                        && cx <= maxX
                        && (finishedRe.test(o.txt) || liveRe.test(o.txt) || clockRe.test(o.txt));
                    });
                    const live = nearScoreStatus.find(o => liveRe.test(o.txt) || clockRe.test(o.txt));
                    const final = nearScoreStatus.find(o => finishedRe.test(o.txt));
                    if (live) {
                      statusEl = live.el;
                      status = live.txt;
                      isFinished = false;
                    } else if (final) {
                      statusEl = final.el;
                      status = 'Full Time';
                      isFinished = true;
                    }
                  }

                  if (!status) {
                    const live = mainLeafs.find(o => liveRe.test(o.txt) || clockRe.test(o.txt));
                    const final = mainLeafs.find(o => finishedRe.test(o.txt) && o.rect.top < 360);
                    if (live) {
                      statusEl = live.el;
                      status = live.txt;
                    } else if (final) {
                      statusEl = final.el;
                      status = 'Full Time';
                      isFinished = true;
                    }
                  }

                  // Basketball sanity guard: reject live/non-final and absurd totals before writing anywhere.
                  if (!isFinished) {
                    score = '';
                  } else {
                    const m = score.match(/^\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$/);
                    if (m) {
                      const total = parseInt(m[1]) + parseInt(m[2]);
                      if (total < 60 || total > 400) score = '';
                    } else {
                      score = '';
                    }
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
                return _check_failure(match, "empty_page")
            parsed_title = parsed.get("title") or ""
            if re.search(r"just a moment|access denied|verify you are human", parsed_title, re.IGNORECASE):
                return _check_failure(match, "blocked")
            if not (parsed.get("status") or parsed.get("score")):
                return _check_failure(match, "parse_failed")

            status = parsed.get("status") or ""
            score = parsed.get("score") or ""
            finished = bool(parsed.get("isFinished")) and is_final_status(status)
            if finished and parse_score_total(score) is None:
                return _check_failure(match, "final_score_parse_failed")

            return {
                "match_id": match["match_id"],
                "match_name": parsed_title or match.get("match_name", ""),
                "status": status,
                "score": score,
                "is_finished": finished,
                "source": "mobile",
            }
        except PlaywrightTimeoutError:
            return _check_failure(match, "timeout")
        except Exception as exc:
            logger.debug("Could not check match %s: %s", match.get("match_id"), exc)
            return _check_failure(match, "browser_error")
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    logger.debug("Could not close finished-match page", exc_info=True)


async def run_active_match_finished_scan(db, config, before_delete=None) -> dict:
    """Scan active (not-yet-deleted) alerts for matches that have ended, and
    soft-delete them so they flow into the finished-match result pipeline."""
    if not _active_finished_scan_lock.acquire(blocking=False):
        return {
            **_empty_result_summary(),
            "busy": True,
            "archive_failed_count": 0,
            "moved_count": 0,
            "message": (
                "Biten maç kontrolü zaten çalışıyor. "
                "Mevcut tarama tamamlanınca tekrar deneyin."
            ),
        }
    try:
        return await _run_active_match_finished_scan(db, config, before_delete)
    finally:
        _active_finished_scan_lock.release()


async def _run_active_match_finished_scan(db, config, before_delete=None) -> dict:
    tracked_matches = db.get_active_matches_with_urls()
    summary = {
        **_empty_result_summary(tracked_count=len(tracked_matches)),
        "finished_match_count": 0,
        "moved_count": 0,
        "archive_failed_count": 0,
    }
    if not tracked_matches:
        summary["message"] = "Taranacak aktif maç bulunamadı."
        return summary

    checker = _finished_checker(config)
    results = await checker.check_matches(tracked_matches)
    _merge_checker_report(summary, checker, len(tracked_matches), results)

    for result in results:
        if not result.get("is_finished"):
            continue
        summary["finished_match_count"] += 1
        match_id = result.get("match_id")
        if not match_id:
            continue
        try:
            archive_result = None
            if before_delete is not None:
                archive_result = before_delete(match_id)

            # The production callback captures the enriched live-dashboard rows
            # and archives them atomically. None retains legacy snapshot-only
            # callback compatibility.
            if isinstance(archive_result, int):
                affected = archive_result
            else:
                affected = db.delete_match_data(
                    match_id,
                    require_display_snapshot=before_delete is not None,
                )
            if affected > 0:
                summary["moved_count"] += 1
                settlement = _empty_result_summary(tracked_count=1)
                _settle_deleted_match_from_final_score(
                    db,
                    settlement,
                    match_id,
                    result.get("score", ""),
                    result.get("status", ""),
                    count_checked=False,
                )
                for key in ("updated_count", "successful_count", "failed_count", "push_count"):
                    summary[key] += settlement[key]
                detail = {
                    "match_id": match_id,
                    "match_name": result.get("match_name", ""),
                    "status": result.get("status", ""),
                    "final_score": result.get("score", ""),
                    "affected_alerts": affected,
                    "settled_alerts": settlement["updated_count"],
                }
                if settlement["details"]:
                    detail["results"] = settlement["details"]
                summary["details"].append(detail)
        except Exception as exc:
            summary["archive_failed_count"] += 1
            summary["details"].append({
                "match_id": match_id,
                "match_name": result.get("match_name", ""),
                "error_code": "ARCHIVE_FAILED",
            })
            logger.exception(
                "Finished match archive failed; remaining matches continue: match_id=%s error=%s",
                match_id,
                exc,
            )

    coverage = f"{summary['checked_count']}/{summary['tracked_count']} maç doğrulandı"
    if summary["check_failed_count"]:
        coverage += (
            f"; {summary['check_failed_count']} maça ulaşılamadı ve aktif bırakıldı"
        )

    if summary["moved_count"] == 0 and summary["finished_match_count"] == 0:
        summary["message"] = (
            f"{coverage}. Biten maç bulunamadı."
        )
    else:
        summary["message"] = (
            f"{summary['moved_count']} biten maç Silinen Maçlar'a taşındı "
            f"({coverage})."
        )
    return summary


async def run_deleted_match_result_cycle(db, config) -> dict:
    tracked_matches = db.get_deleted_matches_for_result_check(limit=None)
    logger.info("Checking %s deleted matches for final results.", len(tracked_matches))

    if not tracked_matches:
        return {**_empty_result_summary(), "message": "Kontrol edilecek silinen maç bulunamadı."}

    return await _run_deleted_match_result_check_for_matches(db, config, tracked_matches)


async def run_single_deleted_match_result_check(db, config, alert_id: int) -> dict:
    # Unlike the bulk cycle, a user-triggered single recheck must re-evaluate the
    # alert regardless of its current `result` — otherwise pre-fix records stuck
    # with absurd scores (e.g. "8-12") can never be corrected via the UI button.
    empty_summary = _empty_result_summary()

    alert = db.get_deleted_alert_by_id(alert_id)
    tracked_match = db.get_deleted_match_for_result_check_by_alert_id(alert_id)
    if not alert or not tracked_match:
        return {**empty_summary, "message": "Kontrol edilecek maç bulunamadı."}
    summary = _empty_result_summary(tracked_count=1)
    # An unresolved legacy row may already carry a trustworthy final score. A
    # resolved row, however, was explicitly rechecked by the user and must be
    # fetched again; reusing its old final fields would only reproduce a stale
    # or previously misparsed result.
    if not str(alert.get("result") or "").strip():
        if _settle_deleted_match_from_final_score(
            db,
            summary,
            alert["match_id"],
            tracked_match.get("score", ""),
            tracked_match.get("status", ""),
        ):
            summary["message"] = _deleted_result_message(summary)
            return summary

    checker = _finished_checker(config)
    results = await checker.check_matches([tracked_match])
    _merge_checker_report(summary, checker, 1, results)

    if not results:
        summary["message"] = "Maç sayfasına ulaşılamadı."
        return summary

    result = results[0]
    if not result.get("is_finished"):
        summary["in_progress_count"] = db.mark_deleted_match_in_progress(alert["match_id"])
        summary["message"] = (
            "Maç sayfası yeniden kontrol edildi; Full Time görülmedi. "
            f"{summary['in_progress_count']} sinyal Devam Ediyor olarak güncellendi."
        )
        return summary

    final_score = result.get("score", "")
    final_total = parse_score_total(final_score)
    if final_total is None:
        summary["message"] = (
            "Maç bitmiş görünüyor ama final skoru güvenilir okunamadı. "
            "Birkaç dakika sonra tekrar deneyin."
        )
        return summary

    _settle_deleted_match_from_final_score(
        db,
        summary,
        alert["match_id"],
        final_score,
        result.get("status", ""),
        count_checked=False,
        force=True,
    )
    summary["message"] = _deleted_result_message(summary)
    return summary


async def _run_deleted_match_result_check_for_matches(db, config, tracked_matches: list[dict]) -> dict:
    summary = _empty_result_summary(tracked_count=len(tracked_matches))
    remaining_matches = []
    for match in tracked_matches:
        settled = _settle_deleted_match_from_final_score(
            db,
            summary,
            match.get("match_id", ""),
            match.get("score", ""),
            match.get("status", ""),
        )
        if not settled:
            remaining_matches.append(match)

    if not remaining_matches:
        summary["message"] = _deleted_result_message(summary)
        return summary

    checker = _finished_checker(config)
    results = await checker.check_matches(remaining_matches)
    _merge_checker_report(summary, checker, len(remaining_matches), results)

    for result in results:
        if not result.get("is_finished"):
            affected = db.mark_deleted_match_in_progress(result.get("match_id", ""))
            if affected:
                summary["in_progress_count"] += affected
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug("Deleted match is finished but score is not parseable: %s", result.get("match_id"))
            continue

        _settle_deleted_match_from_final_score(
            db,
            summary,
            result["match_id"],
            final_score,
            result.get("status", ""),
            count_checked=False,
        )

    summary["message"] = _deleted_result_message(summary)
    return summary
