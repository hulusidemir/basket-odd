"""Bounded Scrapling reader for final match observations; no database writes."""

import asyncio
import logging
import re
import threading
import time
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from aiscore_browser import session_options
from aiscore_match_page import mobile_match_url, read_match_page

try:
    from patchright.async_api import TimeoutError as PlaywrightTimeoutError
    from scrapling.fetchers import AsyncStealthySession
except ModuleNotFoundError:
    AsyncStealthySession = None
    PlaywrightTimeoutError = TimeoutError

logger = logging.getLogger(__name__)
_finished_browser_lock = threading.Lock()
_BROWSER_START_TIMEOUT = 90
_BROWSER_CLOSE_TIMEOUT = 15
_MATCH_ATTEMPT_TIMEOUT = 120
_SCAN_TIMEOUT = 600


class FinishedCheckBusy(RuntimeError):
    """All final checks share one persistent Chromium profile."""


@asynccontextmanager
async def _finished_browser_session(options):
    if not _finished_browser_lock.acquire(blocking=False):
        raise FinishedCheckBusy("Başka bir final kontrolü çalışıyor. Biraz sonra tekrar deneyin.")
    session = None
    try:
        session = AsyncStealthySession(**options)
        await asyncio.wait_for(session.__aenter__(), _BROWSER_START_TIMEOUT)
        yield session
    finally:
        try:
            if session is not None:
                try:
                    await asyncio.wait_for(
                        session.__aexit__(None, None, None), _BROWSER_CLOSE_TIMEOUT,
                    )
                except Exception:
                    logger.warning("Final browser cleanup failed or timed out.")
                finally:
                    # Scrapling.close() can skip cleanup after a partial start,
                    # or stall while closing a page. Stop this session's driver
                    # as well so it cannot retain the persistent profile lock.
                    driver = getattr(session, "playwright", None)
                    if driver is not None:
                        try:
                            await asyncio.wait_for(driver.stop(), _BROWSER_CLOSE_TIMEOUT)
                        except Exception:
                            logger.warning("Final browser driver cleanup failed or timed out.")
        finally:
            _finished_browser_lock.release()


def _check_failure(match: dict, error_code: str, *, attempts: int = 1) -> dict:
    return {
        "match_id": str(match.get("match_id") or ""),
        "match_name": str(match.get("match_name") or ""),
        "_check_error": error_code,
        "_check_attempts": max(1, int(attempts)),
    }


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

    def _scrapling_session_options(self) -> dict:
        return session_options("finished", self.page_timeout_ms, max_pages=self.concurrency)


    async def check_matches(self, tracked_matches: list[dict], *, on_result=None, on_progress=None) -> list[dict]:
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
        if AsyncStealthySession is None:
            raise RuntimeError("Scrapling is not installed.")

        def progress(phase):
            if on_progress:
                on_progress({"phase": phase, "tracked_count": len(tracked_matches),
                             "processed_count": len(results) + len(failures),
                             "checked_count": len(results), "check_failed_count": len(failures)})

        results = []
        failures = []
        retry_count = 0
        progress("opening_browser")
        async with _finished_browser_session(self._scrapling_session_options()) as session:
            if session.context is None:
                raise RuntimeError("Scrapling browser context could not be created")
            # A navigation timeout does not bound Scrapling's Cloudflare solver.
            # Preserve completed observations when the scan budget expires.
            index = 0
            try:
                async with asyncio.timeout(_SCAN_TIMEOUT):
                    for index in range(0, len(tracked_matches), self.concurrency):
                        progress("checking")
                        batch = tracked_matches[index:index + self.concurrency]
                        batch_results = await asyncio.gather(
                            *(self._check_single_with_retry(session, match) for match in batch),
                            return_exceptions=True,
                        )
                        for match, item in zip(batch, batch_results):
                            if isinstance(item, dict) and not item.get("_check_error"):
                                results.append(item)
                                if on_result:
                                    on_result(item)
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
                        progress("checking")
            except TimeoutError:
                failures.extend(
                    _check_failure(match, "scan_timeout")
                    for match in tracked_matches[index:]
                )

        progress("complete")
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

    async def _check_single_with_retry(self, session, match: dict) -> dict:
        last_failure = _check_failure(match, "unknown_error")
        total_attempts = self.retry_attempts + 1
        for attempt in range(1, total_attempts + 1):
            try:
                result = await asyncio.wait_for(
                    self._check_single(session, match), _MATCH_ATTEMPT_TIMEOUT,
                )
            except TimeoutError:
                result = _check_failure(match, "timeout")
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

    async def _check_single(self, session, match: dict) -> dict:
        try:
            target_url = mobile_match_url(match["url"])
            expected_id = target_url.rsplit("/", 1)[-1]
            if expected_id != str(match["match_id"]):
                return _check_failure(match, "match_identity_mismatch")
            state: dict = {}

            async def parse_match_page(page):
                page.set_default_timeout(self.page_timeout_ms)
                try:
                    state["parsed"] = await read_match_page(page, expected_id, self.page_timeout_ms)
                except Exception as exc:
                    state["error"] = exc

            await self._fetch_match_page(session, target_url, expected_id, state, parse_match_page)
            if state.get("error") is not None:
                raise state["error"]
            parsed = state.get("parsed")
            if not parsed:
                return _check_failure(match, "empty_page")
            if parsed.get("_check_error"):
                return _check_failure(match, parsed["_check_error"])
            parsed_title = parsed.get("title") or ""
            access_text = f"{parsed_title} {parsed.get('pageText') or ''}"
            if re.search(
                r"just a moment|access denied|verify you are human|"
                r"erişime engellenmiştir|has been blocked by the decision",
                access_text,
                re.IGNORECASE,
            ):
                return _check_failure(match, "blocked")
            if not (parsed.get("status") or parsed.get("score") or parsed.get("sourceVerified")):
                return _check_failure(match, "parse_failed")

            status = parsed.get("status") or ("Devam Ediyor" if parsed.get("sourceVerified") else "")
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

    async def _fetch_match_page(self, session, url, expected_id, state, page_action):
        """Read a ready scoreboard without waiting for unrelated page resources.

        Scrapling still owns navigation and Cloudflare solving. Its fetch waits
        for the full load event before page_action, which can stall on this site
        even after a valid scoreboard is visible. Observe that same page while
        fetch runs, and cancel navigation only after validating an observation.
        """
        ready = asyncio.Event()
        holder = {}

        async def page_setup(page):
            holder["page"] = page
            ready.set()

        async def observe():
            await ready.wait()
            page = holder["page"]
            path = urlsplit(url).path
            while True:
                # The pool may give us a page still showing the previous game.
                if urlsplit(page.url).path == path:
                    try:
                        parsed = await read_match_page(page, expected_id, self.page_timeout_ms)
                        blocked = re.search(
                            r"just a moment|access denied|verify you are human|has been blocked",
                            f"{parsed.get('title', '')} {parsed.get('pageText', '')}", re.I,
                        )
                        valid = (parsed.get("status") or parsed.get("sourceVerified")) and not blocked
                        if parsed.get("isFinished"):
                            valid = valid and parse_score_total(parsed.get("score", "")) is not None
                        if valid or parsed.get("_check_error"):
                            return parsed
                    except Exception:
                        # Redirects and hydration can replace the execution context.
                        pass
                await asyncio.sleep(0.25)

        fetch = asyncio.create_task(session.fetch(
            f"{url}?_fresh_check={int(time.time() * 1000)}",
            solve_cloudflare=True, page_setup=page_setup, page_action=page_action,
            timeout=max(90_000, self.page_timeout_ms),
        ))
        observation = asyncio.create_task(observe())
        try:
            done, _ = await asyncio.wait({fetch, observation}, return_when=asyncio.FIRST_COMPLETED)
            if observation in done:
                state["parsed"] = observation.result()
                state.pop("error", None)
            else:
                await fetch
        finally:
            for task in (fetch, observation):
                if not task.done():
                    task.cancel()
            await asyncio.gather(fetch, observation, return_exceptions=True)
