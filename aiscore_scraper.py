import asyncio
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager, aclosing
from datetime import datetime, timezone
from typing import Awaitable, Callable
from urllib.parse import urljoin, urlsplit, urlunsplit

from aiscore_browser import session_options
from aiscore_match_page import normalize_match_page
from aiscore_scoreboard import QUARTER_SCORES_JS
from scrapling.fetchers import AsyncStealthySession
from match_state import (
    confirmed_12_minute_quarters, game_clock, normalize_quarter_scores,
    parse_score, quarter_clock_seconds,
)

logger = logging.getLogger(__name__)


LIVE_TOTAL_MARKET_JS = r"""() => {
    const text = el => (el?.innerText || '').replace(/\s+/g, ' ').trim();
    const totalLabel = value =>
        /^(?:total(?: points)?|o\s*\/\s*u|over\s*[/&-]?\s*under)$/i.test(value);
    const locked = el => !el
        || /(?:^|[^a-z])(?:lock(?:ed|icon)?|islocked|suspend(?:ed)?|unavailable|closed)/i
            .test(`${el.className || ''} ${el.innerHTML || ''}`)
        || /^[-–—]+$/.test(text(el));
    const line = el => {
        if (locked(el)) return null;
        const leaves = [el, ...el.querySelectorAll('*')]
            .filter(node => !node.children.length);
        const values = leaves
            .map(text)
            .filter(value => /^(?:[ou]\s*)?\d+(?:\.\d+)?$/i.test(value))
            .map(value => Number(value.replace(/^[ou]\s*/i, '')))
            .filter(value => Number.isFinite(value) && value >= 100 && value <= 400);
        const unique = [...new Set(values.map(value => Number(value.toFixed(1))))];
        return unique.length === 1 ? unique[0] : null;
    };
    const result = {
        market_verified: false,
        has_locked_rows: false,
        opening_lines: [],
        prematch_lines: [],
        inplay_lines: [],
        bookmaker_lines: [],
    };
    for (const market of document.querySelectorAll('.oddsContent')) {
        if (!totalLabel(text(market.querySelector('.oddsType')))) continue;
        result.market_verified = true;
        for (const [index, box] of [...market.querySelectorAll('.oddsBoxContent')].entries()) {
            const openingCell = box.querySelector('.border1');
            const prematchCell = box.querySelector('.border2');
            const inplayCell = box.querySelector('.border3');
            const opening = line(openingCell);
            const prematch = line(prematchCell);
            const inplay = line(inplayCell);
            if (locked(inplayCell) || opening === null || inplay === null) {
                result.has_locked_rows = true;
                continue;
            }
            const company = box.closest('.oddsBox') || box.parentElement;
            const bookmaker = text(company?.querySelector('.companyName, .company, .name'))
                || company?.querySelector('img')?.getAttribute('alt')
                || `row-${index + 1}`;
            result.opening_lines.push(opening);
            result.prematch_lines.push(prematch);
            result.inplay_lines.push(inplay);
            result.bookmaker_lines.push(bookmaker);
        }
    }
    return result;
}"""


LIVE_TOTAL_MARKET_READY_JS = r"""() => {
    const text = el => (el?.innerText || '').replace(/\s+/g, ' ').trim();
    const exactTotal = value =>
        /^(?:total(?: points)?|o\s*\/\s*u|over\s*[/&-]?\s*under)$/i.test(value);
    return [...document.querySelectorAll('.oddsContent')].some(market =>
        exactTotal(text(market.querySelector('.oddsType')))
        && market.querySelector('.oddsBoxContent .border3')
    );
}"""


@dataclass(frozen=True)
class _MatchSkip:
    """A deliberately omitted listing row, with explicit health semantics."""

    reason: str
    degraded: bool = False
    retryable: bool = False


class _TransientNavigationError(RuntimeError):
    """AiScore could not be reached after bounded browser retries."""


def _redact_proxy_url(value: str) -> str:
    """Keep proxy diagnostics useful without logging embedded credentials."""
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        has_scheme = "://" in text
        parsed = urlsplit(text if has_scheme else f"//{text}")
        if parsed.username is None and parsed.password is None:
            return text
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        if has_scheme:
            return urlunsplit(
                (
                    parsed.scheme,
                    f"***:***@{host}",
                    parsed.path,
                    parsed.query,
                    parsed.fragment,
                )
            )
        suffix = parsed.path or ""
        if parsed.query:
            suffix += f"?{parsed.query}"
        if parsed.fragment:
            suffix += f"#{parsed.fragment}"
        return f"***:***@{host}{suffix}"
    except (TypeError, ValueError):
        return re.sub(r"^(.*://)?[^/@]+@", r"\1***:***@", text, count=1)


def _safe_env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using %s.", name, raw, default)
        return default
    if value < minimum or value > maximum:
        clamped = max(minimum, min(maximum, value))
        logger.warning(
            "%s=%s is outside %s-%s; using %s.",
            name,
            value,
            minimum,
            maximum,
            clamped,
        )
        return clamped
    return value


def _status_from_play_by_play_hint(value) -> dict:
    """Build a real clock status from AiScore's latest play-by-play row."""
    hint = value if isinstance(value, dict) else {}
    try:
        period = int(hint.get("period"))
    except (TypeError, ValueError):
        period = 0
    if period < 1 or period > 4:
        return {"status": "", "period_ended": False}

    period_ended = bool(hint.get("period_ended"))
    if period_ended:
        return {"status": f"Q{period}-Ended", "period_ended": True}

    raw_clock = str(hint.get("clock") or "").strip()
    clock_match = re.fullmatch(r"(\d{1,2}):(\d{2})", raw_clock)
    if clock_match:
        minutes = int(clock_match.group(1))
        seconds = int(clock_match.group(2))
        if minutes <= 20 and seconds < 60:
            return {
                "status": f"Q{period} {minutes:02d}:{seconds:02d}",
                "period_ended": False,
            }

    seconds_match = re.fullmatch(r"(\d{1,3})(?:\.\d+)?", raw_clock)
    if seconds_match:
        total_seconds = int(seconds_match.group(1))
        if total_seconds <= 20 * 60:
            minutes, seconds = divmod(total_seconds, 60)
            return {
                "status": f"Q{period} {minutes:02d}:{seconds:02d}",
                "period_ended": False,
            }

    return {"status": "", "period_ended": False}


def _detail_status_from_top_text(value) -> dict:
    """Classify AiScore's odds header without treating a quarter end as final."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    explicit_final = re.search(r"\b(?:Full Time|FT|Finished|Final)\b", text, re.IGNORECASE)
    clock = re.search(
        r"\b(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*(\d{1,2}:\d{2})\b",
        text,
        re.IGNORECASE,
    )
    period_ended = re.search(
        r"\b(?:Q([1-4])|([1-4])Q|([1-4])(?:st|nd|rd|th)(?:\s+Quarter)?)"
        r"\s*[-\s]?\s*Ended\b",
        text,
        re.IGNORECASE,
    )
    standalone_ended = bool(re.search(r"\bEnded\b", text, re.IGNORECASE)) and not period_ended

    if explicit_final or standalone_ended:
        return {"status": "Full Time", "is_finished": True, "period_ended": False}
    if clock:
        return {
            "status": f"{clock.group(1).upper()} {clock.group(2)}",
            "is_finished": False,
            "period_ended": False,
        }
    if period_ended:
        period = period_ended.group(1) or period_ended.group(2) or period_ended.group(3)
        return {
            "status": f"Q{period}-Ended",
            "is_finished": False,
            "period_ended": True,
        }
    return {"status": "", "is_finished": False, "period_ended": False}


def _valid_market_lines(values) -> list[float]:
    lines: list[float] = []
    if not isinstance(values, (list, tuple)):
        return lines
    for raw in values:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and 100 <= value <= 400:
            lines.append(round(value, 1))
    return lines


def _normalize_market_snapshot(value) -> dict:
    """Keep all totals aligned to the same bookmaker, including missing prematch."""
    raw = value if isinstance(value, dict) else {}
    result = {f"{name}_lines": [] for name in ("opening", "prematch", "inplay")}
    result["bookmaker_lines"] = []

    def values(name):
        value = raw.get(f"{name}_lines")
        return value if isinstance(value, (list, tuple)) else []

    prematches = values("prematch")
    bookmakers = values("bookmaker")
    for index, (opening, inplay) in enumerate(zip(values("opening"), values("inplay"))):
        opening = _valid_market_lines([opening])
        inplay = _valid_market_lines([inplay])
        if not opening or not inplay:
            continue
        prematch = _valid_market_lines([prematches[index]]) if index < len(prematches) else []
        result["opening_lines"].append(opening[0])
        result["inplay_lines"].append(inplay[0])
        result["prematch_lines"].append(prematch[0] if prematch else None)
        result["bookmaker_lines"].append(
            str(bookmakers[index]).strip() if index < len(bookmakers) else ""
        )
    return result


def _select_market_line(snapshot: dict, name: str) -> float | None:
    """Use the first readable paired bookmaker row."""
    lines = snapshot[f"{name}_lines"]
    return lines[0] if lines else None


class AiscoreScraper:
    def __init__(
        self,
        aiscore_url: str,
        max_matches_per_cycle: int = 40,
        page_timeout_ms: int = 30000,
        concurrency: int | None = None,
        match_timeout_seconds: float = 90.0,
        stale_line_seconds: float = 90.0,
        stale_score_delta: int = 10,
        stale_game_minutes: float = 2.0,
        persistent_session: bool = False,
    ):
        self.aiscore_url = aiscore_url
        self.max_matches_per_cycle = max_matches_per_cycle
        self.page_timeout_ms = page_timeout_ms
        self.concurrency = concurrency
        self.match_timeout_seconds = max(10.0, float(match_timeout_seconds))
        self.stale_line_seconds = max(10.0, float(stale_line_seconds))
        self.stale_score_delta = max(1, int(stale_score_delta))
        self.stale_game_minutes = max(0.5, float(stale_game_minutes))
        self.last_report: dict = {}
        self._last_listing_diagnostics: dict = {}
        self._line_observations: dict[tuple[str, str], dict] = {}
        self._confirmed_12_minute_matches: set[str] = set()
        self._effective_concurrency = min(2, self._concurrent_tabs())
        self._healthy_concurrency_cycles = 0
        self.persistent_session = persistent_session
        self._session = None
        self._session_started = 0.0

    async def close(self):
        session, self._session = self._session, None
        if session is not None:
            try:
                await asyncio.wait_for(session.__aexit__(None, None, None), 15)
            except Exception:
                driver = getattr(session, "playwright", None)
                if driver is not None:
                    await asyncio.wait_for(driver.stop(), 15)
                raise

    @asynccontextmanager
    async def _live_session(self):
        # Renew periodically as well as after failed/cancelled cycles.
        if self._session is not None and time.monotonic() - self._session_started > 1800:
            await self.close()
        try:
            if self._session is None:
                self._session = AsyncStealthySession(**self._scrapling_session_options())
                await asyncio.wait_for(self._session.__aenter__(), 90)
                self._session_started = time.monotonic()
            yield self._session
        except BaseException:
            await self.close()
            raise
        else:
            if not self.persistent_session or self.last_report.get("status") == "partial":
                await self.close()

    async def _detail_results(self, context, links):
        """Refill vacant slots without waiting for the slowest page in a batch."""
        pending = set()
        iterator = iter(links)
        exhausted = False
        try:
            while pending or not exhausted:
                while not exhausted and len(pending) < self._effective_concurrency:
                    link = next(iterator, None)
                    if link is None:
                        exhausted = True
                        break
                    pending.add(asyncio.create_task(self._extract_single_with_timeout(context, link)))
                if not pending:
                    break
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                results = []
                for task in done:
                    try:
                        results.append(task.result())
                    except Exception as exc:
                        results.append(exc)
                for result in results:
                    if isinstance(result, _TransientNavigationError) or (
                        isinstance(result, _MatchSkip) and result.reason == "match_timeout"
                    ):
                        self._reduce_concurrency()
                    yield result
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    @staticmethod
    def _mobile_url(value: str) -> str:
        parsed = urlsplit(value)
        return urlunsplit((
            "https",
            "m.aiscore.com",
            parsed.path or "/basketball",
            parsed.query,
            "",
        ))

    @staticmethod
    def _sanitize_tournament(value: str, url: str) -> str:
        text = (value or "").strip()
        if text and not re.search(r"standings|popular|trending|featured", text, re.I):
            return text
        m = re.search(r"/tournament-([a-z0-9-]+)", str(url or ""), re.I)
        if m and m.group(1):
            return m.group(1).replace("-", " ").title()
        return ""

    def _scrapling_session_options(self) -> dict:
        return session_options(
            "live",
            self.page_timeout_ms,
            max_pages=self._concurrent_tabs(),
        )

    def _concurrent_tabs(self) -> int:
        if self.concurrency is None:
            return _safe_env_int("AISCORE_CONCURRENCY", 4, minimum=1, maximum=8)
        try:
            return max(1, min(8, int(self.concurrency)))
        except (TypeError, ValueError):
            logger.warning("Invalid scraper concurrency=%r; using 4.", self.concurrency)
            return 4

    def _reduce_concurrency(self) -> None:
        previous = self._effective_concurrency
        self._effective_concurrency = max(1, previous // 2)
        self._healthy_concurrency_cycles = 0
        if self._effective_concurrency < previous:
            logger.warning(
                "Reducing live detail concurrency after navigation failures: %s -> %s",
                previous,
                self._effective_concurrency,
            )

    def _record_healthy_concurrency_cycle(self) -> None:
        ceiling = self._concurrent_tabs()
        if self._effective_concurrency >= ceiling:
            self._healthy_concurrency_cycles = 0
            return
        self._healthy_concurrency_cycles += 1
        if self._healthy_concurrency_cycles < 3:
            return
        previous = self._effective_concurrency
        self._effective_concurrency = min(ceiling, previous + 1)
        self._healthy_concurrency_cycles = 0
        logger.info(
            "Increasing live detail concurrency after healthy cycles: %s -> %s",
            previous,
            self._effective_concurrency,
        )

    def _elapsed_game_minutes(self, match: dict) -> float | None:
        match_id = str(match.get("match_id") or "")
        status = str(match.get("status") or "")
        observed = quarter_clock_seconds(status)
        if match_id and observed is not None and observed > 10 * 60:
            static_clock = game_clock("", tournament=str(match.get("tournament") or ""))
            if static_clock["period_count"] == 4:
                self._confirmed_12_minute_matches.add(match_id)
        with confirmed_12_minute_quarters(match_id in self._confirmed_12_minute_matches):
            clock = game_clock(
                status,
                str(match.get("match_name") or ""),
                str(match.get("tournament") or ""),
            )
        period = clock.get("period")
        remaining = clock.get("remaining_min")
        quarter_length = clock.get("quarter_length")
        if period is None or remaining is None or not quarter_length:
            return None
        return (period - 1) * quarter_length + (quarter_length - remaining)

    def _stale_line_skip(self, match: dict) -> _MatchSkip | None:
        """Detect a line that stays frozen while both score and clock advance."""
        home_score, away_score = parse_score(str(match.get("score") or ""))
        elapsed = self._elapsed_game_minutes(match)
        if home_score is None or away_score is None or elapsed is None:
            return None

        observed_at = float(match.get("_market_captured_monotonic") or time.monotonic())
        bookmaker = str(match.get("bookmaker") or "unknown")
        key = (str(match.get("match_id") or ""), bookmaker)
        current = {
            "line": float(match["inplay_total"]),
            "score_total": home_score + away_score,
            "elapsed": elapsed,
            "observed_at": observed_at,
        }
        previous = self._line_observations.get(key)
        if previous is None or previous["line"] != current["line"]:
            self._line_observations[key] = current
            return None

        stationary_seconds = observed_at - previous["observed_at"]
        score_delta = current["score_total"] - previous["score_total"]
        elapsed_delta = current["elapsed"] - previous["elapsed"]
        if (
            stationary_seconds >= self.stale_line_seconds
            and score_delta >= self.stale_score_delta
            and elapsed_delta >= self.stale_game_minutes
        ):
            logger.warning(
                "Detected stale live total: match=%s bookmaker=%s line=%.1f "
                "age=%.1fs score_delta=%s elapsed_delta=%.2f",
                match.get("match_name"),
                bookmaker,
                current["line"],
                stationary_seconds,
                score_delta,
                elapsed_delta,
            )
            return _MatchSkip("stale_inplay_total", degraded=True, retryable=False)
        return None

    @staticmethod
    def _is_transient_navigation_error(exc: Exception) -> bool:
        message = str(exc or "").upper()
        return any(marker in message for marker in (
            "NS_ERROR_CONNECTION_REFUSED",
            "NS_ERROR_NET_TIMEOUT",
            "NS_ERROR_NET_RESET",
            "ERR_CONNECTION_REFUSED",
            "ERR_CONNECTION_RESET",
            "ERR_CONNECTION_CLOSED",
            "ERR_PROXY_CONNECTION_FAILED",
            "ERR_TIMED_OUT",
            "TIMED OUT",
            "TIMEOUT",
        ))

    async def _goto_detail_with_retry(
        self,
        page,
        url: str,
        *,
        wait_until: str = "commit",
        attempts: int = 3,
    ):
        """Retry transient AiScore/Tor navigation failures without bypassing proxy."""
        bounded_attempts = max(1, min(3, int(attempts)))
        last_error: Exception | None = None
        for attempt in range(1, bounded_attempts + 1):
            try:
                return await page.goto(
                    url,
                    wait_until=wait_until,
                    timeout=self.page_timeout_ms,
                )
            except Exception as exc:
                if not self._is_transient_navigation_error(exc):
                    raise
                last_error = exc
                logger.warning(
                    "Transient AIScore navigation failure (%s/%s): %s",
                    attempt,
                    bounded_attempts,
                    type(exc).__name__,
                )
                if attempt < bounded_attempts:
                    # A browser-side timer may itself remain pending after a
                    # failed navigation leaves the page in a broken state.
                    await asyncio.sleep(0.35 * attempt)
        raise _TransientNavigationError(
            f"AIScore bağlantısı geçici olarak kurulamadı; {bounded_attempts} otomatik deneme başarısız oldu."
        ) from last_error

    async def _wait_for_listing_ready(self, page) -> None:
        try:
            await page.wait_for_function(
                r"""
                () => {
                    if (!document.body || document.readyState === 'loading') return false;
                    if (/just a moment|attention required/i.test(document.title) ||
                        document.querySelector('#challenge-running, #challenge-form')) return false;
                    const match = document.querySelector('a[href*="/basketball/match-"]');
                    const liveControl = Array.from(document.querySelectorAll('a, button, [role="tab"], li, div'))
                        .some(el => /^live(?:\s|\(|$)/i.test((el.innerText || '').trim()) && el.children.length <= 5);
                    return !!match || liveControl;
                }
                """,
                timeout=self.page_timeout_ms,
            )
        except Exception as exc:
            logger.debug("AIScore listing readiness wait ended without a signal: %s", exc)
            title = await page.title()
            if re.search(r"just a moment|attention required", title, re.IGNORECASE):
                raise RuntimeError(
                    "AIScore access verification did not complete before the page timeout; "
                    "the source returned a challenge page instead of the match listing."
                ) from exc
        await page.wait_for_timeout(350)

    async def _wait_for_odds_ready(self, page) -> None:
        try:
            await page.wait_for_function(
                r"""
                () => {
                    const el = document.querySelector('.newOdds, [class*="newOdds"], [class*="oddsContent"]');
                    if (!el) return false;
                    const text = (el.innerText || '').replace(/\s+/g, ' ');
                    return /\b\d{3}(?:\.\d)?\b/.test(text) || /lock|suspend|unavail/i.test(text);
                }
                """,
                timeout=min(5000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("AIScore odds readiness wait ended without a signal: %s", exc)
        await page.wait_for_timeout(250)

    async def _wait_for_match_page_ready(self, page) -> None:
        try:
            await page.wait_for_function(
                r"""
                () => {
                    if (!document.body || document.readyState === 'loading') return false;
                    const score = document.querySelector(
                        '.score, [class*="matchScore"], [class*="scoresDetails"], [class*="scoreDetail"]'
                    );
                    const header = document.querySelector(
                        '[class*="matchTop"], [class*="matchInfo"], [class*="matchHeader"]'
                    );
                    return !!score || !!header || (document.body.innerText || '').length > 800;
                }
                """,
                timeout=min(6000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("AIScore match-page readiness wait ended without a signal: %s", exc)
        await page.wait_for_timeout(250)

    # ── Ana tarama ────────────────────────────────────────────────────
    async def get_live_basketball_totals(
        self,
        on_match: Callable[[dict], Awaitable[None]] | None = None,
    ) -> list[dict]:
        cycle_started = time.monotonic()
        report = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "listing_attempts": 0,
            "listing_navigation_errors": [],
            "listing_parse_errors": [],
            "discovered_count": 0,
            "reported_live_count": 0,
            "unverified_count": 0,
            "attempted_count": 0,
            "unattempted_count": 0,
            "parsed_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "coverage_pct": None,
            "parse_coverage_pct": None,
            "emitted_count": 0,
            "effective_concurrency": self._effective_concurrency,
            "navigation_failure_count": 0,
            "skip_reasons": {},
            "listing": {},
            "errors": [],
        }
        self.last_report = report

        try:
            async with self._live_session() as session:
                context = session.context
                if context is None:
                    raise RuntimeError("Scrapling browser context could not be created")

                links = []
                list_page = None
                for attempt in range(1, 4):
                    report["listing_attempts"] = attempt
                    state = {}

                    async def parse_listing(page):
                        state["page"] = page
                        page.set_default_timeout(self.page_timeout_ms)
                        try:
                            await self._wait_for_listing_ready(page)
                            state["links"] = await self._collect_match_links(page)
                        except Exception as exc:
                            state["error"] = exc

                    try:
                        await session.fetch(
                            self._mobile_url(self.aiscore_url),
                            solve_cloudflare=True,
                            page_action=parse_listing,
                            timeout=max(90_000, self.page_timeout_ms),
                        )
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        report["listing_navigation_errors"].append(error)
                        logger.warning(
                            "AIScore protected listing navigation failed (%s/3): %s",
                            attempt,
                            exc,
                        )
                        continue

                    list_page = state.get("page")
                    parse_error = state.get("error")
                    if parse_error is not None:
                        error = f"{type(parse_error).__name__}: {parse_error}"
                        report["listing_parse_errors"].append(error)
                        report["listing"] = dict(self._last_listing_diagnostics)
                        logger.warning(
                            "AIScore listing parse failed (%s/3): %s",
                            attempt,
                            parse_error,
                        )
                        continue

                    links = list(state.get("links") or [])
                    report["listing"] = dict(self._last_listing_diagnostics)
                    if links or bool(
                        (report.get("listing") or {}).get("authoritative_empty")
                    ):
                        break
                    logger.warning(
                        "AIScore listing returned 0 links; retrying list load (%s/3).",
                        attempt + 1,
                    )

                if not links:
                    listing_failures = len(report["listing_navigation_errors"]) + len(
                        report["listing_parse_errors"]
                    )
                    if listing_failures >= report["listing_attempts"]:
                        raise RuntimeError(
                            "AIScore listing could not be verified after "
                            f"{report['listing_attempts']} attempts"
                        )
                    reported_live_count = int(
                        (report.get("listing") or {}).get("live_tab_reported_count") or 0
                    )
                    report["reported_live_count"] = reported_live_count
                    report["unverified_count"] = reported_live_count
                    if reported_live_count > 0:
                        raise RuntimeError(
                            "AIScore live tab reported "
                            f"{reported_live_count} matches but no verified links were collected."
                        )
                    if not bool((report.get("listing") or {}).get("authoritative_empty")):
                        raise RuntimeError(
                            "AIScore empty live result was not explicitly verified."
                        )

                    page_title = ""
                    page_url = ""
                    body_len = 0
                    if list_page is not None:
                        try:
                            page_title = await list_page.title()
                            page_url = list_page.url
                            body_len = await list_page.evaluate(
                                "document.body?.innerText?.length || 0"
                            )
                        except Exception:
                            pass
                    logger.info(
                        "No live match links found on AIScore listing. "
                        "title=%s, url=%s, body_len=%s",
                        page_title,
                        page_url,
                        body_len,
                    )
                    debug_path = os.getenv("AISCORE_DEBUG_SCREENSHOT")
                    if debug_path and list_page is not None:
                        try:
                            await list_page.screenshot(path=debug_path, full_page=False)
                            logger.info("Debug screenshot: %s", debug_path)
                        except Exception:
                            pass
                    report["status"] = "empty"
                    return []

                logger.info("Found %s match links on AIScore.", len(links))
                report["discovered_count"] = len(links)
                listing = report.get("listing") or {}
                reported_live_count = (
                    int(listing.get("live_tab_reported_count") or 0)
                    if listing.get("live_tab_count_known")
                    else 0
                )
                report["reported_live_count"] = reported_live_count
                report["unverified_count"] = max(
                    0,
                    reported_live_count - len(links),
                )
                out = []

                batch_links = links[: self.max_matches_per_cycle]
                report["attempted_count"] = len(batch_links)
                report["unattempted_count"] = max(0, len(links) - len(batch_links))

                async with aclosing(self._detail_results(context, batch_links)) as results:
                    async for result in results:
                        if isinstance(result, dict):
                            out.append(result)
                            report["emitted_count"] += 1
                            if on_match is not None:
                                await on_match(result)
                        elif isinstance(result, _MatchSkip):
                            skip_reasons = report["skip_reasons"]
                            skip_reasons[result.reason] = (
                                skip_reasons.get(result.reason, 0) + 1
                            )
                            if result.degraded:
                                report["failed_count"] += 1
                                report["errors"].append(
                                    f"{result.reason}: match data incomplete"
                                )
                            else:
                                report["skipped_count"] += 1
                        elif isinstance(result, Exception):
                            report["failed_count"] += 1
                            if isinstance(result, _TransientNavigationError):
                                report["navigation_failure_count"] += 1
                            error = f"{type(result).__name__}: {result}"
                            report["errors"].append(error)
                            logger.warning("Parallel match error: %s", result)
                        else:
                            report["failed_count"] += 1
                            report["errors"].append("unexpected extraction result")
                report["effective_concurrency"] = self._effective_concurrency

                report["parsed_count"] = len(out)
                report["skip_reason_summary"] = ", ".join(
                    f"{reason}={count}"
                    for reason, count in sorted(report["skip_reasons"].items())
                )
                expected_live_count = max(len(links), reported_live_count)
                resolved_count = len(out) + report["skipped_count"]
                report["coverage_pct"] = (
                    round((resolved_count / expected_live_count) * 100, 1)
                    if expected_live_count
                    else None
                )
                report["parse_coverage_pct"] = (
                    round((len(out) / len(batch_links)) * 100, 1)
                    if batch_links
                    else None
                )
                if links and not out and report["failed_count"] and not report["skipped_count"]:
                    raise RuntimeError(
                        f"AIScore discovered {len(links)} live matches but parsed none"
                    )
                if (
                    report["failed_count"] > 0
                    or report["unattempted_count"] > 0
                    or report["unverified_count"] > 0
                ):
                    report["status"] = "partial"
                    logger.warning(
                        "AIScore live scrape was partial: reported=%s verified=%s "
                        "unverified=%s attempted=%s parsed=%s skipped=%s failed=%s "
                        "coverage=%.1f%% parse_coverage=%.1f%%",
                        reported_live_count or "unknown",
                        len(links),
                        report["unverified_count"],
                        len(batch_links),
                        len(out),
                        report["skipped_count"],
                        report["failed_count"],
                        report["coverage_pct"],
                        report["parse_coverage_pct"] or 0.0,
                    )
                else:
                    report["status"] = "ok"
                if report["navigation_failure_count"] == 0:
                    self._record_healthy_concurrency_cycle()
                    report["effective_concurrency"] = self._effective_concurrency
                return out
        except Exception as exc:
            report["status"] = "error"
            report["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            report["duration_seconds"] = round(time.monotonic() - cycle_started, 2)

    async def _extract_single_with_timeout(
        self,
        context,
        link: str,
    ) -> dict | _MatchSkip:
        """Keep one broken browser tab from blocking the complete live cycle."""
        try:
            async with asyncio.timeout(self.match_timeout_seconds):
                return await self._extract_single(context, link)
        except TimeoutError:
            logger.warning(
                "AIScore match detail exceeded %.1fs and was cancelled: %s",
                self.match_timeout_seconds,
                link,
            )
            return _MatchSkip("match_timeout", degraded=True, retryable=False)

    async def _extract_single(self, context, link: str) -> dict | _MatchSkip:
        """Open a single match in a new tab, read data, and close.
        Retries up to 2 times if odds are temporarily locked/unavailable."""
        detail = await context.new_page()
        detail.set_default_timeout(self.page_timeout_ms)
        max_retries = 2
        try:
            logger.debug("Checking match link: %s", link)
            last_skip = _MatchSkip("unexpected_empty_result", degraded=True)
            for attempt in range(1, max_retries + 1):
                logger.debug("Attempt %s/%s for %s", attempt, max_retries, link)
                row = await self._extract_match(detail, link)
                if isinstance(row, dict):
                    return row
                if isinstance(row, _MatchSkip):
                    last_skip = row
                    if not row.retryable:
                        return row
                if attempt < max_retries:
                    logger.debug(
                        "Retry %s/%s for %s after %s",
                        attempt, max_retries - 1, link, last_skip.reason,
                    )
                    # _extract_match navigates to the canonical odds URL on every
                    # attempt, so a separate reload only duplicates network work.
                    await asyncio.sleep(0.5)
            logger.debug(
                "Extraction omitted after %s attempts for %s: reason=%s degraded=%s",
                max_retries,
                link,
                last_skip.reason,
                last_skip.degraded,
            )
            return last_skip
        except Exception as exc:
            logger.warning("Could not read match (%s): %s", link, exc)
            raise
        finally:
            try:
                await asyncio.wait_for(detail.close(), timeout=5.0)
            except Exception as exc:
                logger.warning("Could not close match page (%s): %s", link, exc)

    async def _collect_match_links(self, page) -> list[str]:
        """
        Navigate to AIScore basketball live page and collect live match links.
        Uses multiple strategies to find and activate the Live tab.
        """
        # ── Step 0: Dump page structure for diagnostics ──
        page_diag = await page.evaluate(r"""() => {
            const text = s => (s || '').replace(/\s+/g, ' ').trim();

            const topArea = document.querySelector('header, nav, [class*="header"], [class*="nav"]');
            const topHtml = topArea ? text(topArea.innerHTML).substring(0, 500) : 'NO_HEADER';

            const tabCandidates = [];
            document.querySelectorAll('*').forEach(el => {
                const t = text(el.innerText);
                const cls = el.className || '';
                if (t.length > 0 && t.length < 40 && el.children.length <= 3) {
                    if (/live|canlı|today|bugün|score|match|result|finish|schedul|ended/i.test(t)
                        || /tab|menu|nav|filter|switch/i.test(typeof cls === 'string' ? cls : '')) {
                        tabCandidates.push({
                            tag: el.tagName,
                            text: t.substring(0, 40),
                            cls: (typeof cls === 'string' ? cls : '').substring(0, 100),
                            href: el.getAttribute?.('href') || ''
                        });
                    }
                }
            });

            const allMatchLinks = document.querySelectorAll('a[href*="/basketball/match-"]').length;

            return {
                url: window.location.href,
                title: document.title,
                tabCandidates: tabCandidates.slice(0, 20),
                matchLinkCount: allMatchLinks,
                topHtml: topHtml
            };
        }""")
        logger.info(
            "AIScore page diagnostics: url=%s, title=%s, matchLinks=%s, tabs=%s",
            page_diag.get("url"), page_diag.get("title"),
            page_diag.get("matchLinkCount"), page_diag.get("tabCandidates", [])[:10]
        )

        # ── Step 1: Try to activate the Live tab ──
        live_info = await page.evaluate(r"""() => {
            const text = s => (s || '').replace(/\s+/g, ' ').trim();

            let tab = null;

            tab = document.querySelector('.activeLiveTab, [class*="liveTab"], [class*="live_tab"], [class*="live-tab"]');

            if (!tab) {
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    const t = text(el.innerText);
                    const cls = (typeof el.className === 'string') ? el.className : '';
                    if (t.length > 0 && t.length < 30 && /^live\b/i.test(t.trim())) {
                        if (el.children.length <= 5) {
                            tab = el;
                            break;
                        }
                    }
                }
            }

            let count = 0;
            let countKnown = false;
            let tabText = '';
            let found = false;
            let clicked = false;

            if (tab) {
                found = true;
                tabText = text(tab.innerText);
                const m = tabText.match(/\((\d+)\)/);
                if (m) {
                    count = parseInt(m[1], 10);
                    countKnown = true;
                }

                try {
                    tab.click();
                    clicked = true;
                } catch(e) {
                    try {
                        tab.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                        clicked = true;
                    } catch(e2) {}
                }
            }

            return { found, tabText, count, countKnown, clicked };
        }""")

        live_found = live_info.get("found", False)
        live_count = live_info.get("count", 0)
        live_count_known = bool(live_info.get("countKnown", False))
        tab_text = live_info.get("tabText", "")
        tab_clicked = live_info.get("clicked", False)
        count_verified_empty = bool(
            live_found and live_count_known and int(live_count or 0) == 0
        )

        if live_found and live_count > 0:
            live_max = live_count
            logger.info("Live tab found: '%s' (%s matches), clicked=%s.", tab_text, live_max, tab_clicked)
        elif live_found:
            live_max = 50
            if count_verified_empty:
                logger.info("Live tab explicitly reports 0 matches: '%s'.", tab_text)
            else:
                logger.info("Live tab found: '%s' (no count), clicked=%s. Using max=%s.", tab_text, tab_clicked, live_max)
        else:
            raise RuntimeError(
                "AIScore Live tab could not be verified; an empty live slate cannot be trusted"
            )

        try:
            await page.wait_for_function(
                r"""
                () => {
                    if (document.querySelectorAll('a[href*="/basketball/match-"]').length > 0) {
                        return true;
                    }
                    const visible = el => {
                        const style = window.getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') return false;
                        const rect = el.getBoundingClientRect();
                        return rect.width > 0 && rect.height > 0;
                    };
                    const mobileEmpty = Array.from(document.querySelectorAll('.notData'))
                        .some(el => visible(el) && !el.closest('.searchData'));
                    if (document.querySelector('.activeLiveTab') && mobileEmpty) return true;
                    const body = (document.body?.innerText || '').replace(/\s+/g, ' ').trim();
                    return /there\s+(?:are|is)\s+no\s+live\s+(?:games?|matches?|events?)(?:\s+at\s+(?:the|this)\s+moment)?/i.test(body)
                        || /no\s+live\s+(?:games?|matches?|events?)\s+(?:at\s+(?:the|this)\s+moment|right\s+now)/i.test(body);
                }
                """,
                timeout=min(8000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("Live-tab result readiness wait ended without links: %s", exc)
        await page.wait_for_timeout(300)

        empty_state = await page.evaluate(r"""() => {
            const text = value => (value || '').replace(/\s+/g, ' ').trim();
            const patterns = [
                /^there\s+(?:are|is)\s+no\s+live\s+(?:games?|matches?|events?)(?:\s+at\s+(?:the|this)\s+moment)?[.!]?$/i,
                /^no\s+live\s+(?:games?|matches?|events?)\s+(?:at\s+(?:the|this)\s+moment|right\s+now)[.!]?$/i,
                /^there\s+(?:are|is)\s+currently\s+no\s+live\s+(?:games?|matches?|events?)[.!]?$/i
            ];
            const visible = el => {
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden') return false;
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
            };
            const mobileEmpty = Array.from(document.querySelectorAll('.notData'))
                .find(el => visible(el) && !el.closest('.searchData'));
            if (document.querySelector('.activeLiveTab') && mobileEmpty) {
                return { found: true, text: text(mobileEmpty.textContent) || 'No data' };
            }
            const candidates = document.querySelectorAll('main, section, div, p, span');
            for (const el of candidates) {
                const value = text(el.innerText);
                if (!value || value.length > 160 || !visible(el)) continue;
                if (patterns.some(pattern => pattern.test(value))) {
                    return { found: true, text: value };
                }
            }
            return { found: false, text: '' };
        }""")
        explicit_empty_state = bool(
            isinstance(empty_state, dict) and empty_state.get("found")
        )

        # ── Step 2: Collect links — only from rows with a live time indicator ──
        all_hrefs: dict[str, None] = {}
        max_scrolls = 50
        last_count = 0
        stale_rounds = 0

        for scroll_i in range(max_scrolls):
            hrefs = await page.evaluate(r"""() => {
                const liveHrefs = [];
                const visible = el => {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return style.display !== 'none'
                        && style.visibility !== 'hidden'
                        && rect.width > 0 && rect.height > 0;
                };
                const matchLinks = document.querySelectorAll('a[href*="/basketball/match-"]');
                for (const a of matchLinks) {
                    if (!visible(a)) continue;
                    const row = a.closest(
                        '.allBox, .list-item, [class*="matchItem"], [class*="match-item"]'
                    ) || a;
                    const rowText = (row.innerText || '').replace(/\s+/g, ' ');
                    const hasLivePeriod = /\b(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\b/i.test(rowText);
                    const timeEl = row.querySelector(
                        '[class*="liveTime"], [class*="LiveTime"], [class*="live-status"], .color-r, [style*="color: red"]'
                    );
                    if (hasLivePeriod || (timeEl && visible(timeEl))) {
                        const href = a.getAttribute('href');
                        if (href) liveHrefs.push(href);
                    }
                }
                return liveHrefs;
            }""")
            for href in hrefs:
                if href:
                    all_hrefs.setdefault(href, None)

            if len(all_hrefs) >= live_max:
                break

            if len(all_hrefs) == last_count:
                stale_rounds += 1
                if stale_rounds >= 3:
                    break
            else:
                stale_rounds = 0
                last_count = len(all_hrefs)

            await page.evaluate("window.scrollBy(0, 600)")
            await page.wait_for_timeout(250)

        if not all_hrefs and not (count_verified_empty or explicit_empty_state):
            logger.warning("Live-only filter found 0 verified live match links.")

        _suffixes = re.compile(r'/(h2h|odds|stats|lineups|standings|summary)/?$')
        links: list[str] = []
        seen_urls: set[str] = set()
        for href in all_hrefs:
            normalized = _suffixes.sub('', urljoin(page.url, href))
            if "/basketball/match-" not in normalized or normalized in seen_urls:
                continue
            seen_urls.add(normalized)
            links.append(normalized)
            if len(links) >= live_max:
                break

        authoritative_empty = bool(
            live_found
            and not links
            and (count_verified_empty or explicit_empty_state)
        )
        if authoritative_empty and explicit_empty_state:
            logger.info(
                "Live tab explicitly reports an empty slate: '%s'.",
                str(empty_state.get("text") or "")[:160],
            )

        self._last_listing_diagnostics = {
            "page_url": page_diag.get("url"),
            "page_title": page_diag.get("title"),
            "dom_match_link_count": page_diag.get("matchLinkCount", 0),
            "live_tab_found": bool(live_found),
            "live_tab_text": tab_text,
            "live_tab_reported_count": int(live_count or 0),
            "live_tab_count_known": live_count_known,
            "live_tab_clicked": bool(tab_clicked),
            "authoritative_empty": authoritative_empty,
            "empty_state_found": explicit_empty_state,
            "empty_state_text": (
                str(empty_state.get("text") or "")[:160]
                if isinstance(empty_state, dict)
                else ""
            ),
            "verified_live_link_count": len(links),
            "unverified_live_link_count": max(
                0,
                int(live_count or 0) - len(links),
            ) if live_count_known else 0,
        }

        if not links and not authoritative_empty:
            raise RuntimeError(
                "AIScore Live tab did not provide a verified zero count or any live links"
            )

        logger.info("Collected %s live match links (live_max=%s).", len(links), live_max)
        return links

    async def _extract_match(self, page, url: str) -> dict | _MatchSkip:
        clean_url = self._mobile_url(url.rstrip("/"))
        odds_url = clean_url if clean_url.endswith("/odds") else clean_url + "/odds"
        await self._goto_detail_with_retry(
            page,
            odds_url,
            wait_until="commit",
        )
        await self._wait_for_odds_ready(page)

        try:
            total_tab = page.locator(
                ".oddTypesBox span",
                has_text=re.compile(r"^\s*Total Points\s*$", re.I),
            )
            if await total_tab.count():
                await total_tab.first.click()
            await page.wait_for_function(
                LIVE_TOTAL_MARKET_READY_JS,
                timeout=min(5000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("Total Points selection failed for %s: %s", url, exc)

        market_snapshot = await page.evaluate(LIVE_TOTAL_MARKET_JS)
        market_captured_at = datetime.now(timezone.utc).isoformat()
        market_captured_monotonic = time.monotonic()
        parsed = await page.evaluate(
            r"""
            () => {
                const text = value => (value || '').replace(/\s+/g, ' ').trim();
                const top = text(document.querySelector('.topBox')?.innerText);
                const scoreMatches = Array.from(top.matchAll(/\b(\d{1,3})\s*[-–]\s*(\d{1,3})\b/g));
                const scoreMatch = scoreMatches.length
                    ? scoreMatches[scoreMatches.length - 1]
                    : null;
                const score = scoreMatch ? `${scoreMatch[1]} - ${scoreMatch[2]}` : '';

                const extractQuarterScores = __QUARTER_SCORES_READER__;
                const quarterScores = extractQuarterScores(score);

                const title = text(document.title || '');
                const matchName = title
                    .replace(/\s*\|.*/, '')
                    .replace(/\s*-\s*AiScore.*/i, '')
                    .replace(/\d{4}[\/-]\d{1,2}[\/-]\d{1,2}\s*/g, '')
                    .replace(/\s*betting odds\s*/gi, '')
                    .replace(/\s+vs\.?\s+/gi, ' - ')
                    .trim();
                const tournamentAnchor = Array.from(
                    document.querySelectorAll('a[href*="/tournament-"]')
                ).find(anchor => {
                    const value = text(anchor.innerText);
                    return value && value.length <= 100;
                });
                const tournament = tournamentAnchor ? text(tournamentAnchor.innerText) : '';

                return {
                    matchName,
                    tournament,
                    topText: top,
                    score,
                    quarterScores,
                };
            }
            """.replace("__QUARTER_SCORES_READER__", QUARTER_SCORES_JS)
        )

        detail_status = _detail_status_from_top_text(parsed.get("topText"))
        parsed["status"] = detail_status["status"]
        parsed["isFinished"] = detail_status["is_finished"]
        parsed["periodEnded"] = detail_status["period_ended"]

        if parsed.get("isFinished"):
            return _MatchSkip("finished")

        if not market_snapshot.get("market_verified"):
            return _MatchSkip("total_market_unverified", degraded=True, retryable=True)
        odds_snapshot = _normalize_market_snapshot(market_snapshot)
        opening = _select_market_line(odds_snapshot, "opening")
        inplay = _select_market_line(odds_snapshot, "inplay")
        prematch = _select_market_line(odds_snapshot, "prematch")
        if opening is None or inplay is None:
            if market_snapshot.get("has_locked_rows"):
                return _MatchSkip("odds_locked", retryable=True)
            return _MatchSkip("totals_missing", retryable=True)

        parsed_quarter_scores = normalize_quarter_scores(
            parsed.get("quarterScores"),
            parsed.get("score") or "",
        )
        overview_data = {}
        needs_overview_core = not parsed.get("status") or not parsed.get("score")
        if needs_overview_core:
            overview_data = await self._fetch_overview_data(page, clean_url)
            if needs_overview_core and overview_data.get("status"):
                parsed["status"] = overview_data["status"]
            if needs_overview_core and overview_data.get("score"):
                parsed["score"] = overview_data["score"]
        if overview_data.get("isFinished"):
            return _MatchSkip("finished")

        match_id = self._extract_match_id(clean_url)
        tournament = self._sanitize_tournament(
            parsed.get("tournament") or "",
            clean_url,
        )
        status = str(parsed.get("status") or "").strip()
        score = str(parsed.get("score") or "").strip()
        match_name = str(parsed.get("matchName") or f"Match {match_id}").strip()
        overview_quarter_scores = normalize_quarter_scores(
            overview_data.get("quarterScores"),
            score,
        )
        quarter_scores = overview_quarter_scores or normalize_quarter_scores(
            parsed_quarter_scores,
            score,
        )
        clock = game_clock(status, match_name, tournament)
        home_score, away_score = parse_score(score)

        if parsed.get("periodEnded") or overview_data.get("periodEnded"):
            is_last_period = (
                clock.get("period") is not None
                and clock.get("period") == clock.get("period_count")
            )
            if is_last_period:
                if home_score is not None and home_score == away_score:
                    return _MatchSkip("overtime")
                return _MatchSkip("finished")

        if (
            clock.get("period") is None
            or clock.get("remaining_min") is None
            or home_score is None
            or away_score is None
        ):
            logger.warning(
                "Incomplete live core data: %s status=%r score=%r",
                clean_url,
                status,
                score,
            )
            return _MatchSkip("incomplete_live_core", degraded=True, retryable=True)

        match = {
            "match_id": match_id,
            "match_name": match_name,
            "tournament": tournament or "Unknown",
            "status": status,
            "opening_total": float(opening),
            "prematch_total": float(prematch) if prematch is not None else None,
            "inplay_total": float(inplay),
            "bookmaker": (
                odds_snapshot["bookmaker_lines"][0]
                if odds_snapshot["bookmaker_lines"]
                else ""
            ),
            "market_captured_at": market_captured_at,
            "_market_captured_monotonic": market_captured_monotonic,
            "url": clean_url,
            "score": score,
            "quarter_scores": quarter_scores,
            "has_prematch": prematch is not None,
        }
        stale_skip = self._stale_line_skip(match)
        if stale_skip is not None:
            match["_market_stale"] = True
            match["_market_stale_reason"] = stale_skip.reason
        return match

    async def _fetch_overview_data(self, page, url: str) -> dict:
        try:
            await self._goto_detail_with_retry(
                page,
                self._mobile_url(url.rstrip("/")),
                wait_until="commit",
            )
            await self._wait_for_match_page_ready(page)
            try:
                # Period cells may hydrate after the headline score appears.
                await page.wait_for_function(
                    r"""() => Array.from(document.querySelectorAll(
                        '.scoresDetails, [class*="scoresDetails"], [class*="scoreDetail"]'
                    )).some(root => Array.from(root.querySelectorAll('*')).filter(
                        el => !el.children.length && /^\d{1,3}$/.test((el.textContent || '').trim())
                    ).length >= 4)""",
                    timeout=min(2500, self.page_timeout_ms),
                )
            except Exception:
                # Missing quarter coverage must not replace valid headline data.
                pass
            result = await page.evaluate(r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();

                  let status = '';
                  const statusEl = Array.from(document.querySelectorAll('span, div'))
                    .map(e => text(e.innerText))
                    .find(v => /^(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(v));
                  if (statusEl) status = statusEl;

                  let score = '';
                  const scoreEls = Array.from(document.querySelectorAll('span, div, strong, b'))
                    .filter(el => el.children.length === 0)
                    .map(el => ({
                      txt: text(el.innerText),
                      rect: el.getBoundingClientRect(),
                      size: parseFloat(window.getComputedStyle(el).fontSize) || 0,
                      cls: (el.className || '').toString(),
                    }))
                    .filter(o => /^\d{1,3}$/.test(o.txt));
                  const scoreClassEls = scoreEls.filter(n =>
                    n.cls.split(/\s+/).includes('score') && n.size >= 16
                  );
                  if (scoreClassEls.length >= 2) {
                    scoreClassEls.sort((a, b) => b.size - a.size || a.rect.left - b.rect.left);
                    const a = scoreClassEls[0], b = scoreClassEls[1];
                    const left = a.rect.left <= b.rect.left ? a : b;
                    const right = a.rect.left <= b.rect.left ? b : a;
                    score = `${left.txt} - ${right.txt}`;
                  }

                  const extractQuarterScores = __QUARTER_SCORES_READER__;
                  const quarterScores = extractQuarterScores(score);

                  const arrivedPeriods = Array.from(document.querySelectorAll(
                    '.Qn button.matchArrive, .Qn .matchArrive'
                  ))
                    .map(el => text(el.innerText).match(/^Q([1-4])$/i))
                    .filter(Boolean)
                    .map(match => parseInt(match[1], 10));
                  const period = Math.max(0, ...arrivedPeriods);
                  const latestPbpRow = document.querySelector(
                    '.pbp .dataList, [class*="pbp"] [class~="dataList"]'
                  );
                  const clockEl = latestPbpRow
                    ? latestPbpRow.querySelector('.time, [class~="time"]')
                    : null;
                  const latestEvent = latestPbpRow ? text(latestPbpRow.innerText) : '';
                  const playByPlayStatus = {
                    period,
                    clock: clockEl ? text(clockEl.innerText) : '',
                    period_ended: /\bperiod\s+end(?:ed)?\b/i.test(latestEvent),
                  };

                  const nuxtMatch = window.$nuxt?.$store?.state?.basketball?.basketballDetailMatchData?.match || (window.__NUXT__
                    && window.__NUXT__.state
                    && window.__NUXT__.state.basketball
                    && window.__NUXT__.state.basketball.basketballDetailMatchData
                    && window.__NUXT__.state.basketball.basketballDetailMatchData.match);
                  const nuxtFinished = Boolean(nuxtMatch) && (
                    Number(nuxtMatch.matchStatus) === 3
                    || Number(nuxtMatch.statusId) === 10
                  );

                  return {
                    status,
                    score,
                    quarterScores,
                    playByPlayStatus,
                    isFinished: nuxtFinished,
                    sourceMatch: nuxtMatch ? {
                      id: nuxtMatch.id, statusId: nuxtMatch.statusId, matchStatus: nuxtMatch.matchStatus,
                      homeScores: nuxtMatch.homeScores, awayScores: nuxtMatch.awayScores,
                    } : null,
                  };
                }
            """.replace("__QUARTER_SCORES_READER__", QUARTER_SCORES_JS))
            result = normalize_match_page(result, urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
            if result.get("_check_error"):
                return {}
            if not result.get("status"):
                fallback = _status_from_play_by_play_hint(result.get("playByPlayStatus"))
                if fallback["status"]:
                    result["status"] = fallback["status"]
                    result["periodEnded"] = fallback["period_ended"]
                    logger.debug(
                        "Recovered live status from play-by-play for %s: %s",
                        url,
                        fallback["status"],
                    )
            result.pop("playByPlayStatus", None)
            return result
        except Exception as exc:
            logger.debug("Overview data fetch failed for %s: %s", url, exc)
            return {}

    @staticmethod
    def _extract_match_id(url: str) -> str:
        cleaned = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        parts = cleaned.split('/')
        return parts[-1] if parts else url
