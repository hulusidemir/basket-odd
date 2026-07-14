import asyncio
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from urllib.parse import urljoin, urlsplit, urlunsplit

from playwright.async_api import async_playwright
from projection import game_clock, parse_score

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MatchSkip:
    """A deliberately omitted listing row, with explicit health semantics."""

    reason: str
    degraded: bool = False
    retryable: bool = False


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
    """Calculate bookmaker consensus without removing duplicate observations."""
    raw = value if isinstance(value, dict) else {}
    opening_lines = _valid_market_lines(raw.get("opening_lines"))
    prematch_lines = _valid_market_lines(raw.get("prematch_lines"))
    inplay_lines = _valid_market_lines(raw.get("inplay_lines"))

    def _median(lines: list[float]) -> float | None:
        return round(float(median(lines)), 1) if lines else None

    try:
        bookmaker_count = max(0, int(raw.get("bookmaker_count") or 0))
    except (TypeError, ValueError):
        bookmaker_count = 0
    try:
        paired_bookmaker_count = max(
            0,
            # A legacy bookmaker_count did not prove opening/in-play values
            # were observed from the same bookmakers.
            int(raw.get("paired_bookmaker_count") or 0),
        )
    except (TypeError, ValueError):
        paired_bookmaker_count = 0
    paired_bookmaker_count = min(
        paired_bookmaker_count,
        len(opening_lines),
        len(inplay_lines),
    )
    return {
        **raw,
        "opening_lines": opening_lines,
        "prematch_lines": prematch_lines,
        "inplay_lines": inplay_lines,
        "opening_median": _median(opening_lines),
        "prematch_median": _median(prematch_lines),
        "inplay_median": _median(inplay_lines),
        "opening_min": min(opening_lines) if opening_lines else None,
        "opening_max": max(opening_lines) if opening_lines else None,
        "inplay_min": min(inplay_lines) if inplay_lines else None,
        "inplay_max": max(inplay_lines) if inplay_lines else None,
        "bookmaker_count": bookmaker_count,
        "paired_bookmaker_count": paired_bookmaker_count,
    }


def _select_market_line(parsed: dict, snapshot: dict, name: str) -> float | None:
    """Use the first readable bookmaker line; keep median only as a fallback."""
    lines = _valid_market_lines(snapshot.get(f"{name}_lines"))
    median_value = snapshot.get(f"{name}_median")
    raw_value = parsed.get(name)
    try:
        median_line = float(median_value) if median_value is not None else None
    except (TypeError, ValueError):
        median_line = None
    try:
        raw_line = float(raw_value) if raw_value is not None else None
    except (TypeError, ValueError):
        raw_line = None

    selected_line = lines[0] if lines else median_line
    # A large conflict with the explicit DOM row indicates that positional
    # parsing selected a neighbouring pre-match/odds row.
    if selected_line is not None and raw_line is not None and abs(selected_line - raw_line) > 12:
        return raw_line
    return selected_line if selected_line is not None else raw_line


class AiscoreScraper:
    def __init__(
        self,
        aiscore_url: str,
        max_matches_per_cycle: int = 40,
        page_timeout_ms: int = 30000,
        skip_h2h: bool = False,
        concurrency: int | None = None,
    ):
        self.aiscore_url = aiscore_url
        self.max_matches_per_cycle = max_matches_per_cycle
        self.page_timeout_ms = page_timeout_ms
        self.skip_h2h = skip_h2h
        self.concurrency = concurrency
        # Per-match H2H body cache. H2H data does not change during a match, so
        # re-scraping the H2H tab on every poll cycle is wasted work.
        self._h2h_cache: dict[str, str] = {}
        self.last_report: dict = {}
        self._last_listing_diagnostics: dict = {}

    @staticmethod
    def _sanitize_tournament(value: str, url: str) -> str:
        text = (value or "").strip()
        if text and not re.search(r"standings|popular|trending|featured", text, re.I):
            return text
        m = re.search(r"/tournament-([a-z0-9-]+)", str(url or ""), re.I)
        if m and m.group(1):
            return m.group(1).replace("-", " ").title()
        return ""

    async def _create_browser_context(self, playwright):
        proxy_server = os.getenv("PLAYWRIGHT_PROXY")
        launch_kwargs: dict = {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        }
        if proxy_server:
            launch_kwargs["proxy"] = {"server": proxy_server}
            logger.info("Using proxy: %s", _redact_proxy_url(proxy_server))
        browser = await playwright.chromium.launch(**launch_kwargs)
        context = await self._new_desktop_context(browser)
        return browser, context

    async def _new_desktop_context(self, browser):
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        return context

    async def _close_browser(self, browser):
        await browser.close()

    async def _wait_for_listing_ready(self, page) -> None:
        try:
            await page.wait_for_function(
                r"""
                () => {
                    if (!document.body || document.readyState === 'loading') return false;
                    const body = (document.body.innerText || '').trim();
                    const match = document.querySelector('a[href*="/basketball/match-"]');
                    const liveControl = Array.from(document.querySelectorAll('a, button, [role="tab"], li, div'))
                        .some(el => /^live(?:\s|\(|$)/i.test((el.innerText || '').trim()) && el.children.length <= 5);
                    return !!match || liveControl || body.length > 500;
                }
                """,
                timeout=min(8000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("AIScore listing readiness wait ended without a signal: %s", exc)
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

    async def get_live_basketball_totals(self) -> list[dict]:
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
            "listing": {},
            "errors": [],
        }
        self.last_report = report
        async with async_playwright() as p:
            try:
                browser, context = await self._create_browser_context(p)
            except Exception as exc:
                report["status"] = "error"
                report["error"] = f"{type(exc).__name__}: {exc}"
                report["duration_seconds"] = round(time.monotonic() - cycle_started, 2)
                raise

            try:
                list_page = await context.new_page()
            except Exception as exc:
                report["status"] = "error"
                report["error"] = f"{type(exc).__name__}: {exc}"
                report["duration_seconds"] = round(time.monotonic() - cycle_started, 2)
                try:
                    await context.close()
                finally:
                    await self._close_browser(browser)
                raise
            list_page.set_default_timeout(self.page_timeout_ms)
            try:
                links = []
                for attempt in range(1, 4):
                    report["listing_attempts"] = attempt
                    try:
                        await list_page.goto(
                            self.aiscore_url,
                            wait_until="domcontentloaded",
                            timeout=self.page_timeout_ms,
                        )
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        report["listing_navigation_errors"].append(error)
                        logger.warning(
                            "AIScore listing navigation failed (%s/3): %s",
                            attempt,
                            exc,
                        )
                        if attempt < 3:
                            await list_page.wait_for_timeout(500 * attempt)
                        continue
                    try:
                        await self._wait_for_listing_ready(list_page)
                        links = await self._collect_match_links(list_page)
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        report["listing_parse_errors"].append(error)
                        logger.warning(
                            "AIScore listing parse failed (%s/3): %s",
                            attempt,
                            exc,
                        )
                        if attempt < 3:
                            await list_page.wait_for_timeout(500 * attempt)
                        continue
                    report["listing"] = dict(self._last_listing_diagnostics)
                    if links:
                        break
                    if attempt < 3:
                        logger.warning("AIScore listing returned 0 links; retrying list load (%s/3).", attempt + 1)

                if not links:
                    listing_failures = len(report["listing_navigation_errors"]) + len(
                        report["listing_parse_errors"]
                    )
                    if listing_failures >= report["listing_attempts"]:
                        report["status"] = "error"
                        raise RuntimeError(
                            "AIScore listing could not be verified after "
                            f"{report['listing_attempts']} attempts"
                        )
                    reported_live_count = int(
                        (report.get("listing") or {}).get("live_tab_reported_count") or 0
                    )
                    if reported_live_count > 0:
                        logger.warning(
                            "AIScore live tab reported "
                            f"{reported_live_count} matches but no verified links were collected. Assuming empty."
                        )
                    elif not bool((report.get("listing") or {}).get("authoritative_empty")):
                        logger.warning(
                            "AIScore empty live result was not explicitly verified. Assuming empty."
                        )
                    try:
                        page_title = await list_page.title()
                    except Exception:
                        page_title = ""
                    page_url = list_page.url
                    try:
                        body_len = await list_page.evaluate("document.body?.innerText?.length || 0")
                    except Exception:
                        body_len = 0
                    logger.warning(
                        "No match links found on AIScore listing. "
                        "title=%s, url=%s, body_len=%s",
                        page_title, page_url, body_len,
                    )
                    debug_path = os.getenv("AISCORE_DEBUG_SCREENSHOT")
                    if debug_path:
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

                await list_page.close()
                await context.close()
                context = await self._new_desktop_context(browser)

                if self.concurrency is None:
                    concurrent_tabs = _safe_env_int(
                        "AISCORE_CONCURRENCY",
                        2,
                        minimum=1,
                        maximum=8,
                    )
                else:
                    try:
                        concurrent_tabs = max(1, min(8, int(self.concurrency)))
                    except (TypeError, ValueError):
                        logger.warning(
                            "Invalid scraper concurrency=%r; using 2.",
                            self.concurrency,
                        )
                        concurrent_tabs = 2
                batch_links = links[: self.max_matches_per_cycle]
                report["attempted_count"] = len(batch_links)
                report["unattempted_count"] = max(0, len(links) - len(batch_links))

                for i in range(0, len(batch_links), concurrent_tabs):
                    batch = batch_links[i : i + concurrent_tabs]
                    tasks = [self._extract_single(context, link) for link in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, dict):
                            out.append(r)
                        elif isinstance(r, _MatchSkip):
                            if r.degraded:
                                report["failed_count"] += 1
                                report["errors"].append(f"{r.reason}: match data incomplete")
                            else:
                                report["skipped_count"] += 1
                        elif isinstance(r, Exception):
                            report["failed_count"] += 1
                            error = f"{type(r).__name__}: {r}"
                            report["errors"].append(error)
                            logger.warning("Parallel match error: %s", r)
                        else:
                            report["failed_count"] += 1
                            report["errors"].append("unexpected extraction result")

                report["parsed_count"] = len(out)
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
                    report["status"] = "error"
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
                        "unverified=%s attempted=%s parsed=%s skipped=%s failed=%s coverage=%.1f%% "
                        "parse_coverage=%.1f%%",
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
                return out
            except Exception as exc:
                report["status"] = "error"
                report["error"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                report["duration_seconds"] = round(time.monotonic() - cycle_started, 2)
                try:
                    if not list_page.is_closed():
                        await list_page.close()
                except Exception as exc:
                    logger.warning("AIScore listing page cleanup failed: %s", exc)
                try:
                    await context.close()
                except Exception as exc:
                    logger.warning("AIScore browser context cleanup failed: %s", exc)
                try:
                    await self._close_browser(browser)
                except Exception as exc:
                    logger.warning("AIScore browser cleanup failed: %s", exc)

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
                    await detail.wait_for_timeout(500)
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
            await detail.close()

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
        authoritative_empty = bool(
            live_found and live_count_known and int(live_count or 0) == 0
        )

        if live_found and live_count > 0:
            live_max = live_count
            logger.info("Live tab found: '%s' (%s matches), clicked=%s.", tab_text, live_max, tab_clicked)
        elif live_found:
            live_max = 50
            if authoritative_empty:
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
                () => document.querySelectorAll('a[href*="/basketball/match-"]').length > 0
                """,
                timeout=min(2500, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.debug("Live-tab result readiness wait ended without links: %s", exc)
        await page.wait_for_timeout(300)

        # ── Step 2: Collect links — only from rows with a live time indicator ──
        all_hrefs: dict[str, None] = {}
        max_scrolls = 50
        last_count = 0
        stale_rounds = 0

        for scroll_i in range(max_scrolls):
            hrefs = await page.evaluate(r"""() => {
                const liveHrefs = [];
                const matchLinks = document.querySelectorAll('a[href*="/basketball/match-"]');
                for (const a of matchLinks) {
                    let row = a;
                    for (let i = 0; i < 6; i++) {
                        const parent = row.parentElement;
                        if (!parent) break;
                        const uniqueLinks = new Set(
                            Array.from(parent.querySelectorAll('a[href*="/basketball/match-"]'))
                                .map(link => link.getAttribute('href'))
                                .filter(Boolean)
                        );
                        if (uniqueLinks.size > 1) break;
                        row = parent;
                    }
                    const rowText = (row.innerText || '').replace(/\s+/g, ' ');
                    const hasLiveTime = /\b(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\b/i.test(rowText)
                                     || /\b\d{1,2}[-:]\d{2}[:-]\d{2}\b/.test(rowText);
                    const timeEl = row.querySelector(
                        '[class*="liveTime"], [class*="LiveTime"], [class*="live-status"], [style*="color: red"]'
                    );
                    if (hasLiveTime || timeEl) {
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

        if not all_hrefs:
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
        clean_url = url.rstrip("/")
        odds_url = clean_url if clean_url.endswith("/odds") else clean_url + "/odds"
        await page.goto(
            odds_url,
            wait_until="domcontentloaded",
            timeout=self.page_timeout_ms,
        )
        await self._wait_for_odds_ready(page)

        try:
            clicked_total = await page.evaluate(r"""
                () => {
                    const text = s => (s || '').replace(/\s+/g, ' ').trim();
                    const tabs = Array.from(document.querySelectorAll('*')).filter(el => {
                        const t = text(el.innerText).toLowerCase();
                        const cls = (el.className || '').toString().toLowerCase();
                        const role = (el.getAttribute && (el.getAttribute('role') || '').toLowerCase()) || '';
                        return t.length < 30 && el.children.length <= 3
                            && (role === 'tab' || /tab|market|filter|switch|select/.test(cls))
                            && (/\btotal\b|\bo\/u\b|\bover.*under\b|\büst.*alt\b|\bou\b/i.test(t));
                    });
                    for (const tab of tabs) {
                        try { tab.click(); return true; } catch(e) {}
                    }
                    return false;
                }
            """)
            if clicked_total:
                logger.debug("Clicked Total/O-U tab for %s", url)
                await self._wait_for_odds_ready(page)
        except Exception as exc:
            logger.debug("Total/O-U tab selection failed for %s: %s", url, exc)

        parsed = await page.evaluate(
            r"""
            () => {
              const text = s => (s || '').replace(/\s+/g, ' ').trim();

              let opening = null;
              let prematch = null;
              let inplay = null;
              let oddsSnapshot = { opening_lines: [], prematch_lines: [], inplay_lines: [], bookmaker_count: 0 };

              // Helper: find the first basketball total line value (100-400 range) in text
              const findLine = (txt) => {
                const nums = txt.match(/\d+\.?\d*/g);
                if (!nums) return null;
                for (const n of nums) {
                  const v = parseFloat(n);
                  if (v >= 100 && v <= 400) return v;
                }
                return null;
              };

              // Helper: detect if an element is locked/suspended (bookmaker updating odds)
              const isLocked = (el) => {
                const html = (el.innerHTML || '').toLowerCase();
                const txt = text(el.innerText).toLowerCase();
                if (html.includes('lock') || html.includes('🔒')) return true;
                if (txt === '-' || txt === '--' || txt === '—') return true;
                if (/suspend|locked|unavail/i.test(txt)) return true;
                if (el.querySelector('svg[class*="lock"], [class*="Lock"], [class*="suspend"]')) return true;
                return false;
              };

              // --- Find odds container ---
              const container = document.querySelector('.newOdds')
                             || document.querySelector('[class*="newOdds"]')
                             || document.querySelector('[class*="oddsContent"]');

              if (container) {
                const median = arr => {
                  const xs = arr.filter(v => Number.isFinite(v)).sort((a,b) => a-b);
                  if (!xs.length) return null;
                  const mid = Math.floor(xs.length / 2);
                  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
                };
                const roundedLines = arr => {
                  const out = [];
                  for (const v of arr) {
                    if (!Number.isFinite(v) || v < 100 || v > 400) continue;
                    // One observation per bookmaker must remain one vote. Removing
                    // duplicate line values biases the median toward outliers.
                    out.push(parseFloat(v.toFixed(1)));
                  }
                  return out;
                };

                // ── Strategy 1: class-based (openingBg / inPlayBg) ──
                // Each bookmaker has 3 rows: opening, pre-match, in-play
                const openingEls = container.querySelectorAll('[class*="openingBg"]');
                const inPlayEls = container.querySelectorAll('[class*="inPlayBg"]');

                if (openingEls.length > 0) {
                  for (const el of openingEls) {
                    if (isLocked(el)) continue;
                    const v = findLine(text(el.innerText));
                    if (v !== null) { opening = v; break; }
                  }
                }
                if (inPlayEls.length > 0) {
                  for (const el of inPlayEls) {
                    if (isLocked(el)) continue;
                    const v = findLine(text(el.innerText));
                    if (v !== null) { inplay = v; break; }
                  }
                }

                // Pre-match: the row that is NOT openingBg and NOT inPlayBg, between them
                // In AiScore each bookmaker has 3 rows; the middle one is pre-match
                const contentDivs_s1 = container.querySelectorAll('.content');
                for (const content of contentDivs_s1) {
                  if (isLocked(content)) continue;
                  const allRows = Array.from(content.children).filter(el => !isLocked(el));
                  // Look for row that has neither openingBg nor inPlayBg class
                  const preBgRows = allRows.filter(el => {
                    const cls = (el.className || '');
                    return !cls.includes('openingBg') && !cls.includes('inPlayBg');
                  });
                  for (const el of preBgRows) {
                    const v = findLine(text(el.innerText));
                    if (v !== null) { prematch = v; break; }
                  }
                  if (prematch !== null) break;
                }

                // ── Strategy 2: Column-based (flex-col) layout ──
                if (opening === null || inplay === null) {
                  const contentDivs = container.querySelectorAll('.content');
                  for (const content of contentDivs) {
                    const cols = content.querySelectorAll('.flex.flex-1.align-center.flex-col');
                    for (let i = 2; i < cols.length; i += 3) {
                      const col = cols[i];
                      const opEls = col.querySelectorAll('[class*="openingBg"]');
                      const ipEls = col.querySelectorAll('[class*="inPlayBg"]');
                      if (opening === null) {
                        for (const el of opEls) {
                          const v = findLine(text(el.innerText));
                          if (v !== null) { opening = v; break; }
                        }
                      }
                      if (inplay === null) {
                        for (const el of ipEls) {
                          const v = findLine(text(el.innerText));
                          if (v !== null) { inplay = v; break; }
                        }
                      }
                      if (opening !== null && inplay !== null) break;
                    }
                    if (opening !== null && inplay !== null) break;
                  }
                }

                // ── Strategy 3: Positional — 1st, 2nd and 3rd rows of each bookmaker ──
                if (opening === null || inplay === null) {
                  const contentDivs = container.querySelectorAll('.content');
                  for (const content of contentDivs) {
                    if (isLocked(content)) continue;
                    const rows = Array.from(content.children).filter(el => {
                      return !isLocked(el) && findLine(text(el.innerText)) !== null;
                    });
                    if (rows.length >= 3) {
                      if (opening === null) opening = findLine(text(rows[0].innerText));
                      if (prematch === null) prematch = findLine(text(rows[1].innerText));
                      if (inplay === null) inplay = findLine(text(rows[2].innerText));
                    }
                    if (opening !== null && inplay !== null) break;
                  }
                }

                // ── Strategy 4: Broad search — all leaf elements in the container ──
                if (opening === null || inplay === null) {
                  const leafLines = [];
                  container.querySelectorAll('*').forEach(el => {
                    if (el.children.length === 0 && !isLocked(el)) {
                      const v = findLine(text(el.innerText));
                      if (v !== null) leafLines.push(v);
                    }
                  });
                  if (leafLines.length >= 2) {
                    if (opening === null) opening = leafLines[0];
                    if (inplay === null) inplay = leafLines[leafLines.length - 1];
                  }
                }

                // Consensus snapshot: use all bookmaker rows we can identify,
                // not only the first row. This gives the analysis engine market
                // spread and median line instead of a single bookmaker's number.
                const contentDivs_snapshot = Array.from(container.querySelectorAll('.content'));
                const openingLines = [];
                const prematchLines = [];
                const inplayLines = [];
                for (const content of contentDivs_snapshot) {
                  // AiScore may render extra numeric rows inside a bookmaker
                  // block. Explicit opening/in-play classes are authoritative;
                  // positional 1st/2nd/3rd fallback must not override them.
                  const explicitOpening = Array.from(
                    content.querySelectorAll('[class*="openingBg"]')
                  ).find(el => !isLocked(el) && findLine(text(el.innerText)) !== null);
                  const explicitInplay = Array.from(
                    content.querySelectorAll('[class*="inPlayBg"]')
                  ).find(el => !isLocked(el) && findLine(text(el.innerText)) !== null);
                  if (explicitOpening && explicitInplay) {
                    openingLines.push(findLine(text(explicitOpening.innerText)));
                    inplayLines.push(findLine(text(explicitInplay.innerText)));

                    const prematchRow = Array.from(content.children).find(el => {
                      const cls = (el.className || '').toString();
                      return !isLocked(el)
                        && !cls.includes('openingBg')
                        && !cls.includes('inPlayBg')
                        && findLine(text(el.innerText)) !== null;
                    });
                    if (prematchRow) {
                      prematchLines.push(findLine(text(prematchRow.innerText)));
                    }
                    continue;
                  }

                  const rows = Array.from(content.children).filter(el => !isLocked(el));
                  const lineRows = rows
                    .map(el => ({ el, value: findLine(text(el.innerText)), cls: (el.className || '').toString() }))
                    .filter(item => item.value !== null);
                  if (lineRows.length >= 3) {
                    openingLines.push(lineRows[0].value);
                    prematchLines.push(lineRows[1].value);
                    inplayLines.push(lineRows[2].value);
                  } else {
                    let pairedOpening = null;
                    let pairedPrematch = null;
                    let pairedInplay = null;
                    for (const item of lineRows) {
                      if (item.cls.includes('openingBg')) {
                        pairedOpening = item.value;
                      }
                      else if (item.cls.includes('inPlayBg')) {
                        pairedInplay = item.value;
                      }
                      else pairedPrematch = item.value;
                    }
                    // Opening and in-play must come from the same bookmaker
                    // block. An unpaired row must not bias either median.
                    if (pairedOpening !== null && pairedInplay !== null) {
                      openingLines.push(pairedOpening);
                      inplayLines.push(pairedInplay);
                      if (pairedPrematch !== null) prematchLines.push(pairedPrematch);
                    }
                  }
                }
                const openingConsensus = roundedLines(openingLines);
                const prematchConsensus = roundedLines(prematchLines);
                const inplayConsensus = roundedLines(inplayLines);
                oddsSnapshot = {
                  opening_lines: openingConsensus,
                  prematch_lines: prematchConsensus,
                  inplay_lines: inplayConsensus,
                  opening_median: median(openingConsensus),
                  prematch_median: median(prematchConsensus),
                  inplay_median: median(inplayConsensus),
                  opening_min: openingConsensus.length ? Math.min(...openingConsensus) : null,
                  opening_max: openingConsensus.length ? Math.max(...openingConsensus) : null,
                  inplay_min: inplayConsensus.length ? Math.min(...inplayConsensus) : null,
                  inplay_max: inplayConsensus.length ? Math.max(...inplayConsensus) : null,
                  bookmaker_count: Math.min(openingConsensus.length, inplayConsensus.length),
                  paired_bookmaker_count: Math.min(openingConsensus.length, inplayConsensus.length),
                };
              }

              // --- Match info ---
              const title = text(document.title || '');
              let matchName = title
                .replace(/\s*\|.*/,'')
                .replace(/\s*-\s*AiScore.*/i,'')
                .replace(/\s*live score.*/i,'')
                .replace(/\s*prediction.*/i,'')
                .replace(/\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*/g, '')
                .replace(/\s*betting odds\s*/gi, '')
                .replace(/\s+vs\.?\s+/gi, ' - ')
                .trim();

              let tournament = '';
              let country = '';
              const promoRe = /schedule|standings|teams|stats|live\s*score|popular|trending|featured|odds|prediction|news|home/i;
              // Strategy 0: a.not-allow — AiScore renders the league name above
              // the team names as an anchor with class="not-allow" and href="javascript:;".
              // This is the most direct source and takes priority over breadcrumb heuristics.
              const notAllowEl = Array.from(document.querySelectorAll('a.not-allow'))
                .find(e => { const t = text(e.innerText); return t && t.length >= 3 && t.length <= 80 && !promoRe.test(t); });
              if (notAllowEl) tournament = text(notAllowEl.innerText);
              // Strategy 1: top breadcrumb (most reliable — "Basketball Live
              // Score > <Country/League> > <Match>"). Scope it tightly to
              // breadcrumb-style containers so we don't pick up the footer's
              // "Popular Leagues" widget.
              const breadcrumbRoots = Array.from(document.querySelectorAll(
                '.breadcrumb, [class*="breadcrumb"], [class*="Breadcrumb"], nav, header, ' +
                '[class*="matchTop"], [class*="matchInfo"], [class*="matchHeader"]'
              ));
              const seen = new Set();
              const scopedAnchors = [];
              for (const root of breadcrumbRoots) {
                for (const a of root.querySelectorAll('a')) {
                  if (seen.has(a)) continue;
                  seen.add(a);
                  scopedAnchors.push(a);
                }
              }
              const candidates = scopedAnchors
                .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                .filter(e => e.href.includes('/tournament-'))
                .filter(e => e.text && !promoRe.test(e.text));
              if (candidates.length >= 2) {
                country = candidates[0].text;
                tournament = candidates[candidates.length - 1].text;
              } else if (candidates.length === 1) {
                tournament = candidates[0].text;
              }
              // Strategy 2: first valid /tournament- anchor anywhere on the
              // page (document order). Top-of-page nav comes before footers.
              if (!tournament) {
                const docCandidate = Array.from(document.querySelectorAll('a'))
                  .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                  .filter(e => e.href.includes('/tournament-'))
                  .filter(e => e.text && !promoRe.test(e.text))
                  .find(e => e.text.length >= 3 && e.text.length <= 80);
                if (docCandidate) tournament = docCandidate.text;
              }
              // Strategy 3: slug from any /tournament- href in the document.
              if (!tournament) {
                const slugHref = Array.from(document.querySelectorAll('a'))
                  .map(e => e.getAttribute('href') || '')
                  .find(h => /\/tournament-[a-z0-9-]+/i.test(h));
                if (slugHref) {
                  const m = slugHref.match(/\/tournament-([a-z0-9-]+)/i);
                  if (m && m[1]) {
                    tournament = m[1].replace(/-/g, ' ')
                      .replace(/\b\w/g, c => c.toUpperCase());
                  }
                }
              }
              const cleanRe = /\s*(live\s*score|betting\s*odds|prediction)\s*/gi;
              tournament = tournament.replace(cleanRe, '').trim();
              country = country.replace(cleanRe, '').trim();
              // Reject final string if either half still smells like promo.
              if (country && promoRe.test(country)) country = '';
              if (tournament && promoRe.test(tournament)) tournament = '';
              if (country && tournament && !tournament.toLowerCase().startsWith(country.toLowerCase())) {
                tournament = country + ' : ' + tournament;
              } else if (country && !tournament) {
                tournament = country;
              }

              let status = '';
              const statusCandidates = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => txt.length > 0 && txt.length < 30);

              // Strateji 1: "Q1 03:00" — tek element içinde birleşik
              const periodTime = statusCandidates
                .map(({txt}) => txt)
                .find(v => /^(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(v.trim()));
              if (periodTime) {
                status = periodTime.trim();
              }

              // Strateji 1b: "Q2-Ended" — çeyrek arası, maç bitmiş değil
              if (!status) {
                const periodEnded = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^(Q[1-4]|[1-4]Q)\s*[-\s]?\s*Ended$/i.test(v.trim()));
                if (periodEnded) status = periodEnded.trim();
              }

              // Strateji 2: Period ve zaman ayrı elementlerde — birleştir
              if (!status) {
                const periodEl = statusCandidates.find(({txt}) =>
                  /^(Q[1-4]|[1-4]Q|HT|1st|2nd|3rd|4th)$/i.test(txt.trim())
                );
                const timeEl = statusCandidates.find(({txt}) =>
                  /^\d{1,2}:\d{2}$/.test(txt.trim())
                );
                if (periodEl && timeEl) {
                  status = periodEl.txt.trim() + ' ' + timeEl.txt.trim();
                }
              }

              // Strateji 3: Zaman var ama period yok — çevreleyen DOM'da ara
              if (!status) {
                const timeItem = statusCandidates.find(({txt}) =>
                  /^\d{1,2}:\d{2}$/.test(txt.trim())
                );
                if (timeItem) {
                  let container2 = timeItem.el;
                  let foundPeriod = '';
                  for (let i = 0; i < 6; i++) {
                    if (!container2 || !container2.parentElement) break;
                    container2 = container2.parentElement;
                    const ctxt = text(container2.innerText || '');
                    const pm = ctxt.match(/\b(Q[1-4]|[1-4]Q|HT|OT|1st|2nd|3rd|4th)\b/i);
                    if (pm) { foundPeriod = pm[0]; break; }
                  }
                  status = foundPeriod
                    ? foundPeriod + ' ' + timeItem.txt.trim()
                    : timeItem.txt.trim();
                }
              }

              // Strateji 4: Sadece period — zaman bilinmiyor
              if (!status) {
                const periodOnly = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^(Q[1-4]|[1-4]Q)(?:\s*[-\s]?\s*Ended)?$|^(OT|HT|FT|1st|2nd|3rd|4th)$/i.test(v.trim()));
                if (periodOnly) status = periodOnly.trim();
              }

              // Strateji 5: Sayfa başlığında period ipucu
              if (!status) {
                const titlePm = text(document.title || '').match(/\b(Q[1-4]|[1-4]Q)(?:\s*[-\s]?\s*Ended)?\b|\b(HT|OT)\b/i);
                if (titlePm) status = titlePm[0];
              }

              let isQ4 = /Q4|4Q|4th/i.test(status);
              let remainingMinutes = null;
              if (status) {
                const tm = status.match(/(\d+):(\d+)/);
                if (tm) remainingMinutes = parseInt(tm[1]) + parseInt(tm[2]) / 60;
              }

              const isQuarterEnded = /\b(Q[1-4]|[1-4]Q)\s*[-\s]?\s*Ended\b/i.test(status);
              let isFinished = /\b(FT|Finished|Full Time)\b/i.test(status)
                || (/\bEnded\b/i.test(status) && !isQuarterEnded);
              if (!isFinished) {
                // Only narrow final-score hooks; [class*="score"] / [class*="ended"] alone
                // also match the live-score container and helper classes on AiScore.
                const finishedBadge = document.querySelector(
                  '[class*="final-score"], [class*="finalScore"]'
                );
                if (finishedBadge) isFinished = true;
              }

              let hasLockedRows = false;
              if (container) {
                const allRows = container.querySelectorAll('[class*="openingBg"], [class*="inPlayBg"], .content > *');
                let lockedMarketRows = 0;
                let usableMarketRows = 0;
                for (const el of allRows) {
                  if (isLocked(el)) lockedMarketRows += 1;
                  else if (findLine(text(el.innerText)) !== null) usableMarketRows += 1;
                }
                hasLockedRows = lockedMarketRows > 0 && usableMarketRows === 0;
              }

              // --- Extract live score ---
              // Layered strategy: AiScore's big score is rendered as two separate
              // <div class="score ..."> elements (no combined "93-62" string).
              // See finished_match_service.py for the same logic on finished pages.
              let score = '';
              const leafAll = Array.from(document.querySelectorAll('span, div, strong, b'))
                .filter(el => el.children.length === 0);
              const scoreNums = leafAll
                .map(el => ({
                  el,
                  txt: text(el.innerText),
                  rect: el.getBoundingClientRect(),
                  size: parseFloat(window.getComputedStyle(el).fontSize) || 0,
                  cls: (el.className || '').toString(),
                }))
                .filter(o => /^\d{1,3}$/.test(o.txt));

              // Strategy A: class~="score" token match
              const scoreClassEls = scoreNums.filter(n =>
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
                score = leftEl.txt + ' - ' + rightEl.txt;
              }

              // Strategy B: directly combined "93-62" string element (rare variant)
              if (!score) {
                const combined = Array.from(document.querySelectorAll('span, div'))
                  .map(el => text(el.innerText))
                  .find(t => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(t));
                if (combined) {
                  const parts = combined.split(/\s*[-–]\s*/);
                  if (parts.length === 2 && parseInt(parts[0]) <= 300 && parseInt(parts[1]) <= 300) {
                    score = combined.trim();
                  }
                }
              }

              // Strategy C (last resort): two biggest numbers near the top,
              // but both must share font size and be visibly large.
              if (!score) {
                const top = scoreNums
                  .filter(n => n.rect.top < 260)
                  .sort((a, b) => b.size - a.size);
                if (top.length >= 2 && top[0].size >= 20 && Math.abs(top[0].size - top[1].size) < 2) {
                  const a = top[0], b = top[1];
                  const leftEl  = a.rect.left <= b.rect.left ? a : b;
                  const rightEl = a.rect.left <= b.rect.left ? b : a;
                  score = leftEl.txt + ' - ' + rightEl.txt;
                }
              }

              // Basketball-plausibility guard. A live match can legitimately be
              // below 60 total early on, so only reject absurd totals (> 400) here;
              // the finished-match path applies a stricter floor before settling.
              const mScore = score.match(/^\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$/);
              if (mScore) {
                const total = parseInt(mScore[1]) + parseInt(mScore[2]);
                if (total > 400) score = '';
              } else {
                score = '';
              }

              if (opening !== null && !oddsSnapshot.opening_lines.length) {
                oddsSnapshot.opening_lines = [opening];
                oddsSnapshot.opening_median = opening;
                oddsSnapshot.opening_min = opening;
                oddsSnapshot.opening_max = opening;
              }
              if (prematch !== null && !oddsSnapshot.prematch_lines.length) {
                oddsSnapshot.prematch_lines = [prematch];
                oddsSnapshot.prematch_median = prematch;
              }
              if (inplay !== null && !oddsSnapshot.inplay_lines.length) {
                oddsSnapshot.inplay_lines = [inplay];
                oddsSnapshot.inplay_median = inplay;
                oddsSnapshot.inplay_min = inplay;
                oddsSnapshot.inplay_max = inplay;
              }
              if (!oddsSnapshot.bookmaker_count && (
                oddsSnapshot.opening_lines.length || oddsSnapshot.inplay_lines.length
              )) oddsSnapshot.bookmaker_count = 1;
              if (!oddsSnapshot.paired_bookmaker_count
                  && oddsSnapshot.opening_lines.length
                  && oddsSnapshot.inplay_lines.length) {
                oddsSnapshot.paired_bookmaker_count = 1;
              }

              let quarterScores = { home: [], away: [], source: '', quality: 0 };
              if (score) {
                const parts = score.split(/\s*[-–]\s*/).map(v => parseInt(v, 10));
                const scoreHome = parts[0], scoreAway = parts[1];
                const parseQuarterRow = (txt, total) => {
                  const nums = (txt.match(/\b\d{1,3}\b/g) || []).map(n => parseInt(n, 10));
                  if (nums.length < 2) return null;
                  const totalIdx = nums.lastIndexOf(total);
                  if (totalIdx < 0) return null;
                  const periods = nums.slice(Math.max(0, totalIdx - 4), totalIdx);
                  const usable = periods.filter(v => v >= 0 && v <= 80);
                  if (!usable.length) return null;
                  const sum = usable.reduce((a,b) => a + b, 0);
                  if (Math.abs(sum - total) > 4) return null;
                  return usable;
                };

                const detailBox = document.querySelector('.scoresDetails, [class*="scoresDetails"], [class*="scoreDetail"]');
                if (detailBox) {
                  const contentRows = Array.from(detailBox.querySelectorAll('.content, [class*="content"]'))
                    .map(el => text(el.innerText))
                    .filter(Boolean);
                  let homePeriods = null;
                  let awayPeriods = null;
                  let homeRowIndex = -1;
                  for (let rowIndex = 0; rowIndex < contentRows.length; rowIndex++) {
                    const row = contentRows[rowIndex];
                    const homeRow = parseQuarterRow(row, scoreHome);
                    const awayRow = parseQuarterRow(row, scoreAway);
                    if (homeRow && !homePeriods) {
                      homePeriods = homeRow;
                      homeRowIndex = rowIndex;
                    }
                    if (awayRow && !awayPeriods && rowIndex !== homeRowIndex) awayPeriods = awayRow;
                  }
                  if (homePeriods && awayPeriods) {
                    quarterScores = {
                      home: homePeriods,
                      away: awayPeriods,
                      source: 'scoreboard_dom',
                      quality: 90,
                    };
                  }
                }

                const lines = (document.body.innerText || '')
                  .split(/\n+/)
                  .map(text)
                  .filter(Boolean)
                  .slice(0, 220);
                const rowCandidates = [];
                for (let idx = 0; idx < lines.length; idx++) {
                  const nums = (lines[idx].match(/\b\d{1,3}\b/g) || []).map(n => parseInt(n, 10));
                  if (nums.length < 2 || nums.length > 8) continue;
                  const last = nums[nums.length - 1];
                  if (last !== scoreHome && last !== scoreAway) continue;
                  const periods = nums.slice(0, -1).slice(-4);
                  const sum = periods.reduce((a,b) => a + b, 0);
                  if (periods.length >= 1 && Math.abs(sum - last) <= 3) {
                    rowCandidates.push({ idx, total: last, periods });
                  }
                }
                for (let i = 0; i < rowCandidates.length; i++) {
                  for (let j = i + 1; j < rowCandidates.length; j++) {
                    const a = rowCandidates[i], b = rowCandidates[j];
                    if (Math.abs(a.idx - b.idx) > 8) continue;
                    if (
                      (a.total === scoreHome && b.total === scoreAway)
                      || (a.total === scoreAway && b.total === scoreHome)
                    ) {
                      const homeRow = a.total === scoreHome ? a : b;
                      const awayRow = a.total === scoreHome ? b : a;
                      quarterScores = {
                        home: homeRow.periods,
                        away: awayRow.periods,
                        source: 'scoreboard_rows',
                        quality: 75,
                      };
                      i = rowCandidates.length;
                      break;
                    }
                  }
                }
              }

              return {
                opening, prematch, inplay, matchName, tournament, status,
                isQ4, remainingMinutes, hasLockedRows, isFinished, score,
                quarterScores, oddsSnapshot
              };
            }
            """
        )

        is_finished = parsed.get("isFinished", False)
        status = parsed.get("status", "")
        if is_finished:
            logger.debug("Skipping finished match: %s (status=%s)", url, status)
            return _MatchSkip("finished")

        odds_snapshot = _normalize_market_snapshot(parsed.get("oddsSnapshot"))
        parsed["oddsSnapshot"] = odds_snapshot
        opening = _select_market_line(parsed, odds_snapshot, "opening")
        inplay = _select_market_line(parsed, odds_snapshot, "inplay")
        prematch = (
            odds_snapshot.get("prematch_median")
            if odds_snapshot.get("prematch_median") is not None
            else parsed.get("prematch")
        )
        locked = parsed.get("hasLockedRows", False)
        if opening is None or inplay is None:
            if locked:
                logger.debug("Odds locked/suspended for %s (bookmaker updating)", url)
                return _MatchSkip("odds_locked", retryable=True)
            else:
                logger.debug("Could not find opening/inplay totals for %s", url)
                # A live fixture can legitimately have no totals market on
                # AIScore. Retry once in case the market is still loading, but
                # do not treat a persistently absent market as scraper damage.
                return _MatchSkip("totals_missing", retryable=True)

        is_q4 = parsed.get("isQ4", False)
        remaining = parsed.get("remainingMinutes")
        if not self.skip_h2h and is_q4 and remaining is not None and remaining <= 4.0:
            logger.debug("Q4 <=4min remaining, skipping: %s (%.1f min)", url, remaining)
            return _MatchSkip("late_q4")

        parsed_quarter_scores = parsed.get("quarterScores") or {}
        needs_overview = (
            not parsed.get("status")
            or not parsed.get("score")
            or not parsed_quarter_scores.get("home")
            or not parsed_quarter_scores.get("away")
        )
        overview_data = await self._fetch_overview_data(page, url) if needs_overview else {}
        if overview_data.get("status"):
            parsed["status"] = overview_data.get("status")
        if overview_data.get("score"):
            parsed["score"] = overview_data.get("score")
        quarter_scores = (
            overview_data.get("quarterScores")
            if (overview_data.get("quarterScores") or {}).get("home")
            else parsed_quarter_scores
        ) or {}

        match_id = self._extract_match_id(url)

        h2h_body = ""
        if not self.skip_h2h:
            cached = self._h2h_cache.get(match_id)
            if cached is not None:
                h2h_body = cached
            else:
                h2h_body = await self._fetch_h2h_body(page, url)
                if h2h_body:
                    if len(self._h2h_cache) >= 256:
                        self._h2h_cache.pop(next(iter(self._h2h_cache)), None)
                    self._h2h_cache[match_id] = h2h_body

        tournament = self._sanitize_tournament(parsed.get("tournament") or "", url)
        status_text = str(parsed.get("status") or "").strip()
        score_text = str(parsed.get("score") or "").strip()
        match_name = str(parsed.get("matchName") or f"Match {match_id}").strip()
        clock = game_clock(status_text, match_name, tournament)
        home_score, away_score = parse_score(score_text)
        if (
            clock.get("period") is None
            or clock.get("remaining_min") is None
            or home_score is None
            or away_score is None
        ):
            logger.warning(
                "Incomplete live core data; match is not counted as parsed: %s "
                "status=%r score=%r",
                url,
                status_text,
                score_text,
            )
            return _MatchSkip("incomplete_live_core", degraded=True, retryable=True)

        return {
            "match_id": match_id,
            "match_name": match_name,
            "tournament": tournament or "Unknown",
            "status": status_text,
            "opening_total": float(opening),
            "prematch_total": float(prematch) if prematch is not None else None,
            "inplay_total": float(inplay),
            "url": url,
            "score": score_text,
            "market_locked": bool(parsed.get("hasLockedRows", False)),
            "has_prematch": prematch is not None,
            "h2h_body_text": h2h_body,
            "quarter_scores": quarter_scores,
            "odds_snapshot": odds_snapshot,
        }

    async def _fetch_overview_data(self, page, url: str) -> dict:
        try:
            await page.goto(
                url.rstrip("/"),
                wait_until="domcontentloaded",
                timeout=self.page_timeout_ms,
            )
            await self._wait_for_match_page_ready(page)
            return await page.evaluate(r"""
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

                  const parseQuarterRow = (txt, total) => {
                    const nums = (txt.match(/\b\d{1,3}\b/g) || []).map(n => parseInt(n, 10));
                    if (nums.length < 2) return null;
                    const totalIdx = nums.lastIndexOf(total);
                    if (totalIdx < 0) return null;
                    const periods = nums.slice(Math.max(0, totalIdx - 4), totalIdx).filter(v => v >= 0 && v <= 80);
                    if (!periods.length) return null;
                    const sum = periods.reduce((a,b) => a + b, 0);
                    if (Math.abs(sum - total) > 4) return null;
                    return periods;
                  };

                  let quarterScores = { home: [], away: [], source: '', quality: 0 };
                  if (score) {
                    const parts = score.split(/\s*[-–]\s*/).map(v => parseInt(v, 10));
                    const scoreHome = parts[0], scoreAway = parts[1];
                    const detailBox = document.querySelector('.scoresDetails, [class*="scoresDetails"], [class*="scoreDetail"]');
                    if (detailBox) {
                      const rows = Array.from(detailBox.querySelectorAll('.content, [class*="content"]'))
                        .map(el => text(el.innerText))
                        .filter(Boolean);
                      let homePeriods = null;
                      let awayPeriods = null;
                      let homeRowIndex = -1;
                      for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
                        const row = rows[rowIndex];
                        const homeRow = parseQuarterRow(row, scoreHome);
                        const awayRow = parseQuarterRow(row, scoreAway);
                        if (homeRow && !homePeriods) {
                          homePeriods = homeRow;
                          homeRowIndex = rowIndex;
                        }
                        if (awayRow && !awayPeriods && rowIndex !== homeRowIndex) awayPeriods = awayRow;
                      }
                      if (homePeriods && awayPeriods) {
                        quarterScores = {
                          home: homePeriods,
                          away: awayPeriods,
                          source: 'overview_scoreboard_dom',
                          quality: 90,
                        };
                      }
                    }
                  }

                  return { status, score, quarterScores };
                }
            """)
        except Exception as exc:
            logger.debug("Overview data fetch failed for %s: %s", url, exc)
            return {}

    async def _fetch_h2h_body(self, page, url: str) -> str:
        h2h_url = url.rstrip("/") + "/h2h"
        try:
            await page.goto(
                h2h_url,
                wait_until="domcontentloaded",
                timeout=self.page_timeout_ms,
            )
            try:
                await page.wait_for_function(
                    r"""
                    () => {
                        if (!document.body || document.readyState === 'loading') return false;
                        const text = (document.body.innerText || '').toLowerCase();
                        return text.length > 800 && (
                            text.includes('h2h') || text.includes('head to head')
                            || text.includes('per game') || text.includes('points per match')
                        );
                    }
                    """,
                    timeout=min(7000, self.page_timeout_ms),
                )
            except Exception as exc:
                logger.debug("H2H readiness wait ended without a marker for %s: %s", url, exc)
            await page.wait_for_timeout(250)

            # Try to click an H2H tab if the page lands on a different sub-section
            try:
                await page.evaluate(r"""
                    () => {
                        const text = s => (s || '').replace(/\s+/g, ' ').trim();
                        const candidates = Array.from(document.querySelectorAll('a, button, div, span'))
                            .filter(el => el.children.length <= 3);
                        for (const el of candidates) {
                            const t = text(el.innerText || '').toLowerCase();
                            if (!t || t.length > 18) continue;
                            if (/^h2h\b|head.?to.?head|karş.?la.?ma/.test(t)) {
                                try { el.click(); return true; } catch(e) {}
                            }
                        }
                        return false;
                    }
                """)
            except Exception as exc:
                logger.debug("H2H tab selection failed for %s: %s", url, exc)
            await page.wait_for_timeout(300)
            for _ in range(2):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 3)")
                await page.wait_for_timeout(200)

            body = await page.evaluate(r"""
                () => {
                    const fullBody = (document.body.innerText || '').replace(/\s+/g, ' ').trim();
                    // SF/son-form rows can live outside the narrow H2H stats block.
                    // Return the full page text so the parser can extract both.
                    return fullBody;
                }
            """)
            body = body or ""
            low = body.lower()
            keyword_hits = sum(kw in low for kw in (
                "per game", "points per match", "opponent points", "total points over", "h2h"
            ))
            logger.info(
                "H2H body for %s: %d chars, %d/5 key markers | preview: %s",
                url, len(body), keyword_hits, body[:300].replace("\n", " "),
            )
            return body
        except Exception as exc:
            logger.warning("H2H page fetch failed for %s: %s", url, exc)
            return ""

    @staticmethod
    def _extract_match_id(url: str) -> str:
        cleaned = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        parts = cleaned.split('/')
        return parts[-1] if parts else url
