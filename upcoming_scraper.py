"""
upcoming_scraper.py — Fetches upcoming basketball matches from AIScore.

Self-contained module. Does not modify the existing live-match pipeline.
Returns listing facts, verified total lines and independent pre-game analysis.
"""

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from math import ceil
from urllib.parse import urljoin, urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from camoufox.async_api import AsyncNewBrowser
from playwright.async_api import async_playwright

from signal_lists import split_match_teams
from upcoming_odds import TOTAL_MARKET_JS
from upcoming_signals import analyze_upcoming
from upcoming_history_scraper import HISTORY_TIMEOUT_SECONDS, fetch_team_histories

logger = logging.getLogger(__name__)


def _bounded_int(value, default: int, *, minimum: int, maximum: int) -> int:
    """Parse an integer setting without letting a bad env value stop scraping."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


class UpcomingScraper:
    def __init__(
        self,
        aiscore_url: str = "https://m.aiscore.com/basketball",
        page_timeout_ms: int = 35000,
        max_matches: int | None = None,
        days_ahead: int | None = None,
        timezone_id: str | None = None,
        concurrency: int | None = None,
        match_timeout_seconds: int | None = None,
    ):
        self.aiscore_url = self._mobile_url(aiscore_url)
        self.page_timeout_ms = _bounded_int(
            page_timeout_ms,
            35000,
            minimum=5000,
            maximum=120000,
        )
        parsed_max_matches = (
            _bounded_int(max_matches, 12, minimum=0, maximum=500)
            if max_matches is not None
            else 0
        )
        self.max_matches = parsed_max_matches or None
        self.days_ahead = _bounded_int(
            days_ahead if days_ahead is not None else os.getenv("UPCOMING_DAYS_AHEAD", "0"),
            0,
            minimum=0,
            maximum=14,
        )
        self.timezone_id = timezone_id or os.getenv("AISCORE_TIMEZONE", "Europe/Istanbul")
        try:
            ZoneInfo(self.timezone_id)
        except ZoneInfoNotFoundError:
            logger.warning("Unknown AISCORE_TIMEZONE=%s; falling back to UTC.", self.timezone_id)
            self.timezone_id = "UTC"
        default_match_timeout = max(45, min(90, int(self.page_timeout_ms / 1000) * 2))
        self.match_timeout_seconds = _bounded_int(
            match_timeout_seconds
            if match_timeout_seconds is not None
            else os.getenv("UPCOMING_MATCH_TIMEOUT_SECONDS", str(default_match_timeout)),
            default_match_timeout,
            minimum=30,
            maximum=180,
        )
        self.concurrency = _bounded_int(
            concurrency
            if concurrency is not None
            else os.getenv("UPCOMING_CONCURRENCY", "2"),
            2,
            minimum=1,
            maximum=8,
        )
        self._listing_rows_by_id: dict[str, dict] = {}
        self._listing_source_by_id: dict[str, str] = {}
        self._listing_source_reports: dict[str, dict] = {}
        self.last_report = self._new_report()

    async def fetch(self) -> list[dict]:
        self.last_report = self._new_report()
        try:
            async with async_playwright() as p:
                proxy_server = os.getenv("PLAYWRIGHT_PROXY")
                launch_kwargs: dict = {
                    "headless": True,
                    "humanize": True,
                    "locale": "en-US",
                    "main_world_eval": True,
                }
                if proxy_server:
                    launch_kwargs["proxy"] = {"server": proxy_server}
                browser = await AsyncNewBrowser(p, **launch_kwargs)
                context = await self._new_context(browser)
                try:
                    return await self._fetch_with_context(context)
                finally:
                    await context.close()
                    await browser.close()
        except Exception as exc:
            self._finish_report(status="failed", error=str(exc))
            raise

    async def _fetch_with_context(self, context) -> list[dict]:
        """Run one scrape generation; split out so reliability can be unit tested."""
        links = await self._collect_upcoming_links(context)
        listing_attempts = [dict(self._listing_source_reports)]
        if not self._listing_is_complete():
            logger.warning("Upcoming listing was incomplete; retrying once.")
            first_links = list(links)
            first_rows = dict(self._listing_rows_by_id)
            first_sources = dict(self._listing_source_by_id)
            await asyncio.sleep(0.5)
            retry_links = await self._collect_upcoming_links(context)
            listing_attempts.append(dict(self._listing_source_reports))
            links = list(dict.fromkeys([*first_links, *retry_links]))
            self._listing_rows_by_id = {**first_rows, **self._listing_rows_by_id}
            self._listing_source_by_id = {**first_sources, **self._listing_source_by_id}
            combined_reports: dict[str, dict] = {}
            for source_name in {name for attempt in listing_attempts for name in attempt}:
                candidates = [
                    attempt[source_name]
                    for attempt in listing_attempts
                    if source_name in attempt
                ]
                healthy = [item for item in candidates if item.get("status") == "ok"]
                combined_reports[source_name] = dict((healthy or candidates)[-1])
            self._listing_source_reports = combined_reports

        discovered_ids = [self._extract_match_id(link) for link in links]
        self.last_report.update(
            {
                "listing_sources": dict(self._listing_source_reports),
                "listing_attempts": listing_attempts,
                "listing_complete": self._listing_is_complete(),
                "discovered": len(links),
                "discovered_match_ids": discovered_ids,
            }
        )
        if not links:
            status = "empty" if self._listing_is_complete() else "failed"
            self._finish_report(status=status)
            logger.warning(
                "No upcoming match links collected. url=%s days_ahead=%s timezone=%s status=%s",
                self.aiscore_url,
                self.days_ahead,
                self.timezone_id,
                status,
            )
            return []

        effective_limit = self.max_matches or 500
        selected_links = links[:effective_limit]
        truncated = len(selected_links) < len(links)
        self.last_report.update(
            {
                "selected": len(selected_links),
                "attempted": len(selected_links),
                "truncated": truncated,
            }
        )

        logger.info("Fetching details for %s upcoming matches.", len(selected_links))
        results: list[dict] = []
        failed_ids: list[str] = []
        for i in range(0, len(selected_links), self.concurrency):
            batch = selected_links[i : i + self.concurrency]
            logger.info(
                "Fetching upcoming detail batch %s-%s/%s.",
                i + 1,
                min(i + len(batch), len(selected_links)),
                len(selected_links),
            )
            coros = [self._extract_one_with_timeout(context, link) for link in batch]
            chunk = await asyncio.gather(*coros, return_exceptions=True)
            for link, result in zip(batch, chunk):
                if isinstance(result, dict):
                    results.append(result)
                else:
                    failed_ids.append(self._extract_match_id(link))
                    if isinstance(result, Exception):
                        logger.warning("Upcoming match parse failed (%s): %s", link, result)

        attempted = len(selected_links)
        parsed = len(results)
        coverage = round(parsed / attempted, 4) if attempted else 0.0
        partial_rows = sum(
            1 for row in results if str(row.get("data_status") or "complete") != "complete"
        )
        field_coverage = {
            field: sum(1 for row in results if row.get(field) not in (None, "", {}, []))
            for field in (
                "match_name",
                "kickoff",
                "opening_total",
                "prematch_total",
            )
        }
        listing_complete = self._listing_is_complete()
        status = (
            "complete"
            if listing_complete and not truncated and parsed == attempted and partial_rows == 0
            else "partial"
            if results
            else "failed"
        )
        self.last_report.update(
            {
                "parsed": parsed,
                "failed": attempted - parsed,
                "partial_rows": partial_rows,
                "failed_match_ids": failed_ids,
                "coverage": coverage,
                "field_coverage": field_coverage,
                "reconcile_safe": bool(
                    status == "complete" and discovered_ids and parsed == attempted
                ),
            }
        )
        self._finish_report(status=status)
        return results

    def estimated_outer_timeout_seconds(self, planned_matches: int | None = None) -> int:
        """Return a budget that cannot expire before the bounded detail batches."""
        # Unlimited mode still needs a safe upper bound; the scraper itself
        # accepts at most 500 configured rows, so budget for that same ceiling.
        count = planned_matches or self.max_matches or 500
        detail_budget = ceil(max(1, count) / self.concurrency) * (
            self.match_timeout_seconds + HISTORY_TIMEOUT_SECONDS
        )
        listing_sources = 2 + int(self.days_ahead > 0)
        # A listing generation has two navigation attempts and the generation
        # itself is retried once when incomplete. Include readiness waits too.
        listing_budget = listing_sources * ((self.page_timeout_ms / 1000) * 4 + 20)
        return int(detail_budget + listing_budget + 20)

    def _new_report(self) -> dict:
        return {
            "status": "idle",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "listing_sources": {},
            "listing_attempts": [],
            "listing_complete": False,
            "discovered": 0,
            "discovered_match_ids": [],
            "selected": 0,
            "attempted": 0,
            "parsed": 0,
            "failed": 0,
            "partial_rows": 0,
            "failed_match_ids": [],
            "coverage": 0.0,
            "field_coverage": {},
            "truncated": False,
            "reconcile_safe": False,
            "error": None,
        }

    def _finish_report(self, *, status: str, error: str | None = None) -> None:
        self.last_report.update(
            {
                "status": status,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "listing_sources": dict(self._listing_source_reports),
                "listing_complete": self._listing_is_complete(),
                "error": str(error or "")[:500] or None,
            }
        )

    def _record_listing_source(
        self,
        name: str,
        *,
        status: str,
        count: int = 0,
        error: str | None = None,
    ) -> None:
        self._listing_source_reports[name] = {
            "status": status,
            "count": max(0, int(count or 0)),
            "error": str(error or "")[:300] or None,
        }

    def _listing_is_complete(self) -> bool:
        scheduled_ok = self._listing_source_reports.get("scheduled", {}).get("status") == "ok"
        # Only the scheduled source loads both dates touched by the rolling
        # window. The today page is a useful partial fallback, never proof of
        # complete 24-hour coverage.
        return bool(scheduled_ok)

    # ── Browser plumbing ──────────────────────────────────────────────

    @staticmethod
    def _mobile_url(value: str) -> str:
        parsed = urlsplit(str(value or ""))
        return urlunsplit(("https", "m.aiscore.com", parsed.path or "/basketball", parsed.query, ""))

    async def _new_context(self, browser):
        context = await browser.new_context(
            viewport={"width": 430, "height": 932},
            locale="en-US",
            timezone_id=self.timezone_id,
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        return context

    async def _goto_with_retry(
        self,
        page,
        url: str,
        *,
        label: str,
        attempts: int = 2,
    ) -> bool:
        """Navigate with a bounded timeout and explicit, observable retries."""
        last_error = None
        for attempt in range(1, max(1, attempts) + 1):
            try:
                response = await page.goto(
                    self._mobile_url(url),
                    wait_until="domcontentloaded",
                    timeout=self.page_timeout_ms,
                )
                status = getattr(response, "status", None)
                if status is not None and int(status) >= 400:
                    raise RuntimeError(f"HTTP {status}")
                return True
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "%s navigation failed (%s/%s): %s",
                    label,
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    await page.wait_for_timeout(400 * attempt)
        logger.warning("%s could not be loaded: %s", label, last_error)
        return False

    # ── Listing: collect upcoming match links from AiScore ─────────────

    async def _collect_upcoming_links(self, context) -> list[str]:
        self._listing_rows_by_id = {}
        self._listing_source_by_id = {}
        self._listing_source_reports = {}
        scheduled_links = await self._collect_homepage_scheduled_links(context)
        scheduled_ok = self._listing_source_reports.get("scheduled", {}).get("status") == "ok"
        if not scheduled_ok:
            today_links = await self._collect_today_match_links(context)
        else:
            today_links = []
            self._record_listing_source("today_matches", status="skipped")
        links: list[str] = []
        seen: set[str] = set()
        for href in [*scheduled_links, *today_links]:
            if not href or href in seen:
                continue
            seen.add(href)
            links.append(href)

        logger.info(
            "Collected %s upcoming links (%s scheduled, %s today fallback).",
            len(links),
            len(scheduled_links),
            len(today_links),
        )
        return links

    async def _collect_homepage_scheduled_links(self, context) -> list[str]:
        """Collect the next 24 hours from the mobile Schedule tab."""
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            window_start, window_end = self._window_bounds()
            target_dates: list[date] = []
            cursor = window_start.date()
            while cursor <= window_end.date():
                target_dates.append(cursor)
                cursor += timedelta(days=1)

            base = re.sub(r"/\d{8}/?$", "", self.aiscore_url.rstrip("/"))
            rows: list[dict] = []
            source_errors: list[str] = []
            date_counts: dict[str, int] = {}

            for target_date in target_dates:
                date_key = target_date.strftime("%Y%m%d")
                listing_url = f"{base}/{date_key}"
                if not await self._goto_with_retry(
                    page,
                    listing_url,
                    label=f"mobile Schedule listing {date_key}",
                ):
                    source_errors.append(f"{date_key}: navigation failed")
                    continue
                try:
                    schedule = page.get_by_text("Schedule", exact=True)
                    if not await schedule.count():
                        raise RuntimeError("Schedule tab unavailable")
                    await schedule.first.click()
                    await page.wait_for_function(
                        r"""
                        () => {
                            const visible = el => {
                                const style = window.getComputedStyle(el);
                                const rect = el.getBoundingClientRect();
                                return style.display !== 'none'
                                    && style.visibility !== 'hidden'
                                    && rect.width > 0 && rect.height > 0;
                            };
                            return Array.from(
                                document.querySelectorAll('a[href*="/basketball/match-"]')
                            ).some(visible);
                        }
                        """,
                        timeout=min(8000, self.page_timeout_ms),
                    )
                except Exception as exc:
                    logger.warning("Mobile Schedule tab failed for %s: %s", date_key, exc)
                    source_errors.append(f"{date_key}: Schedule unavailable")
                    continue

                date_rows = await page.evaluate(
                    r"""
                    () => {
                        const text = s => (s || '').replace(/\s+/g, ' ').trim();
                        const visible = el => {
                            const style = window.getComputedStyle(el);
                            const rect = el.getBoundingClientRect();
                            return style.display !== 'none'
                                && style.visibility !== 'hidden'
                                && rect.width > 0 && rect.height > 0;
                        };
                        const out = [];
                        const seen = new Set();
                        for (const anchor of document.querySelectorAll(
                            'a[href*="/basketball/match-"]'
                        )) {
                            if (!visible(anchor)) continue;
                            const href = anchor.getAttribute('href') || '';
                            if (!href || seen.has(href)) continue;
                            const rowText = text(anchor.innerText);
                            const time = rowText.match(/\b(\d{1,2}:\d{2})\s*(AM|PM)?\b/i);
                            if (!time) continue;
                            seen.add(href);
                            out.push({
                                href,
                                kickoff_time: time[1],
                                meridiem: (time[2] || '').toUpperCase(),
                            });
                        }
                        return out;
                    }
                    """
                )
                date_rows = date_rows if isinstance(date_rows, list) else []
                date_counts[date_key] = len(date_rows)
                for row in date_rows:
                    hour_text = str(row.get("kickoff_time") or "")
                    meridiem = str(row.get("meridiem") or "")
                    try:
                        parsed_time = datetime.strptime(
                            f"{hour_text} {meridiem}".strip(),
                            "%I:%M %p" if meridiem else "%H:%M",
                        ).time()
                    except ValueError:
                        continue
                    kickoff = datetime.combine(target_date, parsed_time).replace(
                        tzinfo=window_start.tzinfo
                    )
                    if window_start <= kickoff <= window_end:
                        rows.append({
                            "href": row.get("href"),
                            "kickoff": kickoff.strftime("%Y-%m-%d %H:%M"),
                        })

            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            links: list[str] = []
            seen: set[str] = set()
            for row in rows:
                href = str(row.get("href") or "")
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if "/basketball/match-" not in cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                match_id = self._extract_match_id(cleaned)
                self._listing_rows_by_id[match_id] = {
                    "match_id": match_id,
                    "url": cleaned,
                    "kickoff": row["kickoff"],
                    "listing_source": "scheduled_mobile",
                    "kickoff_source": "scheduled_mobile",
                }
                self._listing_source_by_id[match_id] = "scheduled_mobile"
                links.append(cleaned)

            source_status = "ok" if not source_errors else "partial" if links else "failed"
            self._record_listing_source(
                "scheduled",
                status=source_status,
                count=len(links),
                error="; ".join(source_errors) if source_errors else None,
            )
            self._listing_source_reports["scheduled"]["date_counts"] = date_counts
            self._listing_source_reports["scheduled"]["window_start"] = window_start.isoformat()
            self._listing_source_reports["scheduled"]["window_end"] = window_end.isoformat()
            logger.info("Mobile Schedule produced %s upcoming rows.", len(links))
            return links
        finally:
            if not page.is_closed():
                await page.close()

    async def _collect_today_match_links(self, context) -> list[str]:
        # AiScore's `/today-matches/basketball` view is a useful fallback. It
        # currently renders the real match link on the `VS` score anchor; the
        # surrounding row markup has changed over time, so row detection is
        # intentionally tolerant and does not depend on a specific class name.
        listing_url = "https://m.aiscore.com/today-matches/basketball"
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            if not await self._goto_with_retry(
                page,
                listing_url,
                label="today-matches listing",
            ):
                self._record_listing_source(
                    "today_matches", status="failed", error="navigation failed"
                )
                return []
            try:
                await page.wait_for_selector(
                    'a[href*="/basketball/match-"]',
                    timeout=min(8000, self.page_timeout_ms),
                )
            except Exception as exc:
                logger.info("today-matches has no visible match link yet: %s", exc)

            # Trigger lazy lists by scrolling once.
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 1200)")
                await page.wait_for_timeout(250)
            await page.evaluate("window.scrollTo(0, 0)")

            try:
                listing_rows = await page.evaluate(
                    r"""
                () => {
                    const text = s => (s || '').replace(/\s+/g, ' ').trim();
                    const out = [];
                    const seen = new Set();
                    const links = document.querySelectorAll(
                        '.list-item a[href*="/basketball/match-"]'
                    );
                    for (const a of links) {
                        const href = a.getAttribute('href');
                        if (!href || seen.has(href)) continue;
                        const row = a.closest('.list-item');
                        if (!row) continue;

                        // The score link is the canonical state marker on this
                        // page: upcoming rows say exactly "VS". Do not climb to
                        // a league/page container, otherwise a finished row can
                        // inherit another match's VS marker and first kickoff.
                        const scoreText = text(a.innerText || '');
                        if (!/^VS$/i.test(scoreText)) continue;
                        const status = text((row.querySelector('.status') || {}).innerText || '');
                        if (/\b(FT|Ended|Finished|Q[1-4]|[1-4]Q|HT|OT)\b/i.test(status)) continue;

                        const kickoff = text((row.querySelector('.time') || {}).innerText || '');
                        if (!/^\d{1,2}:\d{2}$/.test(kickoff)) continue;
                        const homeTeam = text((row.querySelector('.home') || {}).innerText || '');
                        const awayTeam = text((row.querySelector('.away') || {}).innerText || '');
                        const league = row.closest('.list-container');
                        const heading = league && Array.from(league.children).find(el => {
                            if (el === row || el.matches('.list-item')) return false;
                            const value = text(el.innerText || '');
                            return value && value.length <= 160 && !/\b\d{1,2}:\d{2}\b/.test(value);
                        });

                        seen.add(href);
                        out.push({
                            href,
                            kickoff,
                            home_team: homeTeam,
                            away_team: awayTeam,
                            match_name: homeTeam && awayTeam ? `${homeTeam} - ${awayTeam}` : '',
                            tournament: text((heading && heading.innerText) || ''),
                        });
                    }
                    return out;
                }
                    """
                )
            except Exception as exc:
                logger.warning("today-matches listing parse failed: %s", exc)
                self._record_listing_source(
                    "today_matches", status="failed", error=str(exc)
                )
                return []
            logger.info(
                "today-matches listing produced %s upcoming candidate hrefs.",
                len(listing_rows),
            )

            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            links: list[str] = []
            seen: set[str] = set()
            for listing_row in listing_rows or []:
                href = listing_row.get("href") if isinstance(listing_row, dict) else None
                if not href:
                    continue
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if "/basketball/match-" not in cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                match_id = self._extract_match_id(cleaned)
                self._listing_rows_by_id[match_id] = {
                    "match_id": match_id,
                    "url": cleaned,
                    "kickoff": self._today_kickoff(
                        str(listing_row.get("kickoff") or "")
                    ),
                    "match_name": str(listing_row.get("match_name") or ""),
                    "home_team": str(listing_row.get("home_team") or ""),
                    "away_team": str(listing_row.get("away_team") or ""),
                    "tournament": str(listing_row.get("tournament") or ""),
                    "listing_source": "today_matches",
                    "kickoff_source": "today_matches",
                }
                self._listing_source_by_id[match_id] = "today_matches"
                links.append(cleaned)
            self._record_listing_source("today_matches", status="ok", count=len(links))
            logger.info("Collected %s candidate upcoming links.", len(links))
            return links
        finally:
            if not page.is_closed():
                await page.close()

    # ── Match detail extraction ───────────────────────────────────────

    async def _extract_one_with_timeout(self, context, link: str) -> dict | None:
        try:
            match = await asyncio.wait_for(
                self._extract_one(context, link),
                timeout=self.match_timeout_seconds,
            )
        except TimeoutError:
            logger.warning(
                "Upcoming match detail timed out after %ss: %s",
                self.match_timeout_seconds,
                link,
            )
            return None
        if match is None:
            return None
        analysis_match = {**match, "kickoff": self._today_kickoff(match.get("kickoff") or "")}
        # History errors must not discard an already fetched schedule/market row.
        try:
            histories = await asyncio.wait_for(
                fetch_team_histories(context, match, self.timezone_id),
                timeout=HISTORY_TIMEOUT_SECONDS,
            )
            match["upcoming_analysis"] = analyze_upcoming(analysis_match, histories)
        except Exception as exc:
            logger.warning("Upcoming team history unavailable: %s (%s)",
                           match.get("match_id"), type(exc).__name__)
            match["upcoming_analysis"] = {
                **analyze_upcoming(analysis_match, {}), "status": "history_unavailable",
            }
        return match

    @staticmethod
    def _has_full_kickoff(value: str) -> bool:
        text = str(value or "").strip()
        match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\b", text)
        if not match:
            return False
        try:
            datetime.strptime(match.group(0), "%Y-%m-%d %H:%M")
        except ValueError:
            return False
        return True

    def _resolve_kickoff(self, detail: dict, listing: dict) -> tuple[str, str]:
        """Prefer dated listing evidence and never infer a date from page-wide text."""
        listing_value = str(listing.get("kickoff") or "").strip()
        detail_value = str(detail.get("kickoff") or "").strip()
        listing_source = str(listing.get("kickoff_source") or listing.get("listing_source") or "")
        detail_source = str(detail.get("kickoff_source") or "")

        if self._has_full_kickoff(listing_value):
            return listing_value, listing_source
        if self._has_full_kickoff(detail_value):
            return detail_value, detail_source or "detail_header"
        if listing_value or listing_source == "today_matches":
            return listing_value, listing_source
        return detail_value, detail_source

    def _kickoff_has_trusted_provenance(
        self,
        kickoff: str,
        *,
        kickoff_source: str,
        listing_source: str,
    ) -> bool:
        if self._has_full_kickoff(kickoff):
            return bool(kickoff_source)
        # Empty/time-only values are retained only when a genuine listing row
        # proves that this is an upcoming candidate, not a random detail-page date.
        return listing_source in {"scheduled_nuxt", "today_matches", "future_nuxt"} and (
            kickoff_source == listing_source
            or (not kickoff and listing_source == "today_matches")
        )

    async def _extract_one(self, context, link: str) -> dict | None:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            match_id = self._extract_match_id(link)
            listing_data = dict(self._listing_rows_by_id.get(match_id) or {})
            odds_data = await self._read_odds_page(page, link)
            detail_page_loaded = bool(odds_data)
            if not odds_data and listing_data.get("match_name"):
                odds_data = dict(listing_data)
            if not odds_data:
                logger.warning("No usable detail or listing metadata: %s", link)
                return None
            for key in (
                "match_name", "home_team", "away_team", "tournament",
                "url",
            ):
                if odds_data.get(key) in (None, "") and listing_data.get(key) not in (None, ""):
                    odds_data[key] = listing_data.get(key)
            # Reject obviously contaminated tournament strings (e.g. promo
            # widget leakage like "EPL Standings 2024-25 : CBA").
            current_tournament = str(odds_data.get("tournament") or "")
            if re.search(r"standings|popular|trending|featured", current_tournament, re.I):
                odds_data["tournament"] = ""
            if not odds_data.get("tournament"):
                odds_data["tournament"] = self._tournament_from_url(odds_data.get("url") or link)
            listing_source = str(
                listing_data.get("listing_source")
                or self._listing_source_by_id.get(match_id)
                or ""
            )
            opening = odds_data.get("opening")
            prematch = odds_data.get("prematch")
            retry_count = 0
            while opening is None and retry_count < 1:
                retry_count += 1
                retry_page = await context.new_page()
                retry_page.set_default_timeout(self.page_timeout_ms)
                try:
                    retry_odds = await self._read_odds_page(retry_page, link)
                    if retry_odds:
                        detail_page_loaded = True
                        market_keys = {"opening", "prematch", "inplay", "bookmaker", "odds_source", "market_verified"}
                        odds_data.update(
                            {
                                key: value
                                for key, value in retry_odds.items()
                                if key not in market_keys and value not in (None, "")
                            }
                        )
                        # A new bookmaker observation replaces the whole market
                        # tuple; never retain the previous company's pre-match line.
                        odds_data.update({key: retry_odds.get(key) for key in market_keys})
                        opening = odds_data.get("opening")
                        prematch = odds_data.get("prematch")
                finally:
                    if not retry_page.is_closed():
                        await retry_page.close()

            kickoff, kickoff_source = self._resolve_kickoff(odds_data, listing_data)
            odds_data["kickoff"] = kickoff
            odds_data["kickoff_source"] = kickoff_source
            if odds_data.get("is_live") or odds_data.get("is_finished"):
                logger.info("Skipping non-upcoming match: %s", link)
                return None
            if not self._kickoff_has_trusted_provenance(
                kickoff,
                kickoff_source=kickoff_source,
                listing_source=listing_source,
            ):
                logger.warning(
                    "Skipping kickoff without trusted provenance: %s kickoff=%s detail_source=%s listing_source=%s",
                    link,
                    kickoff,
                    kickoff_source,
                    listing_source,
                )
                return None
            if not self._kickoff_in_allowed_window(kickoff):
                logger.info("Skipping match outside configured date window: %s kickoff=%s", link, kickoff)
                return None
            data_warnings: list[str] = []
            if opening is None:
                logger.info("No verified opening/prematch total is available: %s", link)
                # Still keep the row — odds may not be open yet — but do not
                # report a data-complete generation when the core market is absent.
                data_warnings.append("total_market_unavailable")

            if not detail_page_loaded:
                data_warnings.append("odds_detail_unavailable_listing_fallback")
            match_name = odds_data.get("match_name") or ""
            home_team, away_team = split_match_teams(match_name)

            return {
                "match_id": match_id,
                "match_name": match_name or f"Match {match_id}",
                "home_team": odds_data.get("home_team") or home_team,
                "away_team": odds_data.get("away_team") or away_team,
                "tournament": odds_data.get("tournament") or "",
                "kickoff": kickoff,
                "listing_source": listing_source,
                "kickoff_source": kickoff_source,
                "odds_source": odds_data.get("odds_source") or "",
                "bookmaker": odds_data.get("bookmaker") or "",
                "opening_total": opening,
                "prematch_total": prematch,
                "url": link,
                "data_status": "partial" if data_warnings else "complete",
                "data_warnings": data_warnings,
            }
        except Exception as exc:
            logger.warning("Detail extract failed (%s): %s", link, exc)
            return None
        finally:
            if not page.is_closed():
                await page.close()

    async def _read_odds_page(self, page, url: str) -> dict | None:
        clean = url.rstrip("/")
        odds_url = clean if clean.endswith("/odds") else clean + "/odds"
        if not await self._goto_with_retry(page, odds_url, label="upcoming odds detail"):
            return None
        try:
            await page.wait_for_function(
                r"""
                () => !!document.querySelector(
                    '.newOdds, [class*="newOdds"], [class*="oddsContent"], [class*="matchTop"], [class*="matchHeader"]'
                )
                """,
                timeout=min(6000, self.page_timeout_ms),
            )
        except Exception as exc:
            logger.info("Odds detail readiness wait ended without a known root (%s): %s", url, exc)

        try:
            tab = page.locator(".oddTypesBox span", has_text=re.compile(r"^\s*Total Points\s*$", re.I))
            if await tab.count():
                await tab.first.click()
            opening_toggle = page.locator('[role="checkbox"]', has_text="Opening odds")
            if await opening_toggle.count() and await opening_toggle.first.get_attribute("aria-checked") == "false":
                await opening_toggle.first.click()
            await page.wait_for_function(
                """() => [...document.querySelectorAll('.oddsContent')].some(el =>
                    /total points/i.test(el.querySelector('.oddsType')?.innerText || '')
                    && el.querySelector('.oddsBoxContent'))""",
                timeout=min(8000, self.page_timeout_ms),
            )
        except Exception:
            logger.info("Total market not ready; unverified values will remain empty.")
        market = await page.evaluate(TOTAL_MARKET_JS)

        metadata = await page.evaluate(
            r"""
            () => {
                const text = s => (s || '').replace(/\s+/g, ' ').trim();

                // Title / match name.
                let matchName = text(document.title || '')
                    .replace(/\s*\|.*/, '')
                    .replace(/\s*-\s*AiScore.*/i, '')
                    .replace(/\s*live score.*/i, '')
                    .replace(/\s*prediction.*/i, '')
                    .replace(/\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*/g, '')
                    .replace(/\s*betting odds\s*/gi, '')
                    .replace(/\s+vs\.?\s+/gi, ' - ')
                    .trim();

                let tournament = '';
                let country = '';
                const promoRe = /schedule|standings|teams|stats|live\s*score|popular|trending|featured/i;
                // Strategy 0: a.not-allow — AiScore renders the league name above
                // the team names as an anchor with class="not-allow" and href="javascript:;".
                // This is the most direct source and takes priority over breadcrumb heuristics.
                const notAllowEl = Array.from(document.querySelectorAll('a.not-allow'))
                    .find(e => { const t = text(e.innerText); return t && t.length >= 3 && t.length <= 80 && !promoRe.test(t); });
                if (notAllowEl) tournament = text(notAllowEl.innerText);
                // Scope strictly to the match header. Wider scopes (e.g.
                // [class*="league"]) catch sidebar/footer "Popular Leagues"
                // widgets and produce cross-contaminated names.
                const breadcrumbRoots = Array.from(document.querySelectorAll(
                    '[class*="matchTop"], [class*="matchInfo"], [class*="matchHeader"]'
                ));
                const scopedAnchors = breadcrumbRoots.flatMap(root => Array.from(root.querySelectorAll('a')));
                const breadcrumbs = scopedAnchors
                    .map(e => ({ text: text(e.innerText), href: e.getAttribute('href') || '' }))
                    .filter(e => e.text && !promoRe.test(e.text))
                    .filter(e => e.href.includes('/tournament-'));
                if (breadcrumbs.length >= 2) {
                    country = breadcrumbs[0].text;
                    tournament = breadcrumbs[breadcrumbs.length - 1].text;
                } else if (breadcrumbs.length === 1) {
                    tournament = breadcrumbs[0].text;
                }
                // Fallback: derive league name from the tournament link slug
                // when the visible breadcrumb text wasn't usable.
                if (!tournament) {
                    const slugAnchor = scopedAnchors
                        .map(e => e.getAttribute('href') || '')
                        .find(h => /\/tournament-[a-z0-9-]+/i.test(h));
                    if (slugAnchor) {
                        const m = slugAnchor.match(/\/tournament-([a-z0-9-]+)/i);
                        if (m && m[1]) {
                            tournament = m[1].replace(/-/g, ' ')
                                .replace(/\b\w/g, c => c.toUpperCase());
                        }
                    }
                }
                const cleanRe = /\s*(live\s*score|betting\s*odds|prediction)\s*/gi;
                tournament = tournament.replace(cleanRe, '').trim();
                country = country.replace(cleanRe, '').trim();
                if (country && tournament && !tournament.toLowerCase().startsWith(country.toLowerCase())) {
                    tournament = country + ' : ' + tournament;
                } else if (country && !tournament) {
                    tournament = country;
                }

                // Status detection — scoped to the match header area so the
                // AiScore footer ("...halftime or final result...") does not
                // false-positive every upcoming page as "Finished".
                const matchTopEl = document.querySelector('[class*="matchTop"]');
                const matchScoreEl = document.querySelector('[class*="matchScore"]');
                const headerText = (
                    text((matchTopEl && matchTopEl.innerText) || '') + ' ' +
                    text((matchScoreEl && matchScoreEl.innerText) || '')
                ).trim();
                const scoreCellText = text((matchScoreEl && matchScoreEl.innerText) || '');
                const upcomingMarker = /^vs$/i.test(scoreCellText) || /\bvs\b/i.test(scoreCellText);

                const isLive = !upcomingMarker && (
                    /\b(Q[1-4]|[1-4]Q|HT|OT)\s*[-\s]?\s*\d{1,2}:\d{2}\b/i.test(headerText)
                    || !!document.querySelector('[class*="liveTime"], [class*="LiveTime"]')
                );
                const isFinished = !upcomingMarker && (
                    /\b(FT|Finished|Full\s*Time)\b/i.test(headerText)
                    || !!document.querySelector('[class*="final-score"], [class*="finalScore"]')
                );

                // Kickoff time — date + time near the score area. AiScore can
                // render this either as ISO-ish text ("2026/05/03 10:00") or
                // English text ("10:00 AM Sunday, May 3, 2026").
                let kickoff = '';
                const pad2 = n => String(n).padStart(2, '0');
                const monthNo = {
                    january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
                    july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
                    jan: 1, feb: 2, mar: 3, apr: 4, jun: 6, jul: 7, aug: 8, sep: 9, sept: 9,
                    oct: 10, nov: 11, dec: 12,
                };
                const normalizeHour = (hour, ampm) => {
                    let h = parseInt(hour, 10);
                    const marker = String(ampm || '').toLowerCase();
                    if (marker === 'pm' && h < 12) h += 12;
                    if (marker === 'am' && h === 12) h = 0;
                    return pad2(h);
                };
                const normalizeEnglishKickoff = source => {
                    const re = /\b(\d{1,2}):(\d{2})\s*(AM|PM)?\s+(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:day)?[,]?\s+)?(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(\d{1,2}),\s*(\d{4})\b/i;
                    const m = String(source || '').match(re);
                    if (!m) return '';
                    const month = monthNo[String(m[4] || '').toLowerCase()];
                    if (!month) return '';
                    return `${m[6]}-${pad2(month)}-${pad2(parseInt(m[5], 10))} ${normalizeHour(m[1], m[3])}:${m[2]}`;
                };
                const normalizeIsoKickoff = source => {
                    const direct = String(source || '').match(/\b(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?\b/i);
                    if (direct) {
                        return `${direct[1]}-${pad2(parseInt(direct[2], 10))}-${pad2(parseInt(direct[3], 10))} ${normalizeHour(direct[4], direct[6])}:${direct[5]}`;
                    }
                    const combined = String(source || '').match(/\b(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})\b.{0,180}?\b(\d{1,2}):(\d{2})\s*(AM|PM)?\b/i);
                    if (combined) {
                        return `${combined[1]}-${pad2(parseInt(combined[2], 10))}-${pad2(parseInt(combined[3], 10))} ${normalizeHour(combined[4], combined[6])}:${combined[5]}`;
                    }
                    return '';
                };

                const kickoffRoots = Array.from(document.querySelectorAll(
                    '[class*="matchTop"], [class*="matchInfo"], [class*="matchHeader"], [class*="matchScore"]'
                ));
                const kickoffNodes = kickoffRoots.flatMap(root => [root, ...Array.from(root.querySelectorAll('span, div, time'))]);
                const kickoffEl = kickoffNodes
                    .map(e => text(e.innerText || ''))
                    .find(v => normalizeIsoKickoff(v) || normalizeEnglishKickoff(v));
                if (kickoffEl) kickoff = normalizeIsoKickoff(kickoffEl) || normalizeEnglishKickoff(kickoffEl);
                if (!kickoff) {
                    const timeOnly = kickoffNodes
                        .map(e => text(e.innerText || ''))
                        .find(v => /^\d{1,2}:\d{2}$/.test(v));
                    if (timeOnly) kickoff = timeOnly;
                }

                return {
                    match_name: matchName,
                    tournament,
                    kickoff,
                    kickoff_source: kickoff && /^\d{4}-\d{2}-\d{2}/.test(kickoff)
                        ? 'detail_header'
                        : (kickoff ? 'detail_header_time' : ''),
                    is_live: isLive,
                    is_finished: isFinished,
                };
            }
            """
        )

        return {**metadata, **market}

    @staticmethod
    def _extract_match_id(url: str) -> str:
        cleaned = re.sub(
            r"/(h2h|odds|stats|lineups|standings|summary)/?$", "", url.rstrip("/")
        )
        parts = cleaned.split("/")
        return parts[-1] if parts else url

    @staticmethod
    def _tournament_from_url(url: str) -> str:
        match = re.search(r"/tournament-([a-z0-9-]+)", str(url or ""), re.I)
        if not match:
            return ""
        slug = match.group(1).strip("-")
        if not slug:
            return ""
        return slug.replace("-", " ").title()

    def _kickoff_in_allowed_window(self, kickoff: str) -> bool:
        """Return whether kickoff is between now and exactly 24 hours from now."""
        text = str(kickoff or "").strip()
        if not text:
            return False
        if re.fullmatch(r"\d{1,2}:\d{2}", text):
            text = self._today_kickoff(text)
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(
                tzinfo=ZoneInfo(self.timezone_id)
            )
        except ValueError:
            return False
        start, end = self._window_bounds()
        return start <= parsed <= end

    def _window_bounds(self) -> tuple[datetime, datetime]:
        try:
            now = datetime.now(ZoneInfo(self.timezone_id))
        except ZoneInfoNotFoundError:
            now = datetime.now(timezone.utc)
        return now, now + timedelta(hours=24)

    def _today_kickoff(self, kickoff_time: str) -> str:
        value = str(kickoff_time or "").strip()
        if not re.fullmatch(r"\d{1,2}:\d{2}", value):
            return value
        hour, minute = value.split(":", 1)
        return f"{self._today().isoformat()} {int(hour):02d}:{minute}"

    def _today(self) -> date:
        try:
            return datetime.now(ZoneInfo(self.timezone_id)).date()
        except ZoneInfoNotFoundError:
            return date.today()
