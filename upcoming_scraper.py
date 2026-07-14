"""
upcoming_scraper.py — Fetches upcoming basketball matches from AIScore.

Self-contained module. Does not modify the existing live-match pipeline.
For each upcoming match it returns:
  - match name, tournament, kickoff time
  - opening total, pre-match total
  - last-6 form (PPG/OPPG/avg) for both teams via H2H page
  - cross-paired expected total (SF average) like the dashboard does

Implementation notes:
  - Reuses signal_analysis._extract_h2h_metrics for the SF computation so that
    "Son 6 maç" numbers match exactly what the dashboard already shows.
  - Browser automation mirrors aiscore_scraper.py patterns but runs against
    the Schedule/Upcoming tab and tolerates a missing in-play total.
"""

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from math import ceil
from urllib.parse import urljoin
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from playwright.async_api import async_playwright

from signal_analysis import _split_match_name, extract_h2h_metrics

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
        aiscore_url: str = "https://www.aiscore.com/basketball",
        page_timeout_ms: int = 35000,
        max_matches: int | None = None,
        days_ahead: int | None = None,
        timezone_id: str | None = None,
        concurrency: int | None = None,
        match_timeout_seconds: int | None = None,
    ):
        self.aiscore_url = aiscore_url
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
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                }
                if proxy_server:
                    launch_kwargs["proxy"] = {"server": proxy_server}
                browser = await p.chromium.launch(**launch_kwargs)
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
                "expected_total",
                "home_last6",
                "away_last6",
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
        detail_budget = ceil(max(1, count) / self.concurrency) * self.match_timeout_seconds
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
        today_ok = self._listing_source_reports.get("today_matches", {}).get("status") == "ok"
        today_scope_ok = scheduled_ok or today_ok
        future_ok = (
            self.days_ahead <= 0
            or self._listing_source_reports.get("future", {}).get("status") == "ok"
        )
        return bool(today_scope_ok and future_ok)

    # ── Browser plumbing ──────────────────────────────────────────────

    async def _new_context(self, browser):
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
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
                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.page_timeout_ms,
                )
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

    @staticmethod
    def _verified_total_from_markets(markets) -> float | None:
        """Accept a Nuxt line only when its market explicitly identifies totals."""
        total_pattern = re.compile(
            r"\btotal(?:\s+points?)?\b|\bo\s*/\s*u\b|over\s*[/&-]?\s*under|üst\s*[/&-]?\s*alt|\bou\b",
            re.I,
        )
        candidates: list[float] = []
        for market in markets or []:
            if not isinstance(market, dict):
                continue
            evidence = str(market.get("market") or "").strip()
            if not evidence or not total_pattern.search(evidence):
                continue
            values = market.get("values")
            if not isinstance(values, list):
                values = [values]
            for raw in values:
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    continue
                if 100 <= value <= 400:
                    candidates.append(value)
        return candidates[-1] if candidates else None

    # ── Listing: collect upcoming match links from AiScore ─────────────

    async def _collect_upcoming_links(self, context) -> list[str]:
        self._listing_rows_by_id = {}
        self._listing_source_by_id = {}
        self._listing_source_reports = {}
        today_links = await self._collect_homepage_scheduled_links(context)
        if not today_links:
            today_links = await self._collect_today_match_links(context)
        else:
            self._record_listing_source("today_matches", status="skipped")
        future_links = []
        if self.days_ahead > 0:
            future_links = await self._collect_homepage_future_links(context)
        else:
            self._record_listing_source("future", status="skipped")

        links: list[str] = []
        seen: set[str] = set()
        for href in [*today_links, *future_links]:
            if not href or href in seen:
                continue
            seen.add(href)
            links.append(href)

        logger.info(
            "Collected %s upcoming links (%s today-matches, %s homepage future).",
            len(links),
            len(today_links),
            len(future_links),
        )
        return links

    async def _collect_homepage_scheduled_links(self, context) -> list[str]:
        """Collect the visible Scheduled/Today's Upcoming payload from /basketball.

        This is the source behind AiScore's "Today's Upcoming Matches" odds
        table. Unlike the footer `matchesFuture` list, it includes today's
        scheduled rows and their current total-points line.
        """
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            listing_url = self._dated_aiscore_url()
            if not await self._goto_with_retry(
                page,
                listing_url,
                label="upcoming scheduled listing",
            ):
                self._record_listing_source(
                    "scheduled", status="failed", error="navigation failed"
                )
                return []
            try:
                await page.wait_for_function(
                    r"""
                    () => {
                        const b = (window.__NUXT__ && window.__NUXT__.state && window.__NUXT__.state.basketball) || {};
                        return Array.isArray(b.matchesData_matches)
                            && Array.isArray(b.matchesData_teams);
                    }
                    """,
                    timeout=min(10000, self.page_timeout_ms),
                )
                await page.wait_for_timeout(600)
            except Exception as exc:
                logger.warning("Scheduled Nuxt payload was not available: %s", exc)
                self._record_listing_source(
                    "scheduled", status="failed", error="Nuxt payload unavailable"
                )
                return []

            try:
                rows = await page.evaluate(
                    r"""
                ({ daysAhead }) => {
                    const b = (window.__NUXT__ && window.__NUXT__.state && window.__NUXT__.state.basketball) || {};
                    const matches = Array.isArray(b.matchesData_matches) ? b.matchesData_matches : [];
                    const teams = Array.isArray(b.matchesData_teams) ? b.matchesData_teams : [];
                    const comps = Array.isArray(b.matchesData_competitions) ? b.matchesData_competitions : [];
                    const teamMap = new Map(teams.map(t => [t.id, t]));
                    const compMap = new Map(comps.map(c => [c.id, c]));
                    const pad2 = n => String(n).padStart(2, '0');
                    const localStamp = ts => {
                        const d = new Date((Number(ts) || 0) * 1000);
                        if (!Number.isFinite(d.getTime())) return '';
                        return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
                    };
                    const dateKey = stamp => String(stamp || '').slice(0, 10);
                    const today = dateKey(localStamp(Date.now() / 1000));
                    const endDate = (() => {
                        const d = new Date();
                        d.setDate(d.getDate() + Number(daysAhead || 0));
                        return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
                    })();
                    const oddsMarkets = match => {
                        const items = match?.ext?.odds?.oddItems || [];
                        return items.map(item => ({
                            market: [
                                item?.name, item?.market, item?.marketName, item?.type,
                                item?.typeName, item?.marketType, item?.betType,
                                item?.title, item?.key,
                            ].filter(v => typeof v === 'string').join(' '),
                            values: Array.isArray(item?.odd) ? item.odd : [],
                        }));
                    };
                    const out = [];
                    for (const match of matches) {
                        if (!match?.id || Number(match.statusId) !== 1 || Number(match.matchStatus) !== 1) continue;
                        const home = teamMap.get(match?.homeTeam?.id);
                        const away = teamMap.get(match?.awayTeam?.id);
                        if (!home?.slug || !away?.slug) continue;
                        const kickoff = localStamp(match.matchTime);
                        const day = dateKey(kickoff);
                        if (day < today || day > endDate) continue;
                        const comp = compMap.get(match?.competition?.id) || {};
                        const compName = comp?.name || '';
                        const country = comp?.category?.name || comp?.country?.name || '';
                        const promoRe = /standings|popular|trending|featured/i;
                        const safeCountry = country && !promoRe.test(country) ? country : '';
                        const safeName = compName && !promoRe.test(compName) ? compName : '';
                        const tournament = [safeCountry, safeName].filter(Boolean).join(' : ');
                        out.push({
                            match_id: match.id,
                            url: `/basketball/match-${home.slug}-${away.slug}/${match.id}`,
                            match_name: `${home.name || home.shortName || home.slug} - ${away.name || away.shortName || away.slug}`,
                            home_team: home.name || home.shortName || '',
                            away_team: away.name || away.shortName || '',
                            tournament,
                            kickoff,
                            _odds_markets: oddsMarkets(match),
                        });
                    }
                    return out;
                }
                    """,
                    {"daysAhead": self.days_ahead},
                )
            except Exception as exc:
                logger.warning("Scheduled listing parse failed: %s", exc)
                self._record_listing_source(
                    "scheduled", status="failed", error=str(exc)
                )
                return []

            links: list[str] = []
            seen: set[str] = set()
            verified_total_count = 0
            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            for row in rows or []:
                href = row.get("url")
                match_id = str(row.get("match_id") or "").strip()
                if not href or not match_id:
                    continue
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if cleaned in seen:
                    continue
                seen.add(cleaned)
                total = self._verified_total_from_markets(row.pop("_odds_markets", []))
                if total is not None:
                    verified_total_count += 1
                row = {
                    **row,
                    "url": cleaned,
                    "listing_source": "scheduled_nuxt",
                    "kickoff_source": "scheduled_nuxt",
                    # The listing payload exposes a current market line, not a
                    # documented opening snapshot. Do not relabel it as opening.
                    "opening_total": None,
                    "prematch_total": total,
                    "odds_source": "scheduled_nuxt_total_market" if total is not None else "",
                }
                self._listing_rows_by_id[match_id] = row
                self._listing_source_by_id[match_id] = "scheduled_nuxt"
                links.append(cleaned)
            self._record_listing_source("scheduled", status="ok", count=len(links))
            self._listing_source_reports["scheduled"]["verified_totals"] = verified_total_count
            logger.info("Scheduled payload produced %s upcoming rows.", len(links))
            return links
        finally:
            if not page.is_closed():
                await page.close()

    async def _collect_homepage_future_links(self, context) -> list[str]:
        """Read the homepage 'future/upcoming' payload and build match URLs.

        The visible homepage footer may render these as head-to-head SEO links,
        but Nuxt state includes the real match ids plus team slugs. Those ids
        are the links we need for `/odds` and `/h2h`.
        """
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            if not await self._goto_with_retry(
                page,
                self.aiscore_url,
                label="upcoming future listing",
            ):
                self._record_listing_source(
                    "future", status="failed", error="navigation failed"
                )
                return []
            try:
                await page.wait_for_function(
                    r"""
                    () => {
                        const state = (window.__NUXT__ && window.__NUXT__.state) || {};
                        const future = state.matchesFuture || {};
                        return Array.isArray(future.matches) && Array.isArray(future.teams);
                    }
                    """,
                    timeout=min(10000, self.page_timeout_ms),
                )
                await page.wait_for_timeout(400)
            except Exception as exc:
                logger.warning("Future Nuxt payload was not available: %s", exc)
                self._record_listing_source(
                    "future", status="failed", error="Nuxt payload unavailable"
                )
                return []

            try:
                rows = await page.evaluate(
                    r"""
                ({ daysAhead }) => {
                    const state = (window.__NUXT__ && window.__NUXT__.state) || {};
                    const future = state.matchesFuture || {};
                    const matches = Array.isArray(future.matches) ? future.matches : [];
                    const teams = Array.isArray(future.teams) ? future.teams : [];
                    const byId = new Map(teams.map(t => [t.id, t]));
                    const pad2 = n => String(n).padStart(2, '0');
                    const localStamp = ts => {
                        const d = new Date((Number(ts) || 0) * 1000);
                        if (!Number.isFinite(d.getTime())) return '';
                        return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
                    };
                    const todayDate = new Date();
                    const endDate = new Date();
                    endDate.setDate(endDate.getDate() + Number(daysAhead || 0));
                    const dateKey = d => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
                    const today = dateKey(todayDate);
                    const end = dateKey(endDate);
                    const rows = [];
                    for (const match of matches) {
                        const home = byId.get(match?.homeTeam?.id);
                        const away = byId.get(match?.awayTeam?.id);
                        if (!match?.id || !home?.slug || !away?.slug) continue;
                        const kickoff = localStamp(match.matchTime);
                        const day = kickoff.slice(0, 10);
                        if (!day || day < today || day > end) continue;
                        rows.push({
                            match_id: match.id,
                            url: `/basketball/match-${home.slug}-${away.slug}/${match.id}`,
                            match_name: `${home.name || home.shortName || home.slug} - ${away.name || away.shortName || away.slug}`,
                            home_team: home.name || home.shortName || '',
                            away_team: away.name || away.shortName || '',
                            kickoff,
                        });
                    }
                    return rows;
                }
                    """,
                    {"daysAhead": self.days_ahead},
                )
            except Exception as exc:
                logger.warning("Future listing parse failed: %s", exc)
                self._record_listing_source("future", status="failed", error=str(exc))
                return []
            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            links: list[str] = []
            seen: set[str] = set()
            for row in rows or []:
                href = row.get("url") if isinstance(row, dict) else None
                match_id = str(row.get("match_id") or "").strip() if isinstance(row, dict) else ""
                if not href:
                    continue
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if "/basketball/match-" not in cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                listing_row = {
                    **row,
                    "url": cleaned,
                    "listing_source": "future_nuxt",
                    "kickoff_source": "future_nuxt",
                }
                if match_id and match_id not in self._listing_rows_by_id:
                    self._listing_rows_by_id[match_id] = listing_row
                    self._listing_source_by_id[match_id] = "future_nuxt"
                links.append(cleaned)
            self._record_listing_source("future", status="ok", count=len(links))
            return links
        finally:
            if not page.is_closed():
                await page.close()

    async def _collect_today_match_links(self, context) -> list[str]:
        # AiScore's `/today-matches/basketball` view is a useful fallback. It
        # currently renders the real match link on the `VS` score anchor; the
        # surrounding row markup has changed over time, so row detection is
        # intentionally tolerant and does not depend on a specific class name.
        listing_url = "https://www.aiscore.com/today-matches/basketball"
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
                    const links = document.querySelectorAll('a[href*="/basketball/match-"]');
                    for (const a of links) {
                        const href = a.getAttribute('href');
                        if (!href || seen.has(href)) continue;
                        let row = a;
                        let hops = 0;
                        while (row && hops < 8) {
                            const rowText = text(row.innerText || '');
                            if (
                                rowText.length >= 20 &&
                                /\b\d{1,2}:\d{2}\b/.test(rowText) &&
                                /\bVS\b/i.test(rowText)
                            ) {
                                break;
                            }
                            row = row.parentElement;
                            hops++;
                        }
                        const rowText = row ? text(row.innerText || '') : text(a.innerText || '');

                        const isLive = /\b(Q[1-4]|[1-4]Q|HT|OT)\b/i.test(rowText)
                            || /Q[1-4]\s*-?\s*Ended/i.test(rowText);
                        const isFinished = /\bFT\b|\bEnded\b|\bFinished\b/i.test(rowText);
                        const hasKickoff = /\b\d{1,2}:\d{2}\b/.test(rowText);
                        const isUpcoming = /\bVS\b/i.test(rowText) || /^VS$/i.test(text(a.innerText || ''));
                        if (isLive || isFinished || !hasKickoff || !isUpcoming) continue;

                        seen.add(href);
                        const kickoffMatch = rowText.match(/\b\d{1,2}:\d{2}\b/);
                        out.push({href, kickoff: kickoffMatch ? kickoffMatch[0] : ''});
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
                    "kickoff": str(listing_row.get("kickoff") or ""),
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
            return await asyncio.wait_for(
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
                "opening_total", "prematch_total", "url",
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
            if odds_data.get("opening") is None and listing_data.get("opening_total") is not None:
                odds_data["opening"] = listing_data.get("opening_total")
            if odds_data.get("prematch") is None and listing_data.get("prematch_total") is not None:
                odds_data["prematch"] = listing_data.get("prematch_total")
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
                        odds_data.update(
                            {
                                key: value
                                for key, value in retry_odds.items()
                                if value not in (None, "")
                            }
                        )
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
            if opening is None and prematch is None:
                logger.info("No verified opening/prematch total is available: %s", link)
                # Still keep the row — odds may not be open yet — but flag it.

            h2h_text = await self._read_h2h_page(page, link)
            data_warnings: list[str] = []
            if not detail_page_loaded:
                data_warnings.append("odds_detail_unavailable_listing_fallback")
            if not h2h_text:
                data_warnings.append("h2h_unavailable")
            match_name = odds_data.get("match_name") or ""
            home_team, away_team = _split_match_name(match_name)

            metrics = {}
            if h2h_text:
                try:
                    metrics = extract_h2h_metrics(h2h_text, match_name) or {}
                except Exception as exc:
                    logger.warning("H2H metrics failed for %s: %s", link, exc)
                    metrics = {}

            home_last6 = metrics.get("home_last6") or {}
            away_last6 = metrics.get("away_last6") or {}
            expected_total = metrics.get("expected_total")
            h2h_avg_total = metrics.get("h2h_avg_total")
            h2h_games = metrics.get("h2h_games")

            return {
                "match_id": match_id,
                "match_name": match_name or f"Match {match_id}",
                "home_team": odds_data.get("home_team") or home_team,
                "away_team": odds_data.get("away_team") or away_team,
                "tournament": odds_data.get("tournament") or "",
                "kickoff": kickoff,
                "listing_source": listing_source,
                "kickoff_source": kickoff_source,
                "odds_source": odds_data.get("odds_source") or listing_data.get("odds_source") or "",
                "opening_total": opening,
                "prematch_total": prematch,
                "url": link,
                "home_last6": home_last6,
                "away_last6": away_last6,
                "expected_total": expected_total,
                "h2h_avg_total": h2h_avg_total,
                "h2h_games": h2h_games,
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

        # Try clicking a Total/O-U tab only when it looks like an actual compact
        # tab/control. The odds table itself also contains a "Total Points"
        # header; clicking that cell can make AiScore's DOM flaky.
        try:
            await page.evaluate(
                r"""
                () => {
                    const text = s => (s || '').replace(/\s+/g, ' ').trim();
                    const tabs = Array.from(document.querySelectorAll('*')).filter(el => {
                        const t = text(el.innerText || '').toLowerCase();
                        const cls = (el.className || '').toString().toLowerCase();
                        const role = (el.getAttribute && (el.getAttribute('role') || '').toLowerCase()) || '';
                        return t.length < 30 && el.children.length <= 3
                            && (role === 'tab' || /tab|market|filter|switch|select/.test(cls))
                            && (/\btotal\b|\bo\/u\b|\bover.*under\b|\büst.*alt\b|\bou\b/i.test(t));
                    });
                    for (const tab of tabs) {
                        tab.click();
                        return;
                    }
                }
                """
            )
            await page.wait_for_timeout(500)
        except Exception as exc:
            logger.info("Total-market tab interaction failed (%s): %s", url, exc)

        return await page.evaluate(
            r"""
            () => {
                const text = s => (s || '').replace(/\s+/g, ' ').trim();

                const findLine = (txt) => {
                    const nums = (txt || '').match(/\d+\.?\d*/g);
                    if (!nums) return null;
                    for (const n of nums) {
                        const v = parseFloat(n);
                        if (v >= 100 && v <= 400) return v;
                    }
                    return null;
                };
                const isLocked = (el) => {
                    const html = (el.innerHTML || '').toLowerCase();
                    const txt = text(el.innerText || '').toLowerCase();
                    if (html.includes('lock') || html.includes('🔒')) return true;
                    if (txt === '-' || txt === '--' || txt === '—') return true;
                    if (/suspend|locked|unavail/i.test(txt)) return true;
                    return false;
                };

                let opening = null;
                let prematch = null;
                let inplay = null;

                const container = document.querySelector('.newOdds')
                                || document.querySelector('[class*="newOdds"]')
                                || document.querySelector('[class*="oddsContent"]');
                const selectedMarketText = Array.from(document.querySelectorAll(
                    '[role="tab"][aria-selected="true"], [class*="tab"][class*="active"], [class*="market"][class*="active"]'
                )).map(el => text(el.innerText || '')).join(' ');
                const marketEvidence = `${selectedMarketText} ${text((container && container.innerText) || '').slice(0, 500)}`;
                const marketVerified = /\btotal(?:\s+points?)?\b|\bo\s*\/\s*u\b|over\s*[/&-]?\s*under|üst\s*[/&-]?\s*alt|\bou\b/i.test(marketEvidence);

                if (container && marketVerified) {
                    const openingEls = container.querySelectorAll('[class*="openingBg"]');
                    for (const el of openingEls) {
                        if (isLocked(el)) continue;
                        const v = findLine(text(el.innerText));
                        if (v !== null) { opening = v; break; }
                    }
                    const inPlayEls = container.querySelectorAll('[class*="inPlayBg"]');
                    for (const el of inPlayEls) {
                        if (isLocked(el)) continue;
                        const v = findLine(text(el.innerText));
                        if (v !== null) { inplay = v; break; }
                    }
                    const contentDivs = container.querySelectorAll('.content');
                    for (const content of contentDivs) {
                        if (isLocked(content)) continue;
                        const allRows = Array.from(content.children).filter(el => !isLocked(el));
                        const preBgRows = allRows.filter(el => {
                            const cls = (el.className || '').toString();
                            return !cls.includes('openingBg') && !cls.includes('inPlayBg');
                        });
                        for (const el of preBgRows) {
                            const v = findLine(text(el.innerText));
                            if (v !== null) { prematch = v; break; }
                        }
                        if (prematch !== null) break;
                    }
                    // Positional fallback: if a bookmaker has 3 rows, the order is
                    // opening / pre-match / in-play.
                    if (opening === null || prematch === null) {
                        for (const content of contentDivs) {
                            if (isLocked(content)) continue;
                            const rows = Array.from(content.children).filter(el => {
                                return !isLocked(el) && findLine(text(el.innerText)) !== null;
                            });
                            if (rows.length >= 2) {
                                if (opening === null) opening = findLine(text(rows[0].innerText));
                                if (prematch === null && rows.length >= 2) {
                                    prematch = findLine(text(rows[1].innerText));
                                }
                                if (inplay === null && rows.length >= 3) {
                                    inplay = findLine(text(rows[2].innerText));
                                }
                            }
                            if (opening !== null && prematch !== null) break;
                        }
                    }

                    // Last-resort text fallback for AiScore layouts where the
                    // class names are present but the odds cells are nested in
                    // an unexpected way. The Total Points column lists opening
                    // and pre-match totals in order.
                    if (opening === null || prematch === null) {
                        const oddsText = text(container.innerText || '');
                        const totalIdx = oddsText.toLowerCase().indexOf('total points');
                        if (totalIdx >= 0) {
                            const scope = oddsText.slice(totalIdx);
                            const totals = (scope.match(/\b\d{3}(?:\.\d)?\b/g) || [])
                                .map(n => parseFloat(n))
                                .filter(v => v >= 100 && v <= 400);
                            if (opening === null && totals.length >= 1) opening = totals[0];
                            if (prematch === null && totals.length >= 2) prematch = totals[1];
                        }
                    }
                }

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
                    opening, prematch, inplay,
                    match_name: matchName,
                    tournament,
                    kickoff,
                    kickoff_source: kickoff && /^\d{4}-\d{2}-\d{2}/.test(kickoff)
                        ? 'detail_header'
                        : (kickoff ? 'detail_header_time' : ''),
                    odds_source: marketVerified && (opening !== null || prematch !== null)
                        ? 'detail_total_market'
                        : '',
                    market_verified: marketVerified,
                    is_live: isLive,
                    is_finished: isFinished,
                };
            }
            """
        )

    async def _read_h2h_page(self, page, url: str) -> str:
        h2h_url = url.rstrip("/") + "/h2h"
        try:
            if not await self._goto_with_retry(page, h2h_url, label="upcoming H2H detail"):
                return ""
            try:
                await page.wait_for_selector(
                    "body",
                    timeout=min(5000, self.page_timeout_ms),
                )
            except Exception as exc:
                logger.info("H2H body readiness wait failed (%s): %s", url, exc)
            try:
                await page.evaluate(
                    r"""
                    () => {
                        const text = s => (s || '').replace(/\s+/g, ' ').trim();
                        const candidates = Array.from(document.querySelectorAll('a, button, div, span'))
                            .filter(el => el.children.length <= 3);
                        for (const el of candidates) {
                            const t = text(el.innerText || '').toLowerCase();
                            if (!t || t.length > 18) continue;
                            if (/^h2h\b|head.?to.?head|karş.?la.?ma/.test(t)) {
                                el.click();
                                return;
                            }
                        }
                    }
                    """
                )
            except Exception as exc:
                logger.info("H2H tab interaction failed (%s): %s", url, exc)
            await page.wait_for_timeout(400)
            for _ in range(2):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 3)")
                await page.wait_for_timeout(250)
            return await page.evaluate(
                r"""
                () => (document.body.innerText || '').replace(/\s+/g, ' ').trim()
                """
            )
        except Exception as exc:
            logger.warning("H2H read failed (%s): %s", url, exc)
            return ""

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
        """Keep AiScore upcoming rows scoped to Today's Upcoming Matches by default.

        The list pages sometimes expose only a time like "17:30"; keep those
        candidates because they come from the today-matches page. Detail pages
        normally normalize to "YYYY-MM-DD HH:MM", which lets us drop stale rows.
        """
        text = str(kickoff or "").strip()
        if not text:
            return True
        match = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
        if not match:
            return bool(re.fullmatch(r"\d{1,2}:\d{2}", text))
        try:
            kickoff_date = datetime.strptime(match.group(0), "%Y-%m-%d").date()
        except ValueError:
            return False
        today = self._today()
        return today <= kickoff_date <= today + timedelta(days=self.days_ahead)

    def _today(self) -> date:
        try:
            return datetime.now(ZoneInfo(self.timezone_id)).date()
        except ZoneInfoNotFoundError:
            return date.today()

    def _dated_aiscore_url(self) -> str:
        base = re.sub(r"/\d{8}/?$", "", self.aiscore_url.rstrip("/"))
        return f"{base}/{self._today().strftime('%Y%m%d')}"
