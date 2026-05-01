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
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from signal_analysis import _extract_h2h_metrics, _split_match_name

logger = logging.getLogger(__name__)


class UpcomingScraper:
    def __init__(
        self,
        aiscore_url: str = "https://www.aiscore.com/basketball",
        page_timeout_ms: int = 35000,
        max_matches: int | None = None,
    ):
        self.aiscore_url = aiscore_url
        self.page_timeout_ms = page_timeout_ms
        self.max_matches = max_matches

    async def fetch(self) -> list[dict]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await self._new_context(browser)
            try:
                links = await self._collect_upcoming_links(context)
                if not links:
                    logger.warning("No upcoming match links collected.")
                    return []

                if self.max_matches:
                    links = links[: self.max_matches]

                logger.info("Fetching details for %s upcoming matches.", len(links))
                concurrency = max(1, int(os.getenv("UPCOMING_CONCURRENCY", "2")))
                results: list[dict] = []
                for i in range(0, len(links), concurrency):
                    batch = links[i : i + concurrency]
                    coros = [self._extract_one(context, link) for link in batch]
                    chunk = await asyncio.gather(*coros, return_exceptions=True)
                    for r in chunk:
                        if isinstance(r, dict):
                            results.append(r)
                        elif isinstance(r, Exception):
                            logger.debug("Upcoming match parse error: %s", r)
                return results
            finally:
                await context.close()
                await browser.close()

    # ── Browser plumbing ──────────────────────────────────────────────

    async def _new_context(self, browser):
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        return context

    # ── Listing: collect upcoming match links from AiScore ─────────────

    async def _collect_upcoming_links(self, context) -> list[str]:
        links = await self._collect_homepage_future_links(context)
        if links:
            logger.info("Collected %s homepage future match links.", len(links))
            return links

        logger.info("Homepage future list was empty; falling back to today-matches.")
        links = await self._collect_today_match_links(context)
        logger.info("Collected %s today-matches upcoming links.", len(links))
        return links

    async def _collect_homepage_future_links(self, context) -> list[str]:
        """Read the homepage 'future/upcoming' payload and build match URLs.

        The visible homepage footer may render these as head-to-head SEO links,
        but Nuxt state includes the real match ids plus team slugs. Those ids
        are the links we need for `/odds` and `/h2h`.
        """
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            try:
                await page.goto(self.aiscore_url, wait_until="domcontentloaded")
            except Exception as exc:
                logger.debug("Homepage nav failed (%s): %s", self.aiscore_url, exc)
                return []
            await page.wait_for_timeout(3000)

            raw_links = await page.evaluate(
                r"""
                () => {
                    const state = (window.__NUXT__ && window.__NUXT__.state) || {};
                    const future = state.matchesFuture || {};
                    const matches = Array.isArray(future.matches) ? future.matches : [];
                    const teams = Array.isArray(future.teams) ? future.teams : [];
                    const byId = new Map(teams.map(t => [t.id, t]));
                    const links = [];
                    for (const match of matches) {
                        const home = byId.get(match?.homeTeam?.id);
                        const away = byId.get(match?.awayTeam?.id);
                        if (!match?.id || !home?.slug || !away?.slug) continue;
                        links.push(`/basketball/match-${home.slug}-${away.slug}/${match.id}`);
                    }
                    return links;
                }
                """
            )
            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            links: list[str] = []
            seen: set[str] = set()
            for href in raw_links or []:
                if not href:
                    continue
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if "/basketball/match-" not in cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                links.append(cleaned)
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
            try:
                await page.goto(listing_url, wait_until="networkidle")
            except Exception:
                await page.goto(listing_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Trigger lazy lists by scrolling once.
            for _ in range(4):
                await page.evaluate("window.scrollBy(0, 1200)")
                await page.wait_for_timeout(500)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(400)

            hrefs = await page.evaluate(
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
                        out.push(href);
                    }
                    return out;
                }
                """
            )
            logger.info(
                "today-matches listing produced %s upcoming candidate hrefs.",
                len(hrefs),
            )

            suffix_re = re.compile(r"/(h2h|odds|stats|lineups|standings|summary)/?$")
            links: list[str] = []
            seen: set[str] = set()
            for href in hrefs or []:
                if not href:
                    continue
                cleaned = suffix_re.sub("", urljoin(page.url, href))
                if "/basketball/match-" not in cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                links.append(cleaned)
            logger.info("Collected %s candidate upcoming links.", len(links))
            return links
        finally:
            if not page.is_closed():
                await page.close()

    # ── Match detail extraction ───────────────────────────────────────

    async def _extract_one(self, context, link: str) -> dict | None:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            odds_data = await self._read_odds_page(page, link)
            if not odds_data:
                return None
            if odds_data.get("is_live") or odds_data.get("is_finished"):
                logger.debug("Skipping (not upcoming): %s", link)
                return None

            opening = odds_data.get("opening")
            prematch = odds_data.get("prematch")
            retry_count = 0
            while opening is None and prematch is None and retry_count < 1:
                retry_count += 1
                retry_page = await context.new_page()
                retry_page.set_default_timeout(self.page_timeout_ms)
                try:
                    retry_odds = await self._read_odds_page(retry_page, link)
                    if retry_odds and (
                        retry_odds.get("opening") is not None
                        or retry_odds.get("prematch") is not None
                    ):
                        odds_data = {**odds_data, **retry_odds}
                        opening = odds_data.get("opening")
                        prematch = odds_data.get("prematch")
                        break
                finally:
                    if not retry_page.is_closed():
                        await retry_page.close()
            if opening is None and prematch is None:
                logger.debug("No opening/prematch totals available: %s", link)
                # Still keep the row — odds may not be open yet — but flag it.

            h2h_text = await self._read_h2h_page(page, link)
            match_name = odds_data.get("match_name") or ""
            home_team, away_team = _split_match_name(match_name)

            metrics = {}
            if h2h_text:
                try:
                    metrics = _extract_h2h_metrics(h2h_text, match_name) or {}
                except Exception as exc:
                    logger.debug("H2H metrics failed for %s: %s", link, exc)
                    metrics = {}

            home_last6 = metrics.get("home_last6") or {}
            away_last6 = metrics.get("away_last6") or {}
            expected_total = metrics.get("expected_total")

            match_id = self._extract_match_id(link)
            return {
                "match_id": match_id,
                "match_name": match_name or f"Match {match_id}",
                "home_team": home_team,
                "away_team": away_team,
                "tournament": odds_data.get("tournament") or "",
                "kickoff": odds_data.get("kickoff") or "",
                "opening_total": opening,
                "prematch_total": prematch,
                "url": link,
                "home_last6": home_last6,
                "away_last6": away_last6,
                "expected_total": expected_total,
            }
        except Exception as exc:
            logger.debug("Detail extract failed (%s): %s", link, exc)
            return None
        finally:
            if not page.is_closed():
                await page.close()

    async def _read_odds_page(self, page, url: str) -> dict | None:
        clean = url.rstrip("/")
        odds_url = clean if clean.endswith("/odds") else clean + "/odds"
        try:
            await page.goto(odds_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            try:
                await page.wait_for_function(
                    r"""
                    () => {
                        const el = document.querySelector('.newOdds, [class*="newOdds"], [class*="oddsContent"]');
                        return !!el && /\b\d{3}(?:\.\d)?\b/.test((el.innerText || '').replace(/\s+/g, ' '));
                    }
                    """,
                    timeout=min(3500, self.page_timeout_ms),
                )
            except Exception:
                pass
        except Exception as exc:
            logger.debug("Odds page nav failed (%s): %s", url, exc)
            return None

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
                        try { tab.click(); return; } catch (e) {}
                    }
                }
                """
            )
            await page.wait_for_timeout(1500)
        except Exception:
            pass

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

                if (container) {
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
                        const scope = totalIdx >= 0 ? oddsText.slice(totalIdx) : oddsText;
                        const totals = (scope.match(/\b\d{3}(?:\.\d)?\b/g) || [])
                            .map(n => parseFloat(n))
                            .filter(v => v >= 100 && v <= 400);
                        if (opening === null && totals.length >= 1) opening = totals[0];
                        if (prematch === null && totals.length >= 2) prematch = totals[1];
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
                const breadcrumbRoots = Array.from(document.querySelectorAll(
                    '.breadcrumb, [class*="breadcrumb"], [class*="matchTop"], [class*="matchInfo"], [class*="league"], [class*="competition"]'
                ));
                const scopedAnchors = breadcrumbRoots.flatMap(root => Array.from(root.querySelectorAll('a')));
                const anchorSource = scopedAnchors.length ? scopedAnchors : Array.from(document.querySelectorAll('a'));
                const breadcrumbs = anchorSource
                    .map(e => ({ text: text(e.innerText), href: e.getAttribute('href') || '' }))
                    .filter(e => e.text && !/schedule|standings|teams|stats|live score/i.test(e.text))
                    .filter(e => e.href.includes('/tournament-'));
                if (breadcrumbs.length >= 2) {
                    country = breadcrumbs[0].text;
                    tournament = breadcrumbs[breadcrumbs.length - 1].text;
                } else if (breadcrumbs.length === 1) {
                    tournament = breadcrumbs[0].text;
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
                const bodyText = (document.body.innerText || '').replace(/\s+/g, ' ');
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

                const kickoffEl = Array.from(document.querySelectorAll('span, div'))
                    .map(e => text(e.innerText || ''))
                    .find(v => normalizeIsoKickoff(v) || normalizeEnglishKickoff(v));
                if (kickoffEl) kickoff = normalizeIsoKickoff(kickoffEl) || normalizeEnglishKickoff(kickoffEl);
                if (!kickoff) {
                    kickoff = normalizeIsoKickoff(bodyText) || normalizeEnglishKickoff(bodyText);
                }
                if (!kickoff) {
                    const timeOnly = Array.from(document.querySelectorAll('span, div'))
                        .map(e => text(e.innerText || ''))
                        .find(v => /^\d{1,2}:\d{2}$/.test(v));
                    if (timeOnly) kickoff = timeOnly;
                }

                return {
                    opening, prematch, inplay,
                    match_name: matchName,
                    tournament,
                    kickoff,
                    is_live: isLive,
                    is_finished: isFinished,
                };
            }
            """
        )

    async def _read_h2h_page(self, page, url: str) -> str:
        h2h_url = url.rstrip("/") + "/h2h"
        try:
            await page.goto(h2h_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
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
                                try { el.click(); return; } catch (e) {}
                            }
                        }
                    }
                    """
                )
            except Exception:
                pass
            await page.wait_for_timeout(900)
            for _ in range(2):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 3)")
                await page.wait_for_timeout(600)
            return await page.evaluate(
                r"""
                () => (document.body.innerText || '').replace(/\s+/g, ' ').trim()
                """
            )
        except Exception as exc:
            logger.debug("H2H read failed (%s): %s", url, exc)
            return ""

    @staticmethod
    def _extract_match_id(url: str) -> str:
        cleaned = re.sub(
            r"/(h2h|odds|stats|lineups|standings|summary)/?$", "", url.rstrip("/")
        )
        parts = cleaned.split("/")
        return parts[-1] if parts else url
