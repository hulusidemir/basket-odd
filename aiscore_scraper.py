import asyncio
import logging
import os
import re
from playwright.async_api import Error as PlaywrightError
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)


class AiscoreScraper:
    def __init__(self, aiscore_url: str, max_matches_per_cycle: int = 40, page_timeout_ms: int = 30000, skip_h2h: bool = False):
        self.aiscore_url = aiscore_url
        self.max_matches_per_cycle = max_matches_per_cycle
        self.page_timeout_ms = page_timeout_ms
        self.skip_h2h = skip_h2h
        # Per-match H2H body cache. Last-6 / SF data does not change during a
        # match, so re-scraping the H2H tab on every poll cycle is wasted work.
        self._h2h_cache: dict[str, str] = {}

    def forget_h2h_cache(self, match_id: str) -> None:
        self._h2h_cache.pop(str(match_id), None)

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
        browser = await playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
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

    # ── Ana tarama ────────────────────────────────────────────────────

    async def get_live_basketball_totals(self) -> list[dict]:
        async with async_playwright() as p:
            browser, context = await self._create_browser_context(p)

            list_page = await context.new_page()
            list_page.set_default_timeout(self.page_timeout_ms)
            try:
                links = []
                for attempt in range(1, 4):
                    await list_page.goto(self.aiscore_url, wait_until="domcontentloaded")
                    await list_page.wait_for_timeout(10 * 1000)
                    links = await self._collect_match_links(list_page)
                    if links:
                        break
                    if attempt < 3:
                        logger.warning("AIScore listing returned 0 links; retrying list load (%s/3).", attempt + 1)

                if not links:
                    page_title = await list_page.title()
                    page_url = list_page.url
                    body_len = await list_page.evaluate("document.body?.innerText?.length || 0")
                    logger.warning(
                        "No match links found on AIScore listing. "
                        "title=%s, url=%s, body_len=%s",
                        page_title, page_url, body_len,
                    )
                    try:
                        await list_page.screenshot(path="debug_aiscore.png", full_page=False)
                        logger.info("Debug screenshot: debug_aiscore.png")
                    except Exception:
                        pass
                    return []

                logger.info("Found %s match links on AIScore.", len(links))
                out = []

                await list_page.close()
                await context.close()
                context = await self._new_desktop_context(browser)

                concurrent_tabs = max(1, int(os.getenv("AISCORE_CONCURRENCY", "1")))
                batch_links = links[: self.max_matches_per_cycle]

                for i in range(0, len(batch_links), concurrent_tabs):
                    batch = batch_links[i : i + concurrent_tabs]
                    tasks = [self._extract_single(context, link) for link in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, dict):
                            out.append(r)
                        elif isinstance(r, Exception):
                            logger.debug("Parallel match error: %s", r)

                return out
            finally:
                if not list_page.is_closed():
                    await list_page.close()
                await context.close()
                await self._close_browser(browser)

    async def _extract_single(self, context, link: str) -> dict | None:
        """Open a single match in a new tab, read data, and close.
        Retries up to 2 times if odds are temporarily locked/unavailable."""
        detail = await context.new_page()
        detail.set_default_timeout(self.page_timeout_ms)
        max_retries = 2
        try:
            logger.debug("Checking match link: %s", link)
            for attempt in range(1, max_retries + 1):
                logger.debug("Attempt %s/%s for %s", attempt, max_retries, link)
                row = await self._extract_match(detail, link)
                if row is not None:
                    return row
                if attempt < max_retries:
                    logger.debug(
                        "Retry %s/%s for %s (odds possibly locked/loading)",
                        attempt, max_retries - 1, link,
                    )
                    await detail.wait_for_timeout(3000)
                    try:
                        await detail.reload(wait_until="domcontentloaded")
                        await detail.wait_for_timeout(3000)
                    except Exception:
                        pass
            logger.debug("All %s attempts failed for %s", max_retries, link)
            return None
        except Exception as exc:
            logger.debug("Could not read match (%s): %s", link, exc)
            return None
        finally:
            await detail.close()

    async def _collect_match_links(self, page) -> list[str]:
        """
        Navigate to AIScore basketball live page and collect live match links.
        Uses multiple strategies to find and activate the Live tab.
        """
        await page.wait_for_timeout(3000)

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
            let tabText = '';
            let found = false;
            let clicked = false;

            if (tab) {
                found = true;
                tabText = text(tab.innerText);
                const m = tabText.match(/\((\d+)\)/);
                if (m) count = parseInt(m[1], 10);

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

            return { found, tabText, count, clicked };
        }""")

        live_found = live_info.get("found", False)
        live_count = live_info.get("count", 0)
        tab_text = live_info.get("tabText", "")
        tab_clicked = live_info.get("clicked", False)

        if live_found and live_count > 0:
            live_max = live_count
            logger.info("Live tab found: '%s' (%s matches), clicked=%s.", tab_text, live_max, tab_clicked)
        elif live_found:
            live_max = 50
            logger.info("Live tab found: '%s' (no count), clicked=%s. Using max=%s.", tab_text, tab_clicked, live_max)
        else:
            live_max = 30
            logger.warning("Could not find Live tab! Using max=%s.", live_max)

        await page.wait_for_timeout(4000)

        # ── Step 2: Collect links — only from rows with a live time indicator ──
        all_hrefs: set[str] = set()
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
                        if (row.parentElement) row = row.parentElement;
                        else break;
                    }
                    const rowText = (row.innerText || '').replace(/\s+/g, ' ');
                    const hasLiveTime = /\b(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\b/i.test(rowText)
                                     || /\b\d{1,2}[-:]\d{2}[:-]\d{2}\b/.test(rowText);
                    const timeEl = row.querySelector('[class*="live"], [class*="Live"], [style*="color: red"], [style*="color:#"]');
                    if (hasLiveTime || timeEl) {
                        const href = a.getAttribute('href');
                        if (href) liveHrefs.push(href);
                    }
                }
                return liveHrefs;
            }""")
            all_hrefs.update(hrefs)

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
            await page.wait_for_timeout(800)

        if not all_hrefs:
            logger.warning("Live-only filter found 0 links. Falling back to all match links on page.")
            fallback = await page.evaluate(r"""() => {
                return Array.from(document.querySelectorAll('a[href*="/basketball/match-"]'))
                    .map(a => a.getAttribute('href'))
                    .filter(Boolean);
            }""")
            all_hrefs.update(fallback)

        links = [urljoin(page.url, href) for href in all_hrefs]
        links = [u for u in links if "/basketball/match-" in u]
        _suffixes = re.compile(r'/(h2h|odds|stats|lineups|standings|summary)/?$')
        links = [_suffixes.sub('', u) for u in links]
        links = sorted(set(links))[:live_max]

        logger.info("Collected %s live match links (live_max=%s).", len(links), live_max)
        return links

    async def _extract_match(self, page, url: str) -> dict | None:
        clean_url = url.rstrip("/")
        odds_url = clean_url if clean_url.endswith("/odds") else clean_url + "/odds"
        await page.goto(odds_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        try:
            clicked_total = await page.evaluate(r"""
                () => {
                    const text = s => (s || '').replace(/\s+/g, ' ').trim();
                    const tabs = Array.from(document.querySelectorAll('*')).filter(el => {
                        const t = text(el.innerText).toLowerCase();
                        return t.length < 30 && el.children.length <= 3
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
                await page.wait_for_timeout(2000)
        except Exception:
            pass

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
                const uniqueRounded = arr => {
                  const out = [];
                  const seen = new Set();
                  for (const v of arr) {
                    if (!Number.isFinite(v) || v < 100 || v > 400) continue;
                    const key = v.toFixed(1);
                    if (seen.has(key)) continue;
                    seen.add(key);
                    out.push(parseFloat(key));
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
                  if (isLocked(content)) continue;
                  const rows = Array.from(content.children).filter(el => !isLocked(el));
                  const lineRows = rows
                    .map(el => ({ el, value: findLine(text(el.innerText)), cls: (el.className || '').toString() }))
                    .filter(item => item.value !== null);
                  if (lineRows.length >= 3) {
                    openingLines.push(lineRows[0].value);
                    prematchLines.push(lineRows[1].value);
                    inplayLines.push(lineRows[2].value);
                  } else {
                    for (const item of lineRows) {
                      if (item.cls.includes('openingBg')) openingLines.push(item.value);
                      else if (item.cls.includes('inPlayBg')) inplayLines.push(item.value);
                      else prematchLines.push(item.value);
                    }
                  }
                }
                const openingUnique = uniqueRounded(openingLines);
                const prematchUnique = uniqueRounded(prematchLines);
                const inplayUnique = uniqueRounded(inplayLines);
                oddsSnapshot = {
                  opening_lines: openingUnique,
                  prematch_lines: prematchUnique,
                  inplay_lines: inplayUnique,
                  opening_median: median(openingUnique),
                  prematch_median: median(prematchUnique),
                  inplay_median: median(inplayUnique),
                  opening_min: openingUnique.length ? Math.min(...openingUnique) : null,
                  opening_max: openingUnique.length ? Math.max(...openingUnique) : null,
                  inplay_min: inplayUnique.length ? Math.min(...inplayUnique) : null,
                  inplay_max: inplayUnique.length ? Math.max(...inplayUnique) : null,
                  bookmaker_count: Math.max(openingUnique.length, inplayUnique.length),
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
                for (const el of allRows) {
                  if (isLocked(el)) { hasLockedRows = true; break; }
                }
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
              oddsSnapshot.bookmaker_count = Math.max(
                oddsSnapshot.bookmaker_count || 0,
                oddsSnapshot.opening_lines.length,
                oddsSnapshot.inplay_lines.length
              );

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
                  for (const row of contentRows) {
                    const homeRow = parseQuarterRow(row, scoreHome);
                    const awayRow = parseQuarterRow(row, scoreAway);
                    if (homeRow && !homePeriods) homePeriods = homeRow;
                    if (awayRow && !awayPeriods && row !== contentRows.find(r => parseQuarterRow(r, scoreHome))) awayPeriods = awayRow;
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

              const overviewStats = { rows: [], totals: {}, quality: 0, source: 'overview_scoreboard' };
              const statLabels = Array.from(document.querySelectorAll('.midDes_text, [class*="midDes"]'));
              const seenStats = new Set();
              for (const labelEl of statLabels) {
                const label = text(labelEl.innerText);
                if (!label || label.length > 28) continue;
                let row = labelEl;
                let rowText = '';
                for (let i = 0; i < 5; i++) {
                  rowText = text(row.innerText);
                  const labelRe = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+');
                  const re = new RegExp('(\\d{1,3})\\s+' + labelRe + '\\s+(\\d{1,3})', 'i');
                  const m = rowText.match(re);
                  if (m) {
                    const home = parseFloat(m[1]);
                    const away = parseFloat(m[2]);
                    const key = label.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
                    if (!seenStats.has(key)) {
                      seenStats.add(key);
                      overviewStats.rows.push({ label, home: { raw: m[1], value: home }, away: { raw: m[2], value: away } });
                      overviewStats.totals[key] = home + away;
                    }
                    break;
                  }
                  if (!row.parentElement) break;
                  row = row.parentElement;
                }
              }
              overviewStats.quality = overviewStats.rows.length >= 3 ? 55 : (overviewStats.rows.length ? 30 : 0);

              return {
                opening, prematch, inplay, matchName, tournament, status,
                isQ4, remainingMinutes, hasLockedRows, isFinished, score,
                quarterScores, oddsSnapshot, overviewStats
              };
            }
            """
        )

        is_finished = parsed.get("isFinished", False)
        status = parsed.get("status", "")
        if is_finished:
            logger.debug("Skipping finished match: %s (status=%s)", url, status)
            return None

        opening = parsed.get("opening")
        inplay = parsed.get("inplay")
        prematch = parsed.get("prematch")
        locked = parsed.get("hasLockedRows", False)
        if opening is None or inplay is None:
            if locked:
                logger.debug("Odds locked/suspended for %s (bookmaker updating)", url)
            else:
                logger.debug("Could not find opening/inplay totals for %s", url)
            return None

        is_q4 = parsed.get("isQ4", False)
        remaining = parsed.get("remainingMinutes")
        if not self.skip_h2h and is_q4 and remaining is not None and remaining <= 4.0:
            logger.debug("Q4 <=4min remaining, skipping: %s (%.1f min)", url, remaining)
            return None

        overview_data = await self._fetch_overview_data(page, url)
        if overview_data.get("status"):
            parsed["status"] = overview_data.get("status")
        if overview_data.get("score"):
            parsed["score"] = overview_data.get("score")
        quarter_scores = (
            overview_data.get("quarterScores")
            if (overview_data.get("quarterScores") or {}).get("home")
            else parsed.get("quarterScores")
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
                    self._h2h_cache[match_id] = h2h_body

        tournament = self._sanitize_tournament(parsed.get("tournament") or "", url)

        return {
            "match_id": match_id,
            "match_name": parsed.get("matchName") or f"Match {match_id}",
            "tournament": tournament or "Unknown",
            "status": parsed.get("status"),
            "opening_total": float(opening),
            "prematch_total": float(prematch) if prematch is not None else None,
            "inplay_total": float(inplay),
            "url": url,
            "score": parsed.get("score") or "",
            "market_locked": bool(parsed.get("hasLockedRows", False)),
            "has_prematch": prematch is not None,
            "h2h_body_text": h2h_body,
            "quarter_scores": quarter_scores,
            "odds_snapshot": parsed.get("oddsSnapshot") or {},
        }

    async def _fetch_overview_data(self, page, url: str) -> dict:
        try:
            await page.goto(url.rstrip("/"), wait_until="domcontentloaded")
            await page.wait_for_timeout(1800)
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
                      for (const row of rows) {
                        if (!homePeriods) homePeriods = parseQuarterRow(row, scoreHome);
                        if (!awayPeriods) awayPeriods = parseQuarterRow(row, scoreAway);
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

                  const overviewStats = { rows: [], totals: {}, quality: 0, source: 'overview_scoreboard' };
                  const statLabels = Array.from(document.querySelectorAll('.midDes_text, [class*="midDes"]'));
                  const seenStats = new Set();
                  for (const labelEl of statLabels) {
                    const label = text(labelEl.innerText);
                    if (!label || label.length > 28) continue;
                    let row = labelEl;
                    for (let i = 0; i < 6; i++) {
                      const rowText = text(row.innerText);
                      const labelRe = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+');
                      const re = new RegExp('(\\d{1,3})\\s+' + labelRe + '\\s+(\\d{1,3})', 'i');
                      const m = rowText.match(re);
                      if (m) {
                        const home = parseFloat(m[1]);
                        const away = parseFloat(m[2]);
                        const key = label.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
                        if (!seenStats.has(key)) {
                          seenStats.add(key);
                          overviewStats.rows.push({ label, home: { raw: m[1], value: home }, away: { raw: m[2], value: away } });
                          overviewStats.totals[key] = home + away;
                        }
                        break;
                      }
                      if (!row.parentElement) break;
                      row = row.parentElement;
                    }
                  }
                  overviewStats.quality = overviewStats.rows.length >= 3 ? 55 : (overviewStats.rows.length ? 30 : 0);

                  return { status, score, quarterScores, overviewStats };
                }
            """)
        except Exception as exc:
            logger.debug("Overview data fetch failed for %s: %s", url, exc)
            return {}

    async def _fetch_h2h_body(self, page, url: str) -> str:
        h2h_url = url.rstrip("/") + "/h2h"
        try:
            await page.goto(h2h_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)

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
            except Exception:
                pass
            await page.wait_for_timeout(1000)

            # Progressive scroll to trigger lazy-loaded last-5 / h2h tables
            for _ in range(2):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 3)")
                await page.wait_for_timeout(700)

            body = await page.evaluate(r"""
                () => {
                    const keyHits = (txt) => {
                        const t = (txt || '').toLowerCase();
                        let score = 0;
                        if (/per game/.test(t)) score += 2;
                        if (/points per match/.test(t)) score += 2;
                        if (/opponent points/.test(t)) score += 2;
                        if (/\bh2h\b/.test(t)) score += 1;
                        if (/total points over%/.test(t)) score += 2;
                        if (/last\s*5/.test(t)) score += 1;
                        return score;
                    };
                    const fullBody = (document.body.innerText || '').replace(/\s+/g, ' ').trim();
                    const candidates = Array.from(document.querySelectorAll(
                        'main, [class*="matchDetail"], [class*="match-detail"], ' +
                        '[class*="h2h"], [class*="H2H"], [class*="head-to-head"], ' +
                        '[class*="statistics"], [class*="stats"], article, #main, ' +
                        '[class*="content"]'
                    )).map(el => ({
                        el,
                        len: (el.innerText || '').trim().length,
                        hits: keyHits(el.innerText || '')
                    })).filter(c => c.len > 200);
                    // Prefer blocks with H2H keywords, fall back to largest.
                    candidates.sort((a, b) => (b.hits - a.hits) || (b.len - a.len));
                    // Return the full page text when it has the relevant H2H/Last
                    // markers. A narrow H2H block often excludes each team's own
                    // latest-match sections, which causes SF to be confused with H2H.
                    if (fullBody.length > 200 && keyHits(fullBody) > 0) {
                        return fullBody;
                    }
                    if (candidates.length > 0 && candidates[0].hits > 0) {
                        return candidates[0].el.innerText.replace(/\s+/g, ' ').trim();
                    }
                    // If no candidate has relevant keywords, use whole body
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
