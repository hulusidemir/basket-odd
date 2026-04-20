import asyncio
import logging
import re
from playwright.async_api import Error as PlaywrightError
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)


class AiscoreScraper:
    def __init__(self, aiscore_url: str, max_matches_per_cycle: int = 40, page_timeout_ms: int = 30000):
        self.aiscore_url = aiscore_url
        self.max_matches_per_cycle = max_matches_per_cycle
        self.page_timeout_ms = page_timeout_ms

    async def _create_browser_context(self, playwright):
        browser = await playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        return browser, context

    async def _close_browser(self, browser):
        await browser.close()

    # ── Ana tarama ────────────────────────────────────────────────────

    async def get_live_basketball_totals(self) -> list[dict]:
        async with async_playwright() as p:
            browser, context = await self._create_browser_context(p)

            list_page = await context.new_page()
            list_page.set_default_timeout(self.page_timeout_ms)
            try:
                await list_page.goto(self.aiscore_url, wait_until="domcontentloaded")
                await list_page.wait_for_timeout(10 * 1000)

                links = await self._collect_match_links(list_page)
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

                concurrent_tabs = 4
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
                await list_page.close()
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
        odds_url = url.rstrip("/") + "/odds"
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
              const breadcrumbs = Array.from(document.querySelectorAll('a'))
                .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                .filter(e => e.href.includes('/tournament-'));
              if (breadcrumbs.length >= 2) {
                country = breadcrumbs[0].text;
                tournament = breadcrumbs[breadcrumbs.length - 1].text;
              } else if (breadcrumbs.length === 1) {
                tournament = breadcrumbs[0].text;
              }
              const urlMatch = window.location.pathname.match(/\/basketball\/match-(.+?)\/\d+/);
              let urlLeague = '';
              if (urlMatch) {
                const parts = urlMatch[1].split('-');
                const vsIdx = parts.indexOf('vs');
                if (vsIdx > 0) {
                  urlLeague = parts.slice(0, Math.min(vsIdx, 3)).join(' ');
                  if (!country && parts.length > 0) {
                    country = parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
                  }
                }
              }
              const cleanRe = /\s*(live\s*score|betting\s*odds|prediction)\s*/gi;
              tournament = tournament.replace(cleanRe, '').trim();
              country = country.replace(cleanRe, '').trim();
              if (!tournament && urlLeague) tournament = urlLeague;
              if (country && tournament && !tournament.toLowerCase().startsWith(country.toLowerCase())) {
                tournament = country + ' : ' + tournament;
              } else if (country && !tournament) {
                tournament = country;
              }

              let status = '';
              const statusCandidates = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => txt.length > 0 && txt.length < 30);
              const periodTime = statusCandidates
                .map(({txt}) => txt)
                .find(v => /^(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(v.trim()));
              if (periodTime) {
                status = periodTime.trim();
              } else {
                const periodOnly = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st|2nd|3rd|4th)$/i.test(v.trim()));
                if (periodOnly) {
                  status = periodOnly.trim();
                } else {
                  const timeOnly = statusCandidates
                    .map(({txt}) => txt)
                    .find(v => /^\d{1,2}:\d{2}$/.test(v.trim()));
                  if (timeOnly) status = timeOnly.trim();
                }
              }

              let isQ4 = /Q4|4Q|4th/i.test(status);
              let remainingMinutes = null;
              if (status) {
                const tm = status.match(/(\d+):(\d+)/);
                if (tm) remainingMinutes = parseInt(tm[1]) + parseInt(tm[2]) / 60;
              }

              let isFinished = /\b(FT|Finished|Ended)\b/i.test(status);
              if (!isFinished) {
                const finishedBadge = document.querySelector(
                  '[class*="finished"], [class*="Finished"], [class*="ended"], [class*="final-score"]'
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
              let score = '';
              const scoreEls = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({el, txt}) => {
                  if (!/^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(txt.trim())) return false;
                  const parts = txt.trim().split(/\s*[-–]\s*/);
                  return parts.length === 2 && parseInt(parts[0]) <= 300 && parseInt(parts[1]) <= 300;
                });
              if (scoreEls.length > 0) {
                score = scoreEls[0].txt.trim();
              }
              if (!score) {
                const topEls = Array.from(document.querySelectorAll('span, div, b, strong'))
                  .filter(el => {
                    const rect = el.getBoundingClientRect();
                    return rect.top < 200 && el.children.length === 0;
                  })
                  .map(el => ({el, txt: text(el.innerText).trim()}))
                  .filter(({txt}) => /^\d{1,3}$/.test(txt));
                if (topEls.length >= 2) {
                  const withSize = topEls.map(({el, txt}) => ({
                    txt,
                    size: parseFloat(window.getComputedStyle(el).fontSize) || 0
                  })).sort((a, b) => b.size - a.size);
                  if (withSize.length >= 2 && withSize[0].size >= 16) {
                    score = withSize[0].txt + ' - ' + withSize[1].txt;
                  }
                }
              }
              if (!score) {
                const scoreContainer = document.querySelector(
                  '[class*="score" i], [class*="Score"], [class*="matchScore"], [class*="match-score"]'
                );
                if (scoreContainer) {
                  const nums = [];
                  scoreContainer.querySelectorAll('*').forEach(el => {
                    if (el.children.length === 0) {
                      const t = text(el.innerText).trim();
                      if (/^\d{1,3}$/.test(t) && parseInt(t) <= 300) nums.push(t);
                    }
                  });
                  if (nums.length >= 2) score = nums[0] + ' - ' + nums[1];
                }
              }

              return { opening, prematch, inplay, matchName, tournament, status, isQ4, remainingMinutes, hasLockedRows, isFinished, score };
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
        if is_q4 and remaining is not None and remaining <= 4.0:
            logger.debug("Q4 <=4min remaining, skipping: %s (%.1f min)", url, remaining)
            return None

        h2h_body = await self._fetch_h2h_body(page, url)

        match_id = self._extract_match_id(url)
        return {
            "match_id": match_id,
            "match_name": parsed.get("matchName") or f"Match {match_id}",
            "tournament": parsed.get("tournament") or "Unknown",
            "status": parsed.get("status") or "Live",
            "opening_total": float(opening),
            "prematch_total": float(prematch) if prematch is not None else None,
            "inplay_total": float(inplay),
            "url": url,
            "score": parsed.get("score") or "",
            "market_locked": bool(parsed.get("hasLockedRows", False)),
            "has_prematch": prematch is not None,
            "h2h_body_text": h2h_body,
        }

    async def _fetch_h2h_body(self, page, url: str) -> str:
        try:
            h2h_url = url.rstrip("/") + "/h2h"
            await page.goto(h2h_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            body = await page.evaluate(r"""
                () => {
                    const selectors = [
                        'main',
                        '[class*="matchDetail"]',
                        '[class*="match-detail"]',
                        '[class*="content"]',
                        'article',
                        '#main',
                    ];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && (el.innerText || '').trim().length > 100) {
                            return el.innerText.replace(/\s+/g, ' ').trim();
                        }
                    }
                    return (document.body.innerText || '').replace(/\s+/g, ' ').trim();
                }
            """)
            logger.debug("H2H body fetched for %s (%d chars).", url, len(body or ""))
            return body or ""
        except Exception as exc:
            logger.debug("H2H page fetch failed for %s: %s", url, exc)
            return ""

    @staticmethod
    def _extract_match_id(url: str) -> str:
        cleaned = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        parts = cleaned.split('/')
        return parts[-1] if parts else url
