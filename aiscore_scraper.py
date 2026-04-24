import asyncio
import logging
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

              // Strateji 1: "Q1 03:00" — tek element içinde birleşik
              const periodTime = statusCandidates
                .map(({txt}) => txt)
                .find(v => /^(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(v.trim()));
              if (periodTime) {
                status = periodTime.trim();
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
                  .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st|2nd|3rd|4th)$/i.test(v.trim()));
                if (periodOnly) status = periodOnly.trim();
              }

              // Strateji 5: Sayfa başlığında period ipucu
              if (!status) {
                const titlePm = text(document.title || '').match(/\b(Q[1-4]|[1-4]Q|HT|OT)\b/i);
                if (titlePm) status = titlePm[0];
              }

              let isQ4 = /Q4|4Q|4th/i.test(status);
              let remainingMinutes = null;
              if (status) {
                const tm = status.match(/(\d+):(\d+)/);
                if (tm) remainingMinutes = parseInt(tm[1]) + parseInt(tm[2]) / 60;
              }

              let isFinished = /\b(FT|Finished|Ended|Full Time)\b/i.test(status);
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
        if not self.skip_h2h and is_q4 and remaining is not None and remaining <= 4.0:
            logger.debug("Q4 <=4min remaining, skipping: %s (%.1f min)", url, remaining)
            return None

        h2h_body = "" if self.skip_h2h else await self._fetch_h2h_body(page, url)

        match_id = self._extract_match_id(url)
        return {
            "match_id": match_id,
            "match_name": parsed.get("matchName") or f"Match {match_id}",
            "tournament": parsed.get("tournament") or "Unknown",
            "status": parsed.get("status"),
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
        h2h_url = url.rstrip("/") + "/h2h"
        try:
            await page.goto(h2h_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3500)

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
            await page.wait_for_timeout(1500)

            # Progressive scroll to trigger lazy-loaded last-5 / h2h tables
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight / 3)")
                await page.wait_for_timeout(900)

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
                    if (candidates.length > 0 && candidates[0].hits > 0) {
                        return candidates[0].el.innerText.replace(/\s+/g, ' ').trim();
                    }
                    // If no candidate has relevant keywords, use whole body
                    return (document.body.innerText || '').replace(/\s+/g, ' ').trim();
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
