import asyncio
import logging
import os
import platform
import re
import shutil
import subprocess
import time
from playwright.async_api import Error as PlaywrightError
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)


class AiscoreOperaScraper:
    """
    Runs in two modes (.env BROWSER_MODE):
      - opera:    Connects via CDP through Opera VPN (when ISP blocks access)
      - headless: Playwright headless Chromium (when system-wide VPN or no block)
    In opera mode, the browser is launched automatically.
    """

    def __init__(
        self,
        cdp_url: str,
        aiscore_url: str,
        max_matches_per_cycle: int = 40,
        page_timeout_ms: int = 30000,
        browser_mode: str = "opera",
        opera_binary: str = "",
        cdp_port: int = 9222,
    ):
        self.cdp_url = cdp_url
        self.aiscore_url = aiscore_url
        self.max_matches_per_cycle = max_matches_per_cycle
        self.page_timeout_ms = page_timeout_ms
        self.browser_mode = browser_mode.lower()
        self.opera_binary = opera_binary
        self.cdp_port = cdp_port
        self._opera_process = None

    # ── Auto-launch Opera ──────────────────────────────────────────────────

    def _find_opera_binary(self) -> str:
        """Find Opera binary path: .env > flatpak export > PATH > common locations."""
        if self.opera_binary:
            return self.opera_binary

        # Flatpak exported binary (most common Linux installation)
        flatpak_export = "/var/lib/flatpak/exports/bin/com.opera.Opera"
        if os.path.exists(flatpak_export):
            return flatpak_export

        # Is opera in PATH?
        found = shutil.which("opera")
        if found:
            return found

        # Flatpak check (if export doesn't exist)
        try:
            result = subprocess.run(
                ["flatpak", "list", "--app", "--columns=application"],
                capture_output=True, text=True, timeout=5
            )
            if "com.opera.Opera" in result.stdout:
                return "flatpak run --branch=stable --arch=x86_64 com.opera.Opera"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Common Windows locations
        if platform.system() == "Windows":
            candidates = [
                os.path.expandvars(r"%LOCALAPPDATA%\Programs\Opera\opera.exe"),
                os.path.expandvars(r"%PROGRAMFILES%\Opera\opera.exe"),
                os.path.expandvars(r"%PROGRAMFILES(X86)%\Opera\opera.exe"),
            ]
            for c in candidates:
                if os.path.isfile(c):
                    return c

        raise RuntimeError(
            "Opera not found! Set OPERA_BINARY in .env or add Opera to PATH."
        )

    def _is_opera_cdp_alive(self) -> bool:
        """Check if CDP port is listening and responding to HTTP."""
        import socket
        import http.client
        try:
            with socket.create_connection(("127.0.0.1", self.cdp_port), timeout=2):
                pass
        except (ConnectionRefusedError, OSError):
            return False
        # Port is open, but can it respond to HTTP?
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.cdp_port, timeout=5)
            conn.request("GET", "/json/version")
            resp = conn.getresponse()
            conn.close()
            return resp.status == 200
        except Exception:
            return False

    def _launch_opera(self):
        """Auto-launch Opera with CDP port."""
        if self._is_opera_cdp_alive():
            logger.info("Opera already running on CDP port %s.", self.cdp_port)
            return

        binary = self._find_opera_binary()

        # user-data-dir: VPN settings are stored in this profile
        user_data_dir = os.path.expanduser("~/.opera-cdp-profile")

        # Binary may be a script/symlink (flatpak export) or shell command
        if " " in binary:
            # Shell command with spaces like "flatpak run ..."
            cmd_parts = binary.split()
        else:
            cmd_parts = [binary]

        args = cmd_parts + [
            f"--remote-debugging-port={self.cdp_port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--disable-popup-blocking",
            "--start-minimized",
        ]
        logger.info("Launching Opera: %s", " ".join(args))
        self._opera_process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for CDP port to become available
        for _ in range(30):
            time.sleep(1)
            if self._is_opera_cdp_alive():
                logger.info("Opera CDP ready (port %s).", self.cdp_port)
                return
        raise RuntimeError(
            f"Opera launched but CDP port {self.cdp_port} did not open. "
            "Check that Opera is installed correctly."
        )

    async def _create_browser_context(self, playwright):
        if self.browser_mode == "headless":
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            return browser, context

        self._launch_opera()
        try:
            browser = await playwright.chromium.connect_over_cdp(self.cdp_url)
        except PlaywrightError as exc:
            raise RuntimeError(
                "Could not establish Opera CDP connection. "
                f"CDP URL: {self.cdp_url}"
            ) from exc
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        return browser, context

    async def _close_browser_if_needed(self, browser):
        if self.browser_mode == "headless":
            await browser.close()

    # ── Ana tarama ────────────────────────────────────────────────────

    async def get_live_basketball_totals(self) -> list[dict]:
        async with async_playwright() as p:
            browser, context = await self._create_browser_context(p)

            list_page = await context.new_page()
            list_page.set_default_timeout(self.page_timeout_ms)
            try:
                await list_page.goto(self.aiscore_url, wait_until="domcontentloaded")
                # AIScore SPA — content loads via JS, wait longer in headless mode
                wait_secs = 10 if self.browser_mode == "headless" else 5
                await list_page.wait_for_timeout(wait_secs * 1000)

                links = await self._collect_match_links(list_page)
                if not links:
                    # Debug: log the page state
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
                await self._close_browser_if_needed(browser)

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
                    # Reload to get fresh DOM state after lock clears
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

    async def get_match_insights(self, url: str) -> dict:
        base_url = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        page_urls = {
            "summary": base_url + "/summary",
            "stats": base_url + "/stats",
            "h2h": base_url + "/h2h",
            "lineups": base_url + "/lineups",
            "standings": base_url + "/standings",
        }

        async with async_playwright() as p:
            browser, context = await self._create_browser_context(p)
            try:
                results = await asyncio.gather(
                    self._read_text_page(context, page_urls["summary"], "summary"),
                    self._read_stats_page(context, page_urls["stats"]),
                    self._read_text_page(context, page_urls["h2h"], "h2h"),
                    self._read_text_page(context, page_urls["lineups"], "lineups"),
                    self._read_text_page(context, page_urls["standings"], "standings"),
                    return_exceptions=True,
                )
            finally:
                await self._close_browser_if_needed(browser)

        merged = {
            "summary": {},
            "stats": {"rows": []},
            "h2h": {},
            "lineups": {},
            "standings": {},
        }
        keys = ["summary", "stats", "h2h", "lineups", "standings"]
        for key, result in zip(keys, results):
            if isinstance(result, dict):
                merged[key] = result
            elif isinstance(result, Exception):
                logger.debug("Could not read %s page for %s: %s", key, base_url, result)
        return merged

    async def _read_text_page(self, context, url: str, page_kind: str) -> dict:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2200)
            parsed = await page.evaluate(
                r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();
                  const bodyText = text(document.body ? document.body.innerText : '');
                  return {
                    title: text(document.title || ''),
                    bodyText,
                    available: bodyText.length > 40 && !/^no data$/i.test(bodyText),
                    noData: /no data/i.test(bodyText),
                  };
                }
                """
            )
            return {
                "url": url,
                "kind": page_kind,
                "title": parsed.get("title", ""),
                "body_text": parsed.get("bodyText", ""),
                "available": bool(parsed.get("available")),
                "no_data": bool(parsed.get("noData")),
            }
        except Exception as exc:
            logger.debug("Text page scrape failed (%s): %s", url, exc)
            return {"url": url, "kind": page_kind, "body_text": "", "available": False, "no_data": True}
        finally:
            await page.close()

    async def _read_stats_page(self, context, url: str) -> dict:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2600)
            parsed = await page.evaluate(
                r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();
                  const numericLike = value => /^(\d+(\.\d+)?%?|\d+\/\d+)$/.test(value);
                  const rows = [];
                  const seen = new Set();

                  const nodes = Array.from(document.querySelectorAll('tr, [role="row"], div'));
                  for (const node of nodes) {
                    const children = Array.from(node.children || [])
                      .map(child => text(child.innerText))
                      .filter(Boolean)
                      .filter(value => value.length < 40);
                    if (children.length < 3 || children.length > 6) continue;

                    const numeric = children.filter(numericLike);
                    const labels = children.filter(value => /[a-zA-Z]/.test(value) && !numericLike(value));
                    if (numeric.length < 2 || labels.length < 1) continue;

                    const label = labels.reduce((best, current) => current.length > best.length ? current : best, labels[0]);
                    const home = numeric[0];
                    const away = numeric[numeric.length - 1];
                    const key = `${label}|${home}|${away}`;
                    if (seen.has(key)) continue;
                    seen.add(key);
                    rows.push({label, home, away});
                    if (rows.length >= 60) break;
                  }

                  return {
                    title: text(document.title || ''),
                    bodyText: text(document.body ? document.body.innerText : ''),
                    rows
                  };
                }
                """
            )
            return {
                "url": url,
                "title": parsed.get("title", ""),
                "body_text": parsed.get("bodyText", ""),
                "rows": parsed.get("rows", []),
                "available": len(parsed.get("rows", [])) > 0,
            }
        except Exception as exc:
            logger.debug("Stats page scrape failed (%s): %s", url, exc)
            return {"url": url, "rows": [], "body_text": "", "available": False}
        finally:
            await page.close()

    async def _collect_match_links(self, page) -> list[str]:
        """
        Navigate to AIScore basketball live page and collect live match links.
        Uses multiple strategies to find and activate the Live tab.
        """
        await page.wait_for_timeout(3000)

        # ── Step 0: Dump page structure for diagnostics ──
        page_diag = await page.evaluate(r"""() => {
            const text = s => (s || '').replace(/\s+/g, ' ').trim();

            // Collect the top-level navigation/tab area HTML
            const topArea = document.querySelector('header, nav, [class*="header"], [class*="nav"]');
            const topHtml = topArea ? text(topArea.innerHTML).substring(0, 500) : 'NO_HEADER';

            // Collect ALL elements that might be tabs/navigation items (with their text + classes)
            const tabCandidates = [];
            document.querySelectorAll('*').forEach(el => {
                const t = text(el.innerText);
                const cls = el.className || '';
                // Short text elements that look like tabs
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

            // Count match links
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

            // Strategy 1: class contains 'activeLiveTab' or 'liveTab' or 'live_tab'
            tab = document.querySelector('.activeLiveTab, [class*="liveTab"], [class*="live_tab"], [class*="live-tab"]');

            // Strategy 2: Look through all elements for ones that look like tabs with "Live" text
            if (!tab) {
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    const t = text(el.innerText);
                    const cls = (typeof el.className === 'string') ? el.className : '';
                    // Must be a short label containing "Live" or a number in parentheses
                    if (t.length > 0 && t.length < 30 && /^live\b/i.test(t.trim())) {
                        // Prefer elements that look like tabs (few children, clickable)
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

                // Click it
                try {
                    tab.click();
                    clicked = true;
                } catch(e) {
                    // Try dispatching a click event
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

        # Wait for content to refresh after tab click
        await page.wait_for_timeout(4000)

        # ── Step 2: Collect links — only from rows with a live time indicator ──
        all_hrefs: set[str] = set()
        max_scrolls = 50
        last_count = 0
        stale_rounds = 0

        for scroll_i in range(max_scrolls):
            hrefs = await page.evaluate(r"""() => {
                const liveHrefs = [];
                // Each match row is an <a> containing the match link
                const matchLinks = document.querySelectorAll('a[href*="/basketball/match-"]');
                for (const a of matchLinks) {
                    // Walk up to the match row container (up to 6 levels)
                    let row = a;
                    for (let i = 0; i < 6; i++) {
                        if (row.parentElement) row = row.parentElement;
                        else break;
                    }
                    const rowText = (row.innerText || '').replace(/\s+/g, ' ');
                    // Live matches have a time indicator like "Q1-05:00", "Q2-10:00",
                    // "03-02:38", "01-00:21", "OT-05:00", "HT" etc.
                    // Pattern: period-time format OR standalone Q/OT/HT indicators
                    const hasLiveTime = /\b(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\b/i.test(rowText)
                                     || /\b\d{1,2}[-:]\d{2}[:-]\d{2}\b/.test(rowText);
                    // Also check for colored/red time elements (live indicator)
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

        # Fallback: if live-only filter found nothing, try all links on page
        if not all_hrefs:
            logger.warning("Live-only filter found 0 links. Falling back to all match links on page.")
            fallback = await page.evaluate(r"""() => {
                return Array.from(document.querySelectorAll('a[href*="/basketball/match-"]'))
                    .map(a => a.getAttribute('href'))
                    .filter(Boolean);
            }""")
            all_hrefs.update(fallback)

        # Build full URLs and deduplicate
        links = [urljoin(page.url, href) for href in all_hrefs]
        links = [u for u in links if "/basketball/match-" in u]
        _suffixes = re.compile(r'/(h2h|odds|stats|lineups|standings|summary)/?$')
        links = [_suffixes.sub('', u) for u in links]
        links = sorted(set(links))[:live_max]

        logger.info("Collected %s live match links (live_max=%s).", len(links), live_max)
        return links

    async def _extract_match(self, page, url: str) -> dict | None:
        # Navigate directly to the /odds URL
        odds_url = url.rstrip("/") + "/odds"
        await page.goto(odds_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)

        # Try to click the Total/O-U tab to show over/under market
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
                // Lock icon (SVG or font icon), "suspended", dash placeholders
                if (html.includes('lock') || html.includes('🔒')) return true;
                if (txt === '-' || txt === '--' || txt === '—') return true;
                if (/suspend|locked|unavail/i.test(txt)) return true;
                // Check for lock SVG icons
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
                // Rows are distinguished by color classes
                // Skip rows that are locked/suspended (bookmaker updating odds)
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

                // ── Strategy 3: Positional — 1st and 3rd rows of each bookmaker ──
                if (opening === null || inplay === null) {
                  const contentDivs = container.querySelectorAll('.content');
                  for (const content of contentDivs) {
                    if (isLocked(content)) continue;
                    const rows = Array.from(content.children).filter(el => {
                      return !isLocked(el) && findLine(text(el.innerText)) !== null;
                    });
                    if (rows.length >= 3) {
                      if (opening === null) opening = findLine(text(rows[0].innerText));
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
              // 1) Tournament/country from breadcrumb links
              const breadcrumbs = Array.from(document.querySelectorAll('a'))
                .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                .filter(e => e.href.includes('/tournament-'));
              if (breadcrumbs.length >= 2) {
                country = breadcrumbs[0].text;
                tournament = breadcrumbs[breadcrumbs.length - 1].text;
              } else if (breadcrumbs.length === 1) {
                tournament = breadcrumbs[0].text;
              }
              // 2) Parse country/league from URL (fallback or extra info)
              const urlMatch = window.location.pathname.match(/\/basketball\/match-(.+?)\/\d+/);
              let urlLeague = '';
              if (urlMatch) {
                const parts = urlMatch[1].split('-');
                const vsIdx = parts.indexOf('vs');
                if (vsIdx > 0) {
                  urlLeague = parts.slice(0, Math.min(vsIdx, 3)).join(' ');
                  // First part of URL path is usually the country
                  if (!country && parts.length > 0) {
                    country = parts[0].charAt(0).toUpperCase() + parts[0].slice(1);
                  }
                }
              }
              const cleanRe = /\s*(live\s*score|betting\s*odds|prediction)\s*/gi;
              tournament = tournament.replace(cleanRe, '').trim();
              country = country.replace(cleanRe, '').trim();
              if (!tournament && urlLeague) tournament = urlLeague;
              // Prefix country to tournament if available and not already included
              if (country && tournament && !tournament.toLowerCase().startsWith(country.toLowerCase())) {
                tournament = country + ' : ' + tournament;
              } else if (country && !tournament) {
                tournament = country;
              }

              let status = '';
              // Search for match status — look for period-time patterns like "Q4 06:42", "Q2-10:00"
              const statusCandidates = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => txt.length > 0 && txt.length < 30);
              // Priority 1: Period + time format like "Q4 06:42", "Q2-10:00", "OT 05:00"
              const periodTime = statusCandidates
                .map(({txt}) => txt)
                .find(v => /^(Q[1-4]|[1-4]Q|OT)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(v.trim()));
              if (periodTime) {
                status = periodTime.trim();
              } else {
                // Priority 2: Just period indicator like "Q4", "HT", "FT"
                const periodOnly = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st|2nd|3rd|4th)$/i.test(v.trim()));
                if (periodOnly) {
                  status = periodOnly.trim();
                } else {
                  // Priority 3: Standalone time "05:32"
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

              // Detect if match is finished/not live
              let isFinished = /\b(FT|Finished|Ended)\b/i.test(status);
              // Check for a dedicated "finished" badge/indicator (not generic page text)
              if (!isFinished) {
                const finishedBadge = document.querySelector(
                  '[class*="finished"], [class*="Finished"], [class*="ended"], [class*="final-score"]'
                );
                if (finishedBadge) isFinished = true;
              }

              // Detect if any bookmaker rows are locked
              let hasLockedRows = false;
              if (container) {
                const allRows = container.querySelectorAll('[class*="openingBg"], [class*="inPlayBg"], .content > *');
                for (const el of allRows) {
                  if (isLocked(el)) { hasLockedRows = true; break; }
                }
              }

              // --- Extract live score ---
              let score = '';
              // Strategy 1: Direct "NN - NN" pattern in short text elements
              const scoreEls = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({el, txt}) => {
                  if (!/^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(txt.trim())) return false;
                  // Must be a score, not something else — check it's in header area
                  // and both numbers are reasonable basketball scores (0-300)
                  const parts = txt.trim().split(/\s*[-–]\s*/);
                  return parts.length === 2 && parseInt(parts[0]) <= 300 && parseInt(parts[1]) <= 300;
                });
              if (scoreEls.length > 0) {
                score = scoreEls[0].txt.trim();
              }
              // Strategy 2: Look for big standalone numbers near the top of the page
              // AIScore shows scores as separate large number elements in the match header
              if (!score) {
                const topEls = Array.from(document.querySelectorAll('span, div, b, strong'))
                  .filter(el => {
                    const rect = el.getBoundingClientRect();
                    return rect.top < 200 && el.children.length === 0;
                  })
                  .map(el => ({el, txt: text(el.innerText).trim()}))
                  .filter(({txt}) => /^\d{1,3}$/.test(txt));
                // Find the two largest font-size numbers (likely the main score)
                if (topEls.length >= 2) {
                  const withSize = topEls.map(({el, txt}) => ({
                    txt,
                    size: parseFloat(window.getComputedStyle(el).fontSize) || 0
                  })).sort((a, b) => b.size - a.size);
                  // Take the two biggest — they should be the team scores
                  if (withSize.length >= 2 && withSize[0].size >= 16) {
                    score = withSize[0].txt + ' - ' + withSize[1].txt;
                  }
                }
              }
              // Strategy 3: Search in any element with score-like classes
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

              return { opening, inplay, matchName, tournament, status, isQ4, remainingMinutes, hasLockedRows, isFinished, score };
            }
            """
        )

        # Skip finished matches early
        is_finished = parsed.get("isFinished", False)
        status = parsed.get("status", "")
        if is_finished:
            logger.debug("Skipping finished match: %s (status=%s)", url, status)
            return None

        opening = parsed.get("opening")
        inplay = parsed.get("inplay")
        locked = parsed.get("hasLockedRows", False)
        if opening is None or inplay is None:
            if locked:
                logger.debug("Odds locked/suspended for %s (bookmaker updating)", url)
            else:
                logger.debug("Could not find opening/inplay totals for %s", url)
            return None

        # Skip match if less than 4 minutes remain in last period
        is_q4 = parsed.get("isQ4", False)
        remaining = parsed.get("remainingMinutes")
        if is_q4 and remaining is not None and remaining <= 4.0:
            logger.debug("Q4 <=4min remaining, skipping: %s (%.1f min)", url, remaining)
            return None

        match_id = self._extract_match_id(url)
        return {
            "match_id": match_id,
            "match_name": parsed.get("matchName") or f"Match {match_id}",
            "tournament": parsed.get("tournament") or "Unknown",
            "status": parsed.get("status") or "Live",
            "opening_total": float(opening),
            "inplay_total": float(inplay),
            "url": url,
            "score": parsed.get("score") or "",
            "market_locked": bool(parsed.get("hasLockedRows", False)),
        }

    @staticmethod
    def _extract_match_id(url: str) -> str:
        # /basketball/match-team1-team2/MATCH_ID
        # Strip sub-page suffixes that may remain
        cleaned = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        parts = cleaned.split('/')
        return parts[-1] if parts else url
