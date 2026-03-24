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

    # ── Ana tarama ────────────────────────────────────────────────────

    async def get_live_basketball_totals(self) -> list[dict]:
        async with async_playwright() as p:
            if self.browser_mode == "headless":
                browser = await p.chromium.launch(
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
                # Hide navigator.webdriver property
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                """)
                logger.debug("Headless Chromium launched (stealth).")
            else:
                # Opera mode — auto-launch and connect via CDP
                self._launch_opera()
                try:
                    browser = await p.chromium.connect_over_cdp(self.cdp_url)
                except PlaywrightError as exc:
                    raise RuntimeError(
                        "Could not establish Opera CDP connection. "
                        f"CDP URL: {self.cdp_url}"
                    ) from exc
                context = browser.contexts[0] if browser.contexts else await browser.new_context()

            list_page = await context.new_page()
            list_page.set_default_timeout(self.page_timeout_ms)
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
                # Debug screenshot
                try:
                    await list_page.screenshot(path="debug_aiscore.png", full_page=False)
                    logger.info("Debug screenshot: debug_aiscore.png")
                except Exception:
                    pass
                await list_page.close()
                return []

            logger.info("Found %s match links on AIScore.", len(links))
            out = []

            # Parallel scraping: open CONCURRENT_TABS matches at once
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

            await list_page.close()
            return out

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
        await page.wait_for_timeout(3000)

        # ── Step 2: Collect links and filter to live-only ──
        all_hrefs: set[str] = set()
        max_scrolls = 50
        last_count = 0
        stale_rounds = 0

        for scroll_i in range(max_scrolls):
            hrefs = await page.evaluate(r"""() => {
                return Array.from(document.querySelectorAll('a[href*="/basketball/match-"]'))
                    .map(a => a.getAttribute('href'))
                    .filter(Boolean);
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

        # Build full URLs and deduplicate
        links = [urljoin(page.url, href) for href in all_hrefs]
        links = [u for u in links if "/basketball/match-" in u]
        _suffixes = re.compile(r'/(h2h|odds|stats|lineups|standings|summary)/?$')
        links = [_suffixes.sub('', u) for u in links]
        links = sorted(set(links))[:live_max]

        logger.info("Collected %s match links (live_max=%s).", len(links), live_max)
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
              // 1) Tournament name from breadcrumb links
              const breadcrumbs = Array.from(document.querySelectorAll('a'))
                .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                .filter(e => e.href.includes('/tournament-'));
              if (breadcrumbs.length) {
                tournament = breadcrumbs[breadcrumbs.length - 1].text;
              }
              // 2) Parse country/league from URL (fallback or extra info)
              const urlMatch = window.location.pathname.match(/\/basketball\/match-(.+?)\/\d+/);
              let urlLeague = '';
              if (urlMatch) {
                const parts = urlMatch[1].split('-');
                const vsIdx = parts.indexOf('vs');
                if (vsIdx > 0) {
                  urlLeague = parts.slice(0, Math.min(vsIdx, 3)).join(' ');
                }
              }
              tournament = tournament
                .replace(/\s*live\s*score\s*/gi, '')
                .replace(/\s*betting\s*odds\s*/gi, '')
                .replace(/\s*prediction\s*/gi, '')
                .trim();
              if (!tournament && urlLeague) tournament = urlLeague;

              let status = '';
              // Search for match status in short text elements only (avoid page headers)
              const statusCandidates = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => txt.length > 0 && txt.length < 30);
              // Priority 1: Specific quarter/period indicators
              const specificStatus = statusCandidates
                .map(({txt}) => txt)
                .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st\s|2nd\s|3rd\s|4th\s|finished|ended|final)/i.test(v));
              if (specificStatus) {
                status = specificStatus;
              } else {
                // Priority 2: Time patterns like "05:32" (game clock)
                const timeStatus = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^\d{1,2}:\d{2}$/.test(v.trim()));
                if (timeStatus) status = timeStatus;
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
              // Strategy 1: Look for score pattern like "85 - 72" or "85-72" near match header
              const scoreEls = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(txt.trim()));
              if (scoreEls.length > 0) {
                score = scoreEls[0].txt.trim();
              } else {
                // Strategy 2: Look for individual team scores (two separate big number elements)
                const headerArea = document.querySelector('[class*="matchHeader"], [class*="match-header"], [class*="score"], [class*="Score"]');
                if (headerArea) {
                  const nums = [];
                  headerArea.querySelectorAll('*').forEach(el => {
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

        # Skip match if less than 5 minutes remain in Q4
        is_q4 = parsed.get("isQ4", False)
        remaining = parsed.get("remainingMinutes")
        if is_q4 and remaining is not None and remaining < 5:
            logger.debug("Q4 <5min remaining, skipping: %s (%.1f min)", url, remaining)
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
        }

    @staticmethod
    def _extract_match_id(url: str) -> str:
        # /basketball/match-team1-team2/MATCH_ID
        # Strip sub-page suffixes that may remain
        cleaned = re.sub(r'/(h2h|odds|stats|lineups|standings|summary)/?$', '', url.rstrip('/'))
        parts = cleaned.split('/')
        return parts[-1] if parts else url
