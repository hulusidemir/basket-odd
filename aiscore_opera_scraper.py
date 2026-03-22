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
    İki modda çalışır (.env BROWSER_MODE):
      - opera:    Opera VPN üzerinden CDP ile bağlanır (ISP engeli varsa)
      - headless: Playwright headless Chromium (sistem geneli VPN varsa)
    Opera modunda tarayıcıyı otomatik başlatır.
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

    # ── Opera otomatik başlatma ──────────────────────────────────────

    def _find_opera_binary(self) -> str:
        """Opera binary yolunu bul: .env > flatpak export > PATH > yaygın konumlar."""
        if self.opera_binary:
            return self.opera_binary

        # Flatpak exported binary (en yaygın Linux kurulumu)
        flatpak_export = "/var/lib/flatpak/exports/bin/com.opera.Opera"
        if os.path.exists(flatpak_export):
            return flatpak_export

        # PATH'te opera var mı?
        found = shutil.which("opera")
        if found:
            return found

        # Flatpak kontrolü (export yoksa)
        try:
            result = subprocess.run(
                ["flatpak", "list", "--app", "--columns=application"],
                capture_output=True, text=True, timeout=5
            )
            if "com.opera.Opera" in result.stdout:
                return "flatpak run --branch=stable --arch=x86_64 com.opera.Opera"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        # Windows yaygın konumlar
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
            "Opera bulunamadı! OPERA_BINARY .env'de ayarlayın veya PATH'e ekleyin."
        )

    def _is_opera_cdp_alive(self) -> bool:
        """CDP portu dinleniyor ve HTTP yanıt veriyor mu?"""
        import socket
        import http.client
        try:
            with socket.create_connection(("127.0.0.1", self.cdp_port), timeout=2):
                pass
        except (ConnectionRefusedError, OSError):
            return False
        # Port açık ama HTTP yanıt verebilir mi?
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.cdp_port, timeout=5)
            conn.request("GET", "/json/version")
            resp = conn.getresponse()
            conn.close()
            return resp.status == 200
        except Exception:
            return False

    def _launch_opera(self):
        """Opera'yı CDP portuyla otomatik başlat."""
        if self._is_opera_cdp_alive():
            logger.info("Opera zaten CDP port %s'de çalışıyor.", self.cdp_port)
            return

        binary = self._find_opera_binary()

        # user-data-dir: VPN ayarları bu profilde saklanıyor
        user_data_dir = os.path.expanduser("~/.opera-cdp-profile")

        # Binary bir script/symlink olabilir (flatpak export) veya shell komutu
        if " " in binary:
            # "flatpak run ..." gibi boşluklu komut
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
        logger.info("Opera başlatılıyor: %s", " ".join(args))
        self._opera_process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # CDP portu açılana kadar bekle
        for _ in range(30):
            time.sleep(1)
            if self._is_opera_cdp_alive():
                logger.info("Opera CDP hazır (port %s).", self.cdp_port)
                return
        raise RuntimeError(
            f"Opera başlatıldı ama CDP port {self.cdp_port} açılmadı. "
            "Opera'nın düzgün yüklenip yüklenmediğini kontrol edin."
        )

    # ── Ana tarama ────────────────────────────────────────────────────

    async def get_live_basketball_totals(self) -> list[dict]:
        async with async_playwright() as p:
            if self.browser_mode == "headless":
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                logger.debug("Headless Chromium başlatıldı.")
            else:
                # Opera modunda — otomatik başlat ve CDP bağlan
                self._launch_opera()
                try:
                    browser = await p.chromium.connect_over_cdp(self.cdp_url)
                except PlaywrightError as exc:
                    raise RuntimeError(
                        "Opera CDP bağlantısı kurulamadı. "
                        f"CDP URL: {self.cdp_url}"
                    ) from exc
                context = browser.contexts[0] if browser.contexts else await browser.new_context()

            list_page = await context.new_page()
            list_page.set_default_timeout(self.page_timeout_ms)
            await list_page.goto(self.aiscore_url, wait_until="domcontentloaded")
            # AIScore SPA — JS ile içerik yükleniyor
            await list_page.wait_for_timeout(5000)

            links = await self._collect_match_links(list_page)
            if not links:
                logger.warning("AIScore listesinde maç linki bulunamadı.")
                await list_page.close()
                return []

            logger.info("AIScore listesinde %s maç linki bulundu.", len(links))
            out = []

            for link in links[: self.max_matches_per_cycle]:
                detail = await context.new_page()
                detail.set_default_timeout(self.page_timeout_ms)
                try:
                    row = await self._extract_match(detail, link)
                    if row:
                        out.append(row)
                except Exception as exc:
                    logger.debug("Maç okunamadı (%s): %s", link, exc)
                finally:
                    await detail.close()

            await list_page.close()
            return out

    async def _collect_match_links(self, page) -> list[str]:
        """
        AIScore ana sayfasında varsayılan olarak 'Live' sekmesi aktif.
        Sayfa virtual scroller kullanıyor — sadece görünen maçlar DOM'da.
        Live sekmesindeki maç sayısını okuyup, o kadar maç toplanınca duruyoruz.
        """
        await page.wait_for_timeout(3000)

        # Live sekmesinin aktif olduğundan emin ol ve canlı maç sayısını al
        live_max = await page.evaluate(r"""() => {
            const tab = document.querySelector('.activeLiveTab');
            if (!tab) return 999;
            const m = (tab.innerText || '').match(/\((\d+)\)/);
            return m ? parseInt(m[1], 10) : 999;
        }""")
        logger.info("AIScore Live sekmesinde %s canlı maç var.", live_max)

        all_hrefs: set[str] = set()
        max_scrolls = 50
        last_count = 0
        stale_rounds = 0

        for _ in range(max_scrolls):
            hrefs = await page.evaluate(r"""() => {
                return Array.from(document.querySelectorAll('a[href*="/basketball/match-"]'))
                    .map(a => a.getAttribute('href'))
                    .filter(Boolean);
            }""")
            all_hrefs.update(hrefs)

            # Live sayısına ulaştıysak dur
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

        # Live sayısı kadar kes (fazlasını alma)
        links = [urljoin(page.url, href) for href in all_hrefs]
        links = [u for u in links if "/basketball/match-" in u]
        links = sorted(set(links))[:live_max]
        return links

    async def _extract_match(self, page, url: str) -> dict | None:
        # Doğrudan /odds URL'sine git
        odds_url = url.rstrip("/") + "/odds"
        await page.goto(odds_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        parsed = await page.evaluate(
            r"""
            () => {
              const text = s => (s || '').replace(/\s+/g, ' ').trim();

              // --- Total Points extraction from newOdds container ---
              const container = document.querySelector('.newOdds');
              let opening = null;
              let inplay = null;

              if (container) {
                // Her bahis şirketi için 3 sütun var: To Win(0), Spread(1), Total Points(2)
                // Tüm flex-col sütunlarını bul
                const contentDivs = container.querySelectorAll('.content');
                // İlk content = header, sonrakiler = bahis şirketleri
                for (const content of contentDivs) {
                  const cols = content.querySelectorAll('.flex.flex-1.align-center.flex-col');
                  // Her 3 sütundan 3. (index 2) = Total Points
                  for (let i = 2; i < cols.length; i += 3) {
                    const col = cols[i];
                    const openingEls = col.querySelectorAll('[class*="openingBg"]');
                    const inPlayEls = col.querySelectorAll('[class*="inPlayBg"]');

                    const parseTotal = (els) => {
                      const allText = Array.from(els).map(e => e.innerText).join(' ');
                      const nums = allText.match(/(\d+\.?\d*)/g);
                      if (!nums) return null;
                      // Total line genelde en büyük sayıdır (100+)
                      const candidates = nums.map(Number).filter(n => n >= 50);
                      return candidates.length ? candidates[0] : null;
                    };

                    const op = parseTotal(openingEls);
                    const ip = parseTotal(inPlayEls);

                    // İlk geçerli bahis şirketini kullan
                    if (op !== null && opening === null) opening = op;
                    if (ip !== null && inplay === null) inplay = ip;

                    if (opening !== null && inplay !== null) break;
                  }
                  if (opening !== null && inplay !== null) break;
                }
              }

              // --- Maç bilgileri ---
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
              const breadcrumbs = Array.from(document.querySelectorAll('a'))
                .map(e => ({text: text(e.innerText), href: e.getAttribute('href') || ''}))
                .filter(e => e.href.includes('/tournament-'));
              if (breadcrumbs.length) tournament = breadcrumbs[breadcrumbs.length - 1].text;

              let status = '';
              const allSpans = Array.from(document.querySelectorAll('span, div'));
              const statusEl = allSpans
                .map(e => text(e.innerText))
                .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st|2nd|3rd|4th|live|finished|half)/i.test(v));
              if (statusEl) status = statusEl;

              // Q4 kalan süre tespiti
              let isQ4 = /Q4|4Q|4th/i.test(status);
              let remainingMinutes = null;
              if (status) {
                const tm = status.match(/(\d+):(\d+)/);
                if (tm) remainingMinutes = parseInt(tm[1]) + parseInt(tm[2]) / 60;
              }

              return { opening, inplay, matchName, tournament, status, isQ4, remainingMinutes };
            }
            """
        )

        opening = parsed.get("opening")
        inplay = parsed.get("inplay")
        if opening is None or inplay is None:
            return None

        # Q4'te 5 dakikadan az kaldıysa maçı atla
        is_q4 = parsed.get("isQ4", False)
        remaining = parsed.get("remainingMinutes")
        if is_q4 and remaining is not None and remaining < 5:
            logger.debug("Q4 <5dk kaldı, atlanıyor: %s (%.1f dk)", url, remaining)
            return None

        match_id = self._extract_match_id(url)
        return {
            "match_id": match_id,
            "match_name": parsed.get("matchName") or f"Match {match_id}",
            "tournament": parsed.get("tournament") or "Bilinmiyor",
            "status": parsed.get("status") or "Canlı",
            "opening_total": float(opening),
            "inplay_total": float(inplay),
            "url": url,
        }

    @staticmethod
    def _extract_match_id(url: str) -> str:
        # /basketball/match-team1-team2/MATCH_ID
        parts = url.rstrip("/").split("/")
        return parts[-1] if parts else url
