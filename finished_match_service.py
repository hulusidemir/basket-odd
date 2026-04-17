"""
finished_match_service.py — Shared finished match checking service.
Used by both the background worker and manual UI-triggered checks.
"""

import asyncio
import logging
import os
import platform
import re
import shutil
import subprocess
import time

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


logger = logging.getLogger("finished_match_service")


def parse_score_total(score: str) -> float | None:
    match = re.match(r"\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*$", score or "")
    if not match:
        return None
    return float(int(match.group(1)) + int(match.group(2)))


def evaluate_signal_result(direction: str, live_line: float, final_total: float) -> str:
    direction_key = (direction or "").strip().upper()
    if abs(final_total - live_line) < 0.0001:
        return "İade"
    if direction_key == "ALT":
        return "Başarılı" if final_total < live_line else "Başarısız"
    if direction_key in {"ÜST", "UST"}:
        return "Başarılı" if final_total > live_line else "Başarısız"
    return ""


class AiscoreFinishedMatchChecker:
    def __init__(
        self,
        *,
        browser_mode: str,
        cdp_url: str,
        cdp_port: int,
        opera_binary: str,
        page_timeout_ms: int,
        concurrency: int = 4,
    ):
        self.browser_mode = browser_mode.lower()
        self.cdp_url = cdp_url
        self.cdp_port = cdp_port
        self.opera_binary = opera_binary
        self.page_timeout_ms = page_timeout_ms
        self.concurrency = concurrency
        self._opera_process = None

    async def _launch_headless_context(self, playwright):
        try:
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
                viewport={"width": 1600, "height": 1000},
                locale="en-US",
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            return browser, context, True
        except Exception as exc:
            raise RuntimeError(
                "Headless Chromium could not be started. "
                "Run 'playwright install chromium' on the server or configure a working Opera CDP target."
            ) from exc

    def _find_opera_binary(self) -> str:
        if self.opera_binary:
            return self.opera_binary

        flatpak_export = "/var/lib/flatpak/exports/bin/com.opera.Opera"
        if os.path.exists(flatpak_export):
            return flatpak_export

        found = shutil.which("opera")
        if found:
            return found

        try:
            result = subprocess.run(
                ["flatpak", "list", "--app", "--columns=application"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if "com.opera.Opera" in result.stdout:
                return "flatpak run --branch=stable --arch=x86_64 com.opera.Opera"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        if platform.system() == "Windows":
            candidates = [
                os.path.expandvars(r"%LOCALAPPDATA%\Programs\Opera\opera.exe"),
                os.path.expandvars(r"%PROGRAMFILES%\Opera\opera.exe"),
                os.path.expandvars(r"%PROGRAMFILES(X86)%\Opera\opera.exe"),
            ]
            for candidate in candidates:
                if os.path.isfile(candidate):
                    return candidate

        raise RuntimeError("Opera not found. Set OPERA_BINARY in .env or install Opera.")

    def _is_opera_cdp_alive(self) -> bool:
        import http.client
        import socket

        try:
            with socket.create_connection(("127.0.0.1", self.cdp_port), timeout=2):
                pass
        except (ConnectionRefusedError, OSError):
            return False

        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.cdp_port, timeout=5)
            conn.request("GET", "/json/version")
            resp = conn.getresponse()
            conn.close()
            return resp.status == 200
        except Exception:
            return False

    def _launch_opera(self):
        if self._is_opera_cdp_alive():
            return

        binary = self._find_opera_binary()
        user_data_dir = os.path.expanduser("~/.opera-cdp-profile")

        cmd_parts = binary.split() if " " in binary else [binary]
        args = cmd_parts + [
            f"--remote-debugging-port={self.cdp_port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--disable-popup-blocking",
            "--start-minimized",
        ]

        logger.info("Launching Opera for finished match checks.")
        self._opera_process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        for _ in range(30):
            time.sleep(1)
            if self._is_opera_cdp_alive():
                return

        raise RuntimeError(f"Opera CDP port {self.cdp_port} did not become ready.")

    async def _build_context(self, playwright):
        if self.browser_mode == "headless":
            return await self._launch_headless_context(playwright)

        try:
            self._launch_opera()
            browser = await playwright.chromium.connect_over_cdp(self.cdp_url)
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            return browser, context, False
        except Exception as exc:
            logger.warning(
                "Opera/CDP finished-match check is unavailable, falling back to headless Chromium: %s",
                exc,
            )
            return await self._launch_headless_context(playwright)

    async def check_matches(self, tracked_matches: list[dict]) -> list[dict]:
        if not tracked_matches:
            return []

        results = []
        async with async_playwright() as playwright:
            browser, context, should_close_browser = await self._build_context(playwright)
            try:
                for index in range(0, len(tracked_matches), self.concurrency):
                    batch = tracked_matches[index:index + self.concurrency]
                    batch_results = await asyncio.gather(
                        *(self._check_single(context, match) for match in batch),
                        return_exceptions=True,
                    )
                    for item in batch_results:
                        if isinstance(item, dict):
                            results.append(item)
                        elif isinstance(item, Exception):
                            logger.debug("Finished check skipped due to error: %s", item)
            finally:
                if should_close_browser:
                    await browser.close()
        return results

    async def _check_single(self, context, match: dict) -> dict | None:
        page = await context.new_page()
        page.set_default_timeout(self.page_timeout_ms)

        try:
            await page.goto(match["url"], wait_until="domcontentloaded")
            await page.wait_for_timeout(2500)
            parsed = await page.evaluate(
                r"""
                () => {
                  const text = s => (s || '').replace(/\s+/g, ' ').trim();
                  const smallTexts = Array.from(document.querySelectorAll('span, div, strong, b'))
                    .map(el => text(el.innerText))
                    .filter(value => value.length > 0 && value.length < 50);

                  const exactFinished = smallTexts.find(value =>
                    /^(FT|Finished|Ended|Final|Full Time)$/i.test(value)
                  ) || '';

                  const liveStatus = smallTexts.find(value =>
                    /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)\s*[-\s]?\s*\d{1,2}:\d{2}$/i.test(value)
                    || /^(Q[1-4]|[1-4]Q|OT|HT|1st|2nd|3rd|4th)$/i.test(value)
                    || /^\d{1,2}:\d{2}$/.test(value)
                  ) || '';

                  const status = exactFinished || liveStatus;

                  let score = '';
                  const directScore = smallTexts.find(value => /^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(value));
                  if (directScore) {
                    score = directScore;
                  }

                  if (!score) {
                    const topNumbers = Array.from(document.querySelectorAll('span, div, strong, b'))
                      .filter(el => {
                        const rect = el.getBoundingClientRect();
                        return rect.top < 220 && el.children.length === 0;
                      })
                      .map(el => ({
                        text: text(el.innerText),
                        size: parseFloat(window.getComputedStyle(el).fontSize) || 0
                      }))
                      .filter(item => /^\d{1,3}$/.test(item.text))
                      .sort((a, b) => b.size - a.size);

                    if (topNumbers.length >= 2 && topNumbers[0].size >= 16) {
                      score = `${topNumbers[0].text} - ${topNumbers[1].text}`;
                    }
                  }

                  const finishBadge = document.querySelector(
                    '[class*="finished"], [class*="Finished"], [class*="ended"], [class*="Ended"], [class*="final-score"], [class*="finalScore"]'
                  );

                  const title = text(document.title || '')
                    .replace(/\s*\|.*/, '')
                    .replace(/\s*-\s*AiScore.*/i, '')
                    .replace(/\s*live score.*/i, '')
                    .trim();

                  const isFinished = Boolean(exactFinished || finishBadge);
                  return { status, score, isFinished, title };
                }
                """
            )
            if not parsed:
                return None

            return {
                "match_id": match["match_id"],
                "match_name": parsed.get("title") or match.get("match_name", ""),
                "status": parsed.get("status") or "",
                "score": parsed.get("score") or "",
                "is_finished": bool(parsed.get("isFinished")),
            }
        except PlaywrightTimeoutError:
            logger.debug("Finished match check timeout: %s", match.get("url"))
            return None
        except Exception as exc:
            logger.debug("Could not check match %s: %s", match.get("match_id"), exc)
            return None
        finally:
            await page.close()


async def run_finished_match_cycle(db, config) -> dict:
    tracked_matches = db.get_tracked_deleted_matches(limit=config.FINISHED_MATCH_BATCH_SIZE)
    logger.info("Tracking %s deleted matches for finish checks.", len(tracked_matches))

    if not tracked_matches:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "archived_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
        }

    checker = AiscoreFinishedMatchChecker(
        browser_mode=config.BROWSER_MODE,
        cdp_url=config.OPERA_CDP_URL,
        cdp_port=config.OPERA_CDP_PORT,
        opera_binary=config.OPERA_BINARY,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=4,
    )

    results = await checker.check_matches(tracked_matches)
    summary = {
        "tracked_count": len(tracked_matches),
        "checked_count": len(results),
        "finished_match_count": 0,
        "archived_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "pending_count": 0,
        "details": [],
    }

    for result in results:
        if not result.get("is_finished"):
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug(
                "Match finished but final score could not be parsed yet: %s",
                result.get("match_id"),
            )
            continue

        summary["finished_match_count"] += 1
        pending_alerts = db.get_pending_deleted_alerts_for_match(result["match_id"])

        for alert in pending_alerts:
            signal_result = evaluate_signal_result(
                alert.get("direction", ""),
                float(alert.get("live") or 0),
                final_total,
            )
            inserted_id = db.archive_finished_alert(
                alert,
                final_status=result.get("status", ""),
                final_score=final_score,
                final_total=final_total,
                result=signal_result,
            )
            if not inserted_id:
                continue

            summary["archived_count"] += 1
            if signal_result == "Başarılı":
                summary["successful_count"] += 1
            elif signal_result == "Başarısız":
                summary["failed_count"] += 1
            elif signal_result == "İade":
                summary["push_count"] += 1
            else:
                summary["pending_count"] += 1

            summary["details"].append({
                "match_id": alert["match_id"],
                "match_name": alert["match_name"],
                "direction": alert["direction"],
                "live_line": float(alert["live"]),
                "final_score": final_score,
                "final_total": final_total,
                "result": signal_result,
            })

    return summary
