"""
finished_match_service.py — Shared finished match checking service.
Used by both the background worker and manual UI-triggered checks.
"""

import asyncio
import logging
import re

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


def _parse_period_from_status(status: str) -> int | None:
    """Extract period number from alert status string."""
    s = (status or "").strip().upper()
    m = re.match(r"Q([1-4])", s)
    if m:
        return int(m.group(1))
    m = re.match(r"([1-4])Q", s)
    if m:
        return int(m.group(1))
    if s == "HT":
        return 2
    m = re.match(r"([1-4])", s)
    if m:
        return int(m.group(1))
    return None


def evaluate_signal_professionally(
    alert: dict,
    final_total: float,
    signal_result: str,
) -> dict:
    """
    Profesyonel basketbol bahisçisi gözüyle sinyal değerlendirmesi.

    Çıktı:
        margin: Barem ile final toplam arasındaki fark (pozitif = bahisçi lehine)
        signal_timing_grade: Sinyalin zamanlaması ne kadar isabetli (A/B/C/D)
        market_read_correct: Market hareketi doğru okunmuş mu? (1/0)
        projection_accuracy: Kalite projeksiyon vs gerçek farkı
        quality_accuracy: Verilen kalite notunun sonuçla uyumu
        verdict: Kısa profesyonel yorum
        lesson: Gelecek sinyaller için çıkarılacak ders
    """
    direction = (alert.get("direction") or "").strip().upper()
    live = float(alert.get("live") or 0)
    opening = float(alert.get("opening") or 0)
    diff = float(alert.get("diff") or 0)
    status = alert.get("status") or ""
    quality_grade = (alert.get("quality_grade") or "").strip().upper()
    quality_score = float(alert.get("quality_score") or 0)
    signal_count = int(alert.get("signal_count") or 1)

    is_success = signal_result == "Başarılı"
    is_fail = signal_result == "Başarısız"

    # ── 1. Margin: Barem ile final toplam farkı ──
    if direction == "ALT":
        margin = round(live - final_total, 1)  # pozitif = kazanç
    elif direction in {"ÜST", "UST"}:
        margin = round(final_total - live, 1)  # pozitif = kazanç
    else:
        margin = None

    # ── 2. Signal Timing Grade ──
    period = _parse_period_from_status(status)
    if period == 2:
        timing_grade = "A"  # Q2: en ideal pencere
    elif period == 3:
        timing_grade = "A"  # Q3: ana fiyatlama penceresi
    elif period == 1:
        timing_grade = "C"  # Q1: çok erken, veri az
    elif period == 4:
        timing_grade = "B"  # Q4: geç ama varyanslı
    else:
        timing_grade = "D"  # Bilinmiyor

    # ── 3. Market Read Correct ──
    # Referans → Canlı hareketi doğru yönde mi okunmuş?
    reference = float(alert.get("prematch") or opening)
    reference_to_live_diff = live - reference
    if direction == "ALT":
        # ALT: barem yükselmiş olmalı.
        market_read_correct = 1 if reference_to_live_diff > 0 else 0
    elif direction in {"ÜST", "UST"}:
        # ÜST: barem düşmüş olmalı.
        market_read_correct = 1 if reference_to_live_diff < 0 else 0
    else:
        market_read_correct = None

    # ── 4. Quality Accuracy ──
    # Kalite notu sonucu doğru tahmin etti mi?
    high_quality = quality_grade in {"A++", "A+", "A", "B"}
    if is_success and high_quality:
        quality_accuracy = "İsabetli"
    elif is_fail and high_quality:
        quality_accuracy = "Yanıltıcı"
    elif is_success and not high_quality:
        quality_accuracy = "Sürpriz Başarı"
    elif is_fail and not high_quality:
        quality_accuracy = "Beklenen Başarısızlık"
    else:
        quality_accuracy = ""

    # ── 5. Projection Accuracy ──
    # quality_summary'den projeksiyon değerini çıkarmaya çalış
    projection_accuracy = None
    quality_summary = alert.get("quality_summary") or ""
    proj_match = re.search(r"(\d+\.?\d*)\s*[<>]", quality_summary)
    if not proj_match:
        proj_match = re.search(r"projeksiyonu?\s*[^(]*\((\d+\.?\d*)", quality_summary, re.IGNORECASE)
    if proj_match:
        try:
            projected = float(proj_match.group(1))
            projection_accuracy = round(abs(final_total - projected), 1)
        except (ValueError, TypeError):
            pass

    # ── 6. Verdict: Profesyonel yorum ──
    verdict_parts = []
    if is_success:
        if margin is not None and margin >= 10:
            verdict_parts.append(f"Güçlü kazanç ({margin:+.1f} puan marjla)")
        elif margin is not None and margin >= 3:
            verdict_parts.append(f"Rahat kazanç ({margin:+.1f} puan)")
        elif margin is not None:
            verdict_parts.append(f"Kıl payı kazanç ({margin:+.1f} puan)")
        else:
            verdict_parts.append("Başarılı sinyal")
    elif is_fail:
        if margin is not None and margin <= -10:
            verdict_parts.append(f"Ağır kayıp ({margin:+.1f} puan farkla)")
        elif margin is not None and margin <= -3:
            verdict_parts.append(f"Net kayıp ({margin:+.1f} puan)")
        elif margin is not None:
            verdict_parts.append(f"Kıl payı kayıp ({margin:+.1f} puan)")
        else:
            verdict_parts.append("Başarısız sinyal")
    else:
        verdict_parts.append("İade/Beklemede")

    if timing_grade in {"A"}:
        verdict_parts.append("zamanlama iyi")
    elif timing_grade == "C":
        verdict_parts.append("erken sinyal riski")

    verdict = " — ".join(verdict_parts)

    # ── 7. Lesson: Çıkarılacak ders ──
    lessons = []
    if is_fail and high_quality:
        lessons.append("Yüksek kalite notu tek başına yeterli değil, maç dinamiğini de izle")
    if is_fail and quality_grade == "D":
        lessons.append("D kalite ters risk bayrağı olan sinyallerde daha dikkatli ol")
    if is_success and signal_count >= 3:
        lessons.append("Tekrarlayan sinyaller (3+) güvenilirliği artırıyor")
    if is_fail and period == 1:
        lessons.append("Q1 sinyallerine daha az güven, veri penceresi yetersiz")
    if is_success and period in {2, 3}:
        lessons.append("Q2-Q3 penceresi en güvenilir zamanlama")
    if is_fail and diff < 12:
        lessons.append("Düşük fark (<12) sinyallerinde riskleri daha dikkatli tart")
    if margin is not None and abs(margin) <= 2 and is_fail:
        lessons.append("Kıl payı kayıp — sinyal yönünde hareket vardı, biraz şanssızlık")

    lesson = ". ".join(lessons[:2]) if lessons else ""

    return {
        "margin": margin,
        "signal_timing_grade": timing_grade,
        "market_read_correct": market_read_correct,
        "projection_accuracy": projection_accuracy,
        "quality_accuracy": quality_accuracy,
        "verdict": verdict,
        "lesson": lesson,
    }



class AiscoreFinishedMatchChecker:
    def __init__(
        self,
        *,
        page_timeout_ms: int,
        concurrency: int = 4,
    ):
        self.page_timeout_ms = page_timeout_ms
        self.concurrency = concurrency

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
                "Run 'playwright install chromium' on the server."
            ) from exc

    async def check_matches(self, tracked_matches: list[dict]) -> list[dict]:
        if not tracked_matches:
            return []

        results = []
        async with async_playwright() as playwright:
            browser, context, should_close_browser = await self._launch_headless_context(playwright)
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
            evaluation = evaluate_signal_professionally(
                alert, final_total, signal_result,
            )
            inserted_id = db.archive_finished_alert(
                alert,
                final_status=result.get("status", ""),
                final_score=final_score,
                final_total=final_total,
                result=signal_result,
                evaluation=evaluation,
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
                "verdict": evaluation.get("verdict", ""),
                "margin": evaluation.get("margin"),
                "lesson": evaluation.get("lesson", ""),
            })

    return summary


async def run_deleted_match_result_cycle(db, config) -> dict:
    tracked_matches = db.get_deleted_matches_for_result_check(limit=None)
    logger.info("Checking %s deleted matches for final results.", len(tracked_matches))

    if not tracked_matches:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "updated_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
        }

    return await _run_deleted_match_result_check_for_matches(db, config, tracked_matches)


async def run_single_deleted_match_result_check(db, config, alert_id: int) -> dict:
    tracked_match = db.get_deleted_match_for_result_check_by_alert_id(alert_id)
    if not tracked_match:
        return {
            "tracked_count": 0,
            "checked_count": 0,
            "finished_match_count": 0,
            "updated_count": 0,
            "successful_count": 0,
            "failed_count": 0,
            "push_count": 0,
            "details": [],
            "message": "Kontrol edilecek maç bulunamadı.",
        }

    summary = await _run_deleted_match_result_check_for_matches(db, config, [tracked_match])
    if summary["finished_match_count"] == 0:
        summary["message"] = "Maç henüz bitmemiş veya final skoru alınamadı."
    elif summary["updated_count"] == 0:
        summary["message"] = "Maç bitmiş görünüyor ama güncellenecek boş sonuç bulunamadı."
    else:
        summary["message"] = f"{summary['updated_count']} sinyal güncellendi."
    return summary


async def _run_deleted_match_result_check_for_matches(db, config, tracked_matches: list[dict]) -> dict:
    checker = AiscoreFinishedMatchChecker(
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        concurrency=4,
    )
    results = await checker.check_matches(tracked_matches)
    summary = {
        "tracked_count": len(tracked_matches),
        "checked_count": len(results),
        "finished_match_count": 0,
        "updated_count": 0,
        "successful_count": 0,
        "failed_count": 0,
        "push_count": 0,
        "details": [],
    }

    for result in results:
        if not result.get("is_finished"):
            continue

        final_score = result.get("score", "")
        final_total = parse_score_total(final_score)
        if final_total is None:
            logger.debug("Deleted match is finished but score is not parseable: %s", result.get("match_id"))
            continue

        summary["finished_match_count"] += 1
        alerts = db.get_deleted_alerts_for_result_check(result["match_id"])
        for alert in alerts:
            signal_result = evaluate_signal_result(
                alert.get("direction", ""),
                float(alert.get("live") or 0),
                final_total,
            )
            if not signal_result:
                continue

            updated = db.update_deleted_alert_final_result(
                alert["id"],
                result=signal_result,
                final_score=final_score,
                final_status=result.get("status", "") or "Full Time",
            )
            if not updated:
                continue

            summary["updated_count"] += 1
            if signal_result == "Başarılı":
                summary["successful_count"] += 1
            elif signal_result == "Başarısız":
                summary["failed_count"] += 1
            elif signal_result == "İade":
                summary["push_count"] += 1

            summary["details"].append({
                "id": alert["id"],
                "match_id": alert["match_id"],
                "match_name": alert["match_name"],
                "direction": alert["direction"],
                "live_line": float(alert["live"]),
                "final_score": final_score,
                "final_total": final_total,
                "result": signal_result,
            })

    return summary
