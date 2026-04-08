import asyncio
import logging
import re
from playwright.async_api import async_playwright

from config import Config
from db import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("result_checker")

def calculate_success(direction: str, inplay_total: float, final_score: str) -> str:
    match = re.match(r'(\d+)\s*[-–]\s*(\d+)', final_score.strip())
    if not match:
        return ""
    total = int(match.group(1)) + int(match.group(2))
    
    if direction == "ALT":
        return "Başarılı" if total < inplay_total else "Başarısız"
    elif direction == "ÜST":
        return "Başarılı" if total > inplay_total else "Başarısız"
    return ""

async def check_match_result(context, url: str) -> dict:
    """A simplified headless check using playwright to extract score and status."""
    page = await context.new_page()
    page.set_default_timeout(30000)
    try:
        odds_url = url.rstrip("/") + "/odds"
        await page.goto(odds_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        
        parsed = await page.evaluate(
            r'''
            () => {
              const text = s => (s || '').replace(/\s+/g, ' ').trim();
              
              let status = '';
              const statusCandidates = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => txt.length > 0 && txt.length < 30);
                
              const periodOnly = statusCandidates
                  .map(({txt}) => txt)
                  .find(v => /^(Q[1-4]|[1-4]Q|OT|HT|FT|1st|2nd|3rd|4th|Finished|Ended)$/i.test(v.trim()));
              if (periodOnly) status = periodOnly.trim();
              
              let isFinished = /\b(FT|Finished|Ended)\b/i.test(status);
              if (!isFinished) {
                const finishedBadge = document.querySelector(
                  '[class*="finished"], [class*="Finished"], [class*="ended"], [class*="final-score"]'
                );
                if (finishedBadge) isFinished = true;
              }
              
              let score = '';
              const scoreEls = Array.from(document.querySelectorAll('span, div'))
                .map(e => ({el: e, txt: text(e.innerText)}))
                .filter(({txt}) => {
                  if (!/^\d{1,3}\s*[-–]\s*\d{1,3}$/.test(txt.trim())) return false;
                  const parts = txt.trim().split(/\s*[-–]\s*/);
                  return parts.length === 2 && parseInt(parts[0]) <= 300 && parseInt(parts[1]) <= 300;
                });
              if (scoreEls.length > 0) {
                score = scoreEls[0].txt.trim();
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
              return { isFinished, score: score };
            }
            '''
        )
        return parsed
    except Exception as e:
        logger.error(f"Error checking {url}: {e}")
        return {"isFinished": False, "score": ""}
    finally:
        await page.close()


async def check_all_pending(older_than_minutes: int = 45):
    config = Config()
    db = Database(config.DB_PATH)
    db.init() # Ensure DB is migrated just in case this starts independently
    alerts = db.get_pending_alerts(older_than_minutes=older_than_minutes)
    
    if not alerts:
        logger.info("No pending alerts to check.")
        return 0

    logger.info(f"Found {len(alerts)} pending alerts. Checking statuses...")
    checked_count = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        
        for alert in alerts:
            logger.info(f"Checking {alert['match_name']} - {alert['url']}")
            res = await check_match_result(context, alert['url'])
            
            if res.get('isFinished') and res.get('score'):
                final_score = res['score']
                result_status = calculate_success(
                    alert['direction'], 
                    alert['live'], 
                    final_score
                )
                if result_status:
                    db.update_alert_result(alert['id'], final_score, result_status)
                    logger.info(f"Updated {alert['match_name']}: {final_score} -> {result_status}")
                    checked_count += 1
                else:
                    logger.warning(f"Could not calculate success for {alert['match_name']} using score {final_score}")
            else:
                logger.info(f"{alert['match_name']} is not finished yet or score not found.")
                
            await asyncio.sleep(4)
            
        await browser.close()
        
    return checked_count

async def run_checker():
    logger.info("Starting Result Checker worker. It will check match results every 30 minutes in the background.")
    
    while True:
        try:
            await check_all_pending()
        except Exception as e:
            logger.error(f"Result checker error: {e}", exc_info=True)
            
        await asyncio.sleep(1800) # 30 minutes


if __name__ == "__main__":
    asyncio.run(run_checker())
