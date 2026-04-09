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
        # We navigate directly to the match page, not the /odds page
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)
        parsed = await page.evaluate(
            r'''
            () => {
              const text = s => (s || '').replace(/\s+/g, ' ').trim();
              let status = '';
              let score = '';
              
              // Find status
              const statusNode = document.querySelector('.status, .period, .match-status, .time, .state, .V3MatchHeader_matchStatus__Gj\\+5j');
              if (statusNode) status = text(statusNode.innerText);
              
              let isFinished = /\b(FT|Finished|Ended|End|O\.T\.)\b/i.test(status);
              if (!isFinished) {
                  const txt = document.body.innerText;
                  if (/\b(FT|Finished|Ended|End)\b/i.test(txt)) isFinished = true;
                  const finishedBadge = document.querySelector('[class*="finished"], [class*="Finished"], [class*="ended"], [class*="final-score"]');
                  if (finishedBadge) isFinished = true;
              }

              // Evaluate safe full score
              // Strategy 1: Look for big standalone numbers near the top of the page (Aiscore layout)
              const topEls = Array.from(document.querySelectorAll('span, div, b, strong'))
                .filter(el => {
                  const rect = el.getBoundingClientRect();
                  return rect.top < 300 && el.children.length === 0;
                })
                .map(el => ({el, txt: text(el.innerText).trim()}))
                .filter(({txt}) => /^\d{1,3}$/.test(txt));
              
              if (topEls.length >= 2) {
                const withSize = topEls.map(({el, txt}) => ({
                  txt,
                  size: parseFloat(window.getComputedStyle(el).fontSize) || 0
                })).sort((a, b) => b.size - a.size);
                if (withSize.length >= 2 && withSize[0].size >= 16) {
                  score = withSize[0].txt + '-' + withSize[1].txt;
                }
              }

              // Strategy 2: class score wrapper
              if (!score) {
                 const scoreContainer = document.querySelector('[class*="score" i], [class*="Score"], [class*="matchScore"], .V3MatchHeader_scorebox__k5vF\\+');
                 if (scoreContainer) {
                   const nums = [];
                   scoreContainer.querySelectorAll('*').forEach(el => {
                     if (el.children.length === 0) {
                       const t = text(el.innerText).trim();
                       if (/^\d{1,3}$/.test(t) && parseInt(t) <= 300) nums.push(t);
                     }
                   });
                   if (nums.length >= 2) score = nums[0] + '-' + nums[1];
                 }
              }

              // Fallback match: avoid numbers under 50 to not match quarter scores randomly
              if (!score && isFinished) {
                  const match = document.body.innerText.match(/(?:\n|^|\s)((?:\d{3})|[4-9]\d)\s*[-:–]\s*((?:\d{3})|[4-9]\d)(?:\n|$|\s)/);
                  if (match) score = match[1] + '-' + match[2];
              }

              return { isFinished, score: score, status: status };
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
