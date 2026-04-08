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
              let status = '';
              let score = '';
              
              // 1. Try DOM elements typically present on match detail pages
              const statusNode = document.querySelector('.status, .period, .match-status, .time, .state, .V3MatchHeader_matchStatus__Gj\\+5j');
              if (statusNode) status = statusNode.innerText.trim();
              
              const scoreNode = document.querySelector('.score, .match-score, .points, .V3MatchHeader_scorebox__k5vF\\+');
              if (scoreNode) score = scoreNode.innerText.trim();
              
              let isFinished = /\b(FT|Finished|Ended|End|O\.T\.)\b/i.test(status);
              
              // 2. Fallback: Parse the raw text of the entire document
              if (!isFinished || !score || !/\d/.test(score)) {
                  const txt = document.body.innerText;
                  
                  if (/\b(FT|Finished|Ended|End)\b/i.test(txt)) {
                      isFinished = true;
                  }
                  
                  // Usually the score is near the top; look for anything like "85 - 90"
                  const match = txt.match(/(?:\n|^|\s)(\d{2,3})\s*[-:–]\s*(\d{2,3})(?:\n|$|\s)/);
                  if (match) {
                      score = match[1] + '-' + match[2];
                  }
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
