import asyncio
from playwright.async_api import async_playwright
import json
import re

async def check_match_result(context, url: str) -> dict:
    page = await context.new_page()
    try:
        # Load the match page
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000) # give JS time to hydrate

        status_score_info = await page.evaluate("""() => {
            let status = '';
            let score = '';

            // Try common status selectors
            const statusNode = document.querySelector('.status, .period, .match-status, .time, .state');
            if (statusNode) status = statusNode.innerText.trim();
            
            // Try common score selectors
            const scoreNode = document.querySelector('.score, .match-score, .points');
            if (scoreNode) score = scoreNode.innerText.trim();

            let isFinished = /\\b(FT|Full Time|Finished|Ended|End|O\\.T\\.)\\b/i.test(status);
            
            // Check for finished badge element only
            if (!isFinished) {
               const finishedBadge = document.querySelector('[class*="finished"], [class*="Finished"], [class*="ended"], [class*="final-score"]');
               if (finishedBadge) isFinished = true;
            }
            
            // try to extract score if not found
            if (!score) {
               // guess score by looking at big headings or specific format
               const match = txt.match(/(\\d{2,3})\\s*[-:–]\\s*(\\d{2,3})/);
               if (match && !score) {
                   score = match[1] + '-' + match[2];
               }
            }

            return { status, score, isFinished, innerText: document.body.innerText.substring(0, 500) };
        }""")
        return status_score_info
    except Exception as e:
        return {"error": str(e)}
    finally:
        await page.close()

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0.0.0 Safari/537.36"
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        test_url = "https://www.aiscore.com/basketball/match-beijing-royal-fighters-vs-liaoning-flying-leopards/5p8nzh55nlpxg4k"
        print(await check_match_result(context, test_url))
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
