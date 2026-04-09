import asyncio
from playwright.async_api import async_playwright
from result_checker import check_match_result

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
        url = "https://www.aiscore.com/basketball/match-orlando-magic-vs-chicago-bulls/2p8nzhxng7rxj8o" # A random match just to see
        res = await check_match_result(context, url)
        print("Scrape Result:", res)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
