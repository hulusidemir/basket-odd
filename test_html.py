import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.aiscore.com/basketball/match-boston-celtics-philadelphia-76ers/527rjsw2wnla4ke/odds", wait_until="domcontentloaded")
        await page.wait_for_timeout(3000)
        html = await page.content()
        with open("odds_dump.html", "w") as f:
            f.write(html)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
