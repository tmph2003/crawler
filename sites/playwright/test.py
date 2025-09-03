import asyncio
import random

from config.config import config
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from common.playwright_helper import PlaywrightHelper
from common.s3_helper import S3Helper
from common.db_helper import DatabaseHelper
from typing import List

BASE_URL = "https://masothue.com/"
MAX_WORKERS = 10
MAX_RETRY_SECONDS = 180
s3 = S3Helper()
db_helper = DatabaseHelper(
    host=config.MARIADB_HOST,
    port=config.MARIADB_PORT,
    user=config.MARIADB_USER,
    password=config.MARIADB_PASS,
    database="mydatabase"
)

headers = {
    'accept': '*/*',
    'accept-language': 'vi,en;q=0.9',
    'origin': 'https://masothue.com',
    'priority': 'u=1, i',
    'referer': 'https://masothue.com/',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-storage-access': 'active',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'x-browser-channel': 'stable',
    'x-browser-copyright': 'Copyright 2025 Google LLC. All rights reserved.',
    'x-browser-validation': 'OhMsc7acNx+0w+NEQM7p961tYAw=',
    'x-browser-year': '2025',
    'x-client-data': 'CIa2yQEIprbJAQipncoBCJGVywEIlqHLAQjGo8sBCIegzQEIjYDPARjh4s4B',
}

async def click_captcha_if_exists(page):
    """Click captcha nếu nó xuất hiện"""
    try:
        # Chờ tối đa 5 giây để captcha xuất hiện
        await page.wait_for_selector('div.recaptcha-checkbox-spinner', timeout=5000)
        await page.click('div.recaptcha-checkbox-spinner')
        print("Captcha clicked!")
    except:
        print("No captcha found.")

async def get_links_province(api: PlaywrightHelper, session_index = 0):
    html = await api.make_request(url=BASE_URL, headers=headers, session_index=session_index)
    soup = BeautifulSoup(html, "html.parser")
    links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]
    for link in links:
        db_helper.execute(
            f"""
            INSERT INTO province (link) VALUES ('{link}')
            ON DUPLICATE KEY UPDATE
            link = VALUES(link)
            """
        )
    print("✅ Got", len(links), "province links")
    return links

async def get_links_district(api: PlaywrightHelper, links_province: List[str], session_index = 0):
    print("✅ Processing", len(links_province))
    links_province = db_helper.execute("SELECT DISTINCT * FROM province WHERE flag = 0")
    link_list = []
    for link_province in links_province:
        html = await api.make_request(url=link_province[0], headers=headers, session_index=session_index)
        _, session = api._get_session(session_index=session_index)
        soup = BeautifulSoup(html, "html.parser")
        links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]
        for link in links:
            db_helper.execute(
                f"""
                INSERT INTO district (link) VALUES ('{link}')
                ON DUPLICATE KEY UPDATE
                link = VALUES(link)
                """
            )
        link_list.extend(links)
        # Wait
        await session.page.wait_for_timeout(10000)
    
    return link_list

async def get_links_ward(api: PlaywrightHelper, session_index_list=None):
    link_list = []
    links_district = db_helper.execute("SELECT DISTINCT * FROM district WHERE link > 'https://masothue.com/tra-cuu-ma-so-thue-theo-tinh/thi-xa-tu-son-331'")
    for link_district in links_district:
        if session_index_list is not None:
            session_index = random.choice(session_index_list)
        else:
            session_index = 0

        html = await api.make_request(url=link_district[0], headers=headers, session_index=session_index)
        _, session = api._get_session(session_index=session_index)

        soup = BeautifulSoup(html, "html.parser")
        if soup.select_one("#sidebar > aside.widget.widget_categories.container > ul > li > a"):
            links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]
            for link in links:
                db_helper.execute(
                    f"""
                    INSERT INTO ward (link) VALUES ('{link}')
                    ON DUPLICATE KEY UPDATE
                    link = VALUES(link)
                    """
                )
            link_list.extend(links)
        # Wait
        await session.page.wait_for_timeout(1000)
    return link_list

async def search_users():
    async with PlaywrightHelper() as api:
        await api.create_sessions(num_sessions=3, browser="chromium", starting_url=BASE_URL, headless=True)
        await get_links_province(api=api, session_index=0)
        await get_links_district(api=api, session_index=0)
        await get_links_ward(api=api, session_index_list=[1,2])
        

if __name__ == "__main__":
    s3 = S3Helper()
    db_helper = DatabaseHelper(
        host=config.MARIADB_HOST,
        port=config.MARIADB_PORT,
        user=config.MARIADB_USER,
        password=config.MARIADB_PASS,
        database="mydatabase"
    )
    x = db_helper.execute(f"SELECT link FROM province where flag = 0")
    if not x:
        print("===========")
    links = [row[0] for row in x]
    print(links)