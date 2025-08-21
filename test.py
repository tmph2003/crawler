import asyncio
import random

from config.config import config
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from common.playwright_helper import PlaywrightHelper
from common.s3_helper import S3Helper

BASE_URL = "https://masothue.com/"
MAX_WORKERS = 10
MAX_RETRY_SECONDS = 180
s3 = S3Helper()

async def get_links_province(api: PlaywrightHelper, session_index = 0):
    html = await api.make_request(url=BASE_URL, session_index=session_index)
    _, session = api._get_session(session_index=session_index)
    await click_captcha_if_exists(session.page)
    soup = BeautifulSoup(html, "html.parser")
    links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]
    
    return links

async def get_links_district(api: PlaywrightHelper, links_province: str, session_index = 0):
    link_list = []
    for link_province in links_province:
        html = await api.make_request(url=link_province, session_index=session_index)
        soup = BeautifulSoup(html, "html.parser")
        links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]

        # Wait
        _, session = api._get_session(session_index=session_index)
        await session.page.wait_for_timeout(500)



        link_list.extend(links)
    return link_list

async def get_links_ward(api: PlaywrightHelper, links_district: str, session_index_list=None):
    link_list = []
    for link_district in links_district:
        if session_index_list is not None:
            session_index = random.choice(session_index_list)
        else:
            session_index = 0

        html = await api.make_request(url=link_district, session_index=session_index)
        soup = BeautifulSoup(html, "html.parser")
        if soup.select_one("#sidebar > aside.widget.widget_categories.container > ul > li > a"):
            links = ["https://masothue.com" + a.get("href") for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")]
            
            # Wait
            _, session = api._get_session(session_index=session_index)
            await session.page.wait_for_timeout(3000)
            
            print("===========================")
            print(links)
            print("===========================")
            link_list.extend(links)
    return link_list

async def click_captcha_if_exists(page):
    """Click captcha nếu nó xuất hiện"""
    try:
        # Chờ tối đa 5 giây để captcha xuất hiện
        await page.wait_for_selector('div.recaptcha-checkbox-spinner', timeout=5000)
        await page.click('div.recaptcha-checkbox-spinner')
        print("Captcha clicked!")
    except:
        print("No captcha found.")



async def search_users():
    async with PlaywrightHelper() as api:
        await api.create_sessions(num_sessions=3, browser="chromium", starting_url=BASE_URL, headless=False)
        links_province = await get_links_province(api=api, session_index=0)
        links_district = await get_links_district(api=api, links_province=links_province, session_index=0)
        links_ward = await get_links_ward(api=api, links_district=links_district, session_index_list=[1,2])
        print(links_ward)

if __name__ == "__main__":
    asyncio.run(search_users())
