import asyncio
import time
from datetime import datetime

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from common.db_helper import DatabaseHelper
from common.s3_helper import S3Helper
from common.network_helper import NetworkHelper
from config.config import config
from crawlee._request import Request, RequestOptions
from crawlee.proxy_configuration import ProxyConfiguration
from urllib.parse import urlparse, urlunparse

s3 = S3Helper()
network_helper = NetworkHelper()
db_helper = DatabaseHelper(
    host=config.MARIADB_HOST,
    port=config.MARIADB_PORT,
    user=config.MARIADB_USER,
    password=config.MARIADB_PASS,
    database="mydatabase"
)   

async def failed_request_handler(context: BeautifulSoupCrawlingContext, error):
    status_code = getattr(error, "status_code", None)
    context.log.warning(f"⚠️ Request lỗi ({status_code}), đổi proxy...")

    try:
        # Delay tránh bị chặn tiếp
        time.sleep(10)
        request = context.request

        request_options = RequestOptions(url=request.url, label=request.label, user_data=request.user_data, unique_key=request.unique_key)
        request = Request.from_url(**request_options)
        await context.add_requests(requests=[request], forefront=True)

    except Exception as e:
        context.log.error(f"❌ Không đổi proxy được: {e}")

async def process_link(context: BeautifulSoupCrawlingContext):
    url = context.request.url
    soup = context.soup

    # Lấy list href
    hrefs = [
        str("https://masothue.com" + a.get("href"))
        for a in soup.select("#main > section > div > table > tbody > tr > td:nth-child(1) > a")
    ]

    # Lấy list text (masothue)
    ma_nganh = [
        a.get_text(strip=True)
        for a in soup.select("#main > section > div > table > tbody > tr > td:nth-child(1) > a")
    ]

    ten_nganh = [
        a.get_text(strip=True)
        for a in soup.select("#main > section > div > table > tbody > tr > td:nth-child(2) > a")
    ] 

    # Ghép thành list tuple (masothue, link)
    nganh_nghe_data = [
        (x, y, z, datetime.now()) for x, y, z in zip(ma_nganh, ten_nganh, hrefs)
    ]

    sql = """
        INSERT INTO industry (ma_nganh, ten_nganh, link, crawled_at)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        ten_nganh = VALUES(ten_nganh),
        link = VALUES(link),
        crawled_at = VALUES(crawled_at)
    """

    # Insert nhiều record cùng lúc
    if nganh_nghe_data:
        db_helper.executemany(sql, nganh_nghe_data)
        print(f"✅ INSERT {len(hrefs)} links từ {url}")
    if len(hrefs) == 60:
        base_url, current_page = url.split("?page=")
        current_page = int(current_page)
        next_page = current_page + 1
        next_url = f"{base_url}?page={next_page}"
        await context.add_requests([next_url], forefront=True)
    else:
        return

async def main():
    # host, port, user, password = network_helper.get_proxy()
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            f"http://user18634:1756989016@103.179.188.215:18634/",
            f"http://user18733:1756989016@103.179.188.215:18733/"
        ],
    )
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=999999,
        proxy_configuration=proxy_configuration
    )
    
    # host, port, user, password = network_helper.get_proxy()
    crawler.failed_request_handler(handler=failed_request_handler)

    crawler.router.default_handler(process_link)

    # lấy batch link từ DB
    await crawler.run(['https://masothue.com/tra-cuu-ma-so-thue-theo-nganh-nghe/?page=1'])

if __name__ == "__main__":
    asyncio.run(main())