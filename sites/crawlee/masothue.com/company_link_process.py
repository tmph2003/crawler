import asyncio
import time

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from common.db_helper import DatabaseHelper
from common.s3_helper import S3Helper
from common.network_helper import NetworkHelper
from config.config import config
from crawlee._request import Request, RequestOptions
from crawlee.proxy_configuration import ProxyConfiguration

s3 = S3Helper()
network_helper = NetworkHelper()
db_helper = DatabaseHelper(
    host=config.MARIADB_HOST,
    port=config.MARIADB_PORT,
    user=config.MARIADB_USER,
    password=config.MARIADB_PASS,
    database="mydatabase"
)   

def update_flag(table: str, link: str):
    """
    Set flag=True for the given link in the specified table.
    """
    sql = f"""
        UPDATE {table}
        SET flag = True
        WHERE link = '{link}'
    """
    db_helper.execute(sql)

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
        for a in soup.select("#main > section > div > div.tax-listing > div > h3 > a")
    ]

    # Lấy list text (masothue)
    titles = [
        a.get_text(strip=True)
        for a in soup.select("#main > section > div > div.tax-listing > div > div > a")
    ]

    # Ghép thành list tuple (masothue, link)
    data_company = [
        (t, h) for t, h in zip(titles, hrefs)
    ]

    sql = """
        INSERT INTO company_link (masothue, link)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE link = VALUES(link)
    """

    # Insert nhiều record cùng lúc
    if data_company:
        db_helper.executemany(sql, data_company)
        print(f"✅ INSERT {len(hrefs)} links từ {url}")
    if len(hrefs) == 25:
        if "?page=" not in url:
            url = url + "?page=1"
        base_url, current_page = url.split("?page=")
        current_page = int(current_page)
        if current_page >= 10:
            print(f"⛔ Dừng crawl tại trang {current_page} của {base_url}")
            update_flag("ward", url)
            return
        next_page = current_page + 1
        next_url = f"{base_url}?page={next_page}"
        await context.add_requests([next_url], forefront=True)
    else:
        update_flag("ward", url)
        return



async def main():
    # host, port, user, password = network_helper.get_proxy()
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            f"http://user12006:0sj4@103.179.188.215:12006/",
            f"http://user11809:0sj4@103.179.188.215:11809/"
        ],
    )
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=10000,
        proxy_configuration=proxy_configuration
    )
    
    # host, port, user, password = network_helper.get_proxy()
    crawler.failed_request_handler(handler=failed_request_handler)

    crawler.router.default_handler(process_link)

    # lấy batch link từ DB
    rows = db_helper.execute("""
        SELECT link FROM ward
        WHERE flag = False
        ORDER BY link ASC
    """)
    urls = [row[0] for row in rows]
    print("=====================================")
    print(len(urls))
    print("=====================================")
    if rows:        
        batch_size = 10
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            print(f"🚀 Chạy batch {i//batch_size + 1}, gồm {len(batch)} links")
            # chạy crawler với batch này
            await crawler.run(batch)

if __name__ == "__main__":
    asyncio.run(main())