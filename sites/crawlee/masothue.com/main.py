import asyncio

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

def insert_links(table: str, links: list):
    """
    Insert links into the specified table with flag=False.
    Ignores duplicates.
    """
    if not links:
        return
    sql = f"""
        INSERT INTO {table} (link, flag) VALUES (%s, False)
        ON DUPLICATE KEY UPDATE id = id
    """
    values = [(link,) for link in links]
    db_helper.executemany(sql, values)

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
        # Lấy proxy mới
        host, port, user, password = network_helper.get_proxy()
        context.proxy_info.url = [f"http://{user}:{password}@{host}:{port}/"]                                                   

        # Delay tránh bị chặn tiếp
        await asyncio.sleep(5)
        request = context.request

        request_options = RequestOptions(url=request.url, label=request.label, user_data=request.user_data, unique_key=request.unique_key)
        request = Request.from_url(**request_options)
        await context.enqueue_links(requests=[request])

    except Exception as e:
        context.log.error(f"❌ Không đổi proxy được: {e}")

async def main() -> None:
    host, port, user, password = network_helper.get_proxy()
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            f"http://{user}:{password}@{host}:{port}/"
        ],
    )
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=10000,
        proxy_configuration=proxy_configuration
    )
    
    host, port, user, password = network_helper.get_proxy()
    crawler._proxy_configuration._proxy_urls = f"http://{user}:{password}@{host}:{port}/"
    crawler.failed_request_handler(handler=failed_request_handler)

    # Handler mặc định -> lấy link province
    @crawler.router.default_handler
    async def province_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing {context.request.url} ...')
        # Get link from navigation pane
        soup = context.soup
        links = [
            "https://masothue.com" + a.get("href")
            for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
        ]
        links_exist = [row[0] for row in db_helper.execute(f"SELECT link FROM province")]
        links_new = [link for link in links if link not in links_exist]
        if links_new:
            insert_links(table="province", links=links)
            context.log.info(f"✅ Inserted {len(links_new)} province links")

        # enqueue links tới province handler
        all_link = list(set(links_exist + links_new))
        requests = []
        for link in all_link:
            request_options = RequestOptions(url=link, label="province")
            request = Request.from_url(**request_options)
            requests.append(request)
        await context.enqueue_links(requests=requests)
        

    # Handler cho province -> lấy district
    @crawler.router.handler("province")
    async def district_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing province: {context.request.url}')
        # # Check đã crawl chưa [TODO: flag = False THEN crawl]
        # if db_helper.execute(f"SELECT flag FROM province WHERE link = '{context.request.url}'")[0][0] == False:
        # Get link from navigation pane
        soup = context.soup
        links = [
            "https://masothue.com" + a.get("href")
            for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
        ]
        links_exist = [row[0] for row in db_helper.execute(f"SELECT link FROM district")]
        links_new = [link for link in links if link not in links_exist]
        if links_new:
            insert_links(table="district", links=links)
            context.log.info(f"✅ Inserted {len(links_new)} district links")
        
        # enqueue links tới province handler
        all_link = [row[0] for row in db_helper.execute(f"SELECT link FROM district WHERE flag = False")]
        requests = []
        for link in all_link:
            request_options = RequestOptions(url=link, label="district")
            request = Request.from_url(**request_options)
            requests.append(request)
        await context.enqueue_links(requests=requests)
        

    @crawler.router.handler("district")
    async def ward_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing district: {context.request.url}')
        
        soup = context.soup
        links = [
            "https://masothue.com" + a.get("href")
            for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
        ]
        links_exist = [row[0] for row in db_helper.execute(f"SELECT link FROM ward")]
        links_new = [link for link in links if link not in links_exist]
        if links_new:
            insert_links(table="ward", links=links)
            update_flag(table="district", link=context.request.url)
            context.log.info(f"✅ Inserted {len(links_new)} ward links")
        
        # enqueue links tới province handler
        # all_link = list(set(links_exist + links_new))
        # requests = []
        # for link in all_link:
        #     request_options = RequestOptions(url=link, label="ward")
        #     request = Request.from_url(**request_options)
        #     requests.append(request)
        # await context.enqueue_links(requests=requests)

    # Chạy crawler sau khi đã khai báo đủ handler
    await crawler.run(['https://masothue.com'])

if __name__ == '__main__':
    asyncio.run(main())
