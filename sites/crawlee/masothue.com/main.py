import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from common.db_helper import DatabaseHelper
from common.s3_helper import S3Helper
from config.config import config
from crawlee._request import Request, RequestOptions


s3 = S3Helper()
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

async def main() -> None:
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=1000,
    )

    # Handler mặc định -> lấy link province
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
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
        await context.enqueue_links(selector="#sidebar > aside.widget.widget_categories.container > ul > li > a",
                                    label="province")
        

    # Handler cho province -> lấy district
    @crawler.router.handler("province")
    async def province_handler(context: BeautifulSoupCrawlingContext) -> None:
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
        if links:
            # INSERT và check trùng link
            insert_links(table="district", links=links)
            # UPDATE flag = True khi đã crawl
            update_flag(table="province", link=context.request.url)
            context.log.info(f"✅ Inserted {len(links)} district links from {context.request.url}")
            
            # Check đã crawl chưa, chưa thì cho vào enqueue [TODO: flag = False THEN crawl]
            requests = []
            for link in links:
                row = db_helper.execute(f"SELECT flag FROM district WHERE link = '{link}'")
                if row and not row[0][0]:
                    request_options = RequestOptions(url=link, label="district")
                    request = Request.from_url(**request_options)
                    requests.append(request)
            await context.enqueue_links(requests=requests)
        
        

    @crawler.router.handler("district")
    async def district_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing district: {context.request.url}')
        
        if db_helper.execute(f"SELECT flag FROM district WHERE link = '{context.request.url}'")[0][0] == False:
            soup = context.soup
            links = [
                "https://masothue.com" + a.get("href")
                for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
            ]

            if links:
                # INSERT và check trùng link
                sql = """
                    INSERT INTO ward (link, flag) VALUES (%s, False)
                    ON DUPLICATE KEY UPDATE
                    id = id
                """
                values = [(link,) for link in links]
                db_helper.executemany(sql, values)
                # UPDATE flag = True khi đã crawl
                db_helper.execute(f"""
                    UPDATE district
                    SET flag = True
                    WHERE link = '{context.request.url}'
                """)
            context.log.info(f"✅ Inserted {len(links)} ward links from {context.request.url}")

    # Chạy crawler sau khi đã khai báo đủ handler
    await crawler.run(['https://masothue.com'])

if __name__ == '__main__':
    asyncio.run(main())
