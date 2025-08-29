import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from common.db_helper import DatabaseHelper
from common.s3_helper import S3Helper
from config.config import config

s3 = S3Helper()
db_helper = DatabaseHelper(
    host=config.MARIADB_HOST,
    port=config.MARIADB_PORT,
    user=config.MARIADB_USER,
    password=config.MARIADB_PASS,
    database="mydatabase"
)

async def main() -> None:
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=1000,
    )

    # Handler mặc định -> lấy link province
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing {context.request.url} ...')

        soup = context.soup
        links = [
            "https://masothue.com" + a.get("href")
            for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
        ]

        if links:
            sql = """
                INSERT INTO province (link, flag) VALUES (%s, False)
                ON DUPLICATE KEY UPDATE
                flag = VALUES(flag)
            """

            values = [(link,) for link in links]
            db_helper.executemany(sql, values)

        # enqueue links tới province handler
        await context.enqueue_links(selector="#sidebar > aside.widget.widget_categories.container > ul > li > a",
                                    label="province")
        context.log.info(f"✅ Inserted {len(links)} province links")

    # Handler cho province -> lấy district
    @crawler.router.handler("province")
    async def province_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing province: {context.request.url}')
        # Check đã crawl chưa [TODO: flag = False THEN crawl]
        if db_helper.execute(f"SELECT flag FROM province WHERE link = '{context.request.url}'")[0][0] == False:
            soup = context.soup
            links = [
                "https://masothue.com" + a.get("href")
                for a in soup.select("#sidebar > aside.widget.widget_categories.container > ul > li > a")
            ]
            if links:
                # INSERT và check trùng link
                sql = """
                    INSERT INTO district (link, flag) VALUES (%s, False)
                    ON DUPLICATE KEY UPDATE
                    flag = VALUES(flag)
                """
                
                values = [(link,) for link in links]
                db_helper.executemany(sql, values)
                # UPDATE flag = True khi đã crawl
                db_helper.execute(f"""
                    UPDATE province
                    SET flag = True
                    WHERE link = '{context.request.url}'
                """)

            await context.enqueue_links(selector="#sidebar > aside.widget.widget_categories.container > ul > li > a",
                                        label="district")
            context.log.info(f"✅ Inserted {len(links)} district links from {context.request.url}")

    @crawler.router.handler("district")
    async def district_handler(context: BeautifulSoupCrawlingContext) -> None:
        await asyncio.sleep(2)
        context.log.info(f'Processing district: {context.request.url}')
        # Check đã crawl chưa [TODO: flag = False THEN crawl]
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
                    flag = VALUES(flag)
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
