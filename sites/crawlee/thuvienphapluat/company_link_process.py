import asyncio
from datetime import datetime

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from common.db_helper import DatabaseHelper
from common.s3_helper import S3Helper
from common.network_helper import NetworkHelper
from config.config import config
from crawlee._request import Request, RequestOptions
from crawlee.proxy_configuration import ProxyConfiguration
from urllib.parse import urlparse, urlunparse
from dateutil.relativedelta import relativedelta

s3 = S3Helper()
network_helper = NetworkHelper()
db_helper = DatabaseHelper(
    host=config.MARIADB_HOST,
    port=config.MARIADB_PORT,
    user=config.MARIADB_USER,
    password=config.MARIADB_PASS,
    database="thuvienphapluat"
)

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    # Giữ lại scheme, domain, path; bỏ query (?...) và fragment (#...)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))

def get_max_ngay_cap() -> str:
    # lấy batch link từ DB
    max_ngay_cap = db_helper.execute("""
        SELECT MAX(STR_TO_DATE(CONCAT('01/', ngay_cap), '%d/%m/%Y')) AS max_ngay_cap
        FROM company_link
    """)[0][0]
    return max_ngay_cap or '01/01/2000'

async def failed_request_handler(context: BeautifulSoupCrawlingContext, error):
    status_code = getattr(error, "status_code", None)
    context.log.warning(f"⚠️ Request lỗi ({status_code}), đổi proxy...")

    try:
        # Delay tránh bị chặn tiếp
        await asyncio.sleep(10)
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
        for a in soup.select("#dvResultSearch > table > tbody > tr > td:nth-child(2) > a")
    ]

    ma_so_thue = [
        a.get_text(strip=True)
        for a in soup.select("#dvResultSearch > table > tbody > tr > td:nth-child(2) > a")
    ]

    ten_cong_ty = [
        a.get_text(strip=True)
        for a in soup.select("#dvResultSearch > table > tbody > tr > td:nth-child(3) > a")
    ]

    ngay_cap = [
        a.get_text(strip=True)
        for a in soup.select("#dvResultSearch > table > tbody > tr > td:nth-child(4) > a")
    ]

    # Ghép thành list tuple
    data_company = [
        (x, y, z, t, datetime.now()) for x, y, z, t in zip(ma_so_thue, ten_cong_ty, ngay_cap, hrefs)
    ]

    sql = """
        INSERT INTO company_link (ma_so_thue, ten_cong_ty, link, ngay_cap, crawled_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        ngay_cap = VALUES(ngay_cap),
        ten_cong_ty = VALUES(ma_so_thue),
        link = VALUES(link),
        crawled_at = VALUES(crawled_at)
    """

    # Insert nhiều record cùng lúc
    if data_company:
        db_helper.executemany(sql, data_company)
        print(f"✅ INSERT {len(hrefs)} links từ {url}")

    if len(hrefs) == 50:
        parts = url.split("page=")
        current_page = int(parts[1].split("&")[0])
        next_page = current_page + 1
        next_url = url.replace(f"page={current_page}", f"page={next_page}")

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
        max_requests_per_crawl=10000,
        proxy_configuration=proxy_configuration,
    )
    
    # host, port, user, password = network_helper.get_proxy()
    crawler.failed_request_handler(handler=failed_request_handler)

    crawler.router.default_handler(process_link)

    max_ngay_cap = get_max_ngay_cap()
    dt = datetime.strptime(max_ngay_cap, "%d/%m/%Y")
    dt_plus_1 = dt + relativedelta(months=1)
    next_ngay_cap = dt_plus_1.strftime("%d/%m/%Y")

    await crawler.run([f"https://thuvienphapluat.vn/ma-so-thue/tra-cuu-ma-so-thue-doanh-nghiep?timtheo=ma-so-thue&tukhoa=&ngaycaptu={max_ngay_cap}&ngaycapden={next_ngay_cap}&ngaydongmsttu=&ngaydongmstden=&vondieuletu=&vondieuleden=&loaihinh=0&nganhnghe=0&tinhthanhpho=0&quanhuyen=0&phuongxa=0&page=1&pageSize=50"])

if __name__ == "__main__":
    asyncio.run(main())