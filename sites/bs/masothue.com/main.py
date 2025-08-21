from config.config import config

import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_delay, wait_exponential

BASE_PATH = "https://www.masothue.com"
MAX_WORKERS = 10
MAX_RETRY_SECONDS = 180


# Retry HTTP request with exponential backoff
@retry(stop=stop_after_delay(MAX_RETRY_SECONDS), wait=wait_exponential(multiplier=1, min=1, max=60))
def get(url):
    print(f"GET: {url}")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp


def get_document(url):
    resp = get(url)
    return BeautifulSoup(resp.text, "html.parser")


def extract_company_info(url):
    company_info = {
        "name": "",
        "tax_info": {},
        "business_info": []
    }

    doc = get_document(url)

    # Extract tax info
    tax_table = doc.select_one("table.table-taxinfo")
    if tax_table:
        name_el = tax_table.select_one("th span.copy")
        if name_el:
            company_info["name"] = name_el.get_text(strip=True)

        for row in tax_table.select("tbody tr"):
            cols = [c.get_text(strip=True) for c in row.select("td")]
            if len(cols) == 2:
                company_info["tax_info"][cols[0]] = cols[1]

    # Extract business info
    for row in doc.select("table.table tbody tr"):
        cols = [c.get_text(strip=True) for c in row.select("td")]
        if len(cols) >= 2:
            company_info["business_info"].append({
                "id": cols[0],
                "carees": cols[1]
            })

    return company_info


def get_company_urls():
    urls = []
    doc = get_document(BASE_PATH + TYPE_BUSINESS_PATH)

    for row in doc.select("table tbody tr"):
        link_el = row.select_one("td:last-child a[href]")
        if link_el:
            href = link_el["href"]
            for page in range(1, 11):  # limit to 10 pages
                list_url = f"{BASE_PATH}{href}?page={page}"
                child_doc = get_document(list_url)
                for a in child_doc.select("div.tax-listing h3 a[href]"):
                    urls.append(BASE_PATH + a["href"])

    return urls


def crawl_masothue():
    all_companies = []
    company_urls = get_company_urls()

    print(f"Total company URLs found: {len(company_urls)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(extract_company_info, url): url for url in company_urls}

        for future in as_completed(future_to_url):
            try:
                data = future.result()
                all_companies.append(data)
            except Exception as e:
                print(f"Error crawling {future_to_url[future]}: {e}")

    output = {
        "company": all_companies,
        "total_company": len(all_companies)
    }

    with open(FILE_NAME, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(all_companies)} companies to {FILE_NAME}")


if __name__ == "__main__":
    crawl_masothue()
