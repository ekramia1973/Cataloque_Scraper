import asyncio
import sqlite3
import aiosqlite
import csv
from parsel import Selector
from curl_cffi import requests
from curl_cffi.requests.exceptions import ConnectionError, HTTPError
from tqdm.asyncio import tqdm_asyncio
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


def init_db():
    conn = sqlite3.connect("scraped_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            part_no TEXT,
            adaptable_for TEXT,
            orig_part_no TEXT
        )
    """)
    conn.commit()
    conn.close()


async def get_existing_urls():
    """Retrieve all existing URLs from the database to speed up checks."""
    async with aiosqlite.connect("scraped_data.db") as conn:
        async with conn.execute("SELECT url FROM pages") as cursor:
            return {row[0] async for row in cursor}


@retry(
    retry=retry_if_exception_type((ConnectionError, HTTPError)),
    stop=stop_after_attempt(5),
    wait=wait_fixed(1)
)
async def save_to_sqlite(url, part_no, adaptable_for, orig_part_no):
    part_no = part_no.encode("utf-8", "ignore").decode("utf-8")
    adaptable_for = adaptable_for.encode("utf-8", "ignore").decode("utf-8")
    orig_part_no = orig_part_no.encode("utf-8", "ignore").decode("utf-8")

    async with aiosqlite.connect("scraped_data.db") as conn:
        try:
            await conn.execute(
                "INSERT INTO pages (url, part_no, adaptable_for, orig_part_no) VALUES (?, ?, ?, ?)",
                (url, part_no, adaptable_for, orig_part_no)
            )
            await conn.commit()
        except aiosqlite.IntegrityError:
            print(f"⚠️ Skipping duplicate entry: {url}")


async def fetch_url(session, url):
    try:
        response = await session.get(url)
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


async def extract_links(page):
    try:
        selector = Selector(text=page)
        return selector.css("loc::text").getall()
    except Exception as e:
        print(f"Error extracting links: {e}")
        return []


def strip_it(inp):
    while inp.strip().endswith(','):
        inp = inp.strip().strip(',')
    return inp


async def fetch_contents(session, link, existing_urls):
    if link in existing_urls:
        print(f"⏭️ Skipping already processed URL: {link}")
        return

    try:
        response = await session.get(link)
        content = response.text
        selector = Selector(text=content, encoding="utf-8")
        part_no = selector.xpath(
            '//div[@id="catalog-tab-article"]//tr[@class="rowodd"]/td[text()="Part-no."]/following-sibling::td/text()').get(
            '').strip()
        adaptable_orig_rows = selector.xpath(
            '//div[@id="catalog-tab-article"]//table[@class="produkt_passend_table"]//tr[contains(@class,"att_val_22")]')

        adaptable_for_array = []
        orig_part_no_array = []
        for item in adaptable_orig_rows:
            adaptable_for_array.append(item.css('td:nth-child(1) *::text').get('').strip())
            orig_part_no_array.append(item.css('td:nth-child(2) *::text').get('').strip())

        adaptable_for = strip_it(', '.join(adaptable_for_array))
        orig_part_no = strip_it(', '.join(orig_part_no_array))

        if part_no or adaptable_for or orig_part_no:
            await save_to_sqlite(link, part_no, adaptable_for, orig_part_no)
    except Exception as e:
        print(f"Error parsing {link}: {e}")
        return None


def export_to_csv():
    conn = sqlite3.connect("scraped_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT url, part_no, adaptable_for, orig_part_no FROM pages")
    rows = cursor.fetchall()

    with open("scraped_data.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["url", "part_no", "adaptable_for", "orig_part_no"])
        writer.writerows(rows)

    conn.close()
    print("\n📂 Data exported to scraped_data.csv successfully.")


async def main():
    urls = [
        "https://www.industriehof.com/sitemap_xml/xml_ordner/en_sitemap_a.xml",
        "https://www.industriehof.com/sitemap_xml/xml_ordner/en_sitemap_b.xml"
    ]

    init_db()
    existing_urls = await get_existing_urls()

    async with requests.AsyncSession() as session:
        print("\n📡 Fetching sitemaps...\n")
        pages = await tqdm_asyncio.gather(*[fetch_url(session, url) for url in urls])

        print("\n🔍 Extracting links from sitemaps...\n")
        sitemap_tasks = [extract_links(page) for page in pages if page]
        sitemap_links = await tqdm_asyncio.gather(*sitemap_tasks)
        extracted_sitemap_links = [link for sublist in sitemap_links for link in sublist]

        print(f"\n🌍 Found {len(extracted_sitemap_links)} links. Fetching content...\n")
        link_tasks = [fetch_contents(session, link, existing_urls) for link in extracted_sitemap_links]
        await tqdm_asyncio.gather(*link_tasks)

        print(f"\n✅ Completed: Fetched and stored {len(extracted_sitemap_links)} pages in the database.\n")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    export_to_csv()
