import os
import aiohttp
import asyncio
import aiofiles
from typing import Optional
from dotenv import load_dotenv
from datetime import datetime, date
from bs4 import BeautifulSoup
import xlrd


DOWNLOAD_DIR = os.environ.get('DOWNLOAD_DIR')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


async def download_file(
        session: aiohttp.ClientSession, 
        url: str, report_date: date, 
        retries: int = 3
    ) -> Optional[str]:

    filename = f"{DOWNLOAD_DIR}/{report_date}.xls"
    for attempt in range(retries):

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(filename, 'wb') as f:
                        await f.write(await response.read())
                    return filename
                print(f"Попытка {attempt + 1}/{retries}: Ошибка HTTP {response.status} при загрузке {url}")
                await asyncio.sleep(2 ** attempt)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Попытка {attempt + 1}/{retries}: Ошибка при загрузке {url}: {str(e)}")
            await asyncio.sleep(2 ** attempt)

    return None


def get_report_links(page_html: str):
    print("Извлечение ссылок и даты...")
    soup = BeautifulSoup(page_html, "html.parser")
    report_links = []
    dates = []
    for item in soup.select(".accordeon-inner__wrap-item"):
        link = item.select_one(".accordeon-inner__item-title.link.xls")
        if link:
            report_links.append("https://spimex.com" + link.get("href"))
            date_elem = item.select_one(".accordeon-inner__item-inner__title span")
            if date_elem:
                date_str = date_elem.text.strip()
                dates.append(datetime.strptime(date_str, "%d.%m.%Y").date())
    return list(zip(report_links, dates))


def extract_report_data(file_path):
    workbook = xlrd.open_workbook(file_path)    
    sheet = workbook.sheet_by_index(0)

    data = []
    found_table = False
    headers = {}

    for row_idx in range(sheet.nrows):
        row = sheet.row_values(row_idx)
        if "".join(row).strip() == "Единица измерения: Метрическая тонна":
            found_table = True
        elif found_table:
            if any(row):
                if not headers:
                    headers = {
                        header.lower().replace("\n", " "): idx
                        for idx, header in enumerate(row)
                    }
                else:
                    exchange_product_id = row[headers["код инструмента"]]
                    exchange_product_name = row[headers["наименование инструмента"]]
                    delivery_basis_name = row[headers["базис поставки"]]
                    volume_value = row[headers["объем договоров в единицах измерения"]]
                    volume = (
                        float(volume_value)
                        if volume_value != "-" and volume_value != ""
                        else 0.0
                    )
                    total_value = row[headers["обьем договоров, руб."]]
                    total = (
                        float(total_value)
                        if total_value != "-" and total_value != ""
                        else 0.0
                    )
                    count_value = row[headers["количество договоров, шт."]]
                    count = (
                        int(count_value)
                        if count_value != "" and count_value != "-"
                        else 0
                    )
                    if count > 0 and exchange_product_id not in (
                        "Итого:",
                        "Итого по секции:",
                    ):

                        data.append(
                            {
                                "exchange_product_id": exchange_product_id,
                                "exchange_product_name": exchange_product_name,
                                "delivery_basis_name": delivery_basis_name,
                                "volume": volume,
                                "total": total,
                                "count": count,
                            }
                        )
    return data