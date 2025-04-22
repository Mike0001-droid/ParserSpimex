import asyncio
import asyncpg
import time
import xlrd
from .config import DB_CONFIG
from .utils.utils import download_file, get_report_links, extract_report_data
from .utils.session import create_session
from .utils.database import save_to_db


BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"


async def scrape_reports(start: int = 1, end: int = 20) -> int:
    print("Запуск парсера...")
    start_time = time.time()
    total_inserted = 0
    
    try:
        async with await create_session() as session:
            pool = await asyncpg.create_pool(**DB_CONFIG)
            for page in range(start, end + 1):
                page_url = f"{BASE_URL}?page=page-{page}"
                print(f"Обработка страницы {page}...")
                
                try:
                    async with session.get(page_url) as response:
                        html = await response.text()                    
                    links_and_dates = get_report_links(html)
                    
                    download_tasks = [
                        download_file(session, url, date)
                        for url, date in links_and_dates
                        if date.year >= 2023
                    ]
                    downloaded_files = await asyncio.gather(*download_tasks)
                    
                    for file_info in zip(downloaded_files, links_and_dates):
                        filename, (url, report_date) = file_info
                        if not filename:
                            continue
                        
                        report_data = await asyncio.to_thread(extract_report_data, filename)
                        if not report_data:
                            continue
                        
                        inserted = await save_to_db(pool, report_data, report_date)
                        total_inserted += inserted
                        print(f"Добавлено {inserted} записей из {filename}")
                
                except Exception as e:
                    print(f"Ошибка на странице {page}: {e}")
                    continue
            
            await pool.close()
    
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    
    print(f"\nЗавершено за {time.time() - start_time:.2f} сек")
    print(f"Всего добавлено записей: {total_inserted}")
    return total_inserted