import asyncio
from project.main import scrape_reports


if __name__ == "__main__":
    asyncio.run(scrape_reports())