import aiohttp
import socket


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


async def create_session():
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        limit=10,
        force_close=True
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=HEADERS
    )