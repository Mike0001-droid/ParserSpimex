import asyncpg
from typing import List, Dict
from datetime import date

async def save_to_db(
        pool: asyncpg.Pool, 
        data: List[Dict], 
        report_date: date
    ) -> int:
    
    if not data:
        return 0
    
    inserted_rows = 0
    
    async with pool.acquire() as conn:
        records = []
        for item in data:
            records.append((
                str(report_date),
                str(item.get('exchange_product_name', '')),
                str(item.get('total', 0)),
                str(item.get('volume', 0)),
                item.get('delivery_basis_name', ''),
            ))

        await conn.executemany("""
            INSERT INTO spimex_trading_results 
            (date, product, price, volume, delivery_region)
            VALUES ($1, $2, $3, $4, $5)
        """, records)
        
        inserted_rows = len(records)
    
    return inserted_rows
