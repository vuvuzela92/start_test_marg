import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
import aiohttp
import requests
from time import sleep
from typing import Literal, Optional
from datetime import datetime, timedelta, time

from utils.my_general import to_iso_z, clean_datetime_from_timezone, save_json
from utils.logger import setup_logger
from utils.utils import load_api_tokens
from psycopg2.extras import execute_values
from utils.my_db_functions import create_connection_w_env

logger = setup_logger("deductions_to_db.log")

def to_iso(d):
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")

def parse_dt(dt):
    if dt is None:
        return None
    return datetime.fromisoformat(dt.replace("Z", ""))

def get_wb_measurements(token,
                        date_from,
                        date_to,
                        tab: Optional[Literal["penalty", "measurement"]] = None,
                        limit=1000):
    """Fetch all warehouse-measurements or penalty reports with pagination."""

    if tab not in ("penalty", "measurement"):
        raise ValueError(f"Параметр tab должен быть одним из двух - 'penalty' или 'measurement', передано {tab}")
    
    url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/warehouse-measurements"
    headers = {"Authorization": token}
    params = {
        "dateFrom": to_iso(date_from),
        "dateTo": to_iso(date_to),
        "tab": tab,
        "limit": limit,
        "offset": 0
    }

    all_reports = []

    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()["data"]

        reports = data.get("reports", [])
        all_reports.extend(reports)
        logger.info(f'Retrieved {len(reports)} rows')

        if len(reports) < limit:
            break
        
        sleep(12)
        params["offset"] += limit

    return all_reports


def insert_records(table_name, records, column_mapping, conn):
    """
    Generic insert function for PostgreSQL with rollback on error.
    
    :param table_name: str, target table
    :param records: list of dicts
    :param column_mapping: dict, {source_key: db_column_name}
    :param conn: psycopg2 connection
    """
    if not records:
        return

    db_columns = list(column_mapping.values())
    values = []

    for rec in records:
        row = []
        for key in column_mapping.keys():
            val = rec.get(key)
            if isinstance(val, str) and 'T' in val and '-' in val and 'Z' in val:
                val = parse_dt(val)
            row.append(val)
        values.append(row)

    query = f"""
        INSERT INTO {table_name} ({", ".join(db_columns)})
        VALUES %s
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


async def process_measurements_client(client: str, token: str, conn, date_from, date_to):
    try:
        # 1. Penalties (удержания)
        mode_penalty = "penalty"
        penalties = await asyncio.to_thread(
            get_wb_measurements, token, date_from, date_to, mode_penalty
        )

        if not penalties:
            logger.info(f"Нет данных за период {date_from}-{date_to}: Отчет - 'Удержания за занижение габаритов упаковки', Кабинет - {client}")
        else:
            penalties_cols = {
                "nmId": "nm_id",
                "subject": "subject",
                "dimId": "dim_id",
                "prcOver": "prc_over",
                "volume": "volume",
                "width": "width",
                "length": "length",
                "height": "height",
                "volumeSup": "volume_sup",
                "widthSup": "width_sup",
                "lengthSup": "length_sup",
                "heightSup": "height_sup",
                "photoUrls": "photo_urls",
                "dtBonus": "dt_bonus",
                "isValid": "is_valid",
                "isValidDt": "is_valid_dt",
                "reversalAmount": "reversal_amount",
                "penaltyAmount": "penalty_amount"
            }

            await asyncio.to_thread(
                insert_records,
                'deductions_warehouse_penalties',
                penalties,
                penalties_cols,
                conn
            )
            logger.info(f"Получены данные за период {date_from}-{date_to}, Внесено строк в БД: {len(penalties)}: Отчет - 'Удержания за занижение габаритов упаковки', Кабинет - {client}")

        await asyncio.sleep(12)

        # 2. Measurements (замеры ВБ)
        mode_measurements = "measurement"

        measures = await asyncio.to_thread(
            get_wb_measurements, token, date_from, date_to, mode_measurements
        )

        if not measures:
            logger.info(f"Нет данных за период {date_from}-{date_to}: Отчет - 'Замеры склада', Кабинет - {client}")
        else:
            measures_cols = {
                "nmId": "nm_id",
                "subject": "subject",
                "dimId": "dim_id",
                "prcOver": "prc_over",
                "volume": "volume",
                "width": "width",
                "length": "length",
                "height": "height",
                "volumeSup": "volume_sup",
                "widthSup": "width_sup",
                "lengthSup": "length_sup",
                "heightSup": "height_sup",
                "photoUrls": "photo_urls",
                "dt": "dt",
                "dateStart": "date_start",
                "dateEnd": "date_end"
            }

            await asyncio.to_thread(
                insert_records,
                'deductions_measurements',
                measures,
                measures_cols,
                conn
            )
            logger.info(f"Получены данные за период {date_from}-{date_to}, Внесено строк в БД: {len(penalties)}: Отчет - 'Замеры склада', Кабинет - {client}")

    except Exception as e:
        logger.error(f'Encountered an unexpected error while uploading data for client {client}: {e}')
        raise


async def get_deductions_replacements(api_key, date_from, date_to, limit=1000):
    headers = {"Authorization": api_key}
    offset = 0
    all_reports = []

    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/deductions"

    date_from = to_iso_z(date_from, t = time(0, 0, 0))
    date_to = to_iso_z(date_to, t = time(23, 59, 59))

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            params = {
                "dateTo": date_to,
                "limit": limit,
                "offset": offset,
            }
            if date_from:
                params["dateFrom"] = date_from

            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                payload = await resp.json()

            reports = payload.get("data", {}).get("reports", [])
            if not reports:
                break

            all_reports.extend(reports)

            if len(reports) < limit:
                break

            offset += limit
            await asyncio.sleep(65)  # non-blocking rate limit wait

    return all_reports


def insert_deductions_replacements(conn, data, client):
    """
    Insert a list of deduction reports into PostgreSQL.
    
    :param conn: psycopg2 connection
    :param data: list of dicts with deduction data
    """
    if not data:
        return

    # Column names in the table
    columns = [
        "dt_bonus",
        "nm_id",
        "old_shk_id",
        "old_color",
        "old_size",
        "old_sku",
        "old_vendor_code",
        "new_shk_id",
        "new_color",
        "new_size",
        "new_sku",
        "new_vendor_code",
        "bonus_summ",
        "bonus_type",
        "photo_urls",
        "account"
    ]

    # Map dict keys to column names
    values = []
    for item in data:
        values.append((
            clean_datetime_from_timezone(item.get("dtBonus")).date(),
            item.get("nmId"),
            item.get("oldShkId"),
            item.get("oldColor"),
            item.get("oldSize"),
            item.get("oldSku"),
            item.get("oldVendorCode"),
            item.get("newShkId"),
            item.get("newColor"),
            item.get("newSize"),
            item.get("newSku"),
            item.get("newVendorCode"),
            item.get("bonusSumm"),
            item.get("bonusType"),
            item.get("photoUrls"),
            client
        ))

    query = f"""
        INSERT INTO deductions_replacements ({', '.join(columns)})
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()



async def process_deductions_replacements(client, api_token, conn, date_from, date_to):
    data = await get_deductions_replacements(
        api_key=api_token,
        date_from=date_from,
        date_to=date_to
    )
    if data:
        insert_deductions_replacements(conn, data, client)
        logger.info(f"Получены данные за период {date_from}-{date_to}, Внесено строк в БД: {len(data)}: Отчет - 'Подмены и неверные вложения', Кабинет - {client}")
    else:
        logger.info(f"Нет данных за период {date_from}-{date_to}: Отчет - 'Подмены и неверные вложения', Кабинет - {client}")


def first_insert_all_deductions_replacements():
    '''
    Использовалась однажды для выгрузки отчета "Подмены и неверные вложения" за весь период
    '''

    tokens = load_api_tokens()
    conn = create_connection_w_env()

    date_from = "2024-01-01T00:00:00Z"
    date_to = "2025-12-21T23:59:59Z"

    # Run async code directly
    async def run_all_clients():
        tasks = [
            process_deductions_replacements(client, token, conn, date_from, date_to)
            for client, token in tokens.items()
        ]
        await asyncio.gather(*tasks)

    asyncio.run(run_all_clients())
    conn.close()


async def main():
    '''
    Функционал: выгружает три отчета по удержаниям из WB API в БД.
    Период: предыдущий день.
    Выгружаемые отчеты:
        - Замеры склада,
        - Удержания за занижение габаритов упаковки,
        - Подмены и неверные вложения.
    '''
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    now = datetime.now()
    yesterday = now - timedelta(days=1)
    date_from = yesterday.replace(hour=23, minute=55, second=0, microsecond=0)
    date_to = now

    tasks = []
    for client, token in tokens.items():

        # Отчеты "Замеры склада" и "Удержания за занижение габаритов упаковки"
        tasks.append(
            asyncio.create_task(
                process_measurements_client(client, token, conn, date_from, date_to)
            )
        )

        # Отчет "Подмены и неверные вложения"
        tasks.append(
            asyncio.create_task(
                process_deductions_replacements(client, token, conn, date_from, date_to)
            )
        )

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())