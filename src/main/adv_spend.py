import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
import requests
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_general import ensure_datetime
from utils.my_db_functions import create_connection_w_env


# ---- LOGS ----
logger = setup_logger("adv_spend.log")

DB_TABLE = 'adv_spend_new' # change


def get_wb_adv_costs(token: str, date_from: str, date_to: str):
    """
    Fetch advertising spending history from the Wildberries API.

    Args:
        token (str): API key for Authorization header.
        date_from (str): Start date in format YYYY-MM-DD.
        date_to (str): End date in format YYYY-MM-DD.

    Returns:
        dict: JSON response from the API, or error details.
    """
    url = "https://advert-api.wildberries.ru/adv/v1/upd"
    
    headers = {
        "Authorization": token
    }
    
    params = {
        "from": date_from,
        "to": date_to
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def insert_advert_spend(data_list, conn):
    """
    Inserts a list of dicts into advert_spend_new.
    
    Args:
        data_list (list of dict): Input data.
        conn: psycopg2 database connection.
    """
    if not data_list:
        return

    # Map input keys to table columns
    key_map = {
        "updTime": "upd_time",
        "campName": "camp_name",
        "paymentType": "payment_type",
        "updNum": "upd_num",
        "updSum": "upd_sum",
        "advertId": "advert_id",
        "advertType": "advert_type",
        "advertStatus": "advert_status",
        "account": "account"
    }

    # Transform data
    rows = []
    for item in data_list:
        row = {}
        for k, v in key_map.items():
            val = item.get(k)
            if k == "updTime" and val is not None:
                # Convert ISO datetime string to Python datetime
                # val = datetime.fromisoformat(val.replace("Z", "+00:00"))
                val = datetime.fromisoformat(val.split(".")[0])
            row[v] = val
        rows.append(row)

    # Prepare SQL
    columns = list(rows[0].keys())
    sql = f"""
        INSERT INTO {DB_TABLE} ({', '.join(columns)})
        VALUES %s
        --ON CONFLICT (upd_time, advert_id) DO NOTHING
    """

    # Prepare data for execute_values
    values = [[r[col] for col in columns] for r in rows]

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

async def process_client(client: str, token: str, start_date: datetime, end_date: datetime, max_chunk: int, conn):
    """
    Process all data for a single client asynchronously, slicing into chunks.
    """
    current_start = start_date
    logger.info(f'Started processing client {client}')
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=max_chunk-1), end_date)
        period = f"{current_start.strftime('%Y-%m-%d')}-{current_end.strftime('%Y-%m-%d')}"
        
        try: 
            data = get_wb_adv_costs(
                token=token,
                date_from=current_start.strftime("%Y-%m-%d"),
                date_to=current_end.strftime("%Y-%m-%d")
            )

            ids = set([i['advertId'] for i in data])
            
            if data:
                logger.info(f"Successfully retrieved {len(data)} for {client}, {period}, ids: {ids}")
            
                for item in data:
                    item['account'] = client

                insert_advert_spend(data, conn)
                logger.info(f"Successfully added data for {client}, {period} to DB")
            else:
                logger.warning(f"No data for client {client}, period {period}")

        except Exception as e:
            logger.error(f"Error for client {client} period {period}: {e}")

        # move to next chunk
        current_start = current_end + timedelta(days=1)

        # respect API limit
        await asyncio.sleep(1)


async def upload_all_data_async():
    """
    Main async function to process multiple clients concurrently.
    """
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    start_date = datetime(2025, 1, 1)
    end_date = datetime.today() - timedelta(days=1)
    max_chunk = 31

    tasks = []
    for client, token in tokens.items():
        # set custom start date for specific clients
        if client in ['Старт2', 'Вектор2']:
            start_date = datetime(2025, 9, 1)  # custom date for these clients
        else:
            start_date = datetime(2024, 1, 1)   # default date

        tasks.append(process_client(client, token, start_date, end_date, max_chunk, conn))

    await asyncio.gather(*tasks)

# # logic for uploading ALL data
# if __name__ == "__main__":
#     asyncio.run(upload_all_data_async())


async def upload_data_for_range(start_date, end_date):
    """
    Process multiple clients concurrently for a specific date range.
    Accepts datetime objects or 'yyyy-mm-dd' strings.
    """
    start_date = ensure_datetime(start_date)
    end_date = ensure_datetime(end_date)

    tokens = load_api_tokens()
    conn = create_connection_w_env()
    max_chunk = 31

    tasks = [
        process_client(client, token, start_date, end_date, max_chunk, conn)
        for client, token in tokens.items()
    ]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    # dynamically get yesterday
    yesterday = datetime.today() - timedelta(days=1)
    
    # call async function for yesterday only
    asyncio.run(upload_data_for_range(yesterday, yesterday))
