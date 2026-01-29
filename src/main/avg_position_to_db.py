# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import aiohttp
import asyncio
import logging
import requests
from time import sleep
from datetime import datetime, timedelta

# my packages
from utils.env_loader import *
from utils.utils import load_api_tokens
from utils.my_general import aggregate_dct_data
from utils.my_db_functions import create_connection_w_env, load_articles_clients_data, insert_dct_data_to_db


# ---- LOGS ----

LOGS_PATH = os.getenv("LOGS_PATH")

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/avg_position_to_db.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)



async def get_pagination_data(api_token, start_date, end_date, nmIds = None, orderBy_field = 'avgPosition', orderBy_mode = 'asc', positionCluster = 'all', limit = 1000, offset = 0):
    '''
    Purpose:
        Забирает с API данные по методу table/details.
        За раз отдаёт данные по одному клиенту и одному периоду.
        По одному периоду для каждого артикула отдаётся один словарь, т.е. числовые значения (заказы, ср. позиция) агрегируются за указанный период 

    Arguments:
        start_date and end_date - format "2024-02-10"
        nmIds - список артикулов (если None, выгружаются все) !!! <=50 !!!
        orderBy_field options: avgPosition,  addToCart, openCard, orders, cartToOrder, openToCart, visibility, minPrice, maxPrice
        positionCluster options: all, firstHundred, secondHundred, below (Товары с какой средней позицией в поиске показывать в отчёте)
        limit <= 1000
    '''

    # если в артикулах есть артикул не от того продавца, выгружаются данные только по подходящим артикулам (апи не ломается)

    url = 'https://seller-analytics-api.wildberries.ru/api/v2/search-report/table/details'
    headers = {'Authorization': api_token, 'Content-Type': 'application/json'}
    json_data = {
        'currentPeriod': {
            'start' : start_date,
            'end' : end_date
        },
        'orderBy': {
            'field':orderBy_field,
            'mode': orderBy_mode
        },
        'nmIds':nmIds,
        'positionCluster': positionCluster,
        'limit': limit,
        'offset': offset
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url=url, headers=headers, json=json_data) as response:
            try:
                response.raise_for_status()
                data = await response.json()
                return data['data']['products']
            except Exception as e:
                data = await response.json()
                logging.error(f'API error: {e}:  {data}')
                return []


def create_avg_position_table():
    try:
        conn = create_connection_w_env()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS avg_position (
            nmId BIGINT PRIMARY KEY,
            name TEXT,
            vendorCode TEXT,
            subjectName TEXT,
            brandName TEXT,
            mainPhoto TEXT,
            isAdvertised BOOLEAN,
            isSubstitutedSKU BOOLEAN,
            isCardRated BOOLEAN,
            rating INTEGER,
            feedbackRating NUMERIC(3,1),
            minPrice INTEGER,
            maxPrice INTEGER,
            avgPosition INTEGER,
            openCard INTEGER,
            addToCart INTEGER,
            openToCart INTEGER,
            orders INTEGER,
            cartToOrder INTEGER,
            visibility INTEGER,
            report_date DATE PRIMARY KEY,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        conn.close()
        logging.info('')
    except Exception as e:
        logging.error('Error during creating the avg_position db table:\n{e}')


def daterange(start, end):
    '''
    Считает range дат, включая start и end
    '''
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)

    
def clean_item_data(item, date_str):
    # Debug: log the item structure if missing key
    required_keys = ['nmId', 'name', 'vendorCode', 'subjectName', 'brandName']
    for key in required_keys:
        if key not in item:
            if key != 'brandName':
                logging.warning(f"Missing key '{key}' in item: {item}")
    
    return {
        'nmId': item.get('nmId'),
        'name': item.get('name'),
        'vendorCode': item.get('vendorCode'),
        'subjectName': item.get('subjectName'),
        'brandName': item.get('brandName'),
        'mainPhoto': item.get('mainPhoto', '').strip() if item.get('mainPhoto') else None,
        'isAdvertised': item.get('isAdvertised', False),
        'isSubstitutedSKU': item.get('isSubstitutedSKU', False),
        'isCardRated': item.get('isCardRated', False),
        'rating': item.get('rating', 0),
        'feedbackRating': item.get('feedbackRating', 0.0),
        'minPrice': item.get('price', {}).get('minPrice', 0) if item.get('price') else 0,
        'maxPrice': item.get('price', {}).get('maxPrice', 0) if item.get('price') else 0,
        'avgPosition': item.get('avgPosition', {}).get('current', 0) if item.get('avgPosition') else 0,
        'openCard': item.get('openCard', {}).get('current', 0) if item.get('openCard') else 0,
        'addToCart': item.get('addToCart', {}).get('current', 0) if item.get('addToCart') else 0,
        'openToCart': item.get('openToCart', {}).get('current', 0) if item.get('openToCart') else 0,
        'orders': item.get('orders', {}).get('current', 0) if item.get('orders') else 0,
        'cartToOrder': item.get('cartToOrder', {}).get('current', 0) if item.get('cartToOrder') else 0,
        'visibility': item.get('visibility', {}).get('current', 0) if item.get('visibility') else 0,
        'report_date': date_str
    }


def run_async_func_to_thread(async_func, *args, **kwargs):
    try:
        loop = asyncio.get_running_loop()
        return asyncio.run_coroutine_threadsafe(async_func(*args, **kwargs), loop)
    except RuntimeError:
        return asyncio.run(async_func(*args, **kwargs))


def load_hist_data(api_token, nmIDs, start_date, end_date):
    '''
    Выгружает все данные по клиенту и списку артикулов за каждый день периода, включая start_date и end_date.
    Разбивает nmIDs в chunks по 50 шт

    Arguments:
        start_date & end_date format: '2024-02-10'
    '''

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    all_data = []

    nmIds_chunks = [nmIDs[i:i + 50] for i in range(0, len(nmIDs), 50)]
    dates = list(daterange(start, end + timedelta(days=1)))
    
    for i, single_date in enumerate(dates):
        date_str = single_date.strftime('%Y-%m-%d')
        daily_data = []

        for j, chunk in enumerate(nmIds_chunks):
            try:
                chunk_data = run_async_func_to_thread(
                    get_pagination_data(
                        api_token=api_token,
                        start_date=date_str,
                        end_date=date_str,
                        nmIds=chunk
                    )
                )
                daily_data.extend(chunk_data)
                logging.info(f"The data for chunk {j+1}/{len(nmIds_chunks)} on {date_str} is loaded")

            except Exception as e:
                logging.error(f'Error while loading chunk for {date_str}:\n{e}')
            finally:
                # Sleep after each chunk except the last one
                if j < len(nmIds_chunks) - 1:
                    sleep(21)

        print(f'The data for {date_str} is loaded')

        for item in daily_data:
            clean_item = clean_item_data(item, date_str)
            all_data.append(clean_item)
        
        # Sleep after each date except the last one
        if i < len(dates) - 1:
            sleep(21)

    return all_data


async def load_and_update_hist_data(api_token, nmIDs, start_date, end_date, conn, client):
    '''
    Выгружает и загружает в БД данные по каждому дню отдельно.
    '''
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    nmIds_chunks = [nmIDs[i:i + 50] for i in range(0, len(nmIDs), 50)]
    dates = list(daterange(start, end + timedelta(days=1)))

    for i, single_date in enumerate(dates):
        date_str = single_date.strftime('%Y-%m-%d')
        daily_data = []

        for j, chunk in enumerate(nmIds_chunks):
            try:
                chunk_data = await get_pagination_data(
                    api_token=api_token,
                    start_date=date_str,
                    end_date=date_str,
                    nmIds=chunk
                )
                if chunk_data:
                    daily_data.extend(chunk_data)
                    logging.info(f"Client: {client:^10} - The data for chunk {j+1}/{len(nmIds_chunks)} on {date_str} is loaded.")

            except Exception as e:
                logging.error(f'Error while loading chunk for {date_str}:\n{e}, client: {client}')
            finally:
                if j < len(nmIds_chunks) - 1:
                    await asyncio.sleep(22)

        cleaned_data = [clean_item_data(item, date_str) for item in daily_data]

        if cleaned_data:
            try:
                insert_dct_data_to_db(cleaned_data, conn)
                conn.commit()
                logging.info(f"Inserted {len(cleaned_data)} rows for {date_str}\n")
            except Exception as e:
                conn.rollback()
                logging.error(f"Failed to insert data for {date_str}: {e}\n")
        else:
            logging.info(f"No data for {date_str}")

        if i < len(dates) - 1:
            await asyncio.sleep(22)


async def get_and_upload_data_to_db(start_date, end_date):
    '''
    Загружает и обновляет данные по каждому клиенту и каждому дню.
    '''
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    try:
        id_client = load_articles_clients_data(conn)
        client_id = aggregate_dct_data(id_client)

        tasks = []

        for client, ids in client_id.items():
            logging.info(f'Processing client {client}...')

            # if client in [ 'Старт', 'Даниелян', 'Пилосян', 'Тоноян', 'Старт2', 'Лопатина', 'Вектор', 'Хачатрян']:                  # !!! CHANGE !!! 'Оганесян', 'Вектор2'
            #     continue

            client_start = start_date

            api_token = tokens.get(client)
            if not api_token:
                logging.warning(f"Missing API token for {client}")
                continue

            logging.info(f"Loading data for {client} from {client_start} to {end_date}")
            tasks.append(load_and_update_hist_data(api_token, ids, client_start, end_date, conn, client))
        
        await asyncio.gather(*tasks, return_exceptions=True)

        logging.info(f"Finished processing all clients.")

    except Exception as e:
        logging.critical(f"Unexpected error in get_and_upload_data_to_db: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    start_date = end_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d') # yesterday
    asyncio.run(get_and_upload_data_to_db(start_date, end_date))