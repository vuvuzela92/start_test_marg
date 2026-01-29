import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import requests
from psycopg2 import extras
import time

# from utils.env_loader import *
from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_db_functions import create_connection_w_env
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger("wb_stocks.log")

def get_wb_stocks(api_token: str, date_from: str = "2019-06-20T00:00:00") -> list:
    """
    Получает все остатки товаров со складов Wildberries, автоматически обрабатывая случаи,
    когда данных больше 60 000 строк (постраничная загрузка).

    Аргументы:
        api_token (str): API-ключ Wildberries.
        date_from (str): Дата в формате RFC3339 (по умолчанию ранняя дата для полной выборки).

    Возвращает:
        list: Список всех записей остатков.
    """

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    headers = {"Authorization": api_token}
    all_stocks = []
    current_date = date_from

    while True:
        params = {"dateFrom": current_date}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logger.error(f"Ошибка {response.status_code}: {response.text}")
            break

        data = response.json()
        if not data:
            logger.warning("Получен пустой ответ")
            break

        all_stocks.extend(data)
        current_date = data[-1]["lastChangeDate"]

        # logger.info(f"Получено {len(data)} записей, всего: {len(all_stocks)}")

        # Прекращаем цикл, если данных меньше лимита (т.е. всё выгружено)
        if len(data) < 60000:
            break

        time.sleep(61)

    return all_stocks


def insert_wb_stocks(conn, stocks: list):
    """
    Вставляет список остатков в таблицу wb_stock.
    Дубликаты по (last_change_date, warehouse_name, nm_id) пропускаются.
    """
    if not stocks:
        return

    records = [
        (
            s['lastChangeDate'],
            s['warehouseName'],
            s['supplierArticle'],
            s.get('nmId'),
            s.get('barcode'),
            s.get('quantity'),
            s.get('inWayToClient'),
            s.get('inWayFromClient'),
            s.get('quantityFull'),
            s.get('category'),
            s.get('subject'),
            s.get('brand'),
            s.get('techSize'),
            s.get('Price'),
            s.get('Discount'),
            s.get('isSupply'),
            s.get('isRealization'),
            s.get('SCCode')
        )
        for s in stocks
    ]

    insert_query = """
    INSERT INTO wb_stock (
        last_change_date, warehouse_name, supplier_article, nm_id, barcode,
        quantity, in_way_to_client, in_way_from_client, quantity_full, category,
        subject, brand, tech_size, price, discount, is_supply, is_realization, sc_code
    )
    VALUES %s
    ON CONFLICT (last_change_date, warehouse_name, nm_id)
    DO NOTHING;
    """

    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            insert_query,
            records,
            template="(to_timestamp(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
    conn.commit()


if __name__ == "__main__":
    
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    for client, token in tokens.items():
        try:
            stocks = get_wb_stocks(token)
            if not stocks:
                logger.info(f"Нет данных для клиента {client}, пропускаем.")
                continue
            
            logger.info(f"Получено {len(stocks)} записей")
            insert_wb_stocks(conn, stocks)
            logger.info(f"Данные по кабинету {client} внесены в БД")

        except Exception as e:
            logger.error(f"Ошибка обработки клиента {client}: {e}")
            continue

    conn.close()