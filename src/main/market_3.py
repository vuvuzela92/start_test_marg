import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import time
import gspread
import requests
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta

from utils.utils import load_api_tokens
from utils.utils import update_df_in_google
from utils.logger import setup_logger
from utils.env_loader import *

logger = setup_logger("market_3.log")

CREDS_PATH = os.getenv("PRO_CREDS_PATH")

def supply_info(account, api_token, begin, end):
    url = 'https://marketplace-api.wildberries.ru/api/v3/orders'
    next_value = 0
    limit = 1000
    full_data = []
    headers = {'Authorization': api_token}

    while True:
        params = {
            'limit': limit,
            'next': next_value,
            'dateFrom': begin,
            'dateTo': end,
        }

        try:
            # print(f"Запрос: {params}")  # Отладочное сообщение
            res = requests.get(url, params=params, headers=headers)
            res.raise_for_status()
            
            data = res.json()
            # print(f"Ответ: {data}")  # Отладочное сообщение

            if 'orders' in data:  # 'orders' - первый ключ в json
                for order in data['orders']:
                    order['account'] = account  # Добавляем информацию об аккаунте
                full_data.extend(data['orders'])

            # Получаем новое значение `next` для продолжения пагинации
            next_value = data.get('next', 0)
            if next_value == 0:
                break

        except requests.RequestException as error:
            print(f"Ошибка: {error}")
            break
        time.sleep(1)

    return full_data  # Возврат данных после завершения цикла


async def supply_info(account, api_token, begin, end):
    url = 'https://marketplace-api.wildberries.ru/api/v3/orders'
    next_value = 0
    limit = 1000
    full_data = []
    headers = {'Authorization': api_token}

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            params = {
                'limit': limit,
                'next': next_value,
                'dateFrom': begin,
                'dateTo': end,
            }

            try:
                async with session.get(url, params=params) as res:
                    res.raise_for_status()
                    data = await res.json()

                    if 'orders' in data:
                        for order in data['orders']:
                            order['account'] = account
                        full_data.extend(data['orders'])

                    next_value = data.get('next', 0)
                    if next_value == 0:
                        break

            except aiohttp.ClientError as error:
                print(f"Ошибка: {error}")
                break
            
            # print(f'{account}: sleeping')
            await asyncio.sleep(1)

    return full_data

async def get_all_clients_supply_info(begin, end):
    tasks = []

    for account, api_token in load_api_tokens().items():
        task = supply_info(account, api_token, begin, end)
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    all_data = []
    for account_data in results:
        if account_data:
            all_data.extend(account_data)

    logger.info(f"Всего получено заказов: {len(all_data)}")
    return all_data


if __name__ == "__main__":

    try:

        # Убедитесь, что разница в днях не превышает 30 дней
        begin = int((datetime.now() - timedelta(days=11)).timestamp())
        end = int(datetime.now().timestamp())

        # Сбор данных по всем аккаунтам
        all_data = asyncio.run(get_all_clients_supply_info(begin, end))

    except Exception as e:
        logger.error(f'Ошибка при загрузке данных: {e}')
            

    try:
        # Преобразование в DataFrame
        df = pd.DataFrame(all_data)

        google_df = df[['id', 'nmId', 'deliveryType', 'article', 'createdAt', 'account']]
        google_df['createdAt'] = pd.to_datetime(google_df['createdAt']) + timedelta(hours=3)
        google_df['date'] = pd.to_datetime(google_df['createdAt']).dt.date
        google_df['date'] = pd.to_datetime(google_df['date'])
        google_df['time'] = pd.to_datetime(google_df['createdAt']).dt.time
        # Регулярное выражение для извлечения нужной части
        pattern = r'(wild\d+)'
        # Используем метод str.extract для извлечения нужного паттерна
        google_df['wild'] = google_df['article'].str.extract(pattern)

        google_df =  google_df.rename(columns={
                                                'nmId' : 'Артикул ВБ',
                                                'deliveryType' : 'Тип доставки',
                                                'article' : 'Артикул поставщика',
                                                'createdAt' : 'Создано',
                                                'account' : 'ЛК',
                                                'date' : 'Дата',
                                                'time' : 'Время',})
        google_df['Создано'] = google_df['Создано'].astype(str)
        google_df['Дата'] = google_df['Дата'].astype(str)
        google_df['Время'] = google_df['Время'].astype(str)

    except Exception as e:
        logger.error(f'Ошибка при обработке данных: {e}')


    try:
        # Доступ к гугл таблице
        gc = gspread.service_account(filename=CREDS_PATH)
        table = gc.open('Для расчетов БД')
        task_sheet = table.worksheet('БД 2 ( ТЕСТ )')

        update_df_in_google(google_df, task_sheet)
        logger.info('Данные успешно добавлены в гугл')
    except Exception as e:
        logger.error(f'Ошибка при добавлении данных в гугл: {e}')


    # # Путь к файлу
    # file_path = r'C:\Users\123\Desktop\Архив\supply_info.csv'

    # db_df = pd.read_csv(file_path)
    # db_df['id'] = db_df['id'].astype(int)

    # unique_task_df = pd.concat([db_df, df], ignore_index=False, axis='rows')
    # unique_task_df = unique_task_df.drop_duplicates(subset='id', keep='first')
    # unique_task_df.to_csv(file_path, index=False)