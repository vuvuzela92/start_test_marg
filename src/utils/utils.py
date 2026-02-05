import re
import hashlib
import json
from pandas import DataFrame
from gspread import worksheet
from datetime import timedelta, datetime
from gspread_dataframe import set_with_dataframe
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from typing import List
from gspread.exceptions import APIError
import time
import logging
import requests
import os
from pathlib import Path

# from .env_loader import *
from dotenv import load_dotenv  
load_dotenv()


BASE_DIR = Path(__file__).resolve().parents[2]
TOKENS_PATH = BASE_DIR / os.getenv("CREDS_DIR") / os.getenv("TOKENS_FILE")

# Функция для загрузки API токенов из файла tokens.json
def load_api_tokens(filename = TOKENS_PATH):
    with open(filename, encoding= 'utf-8') as f:
        tokens = json.load(f)
        return tokens
    
def camel_to_snake(name):
    """
    Converts a camelCase string to snake_case.

    Parameters:
    - name: The camelCase string to convert.

    Returns:
    - A snake_case version of the input string.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def calculate_hash(record):
    """
    Calculates a SHA-256 hash for a given record.

    Parameters:
    - record: A dictionary representing the record.

    Returns:
    - A SHA-256 hash of the record's JSON string representation.
    """
    record_str = json.dumps(record, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(record_str.encode()).hexdigest()


def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def prepare_nms_record(record, account_id):
    record.pop('photos', None)
    record['account_id'] = account_id
    record = {camel_to_snake(k): v for k, v in record.items() if k != 'photos'}
    record['data_hash'] = calculate_hash(record)
    # Ensure 'dimensions', 'characteristics', and 'sizes' are JSON strings
    for key in ['dimensions', 'characteristics', 'sizes']:
        if key in record:
            record[key] = json.dumps(record[key], ensure_ascii=False)
    return record


def prepare_campaign_record(record, account_id):
    record['account_id'] = account_id
    record = {camel_to_snake(k): v for k, v in record.items()}
    record['data_hash'] = calculate_hash(record)
    record['search_pluse_state'] = record.get('search_pluse_state', None)
    for key in ['auto_params', 'params', 'united_params']:
        if key in record:
            record[key] = json.dumps(record[key], ensure_ascii=False)
        else:
            record[key] = None
    return record


def prepare_account_record(record):
    record['data_hash'] = calculate_hash(record)
    return record


def map_colnames(colnames):
    col_name_mapping = {
        'dt': 'дата',
        'month': 'месяц',
        'week': 'неделя',
        'nm_id': 'артикул',
        'item_name': 'товар',
        'campaign_name': 'название рк',
        'campaign_id': 'номера рк',
        'views': 'показы',
        'clicks': 'клики',
        'ad_spend': 'расходы',
        'orders': 'заказы с рекламы',
        'items_sold': 'продажи рк, шт.',
        'sum_price': 'продажи рк, руб.',
        'openCardCount': 'переход в карточку',
        'addToCartCount': 'добавить в корзину',
        'ordersCount': 'продажи всего, шт.',
        'ordersSumRub': 'продажи всего, руб',
        'buyoutsCount': 'выкуплено всего, шт.',
        'buyoutsSumRub': 'выкуплено всего, руб.',
    }
    return [col_name_mapping.get(cn, cn) for cn in colnames]
def send_df_to_google(df, sheet):
    """
    Отправляет DataFrame на указанный лист Google Таблицы.

    Параметры:
    df (DataFrame): DataFrame, который нужно отправить.
    sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

    Возвращаемое значение:
    None
    """
    try:
        # Данные, которые нужно добавить
        df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
        
        # Проверка существующих данных на листе
        existing_data = sheet.get_all_values()
        
        if len(existing_data) <= 1:  # Если данных нет
            print("Добавляем заголовки и данные")
            sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
             # Получаем текущую дату и время
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
        else:
            print("Добавляем только данные")
            sheet.append_rows(df_data_to_append[1:], value_input_option='USER_ENTERED')
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
            
    except Exception as e:
        print(f"An error occurred: {e}")




import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd

def update_df_in_google(df: pd.DataFrame, sheet):
    """
    Перезаписывает данные DataFrame на указанный лист Google Таблицы.
    Также добавляет дату и время последнего обновления в первую строку последней колонки.
    
    Параметры:
    df (DataFrame): DataFrame, который нужно отправить.
    sheet (gspread.models.Worksheet): Объект листа, на который будут записаны данные.

    Возвращаемое значение:
    None
    """
    try:
        # Обрабатываем NaN значения в DataFrame (заменяем на пустые строки)
        df = df.fillna('')

        # Проверка на наличие новых данных
        if df.empty:
            print("DataFrame пуст. Создание резервной копии...")
            # Создание резервной копии данных
            backup_sheet = sheet.spreadsheet.add_worksheet(title=f"Резервная копия{sheet.title}", rows="1000", cols="20")
            backup_sheet.append_rows(sheet.get_all_values(), value_input_option='RAW')
            print("Создана резервная копия текущих данных.")
            return  # Прекращаем выполнение, чтобы не перезаписывать пустыми данными

        # Очищаем лист перед записью новых данных
        sheet.clear()

        # Подготовка данных для записи
        df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()

        # Запись данных на лист
        sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
        print("Данные успешно перезаписаны на лист.")
        
        # Получаем текущую дату и время
        now = datetime.now()
        formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # Получаем количество колонок на листе
        max_columns = sheet.col_count
        
        # Записываем дату и время в первую строку последней колонки
        sheet.update_cell(1, max_columns, formatted_time)
        print(f"Дата и время последнего обновления: {formatted_time}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        # Проверяем на ошибку, связанную с лимитом ячеек
        if "APIError: [400]: This action would increase the number of cells in the workbook" in str(e):
            print("Превышен лимит ячеек Google Таблицы. Создание резервной копии в Excel...")
            # Создание резервной копии данных в Excel
            backup_file = f"Копия БД тестовых заданий {datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
            df.to_excel(backup_file, index=False)
            print(f"Данные сохранены в резервную копию: {backup_file}")


def send_unique_id_to_google(df: DataFrame, sheet: worksheet):
    """
    Отправляет DataFrame на указанный лист Google Таблицы, добавляя только уникальные строки по столбцу 'parametr'.

    Параметры:
    df (DataFrame): DataFrame, который нужно отправить.
    sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

    Возвращаемое значение:
    None
    """
    try:
        # Получаем существующие данные с листа
        existing_data = sheet.get_all_values()

        # Проверяем, есть ли данные на листе
        if len(existing_data) > 1:
            # Получаем заголовки и существующие id
            existing_headers = existing_data[0]
            id_index = existing_headers.index('id')
            existing_ids = {row[id_index] for row in existing_data[1:]}

            # Фильтруем новые данные, оставляя только уникальные id
            df_unique = df[~df['id'].astype(str).isin(existing_ids)]
        else:
            df_unique = df

        if not df_unique.empty:
            # Подготавливаем данные для добавления
            df_data_to_append = [df_unique.columns.values.tolist()] + df_unique.values.tolist()
            result = [[str(item).encode('utf-8').decode('utf-8') for item in row] for row in df_data_to_append]


            if len(existing_data) <= 1:
                print("Добавляем заголовки и данные")
                sheet.append_rows(result, value_input_option='USER_ENTERED')
            else:
                print("Добавляем только уникальные данные")
                sheet.append_rows(result[1:], value_input_option='USER_ENTERED')
        else:
            print("Нет уникальных данных для добавления")

    except Exception as e:
        print(f"An error occurred: {e}")

import pytz
# Устанавливаем московское время
# Функция для получения временной метки "за 24 часа назад"
def get_udf():
    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)
    udf = now - timedelta(days=1)

    return int(udf.timestamp())


# Функция для получения временной метки "за сегодня"
def get_udt():
    moscow_tz = pytz.timezone('Europe/Moscow')

    now = datetime.now(moscow_tz)
    udt = now.replace(hour=23, minute=59, second=0, microsecond=0)

    return int(udt.timestamp())


def collect_for_all(function, tokens:dict):
    """ Функция, которая принимает в себя АПИ токены и собирает информация по нескольким личным кабинетам."""    
    info_list = []
    for account, api_token in tokens.items():
            headers = {
                "Authorization": api_token
            }
            info_list.append(function(account, headers))
    return info_list

# Подключение к базе данных
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print(f"Соединение с БД PostgreSQL успешно установлено в {datetime.now().strftime('%Y-%m-%d')}")
    except OperationalError as error:
        print(f"Произошла ошибка при подключении к БД PostgreSQL {error}")
    return connection

# Исполнение SQL запросов
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()  # явное подтверждение транзакции
        print(f"Запрос успешно выполнен в {datetime.now().strftime('%Y-%m-%d')}")
    except Exception as e:
        connection.rollback()  # откат транзакции в случае ошибки
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        cursor.close()

# Функция на чтение данных из БД
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as error:
        print(f'Произошла ошибка при выводе данных {error}')


# Функция для чтения SQL в df с headers (M)
def read_sql_to_df(connection, query):
    cursor = connection.cursor()
    df = None
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=headers).fillna(0).infer_objects(copy=False)
    except OperationalError as error:
        print(f'Произошла ошибка при выводе данных {error}')
    return df


def google_sheet_to_table(table_title: str, sheet_title: str):
    """Функция преобразует лист гугл таблицы в датфрейм"""
    # Дает права на взаимодействие с гугл-таблицами
    gc = gspread.service_account(filename=r'C:\Users\123\Desktop\adv_test\creds.json')
    table = gc.open(f'{table_title}')
    sheet = table.worksheet(f'{sheet_title}')
    table_data = sheet.get_all_values()
    df = pd.DataFrame(table_data[1:], columns=table_data[0])
    return df
# Функция для получения датафрейма из БД
def get_db_table(db_query: str, connection):
    """Функция получает данные из Базы Данных и преобразует их в датафрейм"""
    execute_read_query(connection, db_query)
    # Преобразуем таблицу в датафрейм
    try:
        df_db = pd.read_sql(db_query, connection).fillna(0).infer_objects(copy=False)
        print('Данные из БД загружены в датафрейм')
        return df_db
    except Exception as e:
        print(f'Ошибка получения данных из БД {e}')

import re
import hashlib
import json
from pandas import DataFrame
from gspread import worksheet
import openpyxl
from gspread_dataframe import set_with_dataframe
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from typing import List


def camel_to_snake(name):
    """
    Converts a camelCase string to snake_case.

    Parameters:
    - name: The camelCase string to convert.

    Returns:
    - A snake_case version of the input string.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def calculate_hash(record):
    """
    Calculates a SHA-256 hash for a given record.

    Parameters:
    - record: A dictionary representing the record.

    Returns:
    - A SHA-256 hash of the record's JSON string representation.
    """
    record_str = json.dumps(record, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(record_str.encode()).hexdigest()


def batchify(data, batch_size):
    """
    Splits data into batches of a specified size.

    Parameters:
    - data: The list of items to be batched.
    - batch_size: The size of each batch.

    Returns:
    - A generator yielding batches of data.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def prepare_nms_record(record, account_id):
    record.pop('photos', None)
    record['account_id'] = account_id
    record = {camel_to_snake(k): v for k, v in record.items() if k != 'photos'}
    record['data_hash'] = calculate_hash(record)
    # Ensure 'dimensions', 'characteristics', and 'sizes' are JSON strings
    for key in ['dimensions', 'characteristics', 'sizes']:
        if key in record:
            record[key] = json.dumps(record[key], ensure_ascii=False)
    return record


def prepare_campaign_record(record, account_id):
    record['account_id'] = account_id
    record = {camel_to_snake(k): v for k, v in record.items()}
    record['data_hash'] = calculate_hash(record)
    record['search_pluse_state'] = record.get('search_pluse_state', None)
    for key in ['auto_params', 'params', 'united_params']:
        if key in record:
            record[key] = json.dumps(record[key], ensure_ascii=False)
        else:
            record[key] = None
    return record


def prepare_account_record(record):
    record['data_hash'] = calculate_hash(record)
    return record


def map_colnames(colnames):
    col_name_mapping = {
        'dt': 'дата',
        'month': 'месяц',
        'week': 'неделя',
        'nm_id': 'артикул',
        'item_name': 'товар',
        'campaign_name': 'название рк',
        'campaign_id': 'номера рк',
        'views': 'показы',
        'clicks': 'клики',
        'ad_spend': 'расходы',
        'orders': 'заказы с рекламы',
        'items_sold': 'продажи рк, шт.',
        'sum_price': 'продажи рк, руб.',
        'openCardCount': 'переход в карточку',
        'addToCartCount': 'добавить в корзину',
        'ordersCount': 'продажи всего, шт.',
        'ordersSumRub': 'продажи всего, руб',
        'buyoutsCount': 'выкуплено всего, шт.',
        'buyoutsSumRub': 'выкуплено всего, руб.',
    }
    return [col_name_mapping.get(cn, cn) for cn in colnames]
def send_df_to_google(df, sheet):
    """
    Отправляет DataFrame на указанный лист Google Таблицы.

    Параметры:
    df (DataFrame): DataFrame, который нужно отправить.
    sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

    Возвращаемое значение:
    None
    """
    try:
        # Данные, которые нужно добавить
        df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
        
        # Проверка существующих данных на листе
        existing_data = sheet.get_all_values()
        
        if len(existing_data) <= 1:  # Если данных нет
            print("Добавляем заголовки и данные")
            sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
             # Получаем текущую дату и время
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
        else:
            print("Добавляем только данные")
            sheet.append_rows(df_data_to_append[1:], value_input_option='USER_ENTERED')
            now = datetime.now()
            formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем количество колонок на листе
            max_columns = sheet.col_count
            
            # Записываем дату и время в первую строку последней колонки
            sheet.update_cell(1, max_columns, formatted_time)
            print(f"Дата и время последнего обновления: {formatted_time}")
            
    except Exception as e:
        print(f"An error occurred: {e}")