import os
import pandas as pd
from decimal import Decimal
from datetime import datetime
from psycopg2.extras import execute_batch
from psycopg2.extras import execute_values
import logging

# my packages
# from .env_loader import *
from .my_pandas import process_decimal
from .my_general import process_decimal_in_dict
from .utils import create_connection, read_sql_to_df
from .clickhouse_utils import ClickHouseConnector
from dotenv import load_dotenv

load_dotenv()


# -------------------------------- CONNECTION, BASIC INFO --------------------------------


def create_connection_w_env():
    '''
    Установление соединения с БД Postgres
    '''
    # load_dotenv()
    user = os.getenv('USER_2')
    name = os.getenv('NAME_2')
    password = (os.getenv('PASSWORD_2'))
    host = os.getenv('HOST_2')
    port = os.getenv('PORT_2')
    # dialect = 'postgresql'

    connection = create_connection(name, user, password, host, port)
    cur = connection.cursor()
    cur.execute("SET TIME ZONE 'Europe/Moscow';")  # ← This ensures NOW() is in Moscow time
    cur.close()
    return connection


def create_clickhouse_connector():
    '''
    Установление соединения с БД Clickhouse
    '''
    # load_dotenv()
    connector = ClickHouseConnector(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        user=os.getenv('CLICKHOUSE_ADMIN_USER'),
        password=os.getenv('CLICKHOUSE_ADMIN_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DB'),
        settings={'secure': True}
    )
    return connector


def load_clickhouse_columns_names(conn, table):
    desc = conn.execute_query(f"DESCRIBE TABLE {table}")
    columns = [i[0] for i in desc]
    return columns



# -------------------------------- GET DATA --------------------------------


def get_df_from_db(db_query, conn=None, cursor=None, decimal_to_num = True):
    own_conn = not (conn or cursor)
    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
                own_conn = True
            cursor = conn.cursor()

        if isinstance(db_query, str):
            res = read_sql_to_df(conn, db_query)
        else:
            res = [read_sql_to_df(conn, query) for query in db_query]

        if decimal_to_num:
            res = process_decimal(res)
            
        return res

    except Exception as e:
        print(f'Возникла ошибка при выгрузке данных из БД: {e}')
        raise

    finally:
        if cursor:
            cursor.close()
        if own_conn and conn:
            conn.close()


def fetch_db_data_into_list(db_query, conn=None, cursor=None, return_headers = False):
    '''
    Возвращает результат fetchall запроса SQL
    '''
    own_conn = not (conn or cursor)

    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
                own_conn = True
            cursor = conn.cursor()
        
        cursor.execute(db_query)
        rows = cursor.fetchall()

        if return_headers:
            headers = [desc[0] for desc in cursor.description]
            return headers, rows
        else:
            return rows

    except Exception as e:
        print(f'Возникла ошибка при выгрузке данных из БД: {e}')
        raise

    finally:
        if cursor:
            cursor.close()
        if own_conn and conn:
            conn.close()


def fetch_db_data_into_dict(db_query, conn=None, cursor=None):
    '''
    Возвращает результат fetchall запроса SQL как список словарей
    '''
    own_conn = not (conn or cursor)

    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
                own_conn = True
            cursor = conn.cursor()
        
        cursor.execute(db_query)
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        
        return process_decimal_in_dict([dict(zip(headers, row)) for row in rows])
        
    finally:
        if own_conn and conn:
            conn.close()


def get_table_column_names(db_table, conn = None, cur = None):
    '''
    Возвращает лист с названием колонок таблицы в БД.
    Для оптимизации работы можно передать необязательные параметры соединение (conn) или курсор (cur).
    '''
    try: 
        if not cur:
            if conn:
                cur = conn.cursor()
            else:
                conn = create_connection_w_env()
                cur = conn.cursor()
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{db_table}'
        """)
        columns = [row[0] for row in cur.fetchall()]
        return columns
    except Exception as e:
        print(f'Ошибка при попытке получить названия колонок из таблицы {db_table}: {e}')
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_and_load_commissions_data():
    db_table = 'comission_wb_data'
    db_query = f'''
                SELECT * FROM {db_table}
                WHERE date  = (SELECT MAX(date) FROM {db_table});'''
    df = get_df_from_db(db_table, db_query)
    df.drop(columns = 'date', inplace = True)
    col_name = 'subject_name'
    col = df.pop(col_name)
    df.insert(len(df.columns), col_name, col)
    df[f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"] = ''
    return df


def get_purchase_price_from_db():
    '''
    Возвращает словарь в формате {артикул : закупочная цена}.
    Закупочную цену берёт из таблицы orders_articles_analyze в БД
    '''

    db_query = '''
    SELECT DISTINCT ON (article_id)
                        article_id,
                        purchase_price
    FROM orders_articles_analyze
    ORDER BY article_id, date DESC
    '''
    db_data = get_df_from_db(db_query)
    article_price_dict = dict(
        zip(
            db_data['article_id'].astype(int),
            db_data['purchase_price'].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
        )
    )
    return article_price_dict


def get_basic_info(columns = 'article_id,  local_vendor_code, subject_name, manager, parent_name'):
    query = f'''
    SELECT DISTINCT ON (article_id)
        {columns}
    FROM orders_articles_analyze
    ORDER BY article_id, date DESC
    '''
    df_info = get_df_from_db(query)
    return df_info


def load_articles_clients_data(conn = None):
    '''
    Loads matched articles w clients in the format of {id : 'Client'}
    '''
    query = '''
    SELECT nm_id, account
    FROM article
    '''
    db_data = fetch_db_data_into_dict(query, conn = conn)
    clean_data = {i['nm_id'] : i['account'].capitalize() for i in db_data}
    return clean_data


def fetch_clickhouse_query_into_dict(query):
    conn = create_clickhouse_connector()
    data = conn.execute_query(query)

    select_part = query.split("SELECT", 1)[1].split("FROM", 1)[0]
    columns = []
    for col in select_part.replace("DISTINCT", "").split(","):
        col_name = col.strip().split()[-1]
        col_name = col_name.strip('`"[] ')
        columns.append(col_name)
    
    result = [dict(zip(columns, row)) for row in data]
    return result




# -------------------------------- INSERT DATA --------------------------------


def insert_new_rows(db_table, df, conn=None, cursor=None):
    """
    Вставляет все значения df в БД как новые строки, откатывает изменения при ошибках.
    ! Если строчки дублируются, пропускает их !
    """
    own_conn = not conn
    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
            cursor = conn.cursor()

        data_tuples = [tuple(x) for x in df.to_numpy()]
        
        execute_values(
            cursor,
            f"INSERT INTO {db_table} ({','.join(df.columns)}) VALUES %s ON CONFLICT DO NOTHING",
            data_tuples
        )
        
        conn.commit()
        print(f'Данные успешно добавлены в таблицу БД {db_table}')

    except Exception as e:
        if conn:
            conn.rollback()
        print(f'Возникла ошибка при работе с БД. Новые изменения отменены, старые данные сохранены. Ошибка:\n{e}')
        raise
    finally:
        if cursor:
            cursor.close()
        if own_conn and conn:
            conn.close()


def create_db_table(conn=None, cursor = None, create_query=None, triggers=None):
    """
    Создает таблицу в БД PostgreSQL с опциональными триггерами, откатывает изменения при ошибках.

    Параметры:
        conn: Существующее соединение с БД (если None, создается новое)  
        cursor: Существующий курсор (если None, создается новый)  
        create_query: SQL-запрос создания таблицы (обязательный)  
        triggers: Список SQL-запросов триггеров (опционально)
    """ 
    own_conn = not conn
    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
            cursor = conn.cursor()
        
        # создаём таблицу
        cursor.execute(create_query)

        # обрабатываем триггеры, если есть
        if triggers:
            for trigger_query in triggers:
                cursor.execute(trigger_query)
        
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f'Ошибка при попытке создания таблицы в БД. Изменения отменены: {e}')
        raise
    finally:
        if cursor:
            cursor.close()
        if own_conn and conn:
            conn.close()


def insert_dct_data_to_db(data, conn = None):
    '''
    Adds dict data to db.
    Keys should have consistent names and numbers
    '''

    own_conn = False
    if conn is None:
        conn = create_connection_w_env()
        own_conn = True
    
    col_names = data[0].keys()
    col_sql = ', '.join(col_names)
    placeholders = ', '.join(['%s'] * len(col_names))
    query = f"INSERT INTO avg_position ({col_sql}) VALUES ({placeholders}) ON CONFLICT (nmId, report_date) DO NOTHING"

    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            query,
            (tuple(d[col] for col in col_names) for d in data),
            page_size=1000
        )
    except Exception as e:
        conn.rollback()
        logging.erro
        raise e
    finally:
        cur.close()
        if conn and own_conn:
            conn.close()


def drop_db_table(table_name, conn = None, cursor = None):
    '''
    Полностью удаляет таблицу и информацию о ней из БД
    '''
    own_conn = not (conn or cursor)
    try:
        if not cursor:
            if not conn:
                conn = create_connection_w_env()
            cursor = conn.cursor()
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        conn.commit()
        print(f'Таблица {table_name} была успешно удалена.')
    except Exception as e:
        if conn:
            conn.rollback()
        print(f'Ошибка при попытке удаления таблицы {table_name} из БД: {e}')
        raise 
    finally:
        if cursor:
            cursor.close()
        if own_conn and conn: 
            conn.close()



def list_to_sql_select(values, extra_quotes = False):
    '''
    ['v1', v2] --> 'v1, v2'
    '''
    if extra_quotes:
        return ', '.join(f"'{v}'" for v in values) 
    else:
        return ', '.join(f"{v}" if isinstance(v, str) else str(v) for v in values)