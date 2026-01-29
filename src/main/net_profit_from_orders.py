import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import date, timedelta
import pandas as pd
import numpy as np

from utils.my_db_functions import create_db_table, insert_new_rows, get_df_from_db, get_purchase_price_from_db
from utils.my_gspread import connect_to_remote_sheet
from utils.logger import setup_logger


logger = setup_logger("net_profit_from_orders.log")

COMMON_COMMISSION = 26

def reset_net_profit_from_orders():
    '''
    Функция берёт ВСЕ строки из orders и добавляет их в net_profit_from_orders
    ! Применялась разово, в основной логике скрипта не используется !
    '''
    df = get_data(False) # False - загружает данные из БД, True - из файла (для тестов)

    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'warehouse_type', 'article_id', 'supplier_article', 'subject',
       'order_count', 'total_sales', 'commission', 'purchase_price', 'tax']]
    
    insert_new_rows("net_profit_from_orders", df)


def create_net_profit_from_orders():
    '''
    Функция создаёт таблицу net_profit_from_orders и задаёт триггеры created_at и net_profit_from_orders.
    ! Применялась разово, в основной логике скрипта не используется !
    '''

    # запрос создания таблицы
    create_db_query = ''' 
    CREATE TABLE IF NOT EXISTS net_profit_from_orders (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        warehouse_type VARCHAR(255) NOT NULL,
        article_id BIGINT NOT NULL,
        supplier_article VARCHAR(255) NOT NULL,
        subject VARCHAR(255) NOT NULL,
        order_count INTEGER NOT NULL,
        total_sales NUMERIC(12, 2) NOT NULL,
        commission NUMERIC(5, 2) NOT NULL,
        purchase_price NUMERIC(12, 2),
        tax NUMERIC(5, 2) NOT NULL,
        result_net_profit NUMERIC(12, 2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

        CONSTRAINT unique_article_per_day UNIQUE (date, article_id, warehouse_type)
    );
    '''

    # запросы для триггеров
    trigger_queries = [

    '''
    CREATE OR REPLACE FUNCTION calculate_net_profit()
    RETURNS TRIGGER AS $$
    DECLARE
        total_commission NUMERIC;
        total_after_commission NUMERIC;
        total_after_tax NUMERIC;
    BEGIN
        -- Calculate total commission amount
        total_commission := NEW.total_sales * (NEW.commission/100);
        
        -- Subtract commission from total sales
        total_after_commission := NEW.total_sales - total_commission;
        
        -- Subtract tax
        total_after_tax := total_after_commission * (1 - NEW.tax/100);
        
        -- Calculate final profit: (sales - commission - tax) - (total purchase cost)
        NEW.result_net_profit := total_after_tax - (NEW.order_count * NEW.purchase_price);
        
        RETURN NEW;
    END
    $$ LANGUAGE plpgsql;
    ''',

    '''
    CREATE TRIGGER trg_net_profit
    BEFORE INSERT OR UPDATE ON net_profit_from_orders
    FOR EACH ROW EXECUTE FUNCTION calculate_net_profit();
    ''',

    '''
    CREATE OR REPLACE FUNCTION set_created_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.created_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    ''',

    '''
    CREATE TRIGGER trg_created_at
    BEFORE INSERT ON net_profit_from_orders
    FOR EACH ROW EXECUTE FUNCTION set_created_at();
    '''
    ]

    # создание таблицы
    create_db_table(create_query = create_db_query, triggers = trigger_queries)



def load_orders(local = True):
    '''
    Загрузка данных из таблицы orders в БД
    '''
    if local:
        df = pd.read_csv('orders_data_db.csv')
        df['date'] = pd.to_datetime(df['date']).dt.date
    else:
        try:

            db_table = 'orders'
            query = f'''
            SELECT 
                date,
                warehouse_type,
                article_id,
                supplier_article,
                subject,
                COUNT(price_with_disc) AS order_count,
                SUM(price_with_disc) AS total_sales
            FROM {db_table}
            WHERE is_cancel = False
            GROUP BY 
                date,
                article_id,
                warehouse_type,
                supplier_article,
                subject
            '''

            df = get_df_from_db(query)
            df['article_id'] = df['article_id'].astype(int)
            df['date'] = pd.to_datetime(df['date']).dt.date
        except Exception as e:
            logger.error(f'Ошибка при попытке выгрузки данных из БД таблицы {db_table}:\n{e}')
    return df


def load_commissions():
    '''
    Загрузка данных по комиссии с листа "Комиссия с июля" таблицы UNIT 
    '''
    sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'Комиссия с июля')
    sh_values = sh.get_all_values()
    com = pd.DataFrame(sh_values[1:], columns=sh_values[0])
    clean_com = com.drop(columns = ['FBO\nКомиссия общая', 'FBS\nКомиссия общая'])
    clean_com = clean_com.rename(columns={'FBO ИУ\nс июля': 'FBO', 'FBS ИУ\nс июля':'FBS', 'Предыдущая комиссия ИУ': 'BeforeJuly'})
    return clean_com


def get_data():
    '''
    Оформляет данные (orders, комиссии, цена закупки) в единый df
    '''

    # 1. загрузка данных
    df = load_yesterday_orders() # данные за вчера

    yesterday = date.today() - timedelta(days=1)
    target_date = date(2026, 1, 1)

    if yesterday < target_date:
        # старая логика для обработки комиссий до 1 января 2026 г. (по категориям)
        # 2. склеиваем данные по типу склада и дате
        clean_com = load_commissions()

        com_change_date = pd.to_datetime('2025-07-01').date()

        com_indexed = clean_com.set_index('Наименование предмета')
        FBO_com = com_indexed['FBO']
        FBS_com = com_indexed['FBS']
        prev_com = com_indexed['BeforeJuly']

        conditions = [
            (df['warehouse_type'] == 'Склад WB') & (df['date'] >= com_change_date),
            (df['warehouse_type'] == 'Склад продавца') & (df['date'] >= com_change_date)
        ]

        choices = [
            df['subject'].map(FBO_com),
            df['subject'].map(FBS_com)
        ]

        df['commission'] = np.select(conditions, choices, default=df['subject'].map(prev_com))

    else:
        df['commission'] = COMMON_COMMISSION


    # 3. добавляем цену закупки и налог

    tax = 6

    purch_price = get_purchase_price_from_db()
    df['purchase_price'] = df['article_id'].map(purch_price)
    df['tax'] = tax

    if yesterday < target_date:
        df['commission'] = (df['commission'].str.replace(',', '.').astype(float))

    for column in ['purchase_price', 'commission', 'tax', 'total_sales']:
        df[column] = df[column].astype(float)
    df['order_count'] = df['order_count'].astype(int)

    # убираем дубликаты
    df['supplier_article'] = df['supplier_article'].replace('wild167', 'wild172d')
    df = df.groupby(['date', 'article_id', 'warehouse_type']).agg({
        'supplier_article': 'first',
        'subject': 'first',
        'order_count': 'sum',
        'total_sales': 'sum',
        'commission': 'first',
        'purchase_price': 'first',
        'tax': 'first'
    }).reset_index()

    return df


def load_yesterday_orders():
    '''
    Выгружает данные по заказам за предыдущий день
    '''
    try:
        db_table = 'orders'
        query = f'''
        SELECT 
            date,
            warehouse_type,
            article_id,
            supplier_article,
            subject,
            COUNT(price_with_disc) AS order_count,
            SUM(price_with_disc) AS total_sales
        FROM {db_table}
        WHERE is_cancel = False
        AND date = CURRENT_DATE - INTERVAL '1 day' -- yesterday
        GROUP BY 
            date,
            article_id,
            warehouse_type,
            supplier_article,
            subject
        '''

        df = get_df_from_db(query)
        df['article_id'] = df['article_id'].astype(int)
        df['date'] = pd.to_datetime(df['date']).dt.date

    except Exception as e:
        logger.error(f'Ошибка при попытке выгрузки данных из БД таблицы {db_table}:\n{e}')
    
    return df
    


if __name__ == "__main__":
    df = get_data() # выгружаем данные за вчера
    df = df[['date', 'warehouse_type', 'article_id', 'supplier_article', 'subject',
       'order_count', 'total_sales', 'commission', 'purchase_price', 'tax']]
    df = df.rename(columns = {'total_sales':'orders_revenue'})
    insert_new_rows("net_profit_from_orders", df)