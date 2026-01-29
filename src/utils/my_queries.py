import requests
import pandas as pd

# my packages
from . import my_db_functions as db
from .utils import load_api_tokens

def check_orders_region(sku, limit = 50):
    '''
    Функция для проверки региона заказов.
    Принимает и артикул, и wild (определяет по типу переменной).
    '''

    # если передан int --> артикул
    if isinstance(sku, int):
        column = 'article_id'
    # если передан str и sku[:4] == 'wild' --> вилд
    elif isinstance(sku, str) and sku[:4] == 'wild':
        column = 'supplier_article'
        sku = f"'{sku}'"
    else:
        raise ValueError("Неверный тип или формат sku. Ожидается int или строка, начинающаяся с 'wild'")
    
    df = db.get_df_from_db(f'''
                        select
                            supplier_article,
                            article_id,
                            date,
                            date_from,
                            warehouse_type,
                            region_name
                        from
                            orders
                        where {column} = {sku}
                        order by date desc
                        limit {limit}
                        ''', decimal_to_num=True)
    return df


def check_sku_price(sku, client_name):
    tokens = load_api_tokens()
    api_token = tokens[client_name]
    sku = int(sku)
    url = 'https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter'
    params = {
            'limit': 10,
            'filterNmID': sku}
    headers = {"Authorization": api_token}
    response = requests.get(url, params=params, headers=headers).json()
    try: 
        res = response['data']['listGoods'][0]['sizes'][0]['discountedPrice']
        return res
    except Exception as e:
        print('Finding the needed data failed, returning the full response')
        return response
    

def load_wild_managers(df = False):
    managers = db.get_df_from_db('''
                             SELECT DISTINCT ON (local_vendor_code)
                             local_vendor_code,
                             manager
                             FROM orders_articles_analyze
                             WHERE local_vendor_code LIKE 'wild%'
                             ORDER BY local_vendor_code, date DESC
                             ''')
    if df:
        return db.get_df_from_db(managers)
    else:
        return db.fetch_db_data_into_dict(managers)