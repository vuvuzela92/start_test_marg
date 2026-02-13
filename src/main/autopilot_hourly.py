# ---- IMPORTS ----

# making it work for cron
from pathlib import Path
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import json
import time
import random
import asyncio
import logging
import requests
import numpy as np
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
from collections import defaultdict
from gspread.exceptions import APIError
from psycopg2.extras import execute_values

# my packages
# from utils.env_loader import *
from utils import my_pandas, my_gspread
from utils.utils import load_api_tokens
from utils.my_db_functions import fetch_db_data_into_dict, create_connection_w_env

from new_adv import get_all_adv_data, processed_adv_data
from  pathlib import Path
from dotenv import load_dotenv
load_dotenv()


# ---- SET UP ----
BASE_DIR = Path(__file__).resolve().parents[2]
CREDS_PATH = BASE_DIR / os.getenv("CREDS_DIR") / os.getenv('CREDS_FILE')

METRIC_TO_COL = {
    "Сумма заказов": "AX",
    "Кол-во заказов": "BI",
    "Сумма затрат": "BQ",
    "Цены": "CD",
    "скидка WB": "CW",
    "Остатки": "DN",
    "Прибыль c заказов по ИУ": "DW",
    "Показы": "EW",
    "Клики": "FF",
    "ctr": "FN",
    "Конверсия в корзину": "FV",
    "Конверсия в заказ": "GD",
    "Добавления в корзину": "GL",
    "Переходы в карточку товара": "GT",
    "cpc": "HJ",
    "Рейтинг": "HR",
    "cpo": "HB",
    "Акции": "DF",
    "ЧП-РК": "EE",
    "ДРР": "EN",
    "cpm": "HZ",
    "ctr": "FN",
    "Органика": "II",
    "Свободный остаток": "DU",
    "Наша цена с СПП":"CK"
}

METRIC_RU = {
    "orders_sum_rub": "Сумма заказов",
    "orders_count": "Кол-во заказов",
    "adv_spend": "Сумма затрат",
    "price_with_disc": "Цены",
    "spp": "скидка WB",
    "total_quantity": "Остатки",
    "profit_by_cond_orders": "Прибыль c заказов по ИУ",
    "views": "Показы",
    "clicks": "Клики",
    "ctr": "ctr",
    "to_cart_convers": "Конверсия в корзину",
    "to_orders_convers": "Конверсия в заказ",
    "add_to_cart_count": "Добавления в корзину",
    "open_card_count": "Переходы в карточку товара",
    "cpc": "cpc",
    "rating": "Рейтинг",
    "cpo":"cpo",
    "Акции":"Акции",
    "ЧП-РК":"ЧП-РК",
    "ДРР":"ДРР"
}



# ---- LOGS ----

LOGS_PATH = BASE_DIR / os.getenv("LOGS_DIR") / os.getenv('LOG_FILE')

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/autopilot_hourly.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)



def get_fun(account: str, api_token: str, nmIDs: list):
    logging.info(f"Начало обработки аккаунта {account}")
    url = 'https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products'
    headers = {'Authorization': api_token}

    my_date = datetime.now()
    hour = int(datetime.now().strftime('%H'))
    start = my_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')
    end = my_date.replace(hour=hour, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d')

    payload = {
        "selectedPeriod": {"start": start, "end": end},
        "pastPeriod": {
            "start": (my_date - timedelta(days=7)).strftime('%Y-%m-%d'),
            "end": (my_date - timedelta(days=1)).strftime('%Y-%m-%d')
        },
        "nmIds": nmIDs,
        "brandNames": [],
        "subjectIds": [],
        "tagIds": [],
        "skipDeletedNm": True,
        "orderBy": {"field": "orderSum", "mode": "asc"},
        "limit": 1000,
        "offset": 0
    }

    max_retries = 5
    base_delay = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            logging.info(f"Попытка {retry_count + 1} для аккаунта {account}")
            start_time = time.time()
            res = requests.post(url=url, headers=headers, json=payload, timeout=30)
            logging.info(f"Ответ от API для {account} получен за {time.time() - start_time:.2f} сек.")

            if res.status_code != 200:
                logging.warning(f"Код ответа {res.status_code} для {account}.")
                delay = min(base_delay * (2 ** retry_count), 60)
                sleep(delay)
                retry_count += 1
                continue

            try:
                data = res.json()
                if not data.get('data', {}).get('products'):
                    logging.warning(f"Пустые данные для {account}")
                    return pd.DataFrame()

                products = data['data']['products']
                df = pd.json_normalize(products)

                if df.empty:
                    logging.warning(f"Пустой DataFrame для {account}")
                    return df

                logging.info(f"Успешно получено {len(df)} карточек для {account}")

                # Flatten structure similar to the old DataFrame
                df['name'] = df['product.title']
                df['date'] = pd.to_datetime(df['statistic.selected.period.end']).dt.date
                df['openCardCount'] = df['statistic.selected.openCount']
                df['addToCartCount'] = df['statistic.selected.cartCount']
                df['ordersCount'] = df['statistic.selected.orderCount']
                df['ordersSumRub'] = df['statistic.selected.orderSum']
                df['buyoutsCount'] = df['statistic.selected.buyoutCount']
                df['buyoutsSumRub'] = df['statistic.selected.buyoutSum']
                df['cancelCount'] = df['statistic.selected.cancelCount']
                df['cancelSumRub'] = df['statistic.selected.cancelSum']
                df['avgPriceRub'] = df['statistic.selected.avgPrice']
                df['avgOrdersCountPerDay'] = df['statistic.selected.avgOrdersCountPerDay']
                df['addToCartPercent'] = df['statistic.selected.conversions.addToCartPercent']
                df['cartToOrderPercent'] = df['statistic.selected.conversions.cartToOrderPercent']
                df['buyoutsPercent'] = df['statistic.selected.conversions.buyoutPercent']
                df['stocksMp'] = df['product.stocks.mp']
                df['stocksWb'] = df['product.stocks.wb']

                pattern = r'(wild\d+)'
                df['wild'] = df['product.vendorCode'].str.extract(pattern)
                df['account'] = account
                df['nmID'] = df['product.nmId']  # ✅ add backward compatibility column

                keep_cols = [
                    'nmID', 'name', 'date', 'openCardCount', 'addToCartCount', 'ordersCount', 'ordersSumRub',
                    'buyoutsCount', 'buyoutsSumRub', 'cancelCount', 'cancelSumRub', 'avgPriceRub',
                    'avgOrdersCountPerDay', 'addToCartPercent', 'cartToOrderPercent', 'buyoutsPercent',
                    'stocksMp', 'stocksWb', 'wild', 'account'
                ]
                df = df[keep_cols]
                return df


            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON для {account}: {e}")
                logging.debug(f"Ответ сервера: {res.text[:200]}...")
                delay = min(base_delay * (2 ** retry_count), 60)
                logging.info(f"Повтор через {delay} сек...")
                sleep(delay)
                retry_count += 1
                continue

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка запроса для {account}: {e} параметры - {payload}")
            delay = min(base_delay * (2 ** retry_count), 60)
            logging.info(f"Повтор через {delay} сек...")
            sleep(delay)
            retry_count += 1

    logging.error(f"Не удалось получить данные для {account} после {max_retries} попыток")
    return pd.DataFrame()



def collect_full_funnel_data(articles_sorted = None):
    '''
    Собирает данные по воронке по всем клиентам. Отдаёт словарь и заголовки колонок.
    '''
    articles_clients = my_gspread.get_articles_and_clients_dict(articles_sorted)
    tokens = load_api_tokens()

    all_dfs = []
    for account, api_token in tokens.items():
        account_sku = [art for art, lk in articles_clients.items() if lk == account]
        fun_df = get_fun(account, api_token, account_sku)
        fun_df = fun_df[['nmID', 'openCardCount', 'addToCartCount', 'ordersCount', 'ordersSumRub', 'addToCartPercent', 'cartToOrderPercent', 'stocksWb']]
        all_dfs.append(fun_df)
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        column_mapping = {
            'openCardCount': 'open_card_count',
            'addToCartCount': 'add_to_cart_count',
            'ordersCount': 'orders_count',
            'ordersSumRub': 'orders_sum_rub',
            'addToCartPercent': 'to_cart_convers',
            'cartToOrderPercent': 'to_orders_convers',
            'stocksWb': 'total_quantity'}
        final_df = final_df.rename(columns=column_mapping)

        final_df['to_cart_convers'] = final_df['to_cart_convers']/100
        final_df['to_orders_convers'] = final_df['to_orders_convers']/100
        result_dict = final_df.set_index('nmID').apply(list, axis=1).to_dict()
        headers = list(final_df.columns)[1:]
        
        if articles_sorted is not None:
            existing_ids = set(result_dict.keys())
            missing_ids = set(articles_sorted) - existing_ids

            zero_row = [0] * len(headers)
            for nmID in missing_ids:
                result_dict[nmID] = zero_row

    if articles_sorted is not None:
        result_dict = {k: result_dict[k] for k in articles_sorted}

    return result_dict, headers



def get_full_prices_from_API_WB(filter_articles = None):
    '''
    Возвращает данные полной цены товаров по всем клиентов из API WB  
    '''

    # словарь с артикулами по клиентам {артикул: ЛК}
    # articles_clients = my_gspread.get_articles_and_clients_dict(filter_articles)
    
    data = fetch_db_data_into_dict('''
        select c.article_id, a.account
        from card_data c
        join article a
        on c.article_id = a.nm_id
        ''')
    articles_clients = {i['article_id'] : str(i['account']).capitalize() for i in data}

    tokens = load_api_tokens()
    url = 'https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter'
    all_prices = {}
    
    for account, api_token in tokens.items():
        try: 
            # берём данные из апи
            api_token = tokens[account]
            data = my_gspread.get_data_offset(url,
                                              {"Authorization": api_token},
                                              extract_callback = lambda r: r['data']['listGoods'],
                                              return_keys = ['nmID', 'sizes'])
            wb_articles_prices = {item['nmID']: item['sizes'][0]['discountedPrice'] for item in data}

            # оставляем только позиции из UNIT    
            unit_articles = [art for art, lk in articles_clients.items() if lk == account]
            client_prices = {art: wb_articles_prices.get(art, None) for art in unit_articles}
            all_prices.update(client_prices)
        
        except Exception as e:
            logging.error(f'Возникла ошибка при работе с API клиента {account}:\n{e}')
            continue
    
    return all_prices

def parse_data_from_WB(articles, return_keys=None, handle_nested_keys=None, show_errors = False):
    '''
    Получает данные товаров с WB по артикулам. Возвращает:
    - При return_keys: {артикул: [значения, 'ключей']}
    - Без return_keys: полные данные products[0]
    Поддержка вложенных полей: handle_nested_keys=[['путь', 'к', 'полю']]
    Пример: [['sizes', 0, 'price']] → data['sizes'][0]['price']
    '''
    
    url = "https://card.wb.ru/cards/v4/detail"
    params = {
        "appType": 1,
        "curr": "rub",
        "dest": -1255987,
        "spp": 30,
        "hide_vflags": 4294967296,
        "hide_dtype": "9;11",
        "ab_testing": "false"
    }
    
    result = {}
    not_found = 0
    for art in articles:
        try:
            params["nm"] = art
            response = requests.get(url, params=params)
            response.raise_for_status()

            js = response.json()['products'][0]

            if return_keys:
                art_values = []

                for key in return_keys:
                    value = js.get(key, None)

                    # если есть вложенные ключи
                    if handle_nested_keys:
                        for path in handle_nested_keys:

                            # если ключ был передан в handle_nested_keys [aka указаны вложенности]
                            if path[0] == key:
                                try:
                                    nested_value = js
                                    for nest in path:
                                        nested_value = nested_value[nest] 
                                    value = nested_value
                                except Exception as e:
                                    value = None
                                    if show_errors:
                                        print(f'Вложенное значение {key} для артикула {art} не существует. Возвращено None. Ошибка: {e}')
                                        continue
                    
                    art_values.append(value)
                
                result[art] = art_values
            
            # если ключи не заданы, возвращает весь ответ
            else:
                result[art] = js

        except (IndexError, KeyError):
            print(f'Товар с артикулом {art} не найден или отсутствуют данные: {response.json()}')
            not_found += 1
            result[art] = [None] * len(return_keys) if return_keys else None
        except Exception as e:
            print(f'Возникла проблема при парсинге данных по артикулу {art} с сайта WB: {e}')
            not_found += 1
            result[art] = [None] * len(return_keys) if return_keys else None

    logging.info(f'Найдены данные для {len(articles) - not_found} из {len(articles)} артикулов.')

    return result



def load_adv_spend(articles_sorted=None):
    '''
    Возвращает данные по Сумме затрат из API Кометы.
    При articles_sorted=None можно использовать как загрузчик данных кометы по активным позициям.
    При передаче articles_sorted форматирует под полный список артикулов: преобразует данные в сводную таблицу (пивот),
    суммируя затраты по артикулам, добавляет отсутствующие артикулы из списка с нулевыми значениями
    '''
    cometa_api_key = os.getenv('COMETA_API_KEY')
    url_autopilots = 'https://api.e-comet.io/v1/autopilots'
    headers = {'Authorization': cometa_api_key}
    response = requests.get(url_autopilots, headers=headers)
    result = {i['product_id']:i['budget_spent_today'] for i in response.json() if i['active'] == True}

    if articles_sorted:

        spend_agg = {}

        # aggregating
        for article, budget in result.items():
            spend_agg[article] = spend_agg.get(article, 0) + budget

        # проставляем нули на позициях, которых нет в апи
        result = {}
        for article in articles_sorted:
            result[article] = spend_agg.get(article, 0) * 1.1

    return result



def get_data_from_WB(articles = None):

    '''
    Склеивает полную цену из API и цену с spp с сайта WB, считает % spp.
    Возвращает словарь: { article : [promo_status, rating, full_price, spp] }
    '''

    # загружаем полную цену из WB API
    full_price_wb_api = get_full_prices_from_API_WB(articles) # discounted price
    logging.info('Загружены полные цены из API WB.')

    # если артикулы не заданы, берём их из ключей словаря
    if not articles:
        articles = full_price_wb_api.keys()
    
    # парсим цену со скидкой с сайта WB
    logging.info('Идёт парсинг данных с сайта WB...')
    parsed_data = parse_data_from_WB(articles, ['promoTextCard', 'reviewRating', 'sizes'], [['sizes', 0, 'price', 'product']])

    # оформляем финальный словарь
    result = {}
    for article, full_price in full_price_wb_api.items():
        article_data = parsed_data.get(article, [None, None, None])
        promo_status = 1 if article_data[0] is not None else 0
        rating = article_data[1]
        discounted_price = article_data[2] / 100 if article_data[2] else None

        # считаем spp
        if full_price and discounted_price:
            spp = round((full_price - discounted_price) / full_price * 100, 1)
        else:
            spp = ''

        result[article] = {'promo_status':promo_status,
                           'rating': rating,
                           'full_price': full_price,
                           'spp': spp,
                           'discounted_price': discounted_price}

    return result


def get_calc_data(adv_spend, fun_data, fun_headers):
    '''
    'Прибыль с заказов по ИУ', ЧП-РК, ДРР, cpo
    '''
    # маржа из UNIT
    unit_sh = my_gspread.connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')
    margin = my_gspread.col_values_by_name('Мар', unit_sh, 1)[1:]
    # margin = [float(i.strip('%')) / 100 for i in margin]
    margin = [float(i.strip('%').replace(',', '.')) / 100 for i in margin]
    articles = unit_sh.col_values(1)[1:]
    margin_by_article = {int(articles[i]):margin[i] for i in range(len(articles))}

    # сумма заказов
    orders_sum_ind = fun_headers.index('orders_sum_rub') # берём индекс
    orders_sum_dct = {int(article): values[orders_sum_ind] for article, values in fun_data.items()} # собираем заказы в отд словарь
    # считаем прибыль
    profit_data = {article : orders_sum_dct.get(article, 0) * margin_by_article.get(article, 1) for article in set(orders_sum_dct)|set(margin_by_article)}

    # кол-во заказов
    orders_count_ind = fun_headers.index('orders_count')
    orders_count_dct = {int(article): values[orders_count_ind] for article, values in fun_data.items()}

    # чп-рк
    net_profit = {article : profit_data.get(article, 0) - adv_spend.get(article, 0) for article in set(profit_data)|set(adv_spend)}

    # дрр aka доля рекламных расходов
    adv_part = {}
    for article in set(adv_spend) | set(orders_sum_dct):
        numerator = adv_spend.get(article, 0)
        denominator = orders_sum_dct.get(article, numerator)
        adv_part[article] = numerator / denominator if denominator != 0 else 1.0

    # cpo
    cpo = {}
    for article in set(adv_spend) | set(orders_count_dct):
        numerator = adv_spend.get(article, 0)
        denominator = orders_count_dct.get(article, numerator)
        cpo[article] = numerator / denominator if denominator != 0 else 1.0

    return profit_data, net_profit, adv_part, cpo


def process_adv_stat_new():
    '''
    Получает рекламную статистику по всем кабинетам с помощью асинхронной функции.
    Берёт только общие просмотры, клики и затраты, агрегирует данные по артикулам.
    Дополнительно считает ctr, cpc, cpm

    Возвращает лист словарей
    '''
    logging.info('Processing adv_stat new...')
    
    raw_data = asyncio.run(get_all_adv_data())
    data = processed_adv_data(raw_data)

    agg = defaultdict(lambda: {'clicks': 0, 'views': 0, 'adv_spend': 0})
    for i in data:
        aid = i['article_id']
        agg[aid]['clicks'] += i['clicks']
        agg[aid]['views'] += i['views']
        agg[aid]['adv_spend'] += i['sum']
    
    clean_data = []
    for article_id, metrics in agg.items():
        clicks = metrics['clicks']
        views = metrics['views']
        spend = metrics['adv_spend']

        ctr = clicks / views if views > 0 else 0
        cpc = spend / clicks if clicks > 0 else 0
        cpm = (spend / views) * 1000 if views > 0 else 0

        clean_data.append({
            'article_id': article_id,
            'clicks': clicks,
            'views': views,
            'adv_spend': spend,
            'ctr': round(ctr, 2),
            'cpc': round(cpc, 2),
            'cpm': round(cpm, 2)
        })

    return clean_data


def push_data(sh, dct, metric_names, gsheet_headers, matched_metrics, articles_sorted, col_num, values_first_row, sh_len):
    '''
    Функция для загрузки значений словарей в гугл таблицу.
    Принимает словари в формате {article : value}, {article : [value]} и {article : [value1, value2, ...]}.
    Предварительно сортирует данные.
    '''
    # если передаём просто значения, для начала преобразуем в листы для корректной обработки
    if isinstance(next(iter(dct.values())), (float, int)):
        dct = {k: [v] for k, v in dct.items()}

    if isinstance(metric_names, str):
        metric_names = [metric_names]

    # сортирует данные, как в гугл таблице, добавляет [None]*len_dct_values, если данных нет
    ordered_dict = my_pandas.order_dict_by_list(dct, articles_sorted)

    for i in range(len(next(iter(dct.values())))):
        metric_data = [[0 if value is None else value] for values in ordered_dict.values() for value in [values[i]]]
        metric_ru = METRIC_RU[metric_names[i]]
        metric_range = my_gspread.define_range(metric_ru, gsheet_headers, col_num, values_first_row, sh_len, all_col=False)

        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                my_gspread.add_data_to_range(sh, metric_data, metric_range, clean_range=False)
                logging.info(f'Данные по {metric_ru} за сегодня были успешно добавлены.')
                break
                
            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.warning(f'Ошибка: превышен лимит запросов для {metric_ru}. Прекращаем попытки.')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logging.warning(f'Лимит запросов. Повторная попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logging.warning(f'Ошибка API при загрузке {metric_ru}: {e}')
                    break
                    
            except Exception as e:
                logging.error(f'Ошибка при загрузке {metric_ru} в гугл таблицу: {e}')
                break



def push_data_static_range(sh, dct, metric_names, gsheet_headers, matched_metrics, articles_sorted, col_num, values_first_row, sh_len):
    '''
    Pushes dictionary data to Google Sheets using STATIC column ranges.
    Supports {article: value}, {article: [value]}, {article: [v1, v2, ...]}.
    Uses pre-defined column letters from METRIC_TO_COL.
    '''

    # Convert scalar values to lists for uniform processing
    if isinstance(next(iter(dct.values())), (float, int)):
        dct = {k: [v] for k, v in dct.items()}

    if isinstance(metric_names, str):
        metric_names = [metric_names]

    # Sort data according to article list
    ordered_dict = my_pandas.order_dict_by_list(dct, articles_sorted)

    for i in range(len(next(iter(dct.values())))):
        # metric_data = [[0 if value is None else value] for values in ordered_dict.values() for value in [values[i]]]

        metric_data = []
        for article in articles_sorted:
            values = ordered_dict.get(article, [0]*len(metric_names))
            metric_data.append([values[i]])

        metric_ru = METRIC_RU[metric_names[i]]

        # === STATIC RANGE LOGIC ===
        if metric_ru not in METRIC_TO_COL:
            logging.warning(f"Metric '{metric_ru}' not found in static column mapping. Skipping.")
            continue

        range_start = METRIC_TO_COL[metric_ru]
        range_end = my_gspread.calculate_range_end(range_start, col_num)  # uses your existing helper
        metric_range = f'{range_end}{values_first_row}:{range_end}{sh_len}'

        # === END STATIC RANGE ===

        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                my_gspread.add_data_to_range(sh, metric_data, metric_range, clean_range=False)
                logging.info(f'Данные по {metric_ru} за сегодня были успешно добавлены в диапазон {metric_range}.')
                break

            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.warning(f'Ошибка: превышен лимит запросов для {metric_ru}. Прекращаем попытки.')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logging.warning(f'Лимит запросов. Повторная попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logging.warning(f'Ошибка API при загрузке {metric_ru}: {e}')
                    break

            except Exception as e:
                logging.error(f'Ошибка при загрузке {metric_ru} в гугл таблицу: {e}')
                break


def load_unit_remains(unit_sh = None):

    if unit_sh is None:
        unit_sh = my_gspread.connect_to_remote_sheet(os.getenv("UNIT_TABLE"), os.getenv("UNIT_MAIN_SHEET"))

    # 1. take remains data from unit
    skus = unit_sh.col_values(1)
    remains = unit_sh.col_values(51)

    expected_col = 'Свободный остаток\n(сервис)'

    if remains[0] != expected_col:
        logging.error(f'''Проблема с выгрузкой остатков из юнит в ПУ: ожидаемое название колонки - {expected_col} - не
                      совпадает с фактическим - {remains[0]}''')
        raise ValueError
    
    skus = skus[1:]
    remains = remains[1:]

    # unit_remains = {
    #     int(skus[i]): int(remains[i]) if remains[i] != '' else None 
    #     for i in range(len(skus))
    # }

    unit_remains = {
    int(skus[i]): int(remains[i]) if i < len(remains) and remains[i] != '' else None
    for i in range(len(skus))
    }
    
    return unit_remains


def insert_spp_data_to_db(connection, wb_data):

    '''
    Функция insert_spp_data_to_db вставляет данные о ценах и скидках товаров в таблицу spp_history.
    - Пропускает вставку, если данные за текущий час уже существуют.
    - Получает последние значения цен из базы для каждого товара.
    - Добавляет только новые записи или записи с изменившейся ценой.
    - Игнорирует товары с отсутствующими или некорректными значениями (нечисловыми).
    ''' 

    # I.
    # ПУ обновляется раз в полчаса, записывать данные нужно раз в час
    # --> проверяем, были ли записи в этом часу
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 1
            FROM spp_history
            WHERE date(created_at) = current_date
                AND date_part('hour', created_at) = date_part('hour', NOW())
            LIMIT 1;
        """)
        if cursor.fetchone():
            logging.info("Найдено обновление цены за последний час. Изменения не внесены в spp_history")
            return
    
        # II. Берем последние данные для каждого артикула из БД
        cursor.execute("""
            SELECT DISTINCT ON (nm_id) nm_id, full_price, spp_price
            FROM spp_history
            ORDER BY nm_id, created_at DESC;
        """)
        last_data = {row[0]: {'full_price': row[1], 'spp_price': row[2]} for row in cursor.fetchall()}

        # III. Добавляем данные, только если есть изменения в цене
        records = []
        for nm_id, info in wb_data.items():
            try:
                full_price = float(info.get('full_price'))
                spp_price = float(info.get('discounted_price'))
                spp_percent = float(info.get('spp'))
            except (TypeError, ValueError):
                continue

            prev_data = last_data.get(nm_id, {})
            if not prev_data or full_price != prev_data.get('full_price') or spp_price != prev_data.get('spp_price'):
                records.append((nm_id, full_price, spp_percent, spp_price))

        if records:
            execute_values(cursor, """
                    INSERT INTO spp_history (nm_id, full_price, spp_percent, spp_price)
                    VALUES %s;
                """, records)
            connection.commit()
            logging.info("Найдены изменения в цене СПП. Изменения записаны в БД")


if __name__ == "__main__":

    pilot_table_name = os.getenv('AUTOPILOT_TABLE_NAME')
    pilot_sheet_name = os.getenv('AUTOPILOT_SHEET_NAME')

    sh = my_gspread.connect_to_remote_sheet(pilot_table_name, pilot_sheet_name) # prod
    
    # заголовки для подсчёта номера колонки
    сurr_headers = None #sh.row_values(2)
    col_num = 7
    values_first_row = 4
    sh_len = sh.row_count

    # for tests
    articles_raw = sh.col_values(1)[3:]
    articles_sorted = [int(n) for n in articles_raw]


    # берём метрики (рус и англ) из файла
    # with open('autopilot_curr_metrics_full.json', 'r', encoding='utf-8') as f:
    #     matched_metrics = json.load(f)

    try:
        

        # ----- funnel -----
        fun_data, fun_headers = collect_full_funnel_data(articles_sorted)

        push_data_static_range(sh = sh, dct = fun_data, metric_names = fun_headers, gsheet_headers = сurr_headers, matched_metrics = METRIC_RU,
                articles_sorted = articles_sorted, col_num = col_num, values_first_row = values_first_row, sh_len=sh_len)
        
        current_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        sh.update(
            values=[[f'Актуализировано на {current_time}']],
            range_name='A2'
        )

        # ----- adv spend -----
        adv_spend = load_adv_spend(articles_sorted)
        adv_header = 'adv_spend'

        push_data_static_range(sh = sh, dct = adv_spend, metric_names = adv_header, gsheet_headers = сurr_headers, matched_metrics = METRIC_RU,
                articles_sorted = articles_sorted, col_num = col_num, values_first_row = values_first_row, sh_len=sh_len)        

        # ----- выгрузка остатков из юнитки -----
        try:
            unit_sh = my_gspread.connect_to_remote_sheet(os.getenv("UNIT_TABLE"), os.getenv("UNIT_MAIN_SHEET"))
            unit_remains = load_unit_remains(unit_sh = unit_sh)
            pilot_remains = {sku:unit_remains.get(sku, None) for sku in articles_sorted}
            output_data = [[value] for key, value in pilot_remains.items()]

            col_letter = METRIC_TO_COL["Свободный остаток"]
            output_range = f"{col_letter}{values_first_row}:{col_letter}{sh_len}"
            my_gspread.add_data_to_range(sh, output_data, output_range)
            logging.info('Остатки склада успешно загружены в ПУ')
        except Exception as e:
            logging.error(f"Не удалось выгрузить остатки из юнитки в ПУ:\n{e}")
            raise ValueError

        # ----- calculations -----
        profit_data, net_profit, adv_part, cpo = get_calc_data(adv_spend, fun_data, fun_headers)
        calc_headers = ['profit_by_cond_orders', 'ЧП-РК', 'ДРР', 'cpo']
        
        for header, calc_data in zip(calc_headers, [profit_data, net_profit, adv_part, cpo]):
            push_data_static_range(sh = sh, dct = calc_data, metric_names = header, gsheet_headers = сurr_headers, matched_metrics = METRIC_RU,
                    articles_sorted = articles_sorted, col_num = col_num, values_first_row = values_first_row, sh_len=sh_len)
            
        
        # ----- клики, ctr, cpc, cpm -----
        adv_data = process_adv_stat_new()
        adv_by_sku = {item['article_id']: {k: v for k, v in item.items() if k != 'article_id'}
                      for item in adv_data
                      }
        adv_ordered = [adv_by_sku[id] for id in articles_sorted if id in adv_by_sku] 
        for metric_en, metric_ru in [['clicks', 'Клики'],['views', 'Показы'],
                                     ['cpm', 'cpm'], ['cpc', 'cpc'], ['ctr', 'ctr']]:
            metric_data = [[i[metric_en]] for i in adv_ordered]
            range_start = METRIC_TO_COL[metric_ru]
            range_end = my_gspread.calculate_range_end(range_start, col_num)
            metric_range = f'{range_end}{values_first_row}:{range_end}{sh_len}'

            try:
                my_gspread.add_data_to_range(sh, metric_data, metric_range, clean_range=False)
                logging.info(f'Данные по {metric_ru} за сегодня были успешно добавлены в диапазон {metric_range}.')
            except Exception as e:
                logging.error(f'Failed to add data for metric {metric_ru}:\n{e}')
                continue
        
        # ----- органика -----
        try:
            open_card_idx = fun_headers.index('open_card_count')
        except ValueError:
            raise KeyError("'open_card_count' not found in funnel headers")

        open_card_dict = {
            int(nm_id): values[open_card_idx]
            for nm_id, values in fun_data.items()
        }

        clicks_dict = {item['article_id']: item['clicks'] for item in adv_data}

        organic_list = []
        for nm_id in articles_sorted:
            open_cnt = open_card_dict.get(nm_id, 0)
            clicks = clicks_dict.get(nm_id, 0)
            organic = max(0, open_cnt - clicks)
            organic_list.append(organic)

        organic_list = [[i] for i in organic_list]

        range_start = METRIC_TO_COL['Органика']
        range_end = my_gspread.calculate_range_end(range_start, col_num)
        metric_range = f'{range_end}{values_first_row}:{range_end}{sh_len}'

        try:
            my_gspread.add_data_to_range(sh, organic_list, metric_range, clean_range=False)
            logging.info(f'Данные по Органика за сегодня были успешно добавлены в диапазон {metric_range}.')
        except Exception as e:
            logging.error(f'Failed to add data for metric Органика:\n{e}')

       # ----- promo, rating, prices, spp, цена с спп -----
        wb_data = get_data_from_WB(articles_sorted)

        # update spp price in db
        try:
            connection = create_connection_w_env()
            insert_spp_data_to_db(connection, wb_data)
            connection.close()
        except Exception as e:
            logging.erorr(f"Ошибка при попытке внесения изменений СПП цены: {e}")

        try:
            # выгружаем promo, rating, prices, spp
            for metric_ru, metric_en in [['Акции', 'promo_status'],
                                        ['Рейтинг', 'rating'],
                                        ['Цены', 'full_price'],
                                        ['скидка WB', 'spp']]:
                metric_data = [[wb_data[i][metric_en]] for i in articles_sorted]
                range_start = METRIC_TO_COL[metric_ru]
                range_end = my_gspread.calculate_range_end(range_start, col_num)
                metric_range = f'{range_end}{values_first_row}:{range_end}{sh_len}'

                try:
                    my_gspread.add_data_to_range(sh, metric_data, metric_range, clean_range=False)
                    logging.info(f'Данные по {metric_ru} за сегодня были успешно добавлены в диапазон {metric_range}.')
                except Exception as e:
                    logging.error(f'Failed to add data for metric {metric_ru}:\n{e}')
                    continue
        except Exception as e:
            logging.error(f"Ошибка при выгрузке {metric_ru}: {e}")

        try:

            # выгружаем цену с спп
            spp_price = [
                [wb_data[i].get('discounted_price', '')] if i in wb_data else ['']
                for i in articles_sorted
            ]
            spp_price_col_letter = METRIC_TO_COL["Наша цена с СПП"]

            metric_range = f'{spp_price_col_letter}{values_first_row}:{spp_price_col_letter}{sh_len}'
            my_gspread.add_data_to_range(sh, spp_price, metric_range, clean_range=False)
            logging.info(f'Данные по Наша цена с СПП за сегодня были успешно добавлены в диапазон {metric_range}.')
        
        except Exception as e:
            logging.error(f"Ошибка при выгрузке Цены с СПП: {e}")
        
    except Exception as e:
        logging.error(f'Error:\n{e}')