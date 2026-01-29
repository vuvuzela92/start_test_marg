# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import json
import time
import random
import gspread
import numpy as np
import pandas as pd
from datetime import date, timedelta
from gspread.exceptions import APIError

# my packages
from db_data_to_purch_gs import update_orders_by_regions
from autopilot_hourly import parse_data_from_WB
from utils.my_gspread import init_client
from utils import my_pandas, my_gspread
from utils import my_db_functions as db
from utils.logger import setup_logger
from dotenv import load_dotenv
load_dotenv()


# ---- SET UP ----
CREDS_PATH=os.getenv('CREDS_PATH')

UNIT_TABLE = os.getenv("UNIT_TABLE")
UNIT_MAIN_SHEET = os.getenv("UNIT_MAIN_SHEET")

AUTOPILOT_TABLE_NAME = os.getenv("AUTOPILOT_TABLE_NAME")
AUTOPILOT_SHEET_NAME = os.getenv("AUTOPILOT_SHEET_NAME")

NEW_ITEMS_TABLE_NAME = os.getenv("NEW_ITEMS_TABLE_NAME")
NEW_ITEMS_ARTICLES_SHEET_NAME = os.getenv("NEW_ITEMS_ARTICLES_SHEET_NAME")

METRICS_RU = {
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
    "rating": "Рейтинг"
}

METRIC_TO_COL = {
    # вилды и аккаунты
    "wild_col" : "D",
    "client_col":"C",
    "category_col": "B",
    
    # Основные метрики
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

    # Исторические (средние) метрики
    "ср. заказы за прошлые 7 дней": "AW",
    "ср.  зак 7 д": "BH",
    "ср. затраты за прошлые 7 дней": "BP",
    "Ср.цена за 7 дней": "CC",
    "Ср. \nскидка WB": "CV",
    "Остатки ФБО ср.за 7 дней": "DM",
    "Ср. прибыль c заказов по ИУ за 7 дней": "DV",
    "Ср. показы за 7 дней": "EV",
    "Ср. клики за 7 дней": "FE",
    "ctr за 7 дней": "FM",
    "Конверсия в корзину за 7 дней": "FU",
    "Конверсия в заказ за 7 дней": "GC",
    "Ср. добавления в корзину за 7 дней": "GK",
    "Ср. переходы в карточку товара за 7 дней": "GS",
    "Ср. \ncpc": "HI",
    "Ср. \nрейтинг": "HQ",
    "Ср.цена за 30 дней": "CB",
    "Медианная цена 30 дней": "CA",
    'ЧП-РК за 7 дней': "ED",
    'Ср. \ncpo': "HA",
    'ДРР факт за 7 дней' : "EM",
    "Ср. cpm" : "HY",
    "Ср. Органика" : "IH"
}



# ---- LOGS ----
LOGS_PATH = os.getenv("LOGS_PATH")
logger = setup_logger("autopilot_daily.log")


def load_data(rename = True):

    # берём метрики (рус и англ) из файла
    # with open('autopilot_curr_metrics_cut.json', 'r', encoding='utf-8') as f:
    #     metrics_dict = json.load(f) 

    percent_metrics = ['ctr', 'to_cart_convers', 'to_orders_convers']
    metric_selects = [
        f"ROUND({col}/100, 5) AS {col}" if col in percent_metrics else col
        for col in METRICS_RU.keys()
    ]

    # select_avg = ', '.join([f'ROUND(avg({col}), 2) as avg_{col}' for col in metrics_dict.keys()])

    select_avg = ', '.join([
        f"ROUND(avg({col})/100, 5) as avg_{col}" if col in percent_metrics else f"ROUND(avg({col}), 2) as avg_{col}"
        for col in METRICS_RU.keys()
    ])

    query_curr = f'''
    SELECT
        -- все метрики
        {', '.join(['date', 'article_id', 'subject_name', 'account', 'local_vendor_code', 'promo_title'] + metric_selects)},
        (profit_by_cond_orders - adv_spend) AS ЧП_РК,

        -- CPM
        CASE 
            WHEN views = 0 THEN 0 
            ELSE ROUND(adv_spend / views * 1000, 2)
        END AS cpm,

        -- Органика
        (open_card_count - clicks) AS Органика,

        -- ДРР
        CASE 
            WHEN orders_sum_rub = 0 THEN 1
            ELSE ROUND((adv_spend / orders_sum_rub), 2)
        END AS "ДРР",

        -- cpo
        CASE 
            WHEN orders_count = 0 THEN adv_spend 
            ELSE ROUND((adv_spend / orders_count), 2)
        END AS "cpo",

        -- Акции
        CASE WHEN promo_title != '' THEN 1 ELSE 0 END AS "Акции"

    FROM orders_articles_analyze
    WHERE date >= CURRENT_DATE - INTERVAL '6 days'
    '''

    query_hist = f'''
    SELECT *
    FROM
        (
            SELECT
                article_id,
                {select_avg},
                (ROUND(avg(profit_by_cond_orders), 2) - ROUND(avg(adv_spend),2)) AS "ЧП-РК за 7 дней",
                ROUND(
                    SUM(adv_spend) * 1000.0 / NULLIF(SUM(views), 0)
                    , 2) AS "Ср. cpm",
                ROUND(AVG(open_card_count - clicks), 2) AS "Ср. Органика",
                
                -- ДРР (ROAS)
                CASE 
                    WHEN SUM(orders_sum_rub) = 0 THEN 1
                    ELSE ROUND((SUM(adv_spend) / SUM(orders_sum_rub)), 2)
                END AS "ДРР факт за 7 дней",
                -- CPO
                CASE 
                    WHEN SUM(orders_count) = 0 THEN SUM(adv_spend)
                    ELSE ROUND((SUM(adv_spend) / SUM(orders_count)), 2)
                END AS "Ср. \ncpo"
            FROM orders_articles_analyze
            WHERE date >= CURRENT_DATE - INTERVAL '2 weeks' + INTERVAL '1 day'
            AND date < CURRENT_DATE - INTERVAL '1 week' + INTERVAL '1 day'
            GROUP BY article_id
        )
        AS week_metrics
    JOIN (
            SELECT
                article_id,
                ROUND(avg(price_with_disc), 2) as month_avg_price_with_disc,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY price_with_disc) as month_median_price_with_disc
            FROM orders_articles_analyze
            WHERE date > CURRENT_DATE - INTERVAL '1 month'
            GROUP BY article_id 
        )
        AS month_metrics
    ON week_metrics.article_id = month_metrics.article_id
    '''
    
    curr, hist = db.get_df_from_db([query_curr, query_hist], decimal_to_num = False)
    hist = hist.loc[:, ~hist.columns.duplicated(keep='first')]

    hist = my_pandas.process_decimal(hist)
    curr = my_pandas.process_decimal(curr)

    curr = curr.rename(columns = {'ЧП_РК':'ЧП-РК'})
    
    if rename:
        curr = curr.rename(columns = METRICS_RU)

        hist_metrics_names = {
            'avg_orders_sum_rub': 'ср. заказы за прошлые 7 дней',
            'avg_orders_count': 'ср.  зак 7 д',
            'avg_adv_spend': 'ср. затраты за прошлые 7 дней',
            'avg_price_with_disc': 'Ср.цена за 7 дней',
            'avg_spp': 'Ср. \nскидка WB',
            'avg_total_quantity': 'Остатки ФБО ср.за 7 дней',
            'avg_profit_by_cond_orders': 'Ср. прибыль c заказов по ИУ за 7 дней',
            'avg_views': 'Ср. показы за 7 дней',
            'avg_clicks': 'Ср. клики за 7 дней',
            'avg_ctr': 'ctr за 7 дней',
            'avg_to_cart_convers': 'Конверсия в корзину за 7 дней',
            'avg_to_orders_convers': 'Конверсия в заказ за 7 дней',
            'avg_add_to_cart_count': 'Ср. добавления в корзину за 7 дней',
            'avg_open_card_count': 'Ср. переходы в карточку товара за 7 дней',
            'avg_cpc': 'Ср. \ncpc',
            'avg_rating': 'Ср. \nрейтинг',
            'month_avg_price_with_disc':'Ср.цена за 30 дней',
            'month_median_price_with_disc': 'Медианная цена 30 дней'
        }
        hist = hist.rename(columns = hist_metrics_names)

    return curr, hist


def push_data(df, headers, col_num, articles_sorted, values_first_row, sh_len, pivot):
    cols = list(df.columns)
    absent_metrics = set(cols) - set(headers)

    if absent_metrics:
        logger.warning(f'\nСледующие метрики отсутствуют в таблице:\n{absent_metrics}\n')
    
    present_metrics = list(set(cols) - set(absent_metrics))
    
    for metric in present_metrics:
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                if pivot:
                    temp_df = df.pivot(columns='date', index='article_id', values=metric)
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                else:
                    temp_df = df[['article_id', metric]].set_index('article_id')
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                    temp_df = temp_df[[metric]]

                metric_range = my_gspread.define_range(metric, headers, col_num, values_first_row, sh_len)
                my_gspread.add_data_to_range(sh, temp_df, metric_range)
                logger.info(f'Данные по "{metric}" успешно добавлены в диапазон {metric_range}')
                break
                
            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f'Достигнуто максимальное количество попыток для метрики "{metric}"')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logger.error(f'Превышен лимит запросов. Попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logger.error(f'Ошибка API при загрузке "{metric}":\n{e}')
                    break
                    
            except Exception as e:
                logger.error(f'Неизвестная ошибка при загрузке "{metric}":\n{e}')
                break


def process_adv_status(unit_sh, autopilot_adv_status, unit_skus = None):
    '''
    Склеивает статус "реклама" из Автопилота со статусом "Товар удалён" из UNIT
    '''
    if not unit_skus:
        unit_skus = my_gspread.get_skus_unit(unit_sh)
    
    # get deleted status
    adv_col_num = my_gspread.find_gscol_num_by_name('Реклама', unit_sh)
    adv_prev_values = unit_sh.col_values(adv_col_num)[1:]
    del_status = {unit_skus[i]:adv_prev_values[i] for i in range(len(adv_prev_values)) if adv_prev_values[i] == 'ТОВАР \nУДАЛЕН '}

    # check that skus w active status are not advertised
    adv_skus = {key:autopilot_adv_status[key] for key in autopilot_adv_status if autopilot_adv_status[key] == 'реклама'}
    errors = set(adv_skus.keys()).intersection(set(del_status.keys()))

    if errors:
        raise ValueError(f'Skus marked as deleted have active adv status: {errors}')

    # add deleted status to autopilot
    for sku in unit_skus:       # используем юнитку, пч не все скю есть в автопилоте 
        if sku in del_status:
            autopilot_adv_status[sku] = del_status[sku]

    # dict to list
    output_data = {sku:autopilot_adv_status.get(sku, '') for sku in unit_skus}
    return output_data
    

def update_adv_status_in_unit(unit_sh, adv_dict):
    '''
    Принимает adv_dict вида {unit_sku : 'реклама', unit_sku : ''}.
    ! Предполагается, что adv_dict уже отсортирован, как в UNIT !

    Преобразует adv_dict в лист вида [['реклама'],['']...] и отправляет в gs
    '''
    output_data = [[adv_dict[key]] for key in adv_dict]
    output_range = my_gspread.define_range('Реклама', unit_sh.row_values(1), number_of_columns=1, values_first_row=2, sh_len = unit_sh.row_count)
    my_gspread.add_data_to_range(unit_sh, output_data, output_range, False)
    logger.info(f'Статус рекламы успешно добавлен в диапазон {output_range}')


def load_and_update_feedbacks_unit(unit_sh, unit_skus):
    feedback_data = parse_data_from_WB(articles=unit_skus, return_keys=['feedbacks'])
    output_data = [value for key, value in feedback_data.items()]
    ouput_range = my_gspread.define_range('Кол-во отзывов ВБ', unit_sh.row_values(1), 1, 2, unit_sh.row_count)
    my_gspread.add_data_to_range(unit_sh, output_data, ouput_range, True)


# new function to avoid 503 error
def push_data_static_range(df, headers, col_num, articles_sorted, values_first_row, sh_len, pivot):
    """
    Pushes data using STATIC column ranges defined in METRIC_TO_COL.
    Only uses sheet length (sh_len) and first row of values.
    All other logic (pivoting, retries) remains unchanged.
    """

    cols = list(df.columns)
    absent_metrics = set(cols) - set(METRIC_TO_COL.keys())
    
    if absent_metrics:
        logger.warning(f'\nСледующие метрики не имеют статического диапазона и будут пропущены:\n{absent_metrics}\n')
    
    present_metrics = list(set(cols) - set(absent_metrics))
    
    for metric in present_metrics:
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Data preparation (unchanged)
                if pivot:
                    temp_df = df.pivot(columns='date', index='article_id', values=metric)
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                else:
                    temp_df = df[['article_id', metric]].set_index('article_id')
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                    temp_df = temp_df[[metric]]

                # === STATIC RANGE LOGIC REPLACES define_range() ===
                range_start = METRIC_TO_COL[metric]  # Start column from dict
                range_end = my_gspread.calculate_range_end(range_start, col_num)  # Expand by col_num columns

                # Format range: StartColRow:EndColRow
                metric_range = f'{range_start}{values_first_row}:{range_end}{sh_len + 1}'

                # Push data (assumes my_gspread.add_data_to_range iss available)
                my_gspread.add_data_to_range(sh, temp_df, metric_range, clean_range=False)
                logger.info(f'Данные по "{metric}" успешно добавлены в диапазон {metric_range}')
                break
                
            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.warning(f'Достигнуто максимальное количество попыток для метрики "{metric}"')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logger.warning(f'Превышен лимит запросов. Попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logger.error(f'Ошибка API при загрузке "{metric}":\n{e}')
                    break
                    
            except Exception as e:
                logger.error(f'Неизвестная ошибка при загрузке "{metric}":\n{e}')
                break


def load_avg_position_curr(articles_sorted = None):
    '''
    Description:
        Loads data for the previous 6 days from the avg_position DB table

    Arguments:
        articles_sorted: if given, the data is filtered and sorted accordingly

    Returns:
        df
    '''
    res = db.get_df_from_db('''
                         select
                            nmid,
                            avgposition,
                            report_date
                         from avg_position
                         where report_date >= CURRENT_DATE - INTERVAL '6 days'
                         ''')
    res['nmid'] = res['nmid'].astype(int)
    pivot = res.pivot(index='nmid', columns='report_date', values='avgposition').fillna('')

    if articles_sorted is not None:
        pivot = pivot.reset_index().set_index('nmid').reindex(articles_sorted).fillna('')
    
    return pivot


def load_avg_position_hist(articles_sorted = None):
    '''
    Description:
        Loads data for the previous week from the avg_position DB table 

    Arguments:
        articles_sorted: if given, the data is filtered and sorted accordingly

    Returns:
        dict {nm_id : avg_price}
    '''
    hist = db.fetch_db_data_into_dict('''
                                   SELECT
                                    nmid,
                                    ROUND(AVG(avgposition), 3) AS avg_position_prior
                                   FROM avg_position
                                   WHERE report_date < CURRENT_DATE - INTERVAL '6 days'
                                   GROUP BY nmid;
                                ''')
    hist = {i['nmid'] : i['avg_position_prior'] for i in hist}

    if articles_sorted is not None: 
        hist = my_pandas.order_dict_by_list(hist, articles_sorted)
    
    return hist
    
def load_vendor_codes_info(filter_skus = None):
    '''
    Отдает словарь {sku : {
                        'local_vendor_code' : local_vendor_code,
                        'account' : account
                        }
    '''

    where = ''
    if filter_skus is not None:
        where = f'where a.nm_id in ({db.list_to_sql_select(filter_skus)})'        

    query = f'''
    select distinct on (a.nm_id)
        a.nm_id,
        a.local_vendor_code,
        a.account,
        cd.subject_name as category
    from
        article a
    left join card_data cd
    on a.nm_id = cd.article_id
    {where}
    '''

    data = db.fetch_db_data_into_dict(query)

    return {i['nm_id']:{'local_vendor_code': i['local_vendor_code'], 'account': i['account'], 'category': i['category']} for i in data}


def load_db_orders():
    query = '''
    select
        fd."date",
        a.local_vendor_code,
        sum(fd.order_count) as "orders_count"
    from funnel_daily fd
    left join article a
        on fd.nm_id = a.nm_id
    where fd."date" > now() - interval '31 days'
        and fd."date" < current_date
        and a.local_vendor_code like 'wild%'
    group by fd."date", a.local_vendor_code
    order by fd."date" desc
    '''
    df = db.get_df_from_db(query)

    # Pivot without reset_index
    pivot_df = df.pivot(index='local_vendor_code', columns='date', values='orders_count')

    # Add yesterday column if missing
    yesterday = date.today() - timedelta(days=1)
    if yesterday not in pivot_df.columns:
        pivot_df[yesterday] = 0

    # Sort only the date columns descending
    pivot_df = pivot_df.reindex(sorted(pivot_df.columns, reverse=True), axis=1).fillna(0)

    return pivot_df.reset_index()

def update_orders_sopost(sopost_sheet):
    n_rows = sopost_sheet.row_count
    wilds_ordered = sopost_sheet.col_values(5)[1:]

    # df with columns: wild, date1, date2, ...
    db_orders = load_db_orders()

    db_dict = db_orders.set_index('local_vendor_code').T.to_dict('list')

    empty_line = [0] * (len(next(iter(db_dict.values()))) if db_dict else 0)
    output_list = [db_dict.get(i, empty_line) for i in wilds_ordered]

    sopost_sheet.update(values = output_list, range_name=f"S2:AV{n_rows}")

    logger.info('Successfully updated orders at the Sopost')

if __name__ == "__main__":

    # ----- 1. загрузка данных из бд -----
    curr_data, hist_data = load_data()


    # ----- 2. берём данные из гугл таблицы -----

    # sh = my_gspread.connect_to_local_sheet(os.getenv("LOCAL_TEST_TABLE"), AUTOPILOT_SHEET_NAME) # test
    sh = my_gspread.connect_to_remote_sheet(AUTOPILOT_TABLE_NAME, AUTOPILOT_SHEET_NAME)
    
    # сколько нужно выделить колонок под каждую метрику (по кол-ву дней)
    col_num = 6

    # начало диапазона
    values_first_row = 4

    # окончание диапазона
    sh_len = sh.row_count
    
    # # заголовки для подсчёта номера колонки
    curr_headers = None #sh.row_values(2)
    hist_headers = None #sh.row_values(3)

    # отсортированный список артикулов, чтобы замэтчить данные
    articles_raw = sh.col_values(1)[3:]
    articles_sorted = [int(n) for n in articles_raw]
    
    # sos_page = my_gspread.connect_to_remote_sheet(NEW_ITEMS_TABLE_NAME, NEW_ITEMS_ARTICLES_SHEET_NAME)
    # articles_sorted = [int(i) for i in sos_page.col_values(1)]


    # ----- 3. обработка данных -----

    push_data_static_range(curr_data, curr_headers, col_num, articles_sorted, values_first_row, sh_len, pivot = True)
    logger.info('Данные за последнюю неделю успешно добавлены.\n')

    push_data_static_range(hist_data, hist_headers, 1, articles_sorted, values_first_row, sh_len, pivot = False)
    logger.info('Более ранние данные успешно добавлены.\n')


    # ----- 4. средняя позиция -----

    # последняя неделя
    avg_curr = load_avg_position_curr(articles_sorted)
    output_range = f'IQ4:IV{sh_len}'
    my_gspread.add_data_to_range(sh, avg_curr, output_range)

    # предпоследняя неделя
    avg_hist = load_avg_position_hist(articles_sorted)
    hist_output = [[value] for key, value in avg_hist.items()]
    hist_range = f'IP4:IP{sh_len}'
    my_gspread.add_data_to_range(sh, hist_output, hist_range)

    logger.info('Данные по средним позициям выгружены')


    # ----- 5. Обновление вилдов и клиентов -----

    info = load_vendor_codes_info()
    vendorcodes = [[info[i].get('local_vendor_code', '')] for i in articles_sorted if i in info]
    accounts = [[str(info[i].get('account', '')).upper()] for i in articles_sorted if i in info]
    categories = [[info[i].get('category', '')] for i in articles_sorted if i in info]
    
    sh.update(values = vendorcodes, range_name = f"{METRIC_TO_COL['wild_col']}{values_first_row}:{METRIC_TO_COL['wild_col']}{sh_len}")
    logger.info('Информация по вилдам успешно обновлена')

    sh.update(values = accounts, range_name = f"{METRIC_TO_COL['client_col']}{values_first_row}:{METRIC_TO_COL['client_col']}{sh_len}")
    logger.info('Информация по кабинетам успешно обновлена')

    sh.update(values = categories, range_name = f"{METRIC_TO_COL['category_col']}{values_first_row}:{METRIC_TO_COL['category_col']}{sh_len}")
    logger.info('Информация по категориям успешно обновлена')




    # ----- юнит -----


    # 1. обновление статуса рекламы

    logger.info('Updating the adv_status in Unit')

    # take yesterday's adv spend data {sku: 'реклама', sku1: ''}
    df_cut_adv_status = curr_data[curr_data['date'] == max(curr_data['date'])][['date', 'article_id', 'Сумма затрат']]

    # convert to dict
    autopilot_adv_status = df_cut_adv_status[['article_id', 'Сумма затрат']].set_index('article_id').to_dict()['Сумма затрат']

    # adv aspend --> adv status
    autopilot_adv_status = {int(key): 'реклама' if value > 0 else '' for key, value in autopilot_adv_status.items()}

    # connect to unit
    client = gspread.service_account(filename=CREDS_PATH)
    unit_table = client.open(UNIT_TABLE)
    unit_sh = unit_table.worksheet(UNIT_MAIN_SHEET)


    unit_skus = my_gspread.get_skus_unit(unit_sh)
    new_adv_status_sorted = process_adv_status(unit_sh, autopilot_adv_status, unit_skus)
    
    # отправляем данные в gs
    update_adv_status_in_unit(unit_sh, new_adv_status_sorted)


    # 2. Обновление данных в Сопосте
    try:
        sopost_sh = unit_table.worksheet('Сопост')
        update_orders_sopost(sopost_sh)
    except Exception as e:
        logger.error(f'Error while updating orders in Sopost: {e}')

    
    # 3. Обновление заказов по регионам в таблице Отгрузки ФБО
    try:
        client = init_client()
        update_orders_by_regions(client, logger=logger)
    except Exception as e:
        logger.error(f'Ошибка при обновлении данных в таблице Отгрузки ФБО: {e}')


    # 4. обновление отзывов

    logger.info('Updating the feedbacks in Unit')

    load_and_update_feedbacks_unit(unit_sh, unit_skus)

    logger.info('Выполнение скрипта завершено')