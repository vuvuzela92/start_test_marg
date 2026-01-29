import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from pathlib import Path

import numpy as np
from datetime import datetime

import utils.my_gspread as gs
from utils.my_pandas import process_decimal
from utils.my_db_functions import get_df_from_db
from utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger("purchase_price_update.log")

def load_data_from_db(round_price = False):
    '''
    Выгружает последнюю запись для каждого уникального local_vendor_code за вчера и позавчера.
    Если round_price == True, округляет цены как в UNIT

    Возвращает:
    pandas.DataFrame: Данные с колонками supply_date, local_vendor_code, 
                     product_name, amount_with_vat, quantity, price_per_item
    
    !!! round_price стоит использовать только при обновлении всех записей, новые цены ок и без округления !!!
    '''

    # запрос
    query = f'''
    WITH latest_purchase_price AS (
        SELECT DISTINCT ON (local_vendor_code)
            supply_date,
            guid,
            document_number,
            local_vendor_code,
            product_name,
            event_status,
            amount_with_vat,
            quantity,
            ROUND(amount_with_vat / quantity, 2) AS latest_price_per_item, -- renamed
            currency,
            planned_cost
        FROM supply_to_sellers_warehouse
        WHERE is_valid = TRUE
        -- AND event_status = "Проведён"
        AND local_vendor_code LIKE 'wild%'
        AND supplier_name != 'РВБ ООО'
        AND quantity != 0
        ORDER BY local_vendor_code, supply_date DESC
    )
    SELECT
        lpp.supply_date,
        lpp.guid,
        lpp.document_number,
        lpp.local_vendor_code,
        lpp.product_name,
        lpp.amount_with_vat,
        lpp.quantity,
        latest_price_per_item,
        -- FINAL price_per_item
        CASE
            WHEN lpp.currency IS NOT NULL AND lpp.currency != '643'
                THEN lpp.planned_cost
            ELSE lpp.latest_price_per_item
        END AS price_per_item,
        lpp.currency,
        lpp.planned_cost,
        -- Alarm column
        CASE
            WHEN lpp.currency IS NOT NULL
            AND lpp.currency != '643'
            AND (lpp.planned_cost IS NULL OR lpp.planned_cost = 0)
                THEN 'ALARM: planned_cost missing'
            ELSE NULL
        END AS alarm_flag
    FROM latest_purchase_price lpp
    ORDER BY lpp.local_vendor_code;
    '''

    # выгружаем в df
    data = get_df_from_db(query)

    # убираем decimal
    clean_data = process_decimal(data)

    if round_price == True:
        # округляем как в сопосте
        clean_data['price_per_item'] = clean_data['price_per_item'].apply(lambda x: round(x + 0.01))

    return clean_data


def load_wilds_with_unchangeable_price(sh = None):
    '''
    Возвращает:
    list: wild с неизменяемой ценой (1 в столбце Неизменяемая цена в UNIT)
    '''

    if not sh:
        sh = gs.connect_to_remote_sheet('UNIT 2.0 (tested)', 'Сопост')

    # загружаем цены, которые нельзя изменять
    never_change_col = gs.col_values_by_name(col_name='Неизменяемая цена', sh = sh) # проблема с этой функцией - берёт данные предыдущей колонки, приходится использовать offset
    wilds_col = gs.col_values_by_name(col_name='wild', sh = sh)
    never_change_wilds = [wilds_col[i] for i in range(min(len(never_change_col), len(wilds_col))) 
                  if never_change_col[i] == '1']
    return never_change_wilds


def process_data(sh, round_price=False):
    '''
    Склеивает данные из БД и Сопоста.
    Исключает СКЮ, у которых нельзя изменять цену.
    '''
    # --- 2. load data ---

    # 1C data
    db_data = load_data_from_db(round_price)

    # wild with price we can't change
    wilds_with_unchangeable_price = load_wilds_with_unchangeable_price(sh)

    article_cost_range = gs.define_range(target_header_name = 'wild',
                                                all_headers = list(sh.row_values(1)),
                                                number_of_columns = 2,
                                                values_first_row = 2,
                                                sh_len = sh.row_count)
    unit_values = sh.get_values(article_cost_range)

    # dict {article : purchase_price} from unit 
    unit_prices = {row[0]: gs.clean_number(row[1]) for row in unit_values}

    # list of articles from unit (to get indexes)
    unit_articles = [row[0] for row in unit_values]


    # --- 3. merge & clean data ---
    data = db_data.copy()

    # 3.1. exclude skus with unchangeable price
    excluded_sku = set(data['local_vendor_code']).intersection(set(wilds_with_unchangeable_price))
    if excluded_sku:
        logger.info(f'Исключённые СКЮ (неизменяемая цена): {excluded_sku}')
    data = data[~data['local_vendor_code'].isin(wilds_with_unchangeable_price)]


    # 3.2. skus present in db but absent from UNIT
    absent_sku = set(db_data['local_vendor_code']) - set(unit_prices.keys())
    if absent_sku:
        logger.warning(f'Следующие артикулы присутствуют в БД, но отсутствуют в UNIT:\n{absent_sku}')
    data = data[data['local_vendor_code'].isin(unit_prices.keys())] # оставляем только те строки, где есть цены в unit_prices (inner join)

    # 3.3. process price

    # добавляем цену из unit к данным
    data['unit_price'] = np.round(data['local_vendor_code'].map(unit_prices), 0)

    # добавляем столбец с разницей в цене в рублях
    data['price_diff_rub'] = data['unit_price'] - data['price_per_item'] 

    # добавляем столбец с разницей в цене в процентах для проверки адекватности данных
    data['price_diff_percent'] = round(((data['price_per_item'] - data['unit_price']) / data['unit_price']),2)
    data['price_diff_percent'] = data['price_diff_percent'].replace([float('inf'), -float('inf')], 0)   # process infs

    # обрабатываем случаи, если цена изменилась больше, чем на 25%
    suspicious_rows = data[abs(data['price_diff_percent']) >= 0.25]
    if len(suspicious_rows) > 0:
        logger.warning(f"По следующим позициям цена изменилась более, чем на 25%:\n{suspicious_rows[['local_vendor_code', 'product_name', 'price_per_item', 'unit_price', 'price_diff_rub', 'price_diff_percent']]}")
    
    # выбираем товары, у которых не совпадают цены
    cut_data = data[data['price_per_item'] != data['unit_price']]

    if 'wild1563' in cut_data['local_vendor_code']:
        logger.warning(f"У wild1563 (вместо которого продавался wild1554) изменилась цена. Цена wild1554: {data[data['local_vendor_code'] == 'wild1554']['price_per_item']}")

    return cut_data, unit_articles


def update_purchase_price_sopost(sh, data):
    '''
    Изменение закупочной цены для конкретных СКЮ в таблице Сопост.
    Не перезаливает колонку полностью, а точечно изменяет закупочную стоимость у СКЮ, у которых изменилась цена.
    
    Номер строки берёт из листа с артикулами, спарсенного из таблицы.
    Перед проставлением новой цены проверяет, что в соседней ячейке именно этот артикул.
    Если артикулы в скрипте и ячейке не совпадают, новая цена не проставляется.
    '''
    # create a dict {local_vendor_code : purchase_price}
    new_purchase_price_per_item = data.set_index('local_vendor_code')['price_per_item'].to_dict()
    old_purchase_price_per_item = data.set_index('local_vendor_code')['unit_price'].to_dict()

    headers = sh.row_values(1)
    sku_col_letter = gs.column_number_to_letter(gs.find_gscol_num_by_name(col_name = 'wild', sh = sh, headers=headers) - 1)
    purchase_price_col_letter = gs.column_number_to_letter(gs.find_gscol_num_by_name(col_name = 'Стоимость в закупке (руб.)', sh = sh, headers=headers) - 1)

    target_skus = list(data['local_vendor_code'])
    logger.info(f'\nКоличество позиций с изменённой ценой: {len(target_skus)}\n')

    for sku in target_skus:
        new_price = new_purchase_price_per_item[sku]
        target_row_num = unit_articles.index(sku) + 2

        unit_wild_cell = f'{sku_col_letter}{target_row_num}'
        unit_wild = sh.get_values(range_name = unit_wild_cell)[0][0]

        # дополнительно проверяем, что вилд совпадает
        if unit_wild == sku:
            try:
                target_cell = f'{purchase_price_col_letter}{target_row_num}'
                sh.update(values = [[new_price]], range_name=target_cell)
                logger.info(f'Данные для {sku} в ячейке {target_cell} успешно обновлены: Старая цена - {old_purchase_price_per_item[sku]}, Новая цена - {new_price}\n')
            except Exception as e:
                logger.error(f'Ошибка при добавлении {sku} в {target_cell}:\n{e}\n')
        else:
            logger.error(f'Возможно, диапазон для {sku} определён неверно, в ячейке {unit_wild_cell} другое значение - {unit_wild}')


def send_report(data):
    '''
    Добавляет строки с товарами с изменённой ценой на лист "Изменение закупочной цены" в таблице UNIT.
    В data обязательно должны быть столбцы ['product_name', 'local_vendor_code', 'unit_price', 'price_per_item', 'price_diff_rub', 'supply_date'].
    Добавляет сегодняшнюю дату.
    '''
    # reorganise the data
    reorganised_data = data.copy()
    reorganised_data['supply_date'] = reorganised_data['supply_date'].dt.date
    reorganised_data = reorganised_data[['product_name', 'local_vendor_code', 'guid', 'document_number', 'quantity', 'unit_price', 'price_per_item', 'price_diff_rub', 'supply_date']]
    reorganised_data['insert_date'] = datetime.now().strftime('%Y-%m-%d')

    # save for safety
    filename = 'changed_purchase_prices.csv'
    reorganised_data.to_csv(filename, index = False)
    logger.info(f'The data with updated prices was saved locally to the {filename}')

    # connect to the sheet
    change_sh = gs.connect_to_remote_sheet('Новый товар', 'UNIT: Изменение закупочной цены')
    reorganised_data['supply_date'] = reorganised_data['supply_date'].astype(str)

    output = reorganised_data.values.tolist()

    try:
        change_sh.append_rows(output)
        change_sh.update(
            values=[[f"Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
        logger.info('The report is successfully added to the table Изменение закупочной цены')
    except Exception as e:
        logger.error(f'Error sending report to the table Изменение закупочной цены:\n{e}')
        raise
    
if __name__ == "__main__":

    try:
        sh = gs.connect_to_remote_sheet('UNIT 2.0 (tested)', 'Сопост')
        data, unit_articles = process_data(sh, True)

        # выдаём ошибку, если нет СКЮ с изменённой ценой
        if len(data) == 0:
            logger.error('Не найдены СКЮ с изменённой ценой')
            change_sh = gs.connect_to_remote_sheet('Новый товар', 'UNIT: Изменение закупочной цены')
            change_sh.update(
                values=[[f"Не найдены СКЮ с изменённой ценой: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
                range_name='A1'
            )
            raise ValueError('Не найдены СКЮ с изменённой ценой')

        # update data in Сопост 
        update_purchase_price_sopost(sh, data)   

        # add report to Изменение закупочной цены
        send_report(data)

    except Exception as e:
        logger.error(f'Failed to update purchase price:\n{e}')