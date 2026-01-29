import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
import gspread
# import logger
import os
from pathlib import Path
from utils.logger import setup_logger
from utils.my_db_functions import fetch_db_data_into_list
from utils.my_gspread import get_col_index, remove_duplicates_from_col, connect_to_remote_sheet, remove_duplicates_by_val, find_duplicates_by_val_and_warn
from utils.my_api import get_product_by_nmid
from utils.my_general import open_json

import requests
import pandas as pd
import gspread
# import logger
import os
import re

logger = setup_logger("add_new_items.log")

# --- мысли ---
# добавить ещё один статус в лист «Новый товар», чтобы точно знать, когда можно добавлять товар
# как добавить форматирование строк?

# Логика:

# 0. удалить дубликаты из Pilot и UNIT

# 1. получить данные из листа «Новый товар»
# 2. проверить, есть ли Wild в Sopost + проверить, присутствуют ли SKU в UNIT / Autopilot
# 3. если товар новый (отсутствует в Sopost):
#       a. добавить товар в Sopost + установить значение 100 в колонку «Добавляем» + протянуть формулы
#       b. добавить товар в «Расчёт закупки» (2 листа) + протянуть формулы
# 4. добавить 3 колонки в UNIT MAIN (протестировано) + протянуть формулы
# 5. добавить SKU в Autopilot + протянуть формулы
# 6. добавить SKU в Promo Analysis + протянуть формулы
# 7. изменить статус в «Новый товар» на «Готово»
# 8. возможно, использовать API Bitrix для создания задачи Вике?


# -1. добавить отдельную логику для удаления товаров
#    a. универсальная функция для удаления строк из любой таблицы — готово
#    b. функция для удаления строк из 3 основных таблиц (UNIT, Pilot, Adv Analysis) — готово (но требует проверки на удалённой копии)
#    c. функция для удаления Wild-артикулов из «Расчёта закупки»


# ---------- take data from Новый товар ----------

def get_sku_card_from_gs_new_items(sh = None, include_all = False):
    '''
    Функция возвращает dict с meaningful значениями из таблицы Новый товар (столбцы A:K)

    Параметры:
        sh - gs Новый товар (если не задана, подключается внутри скрипта)
        include_all - при False возвращает только новые товары (статус "Добавить"),
                      при True - все товары (без разделения статусов)
    '''

    if not sh:
        sh = connect_to_remote_sheet('Новый товар', 'Для юнит')

    # separate into columns and values
    all = sh.get_all_values()
    cols = all[0]
    items = all[1:]

    # find status and sku col index
    status_col_idx = cols.index('Статус')
    sku_col_idx = cols.index('Артикул')
    manager_col_idx = cols.index('Ответственный менеджер')
    useless_col_idx = cols.index('')

    if include_all:
        skus = [([x for j, x in enumerate(row[:manager_col_idx + 1]) if j != useless_col_idx - 1])
                for i, row in enumerate(items) if row[sku_col_idx]]
    else:
        # clean the data
        skus = [([x for j, x in enumerate(row[:manager_col_idx + 1]) if j != useless_col_idx - 1])
                for i, row in enumerate(items) if row[status_col_idx] == 'добавить' and row[sku_col_idx]]

    # organize data into dict
    keys = ['supplier_name', 'sku', 'client', 'supplier_code_duplicates', 'status', 'item_name', 'category', 'supplier_code_unique', 'purchase_price', 'manager']
    result = [dict(zip(keys, row)) for row in skus]

    return result


def extract_new_sup_codes_and_skus(dct):
    '''
    В принимаемом dct обязательно должны быть поля supplier_code_unique и sku.
    Организует эти поля в сеты.

    Возвращает new_sup_codes, new_skus
    '''
    new_sup_codes = set()          
    new_skus = set()            

    for card in dct:
        new_sup_codes.add(str(card['supplier_code_unique']).strip()) # 15.11 added .strip() to avoid duplicates
        sku = int(card['sku'])
        new_skus.add(sku)
    
    return new_sup_codes, new_skus


def add_dummy_value_to_formulas(lst_of_formulas, value_to_replace, dummy_value_name = 'dummy_value'):
    '''
    Принимает list/tuple, собирает все элементы, начинающиеся с '=', заменяет в них value_to_replace на dammy_value_name.
    В основном, предназначена для formulas extending в гугл таблицах.

    Возвращает [(cell_index, formula), ...]
    '''
    all_formulas = [(i, cell) for i, cell in enumerate(lst_of_formulas) if isinstance(cell, str) and cell.startswith("=")]
    return [(i, formula.replace(str(value_to_replace), "{cell_num}")) for i, formula in all_formulas]


# TODO: bad function, has to accept/reuse col_values, rn calls them twice - once inside itself, the 2nd
# the problem is that find_duplicates_gs doesn't accept col_values, only colname or colnum
def process_duplicates(sh, col_name, trash_sheet = None, only_warn = True, header_row = 1, raise_absent_error = False):
    col_num = get_col_index(sh, col_name, header_row=header_row)
    col_values = sh.col_values(col_num)

    # проверяем, что в таблице нет дубликатов
    remove_duplicates_from_col(sh, col_values = col_values, trash_sheet=trash_sheet)

    if only_warn:
        # проверяем, что в таблице нет sku/codes, которые мы хотим добавить
        find_duplicates_by_val_and_warn(sh, new_sup_codes, col_values_to_delete_from = col_values, raise_absent_error = raise_absent_error)
    else:
        remove_duplicates_by_val(sh, new_sup_codes, col_values_to_delete_from = col_values, trash_sheet=trash_sheet)


# def load_last_row_w_dummy_values(sh):
#     '''
#     Берёт последнюю строку из заданной таблицы, собирает все элементы, начинающиеся с '=', заменяет в них value_to_replace на dummy_value_name.

#     Возвращает [(cell_index, formula_w_dummy_values), ...] * n_col, значения без формул заменены на ''
#     '''
#     n_cols, n_rows = sh.col_count, sh.row_count
#     last_row = sh.row_values(n_rows, value_render_option='FORMULA')

#     # получаем формулы с dummy value в формате [(cell_idx, formula), ...]
#     formulas_w_dummies = add_dummy_value_to_formulas(last_row, n_rows, 'cell_num')

#     row_output_prototype = [""] * n_cols
#     for idx, value in formulas_w_dummies:
#         row_output_prototype[idx] = value
    
#     return row_output_prototype


# def load_last_row_w_dummy_values(sh):
#     headers = sh.row_values(1)
#     n_cols = len(headers)
#     n_rows = sh.row_count

#     last_row = sh.row_values(n_rows, value_render_option='FORMULA')

#     formulas_w_dummies = add_dummy_value_to_formulas(last_row, n_rows, 'cell_num')

#     row_output_prototype = [""] * n_cols
#     for idx, value in formulas_w_dummies:
#         if idx < n_cols:
#             row_output_prototype[idx] = value

#     return row_output_prototype

def load_last_row_w_dummy_values(sh):
    headers = sh.row_values(1)
    n_cols = len(headers)
    n_rows = sh.row_count

    last_row = sh.row_values(n_rows, value_render_option='FORMULA')

    formulas_w_dummies = add_dummy_value_to_formulas(last_row, n_rows, 'cell_num')

    row_output_prototype = [""] * n_cols
    for idx, value in formulas_w_dummies:
        if idx < n_cols:
            row_output_prototype[idx] = value

    return row_output_prototype




# all rows at once (no formatting)
def add_new_rows_w_formulas(sh, new_items_data, new_items_idx):
    """
    Add multiple rows to Google Sheet using append_rows() (single API call).
    Preserves formulas by replacing {cell_num} with actual row numbers.

    :param sh: gspread Worksheet
    :param new_items_data: List of lists, e.g. [[val1, val2], ...]
    :param new_items_idx: List of column indices (0-based) where to insert values
    """
    logger.info(f"Adding {len(new_items_data)} rows to sheet '{sh.title}', table '{sh.spreadsheet.title}'")

    try:
        # Get last row to extract formula template
        prototype = load_last_row_w_dummy_values(sh)
        base_row = sh.row_count + 1

        # Build formatted rows
        rows_to_append = []
        for i, item_data in enumerate(new_items_data):
            row = prototype.copy()
            cell_num = base_row + i

            # Insert values
            for idx, value in zip(new_items_idx, item_data):
                row[idx] = value

            # Format formulas: replace {cell_num} with actual row number
            formatted_row = [
                cell.format(cell_num=cell_num) if isinstance(cell, str) and '{cell_num}' in cell else cell
                for cell in row
            ]

            rows_to_append.append(formatted_row)

        # ✅ Single API call – automatically adds rows!
        sh.append_rows(
            rows_to_append,
            value_input_option='USER_ENTERED'
        )

        logger.info(f"Successfully added {len(rows_to_append)} rows to '{sh.title}'")

    except Exception as e:
        logger.error(f"Failed to add new rows to '{sh.title}': {e}", exc_info=True)
        raise


def add_formatted_rows(spreadsheet, sh, new_items_data, new_items_idx):
    """
    Add new rows with values and formulas, then copy formatting from the last existing row.
    
    :param spreadsheet: gspread Spreadsheet object (from gc.open(...))
    :param sh: gspread Worksheet object (specific tab)
    :param new_items_data: List of lists, e.g. [[val1, val2], ...]
    :param new_items_idx: List of column indices (0-based) where to insert values
    """
    logger.info(f"Adding {len(new_items_data)} formatted rows to sheet '{sh.title}' in '{spreadsheet.title}'")

    try:
        # Get formula template from last row
        prototype = load_last_row_w_dummy_values(sh)
        base_row = sh.row_count + 1
        num_rows = len(new_items_data)

        if num_rows == 0:
            logger.info(f"No rows to add for '{sh.title}'.")
            return

        # --- Step 1: Prepare data with formulas ---
        rows_to_append = []
        for i, item_data in enumerate(new_items_data):
            # row = prototype.copy()
            # row = prototype.copy()[:7]
            row = prototype.copy()
            cell_num = base_row + i

            # Insert values at specified columns
            for idx, value in zip(new_items_idx, item_data):
                row[idx] = value

            # Replace {cell_num} in formulas
            formatted_row = [
                cell.format(cell_num=cell_num) if isinstance(cell, str) and '{cell_num}' in cell else cell
                for cell in row
            ]
            rows_to_append.append(formatted_row)

        logger.info(f"sopost_items_indexes: {sopost_items_indexes}")
        logger.info(f"Row length (prototype): {len(prototype)}")
        logger.info(f"Worksheet col_count: {sh.col_count}")
        logger.info(f"Header length: {len(sh.row_values(1))}")



        # --- Step 2: Append data (auto-adds rows) ---
        # sh.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        sh.append_rows(
                        rows_to_append,
                        table_range="B:G",
                        value_input_option="USER_ENTERED"
                    )
        logger.info(f"Data appended to '{sh.title}'.")

        # --- Step 3: Copy formatting from last original row ---
        last_row_num = base_row - 1
        new_rows_range = f"{base_row}:{base_row + num_rows - 1}"

        src_range = gspread.utils.a1_range_to_grid_range(f"{last_row_num}:{last_row_num}", sh.id)
        dst_range = gspread.utils.a1_range_to_grid_range(new_rows_range, sh.id)

        # Use spreadsheet.batch_update() for API requests
        spreadsheet.batch_update({
            "requests": [
                {
                    "copyPaste": {
                        "source": src_range,
                        "destination": dst_range,
                        "pasteType": "PASTE_FORMAT",
                        "pasteOrientation": "NORMAL"
                    }
                }
            ]
        })

        logger.info(f"Successfully added {num_rows} rows with formatting to '{sh.title}'")

    except Exception as e:
        logger.error(f"Failed to add formatted rows to '{sh.title}': {e}", exc_info=True)
        raise


def define_num_index_range_by_col_names(colname_start, colname_end, headers = None, sh = None, headers_row_num = 1, n_cols_for_check = None):
    '''
    Принимает название двух колонок, отдаёт range для вставки данных в лист (0-based)

    Пример ответа:
        [1, 2, 3] ,
    где 1 - colname_start, 3 - colname_end (aka inlcudes both edges)   
    '''
    if not headers:
        if sh and headers_row_num:
            headers = sh.row_values(headers_row_num)
        else:
            logger.error('Have to pass either headers, or sh and headers_row_num')
            raise ValueError
    index_start =  get_col_index(sh, colname_start, header=headers)
    index_end = get_col_index(sh, colname_end, header=headers)
    n_cols = index_end - index_start
    
    if n_cols_for_check and n_cols + 1 != n_cols_for_check:
        if sh is None:
            sh = 'table'
        logger.error(f'Expected number of columns in {sh.title} - {n_cols_for_check}, fact - {n_cols}.')
        raise ValueError
    
    items_indexes = list(range(index_start - 1, index_end))

    return items_indexes

def find_missing_values(sh, values_to_check, col_values_to_delete_from=None, col_num_to_delete_from=None):
    """
    Returns values from values_to_check that are NOT present in the target column
    """
    if col_values_to_delete_from is None:
        if col_num_to_delete_from:
            col_values_to_delete_from = sh.col_values(col_num_to_delete_from)
        else:
            logger.error('Необходимо передать один из аргументов: col_values_to_delete_from или col_num_to_delete_from')
            return []

    existing_values = set(col_values_to_delete_from)
    missing_values = [value for value in values_to_check if str(value) not in existing_values]
    
    duplicates = [value for value in values_to_check if str(value) in existing_values]
    if duplicates:
        logger.info(f"Skipping {len(duplicates)} duplicate values in {sh.title}: {duplicates}")
    
    return missing_values


def filter_data_by_missing_values(data_list, identifier_list, missing_values):
    """
    Filters data_list to include only items whose identifiers are in missing_values
    """
    missing_set = set(str(v) for v in missing_values)
    filtered_data = [
        data for data, identifier in zip(data_list, identifier_list)
        if str(identifier) in missing_set
    ]
    return filtered_data


def process_sheet(table, sh, comparison_col_name, output_data, output_indexes, trash_sheet, 
                  new_identifiers=None, header_row=1):
    """
    Process sheet with two-step duplicate handling:
    1. Remove internal duplicates in the sheet
    2. Skip new items that already exist (notify but don't insert)
    """

    # Step 1: Clean internal duplicates in the sheet
    col_num = get_col_index(sh, comparison_col_name, header_row=header_row)
    col_values = sh.col_values(col_num)
    remove_duplicates_from_col(sh, col_values=col_values, trash_sheet=trash_sheet)
    
    # Step 2: Filter out new items that already exist
    if new_identifiers:
        # Get fresh column values after cleanup
        col_values = sh.col_values(col_num)
        missing_values = find_missing_values(sh, new_identifiers, col_values_to_delete_from=col_values)
        
        if not missing_values:
            logger.info(f"No new values to add to {sh.title}")
            return
            
        # Filter data to only include missing items
        filtered_data = filter_data_by_missing_values(output_data, new_identifiers, missing_values)
        # print(filtered_data) # debug
        if filtered_data:
            add_formatted_rows(table, sh, filtered_data, output_indexes)
        else:
            logger.info(f"No new data to add to {sh.title} after filtering")
    else:
        # No filtering needed - add all data
        add_formatted_rows(table, sh, output_data, output_indexes)


def api_add_product(id, name, photo_link, is_kit = False, share_of_kit = False, kit_components = None):

    json = [
        {
            "id": id,
            "name": name,
            "is_kit": False,
            "share_of_kit": False,
            "photo_link": photo_link,
            "kit_components": kit_components
        }
    ]

    url = '/api/goods_information/add_product'
    response = requests.post(url = url, json = json)

    if response.status_code == 200:
        logger.info(f"Запрос для создания {id} в products успешно отправлен")
    else:
        logger.error(f"Error: {response.status_code}\n{response.text}")
        raise ValueError



if __name__ == "__main__":
    logger.info("Starting script execution.")

    # False для локальных тестов
    remote = True

    # поставить False, если нужно удалить дубликаты между новыми артикулами и уже существующими
    # ps ДУБЛИКАТЫ В САМОЙ ТАБЛИЦЕ УДАЛЯЮТСЯ ПО УМОЛЧАНИЮ, это флаг именно для сопоставления новых скю с уже существующими в таблице
    only_warn = True    # <--- поставить only_warn=False если нужно удалять дубликаты  !!!!!

    try:
    # Строим путь к creds.json относительно расположения текущего файла (add_new_items.py)
        creds_path = Path(__file__).parent.parent.parent / "creds" / "creds.json"
        # creds_path = "creds/creds.json"
        my_client = gspread.service_account(filename=str(creds_path)) 
        if remote:
            new_items_table = my_client.open('Новый товар')
            new_items_sh = new_items_table.worksheet('Для юнит')
            trash_sheet = new_items_table.worksheet('Удалено')
        else:
            local_table = my_client.open('БД универсальное')
            new_items_sh = local_table.worksheet('Для юнит')
            trash_sheet = None
        logger.info("Connected to 'Новый товар' spreadsheet. Worksheets loaded.")

        new_items = get_sku_card_from_gs_new_items(sh = new_items_sh, include_all = False)
        logger.info('New items loaded')
        
        if not new_items:
            logger.error('New items not found')
            raise ValueError('New items not found')

        # organize new skus and supplier_ids into separate lists
        new_sup_codes, new_skus = extract_new_sup_codes_and_skus(new_items)


        # - - - - - - - - - - - - - - - - 1. sopost - - - - - - - - - - - - - - - -
        if remote:
            unit_table = my_client.open('UNIT 2.0 (tested)')
            sopost = unit_table.worksheet('Сопост')
        else:
            unit_table = local_table
            sopost = local_table.worksheet('Сопост')

        # собираем из карточек инфу, которая нужна для сопоста (literally часть, которую нужно вставить, без формул)  
        new_items_unique_wilds = []
        seen = set()
        for item in new_items:
            sup_code = item['supplier_code_unique']
            if sup_code not in seen:
                seen.add(sup_code)
                new_items_unique_wilds.append(item)
        sopost_add_sku = [
            [
                card['category'],
                card['item_name'],
                card['supplier_code_unique'],
                card['supplier_code_unique'],
                card['purchase_price'],
                100
            ]
            for card in new_items_unique_wilds
        ]
        # получаем индексы для вставки значений [1, 2, 3, ...]
        sopost_items_indexes = define_num_index_range_by_col_names('предмет', 'Добавляем', sh = sopost, headers_row_num=1, n_cols_for_check=6)
        sopost_identifiers = [card['supplier_code_unique'] for card in new_items_unique_wilds]
        process_sheet(unit_table, sopost, 'wild', sopost_add_sku, sopost_items_indexes, 
                    trash_sheet, sopost_identifiers)
        

        # - - - new part - add data to db table products - - -

        # -- Check if wilds already exist in the DB table products --
        db_wilds = [i[0] for i in fetch_db_data_into_list('select id from products')]
        missing_wilds = [i['supplier_code_unique'] for i in new_items_unique_wilds if i['supplier_code_unique'] not in db_wilds]
        
        # check_list = [i for i in missing_wilds if 'd' in i]
        # check = any('d' in i for i in missing_wilds)

        DUPLICATE_PATTERN = re.compile(r'^wild\d+d\d*$')

        check_list = [i for i in missing_wilds if DUPLICATE_PATTERN.match(i)]

        if check_list:
            logger.error(f"Найдены дубликаты: {check_list}")
            raise ValueError

        if missing_wilds and not check_list:

            logger.info(f'Найдены вилды, которых нет в БД таблице products: {missing_wilds}')

            tokens = open_json(Path(__file__).parent.parent.parent / "creds" / "tokens.json")
            wild_client_match = {i['supplier_code_unique'] : str(i['client']).capitalize() for i in new_items}
            wild_name_match = {i['supplier_code_unique'] : i['item_name'] for i in new_items}

            failed_wilds = []

            for w in missing_wilds:

                try:
                    card = get_product_by_nmid(tokens[wild_client_match[w]], w)
                    photo = card[0]['photos'][0]['tm']
                    api_add_product(id = w, name = wild_name_match[w], photo_link=photo)

                except Exception as e:
                    logger.error(f"Failed to add {w} to the products: {e}")
                    failed_wilds.append(w)
                    continue
        
            if failed_wilds:
                logger.info(f"Следующие вилды не удалось добавить в БД products: {failed_wilds}")
                # вот тут бы хорошо еще по индексу добавлять статус "Ошибка" в гугл таблицу - но проблема, что wildы дублируются, нужно брать последнее значение
        else: 
            logger.info("Didn't find wilds missing from the products DB table")

        # - - - end of the logic for products - - -


        # i don't like the logic here... I want it to be like:
        # - check if the values exist is sopost
        # - if no, add the values to sopost, plan rk, and purchase
        # but in plan rk i'll still need to check the values for a specific date, so...
        

        # - - - - - - - - - - - - - - - - 2. расчёт закупки - - - - - - - - - - - - - - - -
        # используем кредс Кости
        # ищем лист по названию (поставщик) !!! add logic !!!
        # данные добавляем на 2 листа

        # kostya_client = gspread.service_account(filename='kostya_creds.json')
        
        # if remote:
        #     purch_table = my_client.open('Расчет закупки NEW')
        #     # !!! логика по клиентам?

        # - - - - - - - - - - - - - - - - 3. План РК - - - - - - - - - - - - - - - -

        # plan_rk_table = my_client.open('План РК')
        # plan_rk_sh = plan_rk_table.worksheet('План продаж wild')



        # - - - - - - - - - - - - - - - - 4. UNIT (MAIN TESTED) - - - - - - - - - - - - - - - -
        if remote:
            unit_sh = unit_table.worksheet('MAIN (tested)')
        else:
            unit_sh = local_table.worksheet('MAIN (tested)')
        unit_sh_add_sku = [
            [
                card['sku'],
                card['client'],
                card['supplier_code_unique']
            ]
            for card in new_items
        ]
        unit_items_indexes = define_num_index_range_by_col_names('Артикул', 'wild', sh = unit_sh, headers_row_num=1, n_cols_for_check=3)

        unit_identifiers = [card['sku'] for card in new_items]
        process_sheet(unit_table, unit_sh, 'Артикул', unit_sh_add_sku, unit_items_indexes, 
                    trash_sheet, unit_identifiers)


        # - - - - - - - - - - - - - - - - 5. Автопилот & Анализ акций - - - - - - - - - - - - - - - -
        if remote:
            pilot_table = my_client.open('Панель управления продажами Вектор')
            pilot_sh = pilot_table.worksheet('Автопилот')
            adv_table = my_client.open('Анализ акций')
            adv_sh = adv_table.worksheet('Анализ акций v 2.0')
        else:
            pilot_sh = local_table.worksheet('Автопилот')
            adv_sh = local_table.worksheet('Анализ акций v 2.0')
            pilot_table = local_table
            adv_table = local_table
        new_skus_output = [
            [
                card['sku']
            ]
            for card in new_items
        ]

        new_sku_list = [card['sku'] for card in new_items]
        process_sheet(adv_table, adv_sh, 'Артикул', new_skus_output, [0], trash_sheet, 
                    new_sku_list, header_row=3)

        pilot_list = [[card['sku'], card['category'], str(card['client']).upper(), card['supplier_code_unique']] for card in new_items]


        # - - -
        # 5.5. - добавляем товар на лист Артикулы_ПУ в Новый товар (используется для обновления ПУ)

        # articles_sh = new_items_table.worksheet('Артикулы_ПУ')
        # articles_sh.append_rows(pilot_list)
        # logger.info("Values added to the sheet 'Артикулы_ПУ'")

        # - - -


        temp_df = pd.DataFrame(pilot_list, columns = ['sku', 'name', 'client', 'wild'])
        temp_df.to_excel('skus_pilot.xlsx', index = False)
        logger.info('data for pilot is saved to excel file')
        process_sheet(pilot_table, pilot_sh, 'Артикул', pilot_list, [0, 1, 2, 3], trash_sheet,
                    new_sku_list, header_row=3)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise