#!/usr/bin/env python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.my_gspread import connect_to_local_sheet, get_col_index, remove_duplicates_from_col, delete_rows_by_index, connect_to_remote_sheet, find_gscol_num_by_name, remove_duplicates_by_val, find_duplicates_by_val_and_warn
from utils.my_db_functions import list_to_sql_select, fetch_db_data_into_dict
from utils.my_general import open_json
import pandas as pd
import gspread
import logging
import os

# to do:
# maybe restructure main
# add flag 'done' to the Новый товар
# unit: deleting formula from CQ
# add api error processing

# formatting -- done --


# --- thoughts ---
# add one more status to Новый товар to know when to actually add an item
# how to add row formatting?

# Logic:

# 0. delete duplicates from pilot and UNIT

# 1. get data from Новый товар
# 2. check if wild is in Sopost + check if the skus in UNIT / Autopilot
# 3. if the item is new (not in Sopost):
#       a. add item to Sopost + set 100 to 'Добавляем' + extend the formulas
#       b. add item to Расчёт закупки (2 sheets) + extend the formulas
# 4. add 3 columns to UNIT MAIN (tested) + extend formulas
# 5. add skus to Autopilot + extend formulas
# 6. add skus to Promo Analysis + extend formulas
# 7. change status in Новый товар to 'Готово'
# 8. maybe somehow use Bitrix API to set a task for Vika?


# -1. add separate logic for deleting items
    # a. universal function for rows delete from any table  --- done ---
    # b. function for rows delete from 3 basic tables (unit, pilot, adv analysis)   --- done --- (but needs remote check)
    # c. function for deleting wilds from the Расчёт закупки 


# Notes:
# ! в закупках используем робота Кости
# gspread works fine with dropdown values


os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/add_new_items.log",  encoding='utf-8'),
        logging.StreamHandler()
    ]
)


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
        new_sup_codes.add(card['supplier_code_unique'])
        sku = int(card['sku'])
        new_skus.add(sku)
    
    return new_sup_codes, new_skus


# ---------- удаление товаров ----------

def complete_delete_sku(skus_to_delete, local = False, local_base_url = None):
    '''
    Удаляет строки с артикулами из трёх таблиц: UNIT MAIN, Автопилот, Анализ акций
    ! Не удаляет вилды из Сопоста !

    При local = True подключается к таблице по ссылке local_base_url и ожидает, что будут три листа: 'MAIN (tested)', 'Автопилот', 'Анализ акций v 2.0'
    '''

    # check
    if local:
        if not local_base_url:
            raise ValueError('Need to give local_base_url while using local = True')
        
    if skus_to_delete is None:
        raise ValueError("values_to_delete can't be None") 
    elif not isinstance(skus_to_delete, (list, tuple)):
        skus_to_delete = [skus_to_delete]

    # transform values to str for gs data compatibility
    skus_str = [str(value) for value in skus_to_delete]

    sheets_config = [
        {
            'table_name' : 'UNIT 2.0 (tested)',
            'sheet_name' : 'MAIN (tested)',
            'col_name' : 'Артикул',
            'header_num' : 1
        },
        {
            'table_name' : 'Панель управления продажами Вектор',
            'sheet_name' : 'Автопилот',
            'col_name' : 'Артикул',
            'header_num' : 3
        },
        {
            'table_name' : 'Анализ акций',
            'sheet_name' : 'Анализ акций v 2.0',
            'col_name' : 'Артикул',
            'header_num' : 3
        }]

    for config in sheets_config:
        try:
            print(f"Processing: {config['sheet_name']} in {config['table_name']}")

            if local:
                sh = connect_to_local_sheet(local_base_url, config['sheet_name'])
            else:
                sh = connect_to_remote_sheet(config['table_name'], config['sheet_name'])

            col_num = find_gscol_num_by_name(config['col_name'], sh, headers_col = config['header_num'])

            delete_rows_based_on_values(sh, skus_str, col_num)

        except Exception as e:
            print(f"Failed to process {config['sheet_name']} in {config['table_name']}:\n{e}")



def delete_rows_based_on_values(sh, values_to_delete, col_num, transform_to_str = True, trash_sheet = None):
    '''
    Удаляет строки из таблицы sh, основываясь на значениях values_to_delete из столбца col_num.
    Принимает gs таблицу, значения для удаления и номер столбца.
    Перед чеком преобразует values_to_delete в строки.

    Аргументы:
    sh - gs sheet
    values_to_delete - list of target values which have to be deleted
    col_num - number of column in sh to delete values from (starting with 1, not 0)
    transform_to_str - если нужно преобразовать values_to_delete в str (False на случай, если данные до этого преобразуются в строки)
    '''
    if values_to_delete is None:
        raise ValueError("values_to_delete can't be None") 
    elif not isinstance(values_to_delete, (list, tuple)):
        values_to_delete = [values_to_delete]

    col_values = sh.col_values(col_num)

    if transform_to_str:
        values_str = [str(value) for value in values_to_delete]

    rows_to_delete = [
        i + 1 for i, value in enumerate(col_values)
        if value in values_str
    ]

    if not rows_to_delete:
        logging.info(f"{sh.title}: Duplicates aren't found, no rows to delete.")
        return

    # сортируем в обратном порядке, чтобы избежать смещения строк
    rows_to_delete.sort(reverse=True)
    rows_num = len(rows_to_delete)
    logging.info(f'Found {rows_num} rows to delete')
    print(rows_to_delete)

    try:
        c = 1
        for row_num in rows_to_delete:
            sh.delete_rows(row_num)
            logging.info(f'Deleted row {row_num}: proccessed {c}/{rows_num} rows')

            # trash_sheet.update()
            c += 1
        logging.info(f"Successfully deleted {len(rows_to_delete)} rows: {sorted(rows_to_delete, reverse=False)}")
    except Exception as e:
        logging.error(f"Error during deletion: {e}")



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


def load_last_row_w_dummy_values(sh):
    '''
    Берёт последнюю строку из заданной таблицы, собирает все элементы, начинающиеся с '=', заменяет в них value_to_replace на dummy_value_name.

    Возвращает [(cell_index, formula_w_dummy_values), ...] * n_col, значения без формул заменены на ''
    '''
    n_cols, n_rows = sh.col_count, sh.row_count
    last_row = sh.row_values(n_rows, value_render_option='FORMULA')

    # получаем формулы с dummy value в формате [(cell_idx, formula), ...]
    formulas_w_dummies = add_dummy_value_to_formulas(last_row, n_rows, 'cell_num')

    row_output_prototype = [""] * n_cols
    for idx, value in formulas_w_dummies:
        row_output_prototype[idx] = value
    
    return row_output_prototype

# row by row
# def add_new_rows_w_formulas(sh, new_items_data, new_items_idx):
#     """
#     Add new rows to Google Sheet, each with:
#       - static values at specific columns (new_items_idx)
#       - formulas with {cell_num} placeholder updated to point to current row

#     :param sh: gspread Worksheet
#     :param new_items_data: List of lists, e.g. [[val1, val2], [val1_row2, val2_row2]]
#     :param new_items_idx: List of column indices for the values (same for all rows)
#     """
#     base_row = sh.row_count + 1 
#     prototype = load_last_row_w_dummy_values(sh)

#     logging.info(f"Adding new rows with formulas sheet '{sh.title}', table '{sh.spreadsheet.title}'")
#     try:
#         for i, item_data in enumerate(new_items_data):
#             row = prototype.copy()
#             cell_num = base_row + i

#             for idx, value in zip(new_items_idx, item_data):
#                 row[idx] = value

#             formatted_row = [
#                 cell.format(cell_num=cell_num) if isinstance(cell, str) and '{cell_num}' in cell else cell
#                 for cell in row
#             ]

#             sh.append_row(formatted_row, value_input_option='USER_ENTERED')
#         logging.info(f"Successfully added {len(new_items_data)} rows to sheet '{sh.title}', table '{sh.spreadsheet.title}'")
#     except Exception as e:
#         logging.error("Failed to add new rows to 'Сопост':\n{e}")
#         raise


# all rows at once (no formatting)
def add_new_rows_w_formulas(sh, new_items_data, new_items_idx):
    """
    Add multiple rows to Google Sheet using append_rows() (single API call).
    Preserves formulas by replacing {cell_num} with actual row numbers.

    :param sh: gspread Worksheet
    :param new_items_data: List of lists, e.g. [[val1, val2], ...]
    :param new_items_idx: List of column indices (0-based) where to insert values
    """
    logging.info(f"Adding {len(new_items_data)} rows to sheet '{sh.title}', table '{sh.spreadsheet.title}'")

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

        logging.info(f"Successfully added {len(rows_to_append)} rows to '{sh.title}'")

    except Exception as e:
        logging.error(f"Failed to add new rows to '{sh.title}': {e}", exc_info=True)
        raise


def add_formatted_rows(spreadsheet, sh, new_items_data, new_items_idx):
    """
    Add new rows with values and formulas, then copy formatting from the last existing row.
    
    :param spreadsheet: gspread Spreadsheet object (from gc.open(...))
    :param sh: gspread Worksheet object (specific tab)
    :param new_items_data: List of lists, e.g. [[val1, val2], ...]
    :param new_items_idx: List of column indices (0-based) where to insert values
    """
    logging.info(f"Adding {len(new_items_data)} formatted rows to sheet '{sh.title}' in '{spreadsheet.title}'")

    try:
        # Get formula template from last row
        prototype = load_last_row_w_dummy_values(sh)
        base_row = sh.row_count + 1
        num_rows = len(new_items_data)

        if num_rows == 0:
            logging.info(f"No rows to add for '{sh.title}'.")
            return

        # --- Step 1: Prepare data with formulas ---
        rows_to_append = []
        for i, item_data in enumerate(new_items_data):
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

        # --- Step 2: Append data (auto-adds rows) ---
        sh.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        logging.info(f"Data appended to '{sh.title}'.")

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

        logging.info(f"Successfully added {num_rows} rows with formatting to '{sh.title}'")

    except Exception as e:
        logging.error(f"Failed to add formatted rows to '{sh.title}': {e}", exc_info=True)
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
            logging.error('Have to pass either headers, or sh and headers_row_num')
            raise ValueError
    index_start =  get_col_index(sh, colname_start, header=headers)
    index_end = get_col_index(sh, colname_end, header=headers)
    n_cols = index_end - index_start
    
    if n_cols_for_check and n_cols + 1 != n_cols_for_check:
        if sh is None:
            sh = 'table'
        logging.error(f'Expected number of columns in {sh.title} - {n_cols_for_check}, fact - {n_cols}.')
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
            logging.error('Необходимо передать один из аргументов: col_values_to_delete_from или col_num_to_delete_from')
            return []

    existing_values = set(col_values_to_delete_from)
    missing_values = [value for value in values_to_check if str(value) not in existing_values]
    
    duplicates = [value for value in values_to_check if str(value) in existing_values]
    if duplicates:
        logging.info(f"Skipping {len(duplicates)} duplicate values in {sh.title}: {duplicates}")
    
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
            logging.info(f"No new values to add to {sh.title}")
            return
            
        # Filter data to only include missing items
        filtered_data = filter_data_by_missing_values(output_data, new_identifiers, missing_values)
        # print(filtered_data) # debug
        if filtered_data:
            add_formatted_rows(table, sh, filtered_data, output_indexes)
        else:
            logging.info(f"No new data to add to {sh.title} after filtering")
    else:
        # No filtering needed - add all data
        add_formatted_rows(table, sh, output_data, output_indexes)


if __name__ == "__main__":
    logging.info("Starting script execution.")

    # False для локальных тестов
    remote = True

    # поставить False, если нужно удалить дубликаты между новыми артикулами и уже существующими
    # ps ДУБЛИКАТЫ В САМОЙ ТАБЛИЦЕ УДАЛЯЮТСЯ ПО УМОЛЧАНИЮ, это флаг именно для сопоставления новых скю с уже существующими в таблице
    only_warn = True    # <--- поставить only_warn=False если нужно удалять дубликаты  !!!!!

    try:
        my_client = gspread.service_account(filename='creds/creds.json') 
        if remote:
            new_items_table = my_client.open('Новый товар')
            # new_items_table = my_client.open_by_url('https://docs.google.com/spreadsheets/d/1amlBwWVD37TNLnTiPVa-KPpEPUqCEayXVZN22b0cP5Y/edit?gid=508042793#gid=508042793')
            new_items_sh = new_items_table.worksheet('Для юнит')
            trash_sheet = new_items_table.worksheet('Удалено')
        else:
            local_table = my_client.open('БД универсальное')
            new_items_sh = local_table.worksheet('Для юнит')
            trash_sheet = None
        logging.info("Connected to 'Новый товар' spreadsheet. Worksheets loaded.")

        new_items = get_sku_card_from_gs_new_items(sh = new_items_sh, include_all = False)
        logging.info('New items loaded')
        
        if not new_items:
            logging.error('New items not found')
            raise ValueError

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

        articles_sh = new_items_table.worksheet('Артикулы_ПУ')
        articles_sh.append_rows(pilot_list)
        logging.info("Values added to the sheet 'Артикулы_ПУ'")

        # - - -


        temp_df = pd.DataFrame(pilot_list, columns = ['sku', 'name', 'client', 'wild'])
        temp_df.to_excel('skus_pilot.xlsx', index = False)
        logging.info('data for pilot is saved to excel file')
        # process_sheet(pilot_table, pilot_sh, 'Артикул', pilot_list, [0, 1, 2, 3], trash_sheet,
        #             new_sku_list, header_row=3)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        raise