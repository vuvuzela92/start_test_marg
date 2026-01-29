import json
from decimal import Decimal
from collections import defaultdict
import logging
from datetime import datetime, time, timedelta

def ensure_datetime(d):
    """Convert string 'yyyy-mm-dd' to datetime, or return datetime as-is."""
    if isinstance(d, datetime):
        return d
    elif isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d")
    else:
        raise ValueError(f"Invalid date: {d}")

def dict_to_json(dictionary, filename):
    with open(filename, 'w') as json_file:
        json.dump(dictionary, json_file)

def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def open_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        file = json.load(f)
    return file

def return_single_dict_from_list(list_of_dict, target_key, target_value):
    return next((dct for dct in list_of_dict if dct.get(target_key) == target_value), None)

def match_dimensions(data, key_index, value_index):
    """
    Функция для сопоставления значений из данных по указанным индексам.
    
    Args:
        data: список кортежей/списков с данными
        key_index: индекс элемента, который будет ключом
        value_index: индекс элемента, который будет значением
    
    Returns:
        dict: словарь с сгруппированными значениями
    """
    result = {}
    
    for row in data:
        key = row[key_index]
        value = row[value_index]
        
        if key not in result:
            result[key] = []
        result[key].append(value)
    
    return result

def clean_vendor_code(vendor_code):
    """
    Clean vendor code by removing 'd' + number suffix if present
    Example: 'wild1335d1' -> 'wild1335'
    """
    if vendor_code.count('d') > 1:
        return vendor_code.rsplit('d', 1)[0]
    return vendor_code


def process_decimal_in_dict(data):
    """
    Recursively convert Decimal values to float/int in dict or list of dicts
    """
    if isinstance(data, dict):
        return {key: process_decimal_in_dict(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [process_decimal_in_dict(item) for item in data]
    elif isinstance(data, Decimal):
        # Convert to int if it's a whole number, otherwise to float
        return int(data) if data % 1 == 0 else float(data)
    else:
        return data
    

def find_duplicates(values, start_row=0, return_all=False):
    """
    Возвращает {row_index: value} для элементов с дублирующимися значениями.
    
    :param col_values: список значений столбца
    :param start_row: с какой строки учитывать данные (по умолчанию 0)
    :param return_all: если True — возвращает все вхождения дубликатов (включая первое), 
                       если False — только повторные (после первого)
    """
    counts = {}
    duplicates = {}

    # Считаем количество вхождений
    for i, v in enumerate(values, start=1):
        if i >= start_row:
            counts[v] = counts.get(v, 0) + 1

    # Выбираем, что возвращать
    seen = set()
    for i, v in enumerate(values, start=1):
        if i < start_row:
            continue
        is_duplicate_value = counts[v] > 1
        if is_duplicate_value:
            if return_all:
                duplicates[i] = v  # все вхождения
            else:
                if v in seen:
                    duplicates[i] = v  # только повторные
                else:
                    seen.add(v)
    return duplicates


def aggregate_dct_data(dct):
    '''
    transforms {key1 : value, key2: value, ...} into {value : [key1, key2], ...}
    '''
    res = defaultdict(list)
    for key, value in dct.items():
        res[value].append(key)
    return dict(res)


def collect_valid_dct_fields(data, key_field, fields):
    """
    Принимает список словарей, возвращает словарь {key_field: {fields : ...}}
    
    Build a dict from list of dicts, including only fields that are present.
    Skips item only if key_field is missing.
    Empty or missing fields are simply not included.

    :param data: List of dicts
    :param key_field: The key to use as dict key (e.g. 'vendorCode')
    :param fields: List of fields to try to extract (if present)
    :return: Dict like {key_value: {field: value, ...}} — only existing fields
    """
    result = {}

    for item in data:
        key_value = item.get(key_field)
        if not key_value:
            logging.warning(f"Missing '{key_field}' in item: {item}")
            continue  # Cannot use this item at all

        extracted = {}
        for field in fields:
            value = item.get(field)
            # Only include if field exists AND is not an empty list
            if value is not None:
                if not (isinstance(value, list) and len(value) == 0):
                    extracted[field] = value
                else:
                    logging.debug(f"Skipping empty list for '{field}' in {key_field}='{key_value}'")
        
        result[key_value] = extracted  # Always add entry (may be empty)

    return result

def dct_process_date(dct, date_key, date_format):
    """
    Convert the value of `date_key` in `dct` to a string using `date_format`.
    Assumes the value is a datetime.date or datetime.datetime object.
    Returns the updated dictionary.
    """
    dct = dct.copy()  # Avoid mutating the original
    if date_key in dct and dct[date_key] is not None:
        dct[date_key] = dct[date_key].strftime(date_format)
    return dct

def to_iso_z(date_str, t: time) -> str:

    # if datetime
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%dT%H:%M:%SZ")

    # if already in ISO Z format, return as is
    try:
        datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return date_str
    except ValueError:
        pass

    # Check for YYYY-MM-DD format
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Date must be in '%Y-%m-%d' or '%Y-%m-%dT%H:%M:%SZ' format")
    
    return datetime.combine(d, t).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean_datetime_from_timezone(val):
    """
    Converts ISO datetime string with 'Z' to naive datetime.
    Returns other values unchanged.
    """
    if isinstance(val, str) and 'T' in val and '-' in val and 'Z' in val:
        return datetime.fromisoformat(val.replace("Z", ""))
    return val


def date_from_now(days: int, fmt = '%Y-%m-%d'):
    dt = datetime.now().date() + timedelta(days=days)
    return dt.strftime(fmt) if fmt else dt