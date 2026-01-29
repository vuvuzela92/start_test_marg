import pandas as pd
from decimal import Decimal

def convert_col_to_numeric(data):
    df = data.copy()
    obj_cols = df.select_dtypes(include='object').columns
    
    for col in obj_cols:
        if df[col].apply(lambda x: isinstance(x, str)).any():
            if df[col].str.replace(',', '.', regex=False).str.isnumeric().any():
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', '.', regex=False), 
                    errors='coerce'
                ).fillna('-')
    return df

def process_decimal(data):
    df = data.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df

def process_decimal_column(series):
    return series.apply(lambda x: float(x) if isinstance(x, Decimal) else x)


# def order_dict_by_list(original_dict, order_list):
#     if not original_dict:
#         return {}
#     n = len(next(iter(original_dict.values())))
#     result = {
#         key: original_dict.get(key, [None] * n)
#         for key in order_list
#     }
#     return result

def order_dict_by_list(original_dict, order_list):
    """Auto-detect and handle either all iterables or all single values"""
    if not original_dict:
        return {}
    
    # Check the first value to determine the pattern
    first_value = next(iter(original_dict.values()))
    
    # Check if first value is iterable (but not string)
    if hasattr(first_value, '__iter__') and not isinstance(first_value, (str, bytes)):
        # All values are iterables - use iterable logic
        n = len(first_value)
        result = {
            key: list(original_dict.get(key, [None] * n))
            for key in order_list
        }
    else:
        # All values are single values - use single value logic
        result = {
            key: original_dict.get(key, None)
            for key in order_list
        }
    
    return result

def clean_tz_cols(df):
    '''
    Removes timezones from df
    (Needed for saving data in .xlsx)
    '''
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df


def datetime_to_str(df, datetime_format="%d.%m.%Y %H:%M:%S"):
    """
    Converts a DataFrame to list-of-lists suitable for gspread,
    converting datetime objects to strings.
    """
    df_copy = df.copy()
    datetime_cols = df_copy.select_dtypes(include=["datetime", "datetime64[ns]"]).columns
    for col in datetime_cols:
        df_copy[col] = df_copy[col].dt.strftime(datetime_format).fillna('')
    return df_copy

def format_datetime(df, datetime_cols):
    if not isinstance(datetime_cols, list):
        datetime_cols = [datetime_cols]

    data = df.copy()
    for col in datetime_cols:
        if col in data.columns:
            data[col] = data[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
            data[col] = pd.to_datetime(data[col], errors='coerce')
            data[col] = data[col].dt.strftime('%d.%m.%Y')
            data[col] = data[col].fillna('')
            
    return data