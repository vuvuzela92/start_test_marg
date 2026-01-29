import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import re
import json
import asyncio
import requests
import pandas as pd
from psycopg2.extras import execute_values
from datetime import time, datetime, timedelta

from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_db_functions import create_connection_w_env
from utils.my_general import to_iso_z, save_json, date_from_now
from utils.logger import *

# AI
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.core.credentials import AzureKeyCredential

logger = setup_logger("promotions.log")

MONTHS_RU = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря"
]

API_TIME_DIFF = 3   # добавляем +3 часа ко всем данным из API


def format_promo(promo: dict) -> str:
    '''
    Принимает словарь с информацией об акции из API и форматирует в строку типа:
    "{название акции} {дата и время начала} → {дата и время конца}"
    '''
    # parse UTC datetimes
    start = datetime.fromisoformat(promo["startDateTime"].replace("Z", ""))
    end = datetime.fromisoformat(promo["endDateTime"].replace("Z", ""))

    # add +2 hours
    start += timedelta(hours=API_TIME_DIFF)
    end += timedelta(hours=API_TIME_DIFF)

    # format dates
    start_str = f"{start.day} {MONTHS_RU[start.month]} {start:%H:%M}"
    end_str = f"{end.day} {MONTHS_RU[end.month]} {end:%H:%M}"

    return f"{promo['name']} {start_str} → {end_str}"


def merge_excels(folder_path, keep_cols=None):
    '''
    Принимает путь до папки с акциями, склеивает все файлы из папки, возвращает df
    '''

    # Название акции берется из названия самого файла
    if keep_cols is None:
        keep_cols = [
            "Артикул WB",
            "Плановая цена для акции",
            "Текущая розничная цена",
            "Текущая скидка на сайте, %",
        ]

    excel_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".xlsx")]
    dataframes = []

    for file in excel_files:
        df = pd.read_excel(file)
        
        # Extract clean filename (remove path, date, and underscore)
        base = os.path.basename(file)
        clean_name = re.split(r"_\d{2}\.\d{2}\.\d{4}", base)[0]
        
        # Add filename column
        df["Файл"] = clean_name
        
        # Keep only requested columns (if they exist)
        cols_to_keep = ["Файл"] + [c for c in keep_cols if c in df.columns]
        dataframes.append(df[cols_to_keep])

    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df


def get_promotions(api_key, start_dt, end_dt, all_promo=False, limit=1000, offset=0):
    '''
    Возвращает список акций Wildberries за указанный период.

    Параметры:
        start_dt (str): дата начала периода, формат YYYY-MM-DD или YYYY-MM-DDTHH:MM:SSZ
        end_dt (str): дата конца периода, формат YYYY-MM-DD или YYYY-MM-DDTHH:MM:SSZ
        all_promo (bool): если True — вернуть все акции, иначе только доступные для участия
        limit (int): количество акций для запроса (1–1000)
        offset (int): смещение для пагинации
    '''

    start_dt = to_iso_z(start_dt, time(0, 0, 0))
    end_dt = to_iso_z(end_dt, time(23, 59, 59))

    url = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions"
    headers = {"Authorization": api_key}
    params = {
        "startDateTime": start_dt,
        "endDateTime": end_dt,
        "allPromo": all_promo,
        "limit": limit,
        "offset": offset,
    }
    res = requests.get(url, headers=headers, params=params).json()
    return res['data']['promotions']


async def get_all_clients_promo(api_tokens, start_dt, end_dt, **kwargs):
    """
    Runs get_promotions for multiple tokens concurrently and merges the results.
    """
    
    # 1. Create a list of tasks (one for each token)
    # We use to_thread so the blocking requests.get doesn't freeze the loop
    tasks = [
        asyncio.to_thread(get_promotions, token, start_dt, end_dt, **kwargs)
        for token in api_tokens
    ]
    
    # 2. Run all tasks simultaneously and wait for results
    results = await asyncio.gather(*tasks)
    
    # 3. Flatten the list of lists into a single list of dicts
    return [promo for cabinet_list in results for promo in cabinet_list]


def get_promotion_details(api_key, promotion_ids):
    """
    Получает детальную информацию об акциях по их ID

    Args:
        api_key (str): API ключ
        promotion_ids (list[int]): Список ID акций
    """
    url = "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions/details"
    headers = {"Authorization": api_key}
    params = [("promotionIDs", str(pid)) for pid in promotion_ids]
    response = requests.get(url, headers=headers, params=params)
    return response.json()


def match_promo_names(promo_names_lk, promo_names_api):
    '''
    Запрос к нейросетке (chatgpt через github models).
    Мэтчит названия акций из ЛК с чистыми названиями этих же акций из API.
    Нет, использовать для других мэтчей нельзя, потому что промпт под акции :)
    '''
    token = os.environ["GITHUB_TOKEN"]
    endpoint = "https://models.inference.ai.azure.com"
    
    client = ChatCompletionsClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(token),
    )

    response = client.complete(
        messages=[
            SystemMessage(content=(
                "You are a data assistant specialized in matching marketing campaign names.\n"
                "CONTEXT:\n"
                "List 1 contains internal report names (e.g., 'Items to exclude from [Campaign Name]').\n"
                "List 2 contains the actual Campaign Names.\n\n"
                "RULES:\n"
                "1. Match List 1 items to List 2 by identifying the shared campaign title, ignoring prefixes like 'Товары для...' or 'Исключение из...'.\n"
                "2. Return ONLY a valid JSON object. No prose.\n"
                "3. Format: {\"list1_item\": \"list2_item\"}.\n"
                "4. If no logical match exists (even with fuzzy matching), return: {\"error\": \"Detailed reason\"}."
            )),
            UserMessage(content=f"List 1: {promo_names_lk}\nList 2: {promo_names_api}")
        ],
        model="gpt-4o-mini",
        temperature=0.1 # Low temperature ensures more consistent, "robotic" output
    )    

    return response.choices[0].message.content


def insert_promotions(api_df):
    """
    Insert pandas DataFrame into the promotions table with column order:
    nm_id, promo_name, promo_id, ...
    """

    conn = create_connection_w_env()

    # Columns in the DB table (matching new order)
    cols = [
        "nm_id",
        "promo_name",
        "promo_id",
        "promo_start",
        "promo_end",
        "current_price",
        "current_discount",
        "plan_price",
        "promo_type"
    ]

    # Convert DataFrame to list of tuples
    values = [tuple(row) for row in api_df[cols].to_numpy()]

    # Build insert query
    query = f"""
        INSERT INTO promotions ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (promo_id, nm_id) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()
        conn.close()


def load_api_data(start_dt, end_dt):
    '''
    load promo names for ALL cabinets
    '''
    tokens = load_api_tokens()

    api_data = asyncio.run(
        get_all_clients_promo(
            api_tokens=list(tokens.values()),
            start_dt=start_dt,
            end_dt=end_dt
        )
    )

    logger.info('api data is loaded')
    return api_data


def main(folder = None):

    # 1. load api data
    api_data = load_api_data(start_dt=date_from_now(-7), end_dt=date_from_now(14)) # передаем начало и конец периода
    promo_names_api = list(set([i['name'] for i in api_data])) # process - leave only unique promo


    # 2. load lk data
    if folder is None:
        logging.info(f'Название папки не указано, выгружаю данные из папки {datetime.now().strftime('%d.%m')}')
        folder = datetime.now().strftime('%d.%m')

    path = f'/Users/margaretko/Desktop/IT/Start/акции/{folder}' # TODO: put to the .env
    df = merge_excels(path)
    promo_names_lk = list(df['Файл'].unique())
    logger.info('lk data is loaded')


    # 3. match promo names from lk and api (AI)
    matched_lk_api_promo = json.loads(match_promo_names(
        promo_names_lk = promo_names_lk,
        promo_names_api = promo_names_api
    ))

    logger.info(f'AI-matched promo: {matched_lk_api_promo}')

    # match time by name, add both to the promo name and as separate columns
    formatted_api_names = {i['name'] : format_promo(i) for i in api_data}

    df['Файл'] = df['Файл'].map(matched_lk_api_promo) # match clean names


    # 4. merge two dfs LEFT - only leave data from lk
    api_df = pd.DataFrame(api_data).drop_duplicates(subset=['id'])


    # --- this whole block preprocesses API datetime --
    # convert to datetime (UTC)
    api_df['startDateTime'] = pd.to_datetime(api_df['startDateTime'], utc=True)
    api_df['endDateTime'] = pd.to_datetime(api_df['endDateTime'], utc=True)
    # add +2 hours
    api_df['startDateTime'] += pd.Timedelta(hours= API_TIME_DIFF)
    api_df['endDateTime'] += pd.Timedelta(hours= API_TIME_DIFF)
    # remove timezone (naive datetime for DB)
    api_df['startDateTime'] = api_df['startDateTime'].dt.tz_localize(None)
    api_df['endDateTime'] = api_df['endDateTime'].dt.tz_localize(None)
    # --------------------------------------------------


    merged_df = df.merge(right = api_df, how='left', left_on='Файл', right_on='name')
    merged_df.drop(columns='name', inplace = True) # drop duplicated column

    # change normal name to the name with date
    merged_df['Файл'] = merged_df['Файл'].map(formatted_api_names)
    

    # 5. rename and add to db
    rename_dct = {
        'endDateTime' : 'promo_end',
        'id' : 'promo_id',
        'startDateTime' : 'promo_start',
        'type' : 'promo_type',
        'Артикул WB' : 'nm_id',
        'Плановая цена для акции' : 'plan_price',
        'Текущая розничная цена' : 'current_price',
        'Текущая скидка на сайте, %' : 'current_discount',
        'Файл' : 'promo_name'}

    merged_df.rename(columns = rename_dct, inplace = True)

    # change columns order
    merged_df = merged_df[['nm_id', 'promo_name', 'promo_id', 'promo_start', 'promo_end',
                           'current_price', 'current_discount', 'plan_price', 'promo_type']]

    insert_promotions(
        api_df=merged_df
    )
    logger.info('data is inserted into db')
    
    df_for_gs = merged_df[['promo_name', 'nm_id', 'plan_price', 'current_price', 'current_discount']]
    df_for_gs.to_excel(f'{path}.xlsx', index = False)
    logger.info('data is save to an excel file')

if __name__ == "__main__":
    main()