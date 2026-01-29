# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import logging
import requests
import pandas as pd
from datetime import datetime

# my packages
from utils.env_loader import *
from utils.my_gspread import connect_to_local_sheet
from utils.my_db_functions import fetch_db_data_into_dict


# ---- LOGS ----

LOGS_PATH = os.getenv("LOGS_PATH")

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/rate_of_return.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def load_db_data(date_start = '2025-07-16'):
    query_curr = f'''
    SELECT
        date,
        subject_name,
        manager,
        SUM(profit_by_cond_orders - adv_spend) AS ЧП_РК,
        SUM(orders_sum_rub) AS orders_sum_rub,
        CASE 
            WHEN SUM(orders_sum_rub) = 0 THEN NULL 
            ELSE ROUND(SUM(profit_by_cond_orders - adv_spend) / SUM(orders_sum_rub), 4)
        END as Рентабельность
    FROM (
        SELECT 
            date,
            subject_name,
            FIRST_VALUE(manager) OVER (
                PARTITION BY subject_name 
                ORDER BY date DESC, date DESC
            ) as manager,
            profit_by_cond_orders,
            adv_spend,
            orders_sum_rub
        FROM orders_articles_analyze
        WHERE date >= '{date_start}'
    ) subquery
    GROUP BY date, subject_name, manager
    '''
    data = fetch_db_data_into_dict(query_curr)
    # data = process_decimal_in_dict(data)
    return data

def load_ror_by_day(date_start = '2025-07-16'):

    query = f'''
    SELECT
        date,
        CASE 
            WHEN SUM(orders_sum_rub) = 0 THEN NULL
            ELSE ROUND(SUM(profit_by_cond_orders - adv_spend) / SUM(orders_sum_rub), 4)
        END AS Рентабельность
    FROM orders_articles_analyze
    WHERE date >= DATE '{date_start}'
    GROUP BY date;
    '''

    return fetch_db_data_into_dict(query)


if __name__ == "__main__":

    try: 
        # load data
        data = load_db_data()
        df = pd.DataFrame(data)

        # gather managers' names to add to the final df later
        latest_managers = df.loc[df.groupby('subject_name')['date'].idxmax()]
        subject_manager_dict = dict(zip(latest_managers['subject_name'], latest_managers['manager']))

        # process data
        df_pivot = df.pivot(columns = 'date', index = 'subject_name', values = 'Рентабельность')
        df_pivot.insert(0, 'manager', df_pivot.index.map(subject_manager_dict)) # add managers' names
        df_pivot = df_pivot.fillna('')
        df_pivot.columns = df_pivot.columns.astype(str)
        df_pivot.reset_index(inplace=True)
        df_pivot = df_pivot[df_pivot['subject_name']!='0']

        # send to gs
        sh = connect_to_local_sheet(os.getenv('ROR_link'), 'Автовыгрузка')
        
        # worksheet.row_values(1)  # optional: preserve existing row 1 formulas
        sh.update([df_pivot.columns.tolist()], 'A2')  # write headers starting at A2
        sh.update(df_pivot.values.tolist(), 'A3')
    except Exception as e:
        logging.error(f'Failed to upload ROR data: {e}')
        raise

    try: 
        mean_data = load_ror_by_day()
        output_mean = [i['Рентабельность'] for i in mean_data]
        sh.update([output_mean], range_name = 'C1')
    except Exception as e:
        logging.error(f'Failed to upload mean ROR data: {e}')
        raise

    # макрос для автоматического условного форматирования
    web_app_url = os.getenv("ROR_macro")
    try:
        response = requests.get(web_app_url, timeout=60)
        print("Status code:", response.status_code)
        print("Response headers:", response.headers)
        print("Response text:", response.text)
    except Exception as e:
        print(f"Request failed: {e}")
    
    current_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    sh.update(
        values=[[f'Обновлено {current_time}']],
        range_name='A1'
    )