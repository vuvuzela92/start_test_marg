import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import gspread
import pandas as pd
from datetime import datetime, date, timedelta

from utils.my_db_functions import get_df_from_db, list_to_sql_select
from utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv

logger = setup_logger("make_wb_pay_daily.log")

CLIENT = gspread.service_account(filename=os.getenv('PRO_CREDS_PATH'))
# TABLE = CLIENT.open(os.getenv('MAIN_TABLE'))
TABLE = CLIENT.open(os.getenv('CONDITIONAL_CALCULATION'))

COL_MATCH = {
    'account' : 'Кабинет',
    'realizationreport_id': 'Номер отчёта',
    'date_from' : 'Начало периода',
    'create_dt' : 'Дата формирования отчёта',
    'supplier_oper_name':'Обоснование для оплаты',
    'retail_amount': 'Вайлдберриз реализовал Товар (Пр)',
    'retail_price_withdisc_rub': 'Цена розничная с учетом согласованной скидки',
    "delivery_rub": "Возмещение за выдачу и возврат товаров на ПВЗ",
    'acquiring_fee': 'Эквайринг/Комиссии за организацию платежей',
    'ppvz_for_pay': 'К перечислению Продавцу за реализованный Товар',
    'delivery_amount': 'Услуги по доставке товара покупателю',
    'penalty': 'Общая сумма штрафов',
    'bonus_type_name': 'Виды логистики, штрафов и корректировок ВВ',
    'storage_fee': 'Хранение',
    'deduction': 'Удержания',
    'acceptance': 'Платная приемка',
    'cashback_discount': 'Компенсация скидки по программе лояльности',
    'ppvz_reward':'Возмещение издержек по перевозке/по складским операциям с товаром'
}

def load_period_report_db(date_from):
    '''
    Выгружает ежедневные финансовые отчёты за дату date_from
    '''
    try: 
        cols = list(set(COL_MATCH.keys()) - {'account', 'create_dt', 'realizationreport_id', 'bonus_type_name'})
        cols_sql = list_to_sql_select(cols)
        df = get_df_from_db(f'''
            SELECT {cols_sql}
            FROM daily_fin_reports_full
            WHERE date_from = '{date_from}';
        ''')
        return df
    except Exception as e:
        logger.error(f'Failed to load data from db:\n{e}')
        raise


def process_db_data(db_data):
    '''
    '''
    df = db_data.drop(columns = ['date_from']).rename(columns = COL_MATCH)
    pivot_df = df.pivot_table(index = ['Обоснование для оплаты'], aggfunc='sum')

    payment_col = 'К перечислению Продавцу за реализованный Товар'
    idx = pivot_df.index

    sale_val = pivot_df[payment_col].loc['Продажа'] if 'Продажа' in idx else 0.0
    comp_val = pivot_df[payment_col].loc['Добровольная компенсация при возврате'] \
        if 'Добровольная компенсация при возврате' in idx else 0.0
    return_val = pivot_df[payment_col].loc['Возврат'] if 'Возврат' in idx else 0.0

    transf = round(sale_val + comp_val - return_val, 2)

    summed_data = pivot_df.reset_index().drop(columns='Обоснование для оплаты').sum(numeric_only=True)

    deductions = 0.0
    deduction_cols = [
        'Услуги по доставке товара покупателю',
        'Общая сумма штрафов',
        'Платная приемка',
        'Хранение',
        'Удержания'
    ]
    for col in deduction_cols:
        if col in summed_data.index:
            deductions += summed_data[col]
        else:
            print(f"Missing deduction column: '{col}'")

    wb_pay = transf - deductions

    return wb_pay

def load_periods():
    ws = TABLE.worksheet("Дашборд")
    raw = ws.col_values(1)[:70]
    year = date.today().year

    periods = []
    for p in raw:
        if "-" not in p:
            continue
        d1, d2 = p.split("-")
        d1 = datetime.strptime(f"{year} {d1.strip()}", "%Y %d.%m").date()
        d2 = datetime.strptime(f"{year} {d2.strip()}", "%Y %d.%m").date()

        periods.append((p, d1, d2))

    return periods


def match_period(date_, periods):
    if isinstance(date_, datetime):
        date_ = date_.date()

    for label, start, end in periods:
        if start <= date_ <= end:
            return label
    return None

def process_daily_report(target_date):
    target_date = target_date if isinstance(target_date, date) else target_date.date()

    logger.info(f'start processing {target_date}')
    
    # load DB data for that date
    db_data = load_period_report_db(date_from=target_date)
    logger.info("loaded db data")
    
    # load periods from GS
    PERIODS = load_periods()
    target_period = match_period(target_date, PERIODS)

    if target_period is None:
        logger.error("couldn't identify")
    
    # process DB data
    calc = process_db_data(db_data=db_data)
    
    # append to Google Sheets
    yesterday_str = target_date.strftime('%d.%m.%Y')
    row = [yesterday_str, target_period, calc]
    output_sh = TABLE.worksheet("ВБ_к_оплате")
    output_sh.append_row(row)

# PERIODS = load_periods()

if __name__ == "__main__":
    # for yesterday
    yesterday = (datetime.now() - timedelta(days=1)).date()
    process_daily_report(yesterday)

    # # for a time period
    # for day in range(18, 0, -1):
    #     target_date = date(year = 2025, month = 12, day = day)
    #     process_daily_report(target_date)