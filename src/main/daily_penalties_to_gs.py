# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import gspread
from datetime import datetime, timedelta

# my packages
from utils.env_loader import *
from utils.my_db_functions import create_connection_w_env
from utils.utils import get_db_table, update_df_in_google


# ---- SET UP ----
CREDS_PATH = os.getenv('CREDS_PATH')
PRO_CREDS_PATH = os.getenv('PRO_CREDS_PATH')
DB_DAILY_FIN = os.getenv('DB_DAILY_FIN')

AUTOPILOT_TABLE_NAME = os.getenv('AUTOPILOT_TABLE_NAME')
MAIN_TABLE = os.getenv('MAIN_TABLE')



def load_db_data():
    query = f'''
    WITH latest_atsm AS (
        SELECT DISTINCT ON (id)
            id,
            supplier_status,
            wb_status,
            supply_id
        FROM assembly_task_status_model
        ORDER BY id, created_at_db DESC
    )
    SELECT 
        dp.date_from, 
        sale_dt, 
        SUM(penalty) AS penalty, 
        COUNT(dp.nm_id) AS count_items, 
        bonus_type_name, 
        dp.nm_id, 
        subject_name, 
        dp.account, 
        dp.srid, 
        o.warehouse_type, 
        o."date" AS order_date, 
        a.local_vendor_code,
        dp.shk_id, 
        dp.assembly_id, 
        atsm.supplier_status, 
        atsm.wb_status,
        atsm.supply_id
    FROM {DB_DAILY_FIN} dp
    LEFT JOIN orders o ON dp.srid = o.srid
    LEFT JOIN article a ON dp.nm_id = a.nm_id
    LEFT JOIN latest_atsm atsm ON dp.assembly_id = atsm.id
    WHERE penalty != 0
    GROUP BY 
        dp.date_from, sale_dt, bonus_type_name, dp.nm_id, subject_name, dp.account,
        dp.srid, o.warehouse_type, order_date, a.local_vendor_code, 
        dp.shk_id, dp.assembly_id, atsm.supplier_status, atsm.wb_status, atsm.supply_id
    ORDER BY dp.date_from DESC, penalty DESC;
    '''
    conn = create_connection_w_env()
    df = get_db_table(query, conn)
    conn.close()
    return df

def process_data(df):

    df_for_gs = df.copy()

    df_for_gs['supplier_status'] = df_for_gs['supplier_status'].map({
        'new':'Новое сборочное задание',
        'confirm':'На сборке',
        'complete':'В доставке',
        'cancel' : 'Отменено продавцом'})

    df_for_gs['wb_status'] = df_for_gs['wb_status'].map({
        "waiting": "сборочное задание в работе",
        "sorted": "сборочное задание отсортировано",
        "sold": "заказ получен покупателем",
        "canceled": "отмена сборочного задания",
        "canceled_by_client": "покупатель отменил заказ при получении",
        "declined_by_client": "покупатель отменил заказ. Отмена доступна покупателю в первый час с момента заказа, если заказ не переведён на сборку",
        "defect": "отмена заказа по причине брака",
        "ready_for_pickup": "сборочное задание прибыло на пункт выдачи заказов (ПВЗ)"
    })

    df_for_gs = df_for_gs.rename(columns={
                                        'date_from' : 'Дата отчета',
                                        'sale_dt' : 'Дата продажи',
                                        'penalty' : 'Штраф',
                                        'count_items' : 'Количество',
                                        'bonus_type_name' : 'Вид штрафа',
                                        'nm_id' : 'SKU',
                                        'subject_name' : 'Предмет',
                                        'account' : 'ЛК',
                                        'local_vendor_code':'wild',
                                        'shk_id':'Стикер',
                                        'assembly_id' : 'Сборочное задание',
                                        'supplier_status' : 'Статус СЗ у продавца',
                                        'wb_status' : 'Cтатус СЗ в системе WB',
                                        'supply_id':'Номер поставки'})
    df_for_gs['Дата продажи'] = df_for_gs['Дата продажи'].astype(str)
    df_for_gs['Дата отчета'] = df_for_gs['Дата отчета'].astype(str)
    df_for_gs['order_date'] = df_for_gs['order_date'].astype(str)
    df_for_gs = df_for_gs.sort_values(['Дата отчета', 'Штраф'], ascending=[False, False])

    return df_for_gs

if __name__ == "__main__":
    df = load_db_data()
    clean_df = process_data(df)

    # в ПУ выгружаем только последние две недели
    df_for_pilot = clean_df[clean_df['Дата отчета'] >= (datetime.today() - timedelta(weeks=14)).strftime('%Y-%m-%d')]

    gc = gspread.service_account(filename=PRO_CREDS_PATH)
    wks = gc.open(MAIN_TABLE)
    orders_sheet = wks.worksheet('Штрафы')
    update_df_in_google(clean_df, orders_sheet)

    gc = gspread.service_account(filename=CREDS_PATH)
    wks = gc.open(AUTOPILOT_TABLE_NAME)
    orders_sheet = wks.worksheet('Штрафы')
    update_df_in_google(df_for_pilot, orders_sheet)