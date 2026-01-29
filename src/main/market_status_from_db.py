import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import gspread
import pandas as pd

from utils.utils import execute_read_query, update_df_in_google
from utils.my_db_functions import create_connection_w_env
from utils.logger import setup_logger
from utils.env_loader import *

logger = setup_logger("market_status_from_db.log")

CREDS_PATH = os.getenv("PRO_CREDS_PATH")


# Функция для получения датафрейма из БД
def get_db_table(db_query: str, connection):
    """Функция получает данные из Базы Данных и преобразует их в датафрейм"""
    execute_read_query(connection, db_query)
    # Преобразуем таблицу в датафрейм
    try:
        df_db = pd.read_sql(db_query, connection).fillna(0).infer_objects(copy=False)
        print('Данные из БД загружены в датафрейм')
        return df_db
    except Exception as e:
        print(f'Ошибка получения данных из БД {e}')


if __name__ == "__main__":

    try:

        query = f"""SELECT assembly_task_id, wb_status, supplier_status
        FROM status_assembly_task sat;"""

        # Создание подключения с использованием SQLAlchemy
        connection = create_connection_w_env()
        df_status = get_db_table(query, connection)
        connection.close()

        # Дает права на взаимодействие с гугл-таблицами
        gc = gspread.service_account(filename=CREDS_PATH)
        table_tasks = gc.open('Для расчетов БД')
        sheet_tasks = table_tasks.worksheet('БД 2 ( ТЕСТ )').get_all_values()

        df_sheet_tasks = pd.DataFrame(sheet_tasks[1:], columns=sheet_tasks[0])
        df_sheet_tasks = df_sheet_tasks[['id', 'ЛК']]
        df_sheet_tasks['id'] = df_sheet_tasks['id'].astype(int)

        final_df = pd.merge(df_sheet_tasks, df_status, how='left', left_on='id', right_on='assembly_task_id')
        final_df = final_df[['supplier_status', 'wb_status', 'id', 'ЛК']]

        # Получаем доступ к листу
        status_sheet = table_tasks.worksheet('Статусы сборки 2')
        update_df_in_google(final_df, status_sheet)

        logger.info('Данные успешно добавлены в гугл')

    except Exception as e:
        logger.error(str(e))