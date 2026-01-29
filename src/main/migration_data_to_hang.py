import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
import gspread

from utils.utils import update_df_in_google
from utils.logger import setup_logger
from utils.env_loader import *

logger = setup_logger("migration_data_to_hang.log")

CREDS_PATH = os.getenv("PRO_CREDS_PATH")

if __name__ == "__main__":

    try: 

        # Дает права на взаимодействие с гугл-таблицами
        gc = gspread.service_account(filename=CREDS_PATH)

        # Таблица из которой берем данные
        table_from = gc.open("Для расчетов БД")

        #  Таблица в которую закидываем данные
        table_to = gc.open("ВИСЯЧИЕ + ДОСТАВКА")

        # Добавляем данные о заданиях в статусах Новые и В сборке
        sheet_from_tasks = table_from.worksheet('Сборочные задания 2').get_all_values()
        sheet_from_tasks_df = pd.DataFrame(sheet_from_tasks[1:], columns=sheet_from_tasks[0])

        sheet_from_tasks_df = sheet_from_tasks_df[['id', 'wild', 'Дата', 'Время', 'Статус', 'уд', 'Время московское', 'Тест', 'Стадия', 'ЛК', 'Прошло часов']]
        # Лист куда вставляем данные
        else_sheet_to = table_to.worksheet('Сборочные задания 2')
        update_df_in_google(sheet_from_tasks_df, else_sheet_to)

        logger.info('Данные успешно добавлены в гугл')
        
    except Exception as e:

        logger.error(str(e))