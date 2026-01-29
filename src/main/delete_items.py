import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# from utils.env_loader import *
from dotenv import load_dotenv
from utils.logger import setup_logger
from utils.my_gspread import connect_to_remote_sheet, delete_rows_based_on_values

load_dotenv()

logger = setup_logger("add_new_items.log")

UNIT_TABLE=os.getenv('UNIT_TABLE')
UNIT_MAIN_SHEET=os.getenv('UNIT_MAIN_SHEET')

AUTOPILOT_TABLE_NAME=os.getenv('AUTOPILOT_TABLE_NAME')
AUTOPILOT_SHEET_NAME=os.getenv('AUTOPILOT_SHEET_NAME')

NEW_ITEMS_TABLE_NAME=os.getenv('NEW_ITEMS_TABLE_NAME')
NEW_ITEMS_SHEET_NAME=os.getenv('NEW_ITEMS_SHEET_NAME')

TABLES_LIST = [{"table": UNIT_TABLE, "spreadsheet": UNIT_MAIN_SHEET},
               {"table": AUTOPILOT_TABLE_NAME, "spreadsheet": AUTOPILOT_SHEET_NAME},
               {"table": NEW_ITEMS_TABLE_NAME, "spreadsheet": NEW_ITEMS_SHEET_NAME}]


if __name__ == "__main__":

    # загрузка товаров для удаления из таблицы Новый товар
    new_items_sh = connect_to_remote_sheet('Новый товар', 'На удаление')
    articles = new_items_sh.col_values(1)
    status = new_items_sh.col_values(8)
    info = {a : s for a, s in zip(articles, status)}
    skus_to_delete = [int(i) for i in info if info[i] == 'Удалить']
    logger.info(f"Найдено {len(skus_to_delete)} для удаления: {skus_to_delete}")

    # удаление товаров
    for table in TABLES_LIST:
        try: 
            sh = connect_to_remote_sheet(table["table"], table["spreadsheet"])
            delete_rows_based_on_values(
                sh = sh,
                values_to_delete = skus_to_delete,
                col_num = 1)
        except Exception as e:
            logger.error(
    f"Ошибка при удалении товаров из таблицы {table['table']}: {e}",
    exc_info=True
)
