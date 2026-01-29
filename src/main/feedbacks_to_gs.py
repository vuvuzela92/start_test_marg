import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import logging
from datetime import datetime

from utils.my_gspread import connect_to_remote_sheet
from utils.my_db_functions import get_df_from_db
from utils.env_loader import *


# ---- LOGS ----
LOGS_PATH = os.getenv("LOGS_PATH")
os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/feedbacks_to_gs.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def load_db_data():
    query = '''
    SELECT
        a.local_vendor_code,
        COUNT(CASE WHEN f.productvaluation = 5 THEN 1 END) AS rating_5,
        COUNT(CASE WHEN f.productvaluation = 4 THEN 1 END) AS rating_4,
        COUNT(CASE WHEN f.productvaluation = 3 THEN 1 END) AS rating_3,
        COUNT(CASE WHEN f.productvaluation = 2 THEN 1 END) AS rating_2,
        COUNT(CASE WHEN f.productvaluation = 1 THEN 1 END) AS rating_1,
        COUNT(*) AS "Кол-во отзывов 30 дней",
        ROUND(AVG(f.productvaluation), 3) AS "Ср.рейтинг 30 дней",
        ROUND(AVG(CASE 
                    WHEN f.createddate >= (CURRENT_TIMESTAMP - INTERVAL '1 month')
                     AND f.createddate < (CURRENT_TIMESTAMP - INTERVAL '2 weeks') 
                    THEN f.productvaluation 
                 END), 3) AS "Ср.рейтинг первые 2 недели",
        ROUND(AVG(CASE 
                    WHEN f.createddate >= (CURRENT_TIMESTAMP - INTERVAL '2 weeks')
                    THEN f.productvaluation 
                 END), 3) AS "Ср.рейтинг последние 2 недели"
    FROM wb_feedbacks f
    JOIN article a 
        ON f.nmid = a.nm_id
    WHERE f.productvaluation IS NOT NULL
    AND a.local_vendor_code LIKE 'wild%'
    AND f.createddate >= (CURRENT_TIMESTAMP - INTERVAL '1 month')
    GROUP BY a.local_vendor_code
    ORDER BY "Кол-во отзывов 30 дней" DESC;
    '''
    return get_df_from_db(query)

if __name__ == "__main__":
    PRO_CREDS_PATH = os.getenv("PRO_CREDS_PATH")

    purch_sh = connect_to_remote_sheet('Расчет закупки NEW', 'Рейтинг_товаров', creds_file=PRO_CREDS_PATH)
    db_data = load_db_data()

    output = [db_data.columns.tolist()] + db_data.values.tolist()

    purch_sh.update(values = output, range_name = 'A1')

    current_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    purch_sh.update(
        values=[[f'Обновлено {current_time}']],
        range_name='K1'
    )