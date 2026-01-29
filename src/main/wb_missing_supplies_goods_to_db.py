# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
# import requests
# from time import sleep
# from datetime import datetime, timedelta
# from psycopg2.extras import execute_values

from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_db_functions import create_connection_w_env, fetch_db_data_into_list
from wb_supplies_to_db import process_missing_data_all_clients, process_missing_data

# ---- LOGS ----
logger = setup_logger("wb_missing_supplies_goods_to_db.log")


if __name__ == "__main__":
    asyncio.run(process_missing_data_all_clients(logger))


# # test - process only one client
# # if don't need insert to db - need to mute it in another file wb_supplies_to_db
# if __name__ == "__main__":
#     tokens = load_api_tokens()
#     client = 'Вектор'
#     client_token = tokens[client]
#     conn = create_connection_w_env()
#     asyncio.run(process_missing_data(client, client_token, conn, logger))
