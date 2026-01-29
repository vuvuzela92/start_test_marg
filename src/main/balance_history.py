# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.logger import setup_logger
from utils.my_db_functions import create_connection_w_env


# ---- LOGS ----
logger = setup_logger("balance_history.log")


def transfer_current_balances_to_history():
    conn = create_connection_w_env()
    cursor = conn.cursor()

    try:
        insert_query = """
            INSERT INTO public.balance_history (
                product_id, warehouse_id, physical_quantity, reserved_quantity, available_quantity
            )
            SELECT product_id, warehouse_id, physical_quantity, reserved_quantity, available_quantity
            FROM public.current_balances;
        """

        cursor.execute(insert_query)
        conn.commit()
        logger.info("Данные успешно скопированы из current_balances в balance_history.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при переносе данных: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    transfer_current_balances_to_history()