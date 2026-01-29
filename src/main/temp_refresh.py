import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.logger import setup_logger
from utils.my_db_functions import create_connection_w_env

logger = setup_logger("temp_refresh.log")

if __name__ == "__main__":
    conn = None
    cur = None

    try:
        conn = create_connection_w_env()
        cur = conn.cursor()

        refresh_query = (
            'REFRESH MATERIALIZED VIEW CONCURRENTLY public.buyout_return_percent_mv;'
        )
        cur.execute(refresh_query)
        conn.commit()

        logger.info(
            "Materialized view buyout_return_percent_mv refreshed successfully"
        )

    except Exception as e:
        if conn:
            conn.rollback()

        logger.error(
            "Failed to refresh materialized view buyout_return_percent_mv",
            exc_info=True
        )

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()