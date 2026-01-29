# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
import requests
from time import sleep
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_db_functions import create_connection_w_env, fetch_db_data_into_list

# ---- LOGS ----
logger = setup_logger("wb_supplies_to_db.log")


def get_supplies_paginated(token, limit=1000):
    '''
    Отдает номера поставок и заказов по одному клиенту.
    В БД не хранится, т.к. все отдаваемые данные есть в другом методе
    '''
    base_url = "https://supplies-api.wildberries.ru/api/v1/supplies"

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    payload = {
        "dates": [
            {
                "type": "createDate"
            }
        ]
    }

    offset = 0
    all_items = []

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        response = requests.post(base_url, headers=headers, params=params, json=payload)
        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        all_items.extend(batch)

        if len(batch) < limit:
            break  # no more pages

        offset += limit

    return all_items


def get_supply_by_id(ID: int, token: str, is_preorder: bool = False) -> dict:
    """
    Fetches a single supply by ID.
    """
    url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{ID}"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    params = {
        "isPreorderID": is_preorder
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    data['ID'] = ID
    return data


def get_supplies_by_ids(IDs: list, token: str, is_preorder: bool = False) -> list:
    """
    Fetches multiple supplies by a list of IDs.
    Returns a list of dictionaries, each with 'supply_id' added.
    """
    results = []
    count = 0
    ids_num = len(IDs)
    for supply_id in IDs:
        print(f'{count}/{ids_num} processed')
        count += 1
        try:
            supply_data = get_supply_by_id(supply_id, token, is_preorder)

            if supply_id != IDs[-1]:
                sleep(2)

            results.append(supply_data)
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch supply {supply_id}: {e}")
            results.append({"supply_id": supply_id, "error": str(e)})
    return results


def get_supply_goods(ID: int, token: str, limit: int = 1000, is_preorder: bool = False) -> list:
    """
    Fetch all goods for a single supply ID, handling pagination (offset).
    Returns a list of dictionaries, each with 'ID' added.
    """
    url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{ID}/goods"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    all_goods = []
    offset = 0

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "isPreorderID": is_preorder
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        goods = response.json()
        
        for item in goods:
            item['ID'] = ID
        
        all_goods.extend(goods)
        
        if len(goods) < limit:
            break
        
        offset += limit

    return all_goods


def get_multiple_supplies_goods(IDs: list, token: str, limit: int = 1000, is_preorder: bool = False) -> list:
    """
    Fetch goods for multiple supply IDs.
    Returns a combined list of dictionaries with 'ID' added.
    """
    all_results = []
    count = 1
    ids_num = len(IDs)
    for ID in IDs:
        print(f'{count}/{ids_num} processed')
        count +=1
        try:
            goods = get_supply_goods(ID, token, limit, is_preorder)
            all_results.extend(goods)

            if ID != IDs[-1]:
                sleep(2)
                
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch goods for supply {ID}: {e}")
            all_results.append({"ID": ID, "error": str(e)})
    return all_results

def insert_wb_supplies_to_db(records, conn):
    """
    Insert a list of supply dicts into wb_supplies table.
    Unmapped fields are ignored.
    ON CONFLICT (id, updated_date) DO NOTHING.
    """

    # camelCase → snake_case mapping
    rename_map = {
        "ID": "id",
        "phone": "phone",
        "statusID": "status_id",
        "boxTypeID": "box_type_id",
        "createDate": "create_date",
        "supplyDate": "supply_date",
        "factDate": "fact_date",
        "updatedDate": "updated_date",
        "warehouseID": "warehouse_id",
        "warehouseName": "warehouse_name",
        "actualWarehouseID": "actual_warehouse_id",
        "actualWarehouseName": "actual_warehouse_name",
        "transitWarehouseID": "transit_warehouse_id",
        "transitWarehouseName": "transit_warehouse_name",
        "acceptanceCost": "acceptance_cost",
        "paidAcceptanceCoefficient": "paid_acceptance_coefficient",
        "rejectReason": "reject_reason",
        "supplierAssignName": "supplier_assign_name",
        "storageCoef": "storage_coef",
        "deliveryCoef": "delivery_coef",
        "quantity": "quantity",
        "readyForSaleQuantity": "ready_for_sale_quantity",
        "acceptedQuantity": "accepted_quantity",
        "unloadingQuantity": "unloading_quantity",
        "depersonalizedQuantity": "depersonalized_quantity",
    }

    # Normalize each record
    normalized = []
    # for item in records:
    #     row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
    #     normalized.append(row)

    for item in records:
        row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
        if row.get("id") is not None:
            normalized.append(row)

    if not normalized:
        return

    # All columns we will insert
    columns = list(normalized[0].keys())
    col_names_sql = ", ".join(columns)

    # Build values list
    values = [[row.get(col) for col in columns] for row in normalized]

    query = f"""
        INSERT INTO wb_supplies ({col_names_sql})
        VALUES %s
        ON CONFLICT (id, updated_date, ready_for_sale_quantity, accepted_quantity, unloading_quantity) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


def insert_wb_supplies_goods(records, conn):
    """
    Insert a list of goods dicts into wb_supplies_goods.
    Column names are mapped using rename_map.
    ON CONFLICT DO NOTHING.
    """

    rename_map = {
        "ID": "id",
        "barcode": "barcode",
        "vendorCode": "vendor_code",
        "nmID": "nm_id",
        "needKiz": "need_kiz",
        "tnved": "tnved",
        "techSize": "tech_size",
        "color": "color",
        "supplierBoxAmount": "supplier_box_amount",
        "quantity": "quantity",
        "readyForSaleQuantity": "ready_for_sale_quantity",
        "unloadingQuantity": "unloading_quantity",
        "acceptedQuantity": "accepted_quantity"
    }

    normalized = []
    # for item in records:
    #     row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
    #     normalized.append(row)

    for item in records:
        row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
        if row.get("id") is not None:
            normalized.append(row)

    if not normalized:
        return

    columns = list(normalized[0].keys())
    col_names_sql = ", ".join(columns)

    values = [[row.get(col) for col in columns] for row in normalized]

    query = f"""
        INSERT INTO wb_supplies_goods ({col_names_sql})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()

def fetch_existing_supply_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM wb_supplies")
    return [int(row[0]) for row in cursor.fetchall()]

async def process_client(client: str, token: str, conn):
    supplies = await asyncio.to_thread(get_supplies_paginated, token)

    # сортировка supplyID

    # вариант 1 - если нужны все данные
    # supplies_ids = [i['supplyID'] for i in supplies if i['supplyID']]

    # вариант 2 - берем supplyID, по которым есть изменения ('updatedDate') за последний день
    one_week_ago = datetime.now() - timedelta(days=60)
    supplies_ids = [
        i['supplyID'] 
        for i in supplies 
        if i['supplyID'] and i.get('updatedDate') and datetime.fromisoformat(i['updatedDate'][:-6]) >= one_week_ago
    ]

    logger.info(f'Found {len(supplies_ids)} ids for client {client}')

    try:

        for i in range(0, len(supplies_ids), 50):
            batch_ids = supplies_ids[i:i+50]

            supplies_info = await asyncio.to_thread(get_supplies_by_ids, batch_ids, token)
            insert_wb_supplies_to_db(supplies_info, conn)

            supplies_goods = await asyncio.to_thread(get_multiple_supplies_goods, batch_ids, token)
            insert_wb_supplies_goods(supplies_goods, conn)

            logger.info(f"{client} batch {i//50+1} done")

    except Exception as e:
        logger.error(f"Client {client} error: {e}")
        raise

def load_existing_supplyids_wilds(conn):
    query = '''
    select
        wsg.id,
        wsg.vendor_code
    from wb_supplies_goods wsg
    '''
    data = fetch_db_data_into_list(query)
    grouped = {}

    for key, value in data:
        grouped.setdefault(key, []).append(value)

    return grouped


async def process_missing_data(client: str, token: str, conn, logger = logger):
    supplies = await asyncio.to_thread(get_supplies_paginated, token)

    time_ago = datetime.now() - timedelta(days=30) # last month
    supplies_ids = [
        i['supplyID'] 
        for i in supplies 
        if i['supplyID'] and i.get('updatedDate') and datetime.fromisoformat(i['updatedDate'][:-6]) >= time_ago
    ]

    existing_data = load_existing_supplyids_wilds(conn) # {'supplyID : [list of wilds]}
    existing_ids = set(existing_data.keys())

    ids_to_process = list(set(supplies_ids).intersection(existing_ids))

    n_data = len(ids_to_process)
    logger.info(f'Started processing {n_data} supply ids for client {client}')

    for i, id in enumerate(ids_to_process):
        try:

            supplies_goods = await asyncio.to_thread(get_supply_goods, id, token)
            api_goods = [j['vendorCode'] for j in supplies_goods]
            existing_goods = existing_data[id]

            diff = set(api_goods).difference(set(existing_goods))
            
            if diff:
                logger.info(f'Client {client}, id {id} found {len(diff)} missing supply ids: {diff}  {i + 1}/{n_data}')

                insert_data = [j for j in supplies_goods if j['vendorCode'] in diff]
                logger.info(f'Adding the following data for client {client} to the db table: {insert_data}')
                insert_wb_supplies_goods(insert_data, conn)
            else:
                logger.info(f'No missing data for client {client} id {id}               {i + 1}/{n_data}')

            if id != ids_to_process[-1]:
                await asyncio.sleep(2.1)

        except Exception as e:
            logger.error(f"Client {client} id {id} error: {e}")


async def process_missing_data_all_clients(logger = logger):
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    tasks = []
    for client, token in tokens.items():
        tasks.append(asyncio.create_task(process_missing_data(client, token, conn, logger)))

    # run all clients concurrently
    await asyncio.gather(*tasks)

async def main():
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    tasks = []
    for client, token in tokens.items():
        tasks.append(asyncio.create_task(process_client(client, token, conn)))

    # run all clients concurrently
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())