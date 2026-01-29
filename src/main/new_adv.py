import requests
import json
import pandas as pd
from datetime import date
import itertools
import asyncio
import aiohttp
import logging

from utils.utils import batchify, load_api_tokens


async def adv_stat_async(campaign_ids: list, date_from: str, date_to: str, api_token: str, account: str):
    """
    Получение статистики по списку ID кампаний за указанный период.

    :param campaign_ids: список ID кампаний
    :param date_from: дата начала периода в формате YYYY-MM-DD
    :param date_to: дата окончания периода в формате YYYY-MM-DD
    :param api_token: токен для API WB
    :param account: название аккаунта
    """
    semaphore = asyncio.Semaphore(10)
    url = "https://advert-api.wildberries.ru/adv/v3/fullstats"
    headers = {"Authorization": api_token}
    batches = list(batchify(campaign_ids, 50))
    data = []
    async with semaphore:
        async with aiohttp.ClientSession(headers=headers) as session:
            for batch in batches:
                ids_str = ",".join(str(c) for c in batch)
                params = {"ids": ids_str, "beginDate": date_from, "endDate": date_to}

                # print(f"Запрос для {account}: {params}")

                retry_count = 0
                while retry_count < 5:
                    try:
                        async with session.get(url, params=params) as response:
                            logging.info(f"HTTP статус: {response.status}")

                            if response.status == 400:
                                err = await response.json()
                                logging.error(f"Ошибка 400 {account}: {err.get('message') or err}")
                                # retry_count += 1
                                # await asyncio.sleep(60)
                                continue

                            if response.status == 429:
                                logging.error("429 Too Many Requests — ждём минуту")
                                retry_count += 1
                                await asyncio.sleep(60)
                                continue

                            response.raise_for_status()
                            batch_data = await response.json()

                            # добавляем поле account в каждый элемент
                            for item in batch_data or []:
                                item["account"] = account
                                item["date"] = date_from
                            data.extend(batch_data or [])
                            break

                    except aiohttp.ClientError as e:
                        logging.error(f"Сетевая ошибка для {account}: {e}")
                        retry_count += 1
                        await asyncio.sleep(30)

                # WB ограничивает 1 запрос/мин → ждём после каждого батча
                await asyncio.sleep(60)

        return data
    

def camp_list(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id,
        'order': 'id'
                }
        payload = []
        try:
            res = requests.post(url, headers=headers, params=params, json=payload)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            logging.error(f"Error loading adverts: {e}")
            data = []

        if data:
            # Добавляем информацию о кабинете в данные
            for item in data:
                item['account'] = account
            camps.append(data)
    return camps


def camp_list_manual(api_token: str, account: str):
    url = 'https://advert-api.wildberries.ru/adv/v0/auction/adverts'
    camps = []
    campaign_statuses = [9, 11]
    headers = {'Authorization': api_token}
    for status_id in campaign_statuses:
        params = {
        'status': status_id
                }
        try:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            logging.error(f"Error loading adverts manually: {e}")
            data = []

        if data:
                # Добавляем информацию о кабинете в данные
                for item in data['adverts']:
                    item['account'] = account
                camps.append(data['adverts'])
    return camps

async def get_all_adv_data():
    all_adv_data = []
    tasks = []
    for account, api_token in load_api_tokens().items():
        camps_list = camp_list(api_token, account)
        campaigns = list(itertools.chain(*camps_list))
        campaign_ids = [c['advertId'] for c in campaigns]
        camps_list_2 = camp_list_manual(api_token, account)
        campaigns_2 = list(itertools.chain(*camps_list_2))
        campaign_ids_2 = [c['id'] for c in campaigns_2 if c['status'] in (9, 11)]
        campaign_ids.extend(campaign_ids_2)
        campaign_ids = list(set(campaign_ids))

        date_from = date_to = date.today().strftime("%Y-%m-%d")
        logging.info(f"Получаем данные за {date_from} по ЛК {account}")
        tasks.append(adv_stat_async(campaign_ids, date_from, date_to, api_token, account))
    stats = await asyncio.gather(*tasks)
    for stat in stats:
        all_adv_data.extend(stat)
    return all_adv_data

def processed_adv_data(adv_data):
    processed_data = []
    for camp in adv_data:
        # print(camp)
        # Для АРК берем среднюю позицию из boosterStats
        try:
            camp['avg_position'] = camp['boosterStats'][0]['avg_position']
        except (KeyError, IndexError):
            camp['avg_position'] = None
        # Получаем данные по всем платформам ios, PC, android
        try:
            platforms = camp['days'][0]['apps']
            # print(platforms)
            for platform in platforms:
                # print(platform)
                # Если appType = 1, то это ПК
                if platform['appType'] == 1:
                    camp['atbs_pc'] = platform['atbs']
                    camp['canceled_pc'] = platform['canceled']
                    camp['clicks_pc'] = platform['clicks']
                    camp['cpc'] = platform['cpc']
                    camp['cr_pc'] = platform['cr']
                    camp['ctr_pc'] = platform['ctr']
                    camp['orders_pc'] = platform['orders']
                    camp['shks_pc'] = platform['shks']
                    camp['sum_price_pc'] = platform['sum_price']
                    camp['views_pc'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
                # Если appType = 32, то это андроид
                elif platform['appType'] == 32: 
                    camp['atbs_android'] = platform['atbs']
                    camp['canceled_android'] = platform['canceled']
                    camp['clicks_android'] = platform['clicks']
                    camp['cr_android'] = platform['cr']
                    camp['ctr_android'] = platform['ctr']
                    camp['orders_android'] = platform['orders']
                    camp['shks_android'] = platform['shks']
                    camp['sum_price_android'] = platform['sum_price']
                    camp['views_android'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
                elif platform['appType'] == 64:  # Если appType = 4, то это ios
                    camp['atbs_ios'] = platform['atbs']
                    camp['canceled_ios'] = platform['canceled']
                    camp['clicks_ios'] = platform['clicks']
                    camp['cr_ios'] = platform['cr']
                    camp['ctr_ios'] = platform['ctr']
                    camp['orders_ios'] = platform['orders']
                    camp['shks_ios'] = platform['shks']
                    camp['sum_price_ios'] = platform['sum_price']
                    camp['views_ios'] = platform['views']
                    camp['article_id'] = platform['nms'][0]['nmId']
        except KeyError:
            print('no days key')
        
        # Удаляем ненужные ключи, перед созданием датафрейма
        try:
            del camp['boosterStats']
        except KeyError:
            print("Нет ключа boosterStats")
        # 
        try:
            del camp['days']
        except KeyError:
            print("Нет ключа boosterStats")
        # print(camp)
        processed_data.append(camp)
    return processed_data

if __name__ == "__main__":
    data = asyncio.run(get_all_adv_data())
    ready_data = processed_adv_data(data)
    with open('final_adv_data_example.json', "w", encoding="utf-8") as f:
        json.dump(ready_data, f, ensure_ascii=False, indent=4) 