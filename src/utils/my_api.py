import os
import json
import time
import requests
from requests.exceptions import RequestException
import logging


# -------------------------------- Product Cards --------------------------------


def get_all_product_cards(api_token):
    """
    Получает все карточки товаров с учетом пагинации через API Wildberries
    """

    url = 'https://content-api.wildberries.ru/content/v2/get/cards/list'
    headers = {'Authorization': api_token}
    
    # Initial payload
    payload = {
        "settings": {
            "filter": {
                "withPhoto": -1
            },
            "cursor": {
                "limit": 100
            }
        }
    }
    
    all_cards = []
    
    while True:
        # Make the request
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            print(response.text)
            break
            
        data = response.json()
        
        # Add cards to our collection
        if 'cards' in data:
            all_cards.extend(data['cards'])
            print(f"Retrieved {len(data['cards'])} cards. Total: {len(all_cards)}")
        
        # Check if we have reached the end
        if 'cursor' in data and 'total' in data['cursor']:
            total = data['cursor']['total']
            limit = payload['settings']['cursor']['limit']
            
            # If total is less than limit, we've got all cards
            if total < limit:
                print("All cards retrieved!")
                break
            else:
                # Set up pagination for next request
                if 'updatedAt' in data['cursor'] and 'nmID' in data['cursor']:
                    payload['settings']['cursor']['updatedAt'] = data['cursor']['updatedAt']
                    payload['settings']['cursor']['nmID'] = data['cursor']['nmID']
                else:
                    print("Pagination information not found in response")
                    break
        else:
            print("Cursor information not found in response")
            break
        
        time.sleep(0.1)
    
    return all_cards


def get_product_by_nmid(api_token, nmid):
    """
    Получает карточку товара по nmid.
    В качестве nmid можно передать и артикул, и вилд
    """
    url = 'https://content-api.wildberries.ru/content/v2/get/cards/list'
    headers = {'Authorization': api_token}
    
    payload = {
        "settings": {
            "filter": {
                "textSearch": str(nmid),  # Поиск по артикулу WB (nmID)
                "withPhoto": -1
            },
            "cursor": {
                "limit": 100
            }
        }
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        # Найденная карточка будет в data['cards']
        return data['cards']
    else:
        logging.error(f"Error: {response.status_code}\n{response.text}")
        return None
    

def clean_product_data_for_api(product_data):
    """
    Очищает данные карточки товара, полученные из API Wildberries, 
    чтобы подготовить их для отправки обратно в API (например, для обновления).
    
    Args:
        product_data (dict): Словарь с данными карточки товара из API.
        
    Returns:
        dict: Очищенный словарь, готовый для отправки в API.
    """

    if isinstance(product_data, list):
        product_data = product_data[0]

    # Определяем поля, которые нужно оставить
    allowed_fields = {
        'nmID', 'vendorCode', 'brand', 'title', 'description', 'subjectID',
        'dimensions', 'characteristics', 'sizes'
    }
    
    # Создаем новый словарь только с разрешенными полями
    cleaned_data = {key: product_data[key] for key in allowed_fields if key in product_data}
    
    # Обрабатываем характеристики: убираем поле 'name' и оставляем только 'id' и 'value'
    if 'characteristics' in cleaned_data:
        cleaned_data['characteristics'] = [
            {'id': char['id'], 'value': char['value']} 
            for char in cleaned_data['characteristics']
        ]
        
    # Обрабатываем размеры: убираем лишние поля, оставляем только необходимые
    if 'sizes' in cleaned_data:
        cleaned_data['sizes'] = [
            {k: v for k, v in size.items() if k in {'chrtID', 'techSize', 'wbSize', 'skus'}}
            for size in cleaned_data['sizes']
        ]
        
    # Обрабатываем габариты: убираем поле 'isValid', если оно есть
    if 'dimensions' in cleaned_data and isinstance(cleaned_data['dimensions'], dict):
        cleaned_data['dimensions'] = {
            k: v for k, v in cleaned_data['dimensions'].items() 
            if k in {'length', 'width', 'height', 'weightBrutto'}
        }
    return cleaned_data
    
    
def get_clean_product_card(api_token, article):
    # get single product card from api and clean it <-- we'll insert changed data here
    raw_product_card = get_product_by_nmid(api_token, article)

    try:
        # clean data to load it back to WB API
        clean_product_card = clean_product_data_for_api(raw_product_card)
        return clean_product_card
    except Exception as e:
        print(f'Failed to clean product card for {article}.Error:\n{e}')
        try:
            filename = f'failed_prod_card_{article}.json'
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(raw_product_card, f, ensure_ascii=False, indent=2)
            print(f'The unprocessed product card is saved in failed_prod_card_{article}.json.')
        except Exception as e:
            print(f"The unprocessed product card wasn't saved. Error:\n{e}\n")
        print(f'Error:\n{e}')


def update_wb_product_card(api_token, product_card_data):
    """
    Обновляет карточку товара в Wildberries.
    
    Args:
        api_token (str): API токен продавца.
        product_card_data (dict or list): Данные карточки товара или список карточек.
                                         Должен соответствовать формату API WB.
    
    Returns:
        dict: Ответ от API Wildberries.
              В случае успеха (200) может содержать список карточек, которые не были обновлены.
              В случае ошибки запроса - информацию об ошибке.
    """
    
    # Убедимся, что данные являются списком (массивом), как требует API
    if isinstance(product_card_data, dict):
        cards_to_update = [product_card_data]
    elif isinstance(product_card_data, list):
        cards_to_update = product_card_data
    else:
        raise ValueError("product_card_data должен быть словарем (dict) или списком (list)")
    
    # URL и заголовки для запроса
    url = 'https://content-api.wildberries.ru/content/v2/cards/update'
    headers = {
        'Authorization': api_token,
        'Content-Type': 'application/json'
    }
    
    # Выполнение POST-запроса
    try:
        response = requests.post(url, headers=headers, json=cards_to_update)
        
        # Попытка распарсить JSON-ответ
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            # Если ответ не JSON, возвращаем текст
            response_data = {
                "status_code": response.status_code,
                "text": response.text
            }
            
        # Возвращаем результат
        return {
            "status_code": response.status_code,
            "data": response_data
        }
        
    except requests.exceptions.RequestException as e:
        # Обработка сетевых ошибок
        return {
            "error": True,
            "message": f"Ошибка сети при выполнении запроса: {str(e)}"
        }
    except Exception as e:
        # Обработка других ошибок
        return {
            "error": True,
            "message": f"Неизвестная ошибка: {str(e)}"
        }
    

def get_product_cards_errors(api_token):
    return get_json('https://content-api.wildberries.ru/content/v2/cards/error/list', headers = {'Authorization': api_token})['data']


def get_json(url, headers=None, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        # ошибки подключения
        raise RequestException(f"Request failed: {str(e)}")
    except ValueError as e:
        # если ответ есть, но не читается как json
        raise ValueError(f"Failed to decode JSON response: {str(e)}")


def post_json(url, headers=None, data=None, json=None, params=None):
    try:
        response = requests.post(
            url,
            headers=headers,
            data=data,
            json=json,
            params=params,
        )
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        # ошибки подключения
        raise RequestException(f"POST request failed: {str(e)}")
    except ValueError as e:
        # если ответ есть, но не читается как json
        raise ValueError(f"Failed to decode JSON response: {str(e)}")
    

def get_all_trashed_cards(api_token: str, locale: str = 'ru', with_photo: int = -1):
    """
    Fetches all trashed product cards from Wildberries Content API with pagination.
    
    Args:
        api_token (str): Your Wildberries API token (Content or Promotion category).
        locale (str): Response language ('ru', 'en', or 'zh'). Default: 'ru'.
        with_photo (int): Filter by photo presence (-1 = all, 1 = with photo, 0 = without). Default: -1.
    
    Returns:
        List[Dict]: A list of all trashed cards (each card is a dict).
    """
    url = 'https://content-api.wildberries.ru/content/v2/get/cards/trash'
    headers = {'Authorization': api_token}
    params = {'locale': locale}
    
    all_cards = []
    cursor = {"limit": 100}  # initial cursor without trashedAt/nmID

    while True:
        payload = {
            "settings": {
                "cursor": cursor,
                "filter": {"withPhoto": with_photo}
            }
        }

        response = requests.post(url, headers=headers, params=params, json=payload)
        response.raise_for_status()
        data = response.json()

        cards = data.get('cards', [])
        all_cards.extend(cards)

        # Check if we've fetched all items
        total = data.get('total', 0)
        if len(cards) == 0 or len(all_cards) >= total:
            break

        # Prepare cursor for next page using the LAST card in current response
        last_card = cards[-1]
        cursor = {
            "limit": 100,
            "trashedAt": last_card['trashedAt'],
            "nmID": last_card['nmID']
        }

    return all_cards




# -------------------------------- Documents --------------------------------

def get_docs_list(api_token, beginTime, endTime, **kwargs):
    '''
    Функция достаёт названия (!) документов из WB API.
    Дату принимает в формате "2025-07-15".
    В kwargs можно передать category
    '''
    url = 'https://documents-api.wildberries.ru/api/v1/documents/list'
    headers = {"Authorization": api_token}
    params = {
        "beginTime": beginTime,
        "endTime": endTime,
        **kwargs
    }
    return get_json(url, headers, params)




# -------------------------------- Orders --------------------------------

def get_orders(api_token, dateFrom, flag = 0):
    url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
    headers = {"Authorization": api_token}
    params = {
        "dateFrom": dateFrom,
        "flag": flag
    }
    res = requests.get(url = url, headers = headers, params = params)
    res.raise_for_status()
    return res.json()
