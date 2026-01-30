import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
import pandas as pd

# from utils.my_gspread import connect_to_local_sheet

from utils.my_db_functions import get_df_from_db
from utils.my_gspread import init_client, clean_extra_rows
from utils.logger import setup_logger
from utils.my_pandas import format_datetime

from dotenv import load_dotenv
load_dotenv()

logger = setup_logger("db_data_to_purch_gs.log")

LOCAL_TABLE=os.getenv('LOCAL_TABLE')

ORDERS_RENAME = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "warehouse_id": "ID склада",
    "is_valid": "Корректность документа",
    "in_acceptance": "В приёмке",
    "created_at": "Дата создания записи",
    "is_printed_barcode": "Штрихкод напечатан",
    "acceptance_completed": "Приёмка завершена",
    "expected_receipt_date": "Ожидаемая дата поступления",
    "actual_quantity": "Фактическое количество",
    "currency": "Валюта",
    "unit_price": "Цена за единицу",
    "last_purchase_price": "Цена последней закупки",
    "last_purchase_supplier": "Поставщик последней закупки",
    "payment_indicator": "Признак/способ оплаты",
    "payment_document_number": "Номер платёжного документа",
    "shipment_date": "Дата отгрузки",
    "receipt_transaction_number": "Номер приходной операции",
    "cancelled_due_to": "Причина аннулирования",
    "comment": "Комментарий",
    "planned_cost": "Плановая себестоимость",
    "reciept_number":"Номер чека",
    "receipt_date": "Дата чека",
    "reciept_quantity": "Чек кол-во",
    "reciept_amount": "Чек стоимость"
}

SUPPLY_RENAME = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "is_valid": "Корректность документа",
    "price_per_item": "Цена за единицу",
    "planned_cost": "Плановая себестоимость"
}


def load_orders_data(months):
    """
    Загружает данные из БД ordered_goods_from_buyers за последние `months` месяцев.
    Загружает только конкретные колонки для стабильности.
    """

    query = '''
        select
        main.id,
        main.guid,
        main.document_number,
        main.document_created_at::date AS document_created_at,
        main.supply_date::date AS supply_date,
        main.local_vendor_code,
        main.product_name,
        main.event_status,
        main.quantity,
        main.amount_with_vat,
        main.amount_without_vat,
        main.supplier_name,
        main.supplier_code,
        main.update_document_datetime::date AS update_document_datetime,
        main.author_of_the_change,
        main.our_organizations_name,
        main.warehouse_id,
        main.is_valid,
        main.in_acceptance,
        main.created_at::date AS created_at,
        main.is_printed_barcode,
        main.acceptance_completed,
        main.expected_receipt_date::date AS expected_receipt_date,
        main.actual_quantity,
        main.currency,
        main.unit_price,
        main.last_purchase_price,
        main.last_purchase_supplier,
        main.payment_indicator,
        main.payment_document_number,
        main.shipment_date::date AS shipment_date,
        main.receipt_transaction_number,
        main.cancelled_due_to,
        main.comment,
        main.planned_cost,
        receipts.reciept_number,
        receipts.reciept_date::date AS receipt_date,
        receipts.reciept_quantity,
        receipts.reciept_amount
    FROM ordered_goods_from_buyers AS main
    LEFT JOIN receipts_for_ordered_goods_from_buyers AS receipts
        ON main.id = receipts.ordered_goods_from_buyers_id
    WHERE main.is_valid = TRUE
    AND main.update_document_datetime >= DATE '2025-05-01';
    '''
    df = get_df_from_db(query)
    return df.fillna('')


def load_supply_data(months):
    """
    Загружает данные из БД supply_to_sellers_warehouse за последние `months` месяцев.
    Загружает только конкретные колонки для стабильности.
    """
    columns = [
        "id", "guid", "document_number", "document_created_at", "supply_date",
        "local_vendor_code", "product_name", "event_status", "quantity",
        "amount_with_vat", "amount_without_vat", "supplier_name", "supplier_code",
        "update_document_datetime", "author_of_the_change", "our_organizations_name",
        "is_valid", "amount_with_vat / NULLIF(quantity, 0) as price_per_item", "planned_cost"
    ]
    cols_str = ", ".join(columns)
    query = f'''
    SELECT {cols_str}
    FROM public.supply_to_sellers_warehouse
    WHERE is_valid = TRUE
      AND update_document_datetime >= '2025-05-01' -- NOW() - INTERVAL '{months} months';
    '''
    df = get_df_from_db(query)
    return df.fillna('')

def load_wb_supplies():
    query = '''
        SELECT DISTINCT ON (wsg.id, wsg.vendor_code)
            wsg.id                                   AS "Номер поставки",
            ws.supply_date                           AS "Плановая дата поставки",
            ws.fact_date                             AS "Фактическая дата поставки",
            ws.status_id                             AS "Статус",
            wsg.quantity                              AS "Добавлено в поставку",
            wsg.unloading_quantity                    AS "Раскладывается",
            wsg.accepted_quantity                     AS "Принято, шт",
            wsg.ready_for_sale_quantity               AS "Поступило в продажу",
            wsg.vendor_code                          AS "Артикул продавца",
            wsg.nm_id                                AS "Артикул WB",
            wsg.supplier_box_amount                  AS "Указано в упаковке, шт"
        FROM wb_supplies_goods wsg
        LEFT JOIN wb_supplies ws 
            ON wsg.id = ws.id
        WHERE ws.create_date >= NOW() - INTERVAL '2 months'
        ORDER BY wsg.id, wsg.vendor_code, ws.updated_date DESC, wsg.created_at DESC;
        '''
    return get_df_from_db(query)


def load_orders_by_regions(logger = logger):

    # query = '''
    # SELECT
    #     date as "Дата",
    #     article_id as "Артикул",
    #     region_name as "Регион",
    #     COUNT(is_realization) as "Количество заказов"
    # FROM orders o
    # WHERE "date" >= CURRENT_DATE - 10
    # GROUP BY date,
    #     article_id,
    #     region_name
    # ORDER BY
    #     article_id,
    #     date DESC;
    # '''

    query = '''
    SELECT
        date AS "Дата",
        article_id AS "Артикул",
        CASE
            WHEN region_name IN (
                'Москва','Московская область','Белгородская область','Брянская область','Владимирская область','Воронежская область',
                'Ивановская область','Калужская область','Костромская область','Курская область','Липецкая область','Орловская область',
                'Рязанская область','Смоленская область','Тамбовская область','Тверская область','Тульская область','Ярославская область'
            ) THEN 'Центральный'
            WHEN region_name IN (
                'Санкт-Петербург','Ленинградская область','Архангельская область','Вологодская область','Калининградская область',
                'Мурманская область','Новгородская область','Псковская область','Республика Карелия','Республика Коми',
                'Ненецкий автономный округ'
            ) THEN 'Северо-Западный'
            WHEN region_name IN (
                'Краснодарский край','Астраханская область','Волгоградская область','Ростовская область','Республика Крым',
                'г. Севастополь','Севастополь','Республика Адыгея','Республика Калмыкия','Республика Дагестан','Республика Ингушетия',
                'Кабардино-Балкарская Республика','Карачаево-Черкесская Республика','Республика Северная Осетия-Алания',
                'Республика Северная Осетия — Алания','Чеченская Республика','Ставропольский край','федеральная территория Сириус'
            ) THEN 'Южный + Северо-Кавказский'
            WHEN region_name IN (
                'Нижегородская область','Республика Башкортостан','Кировская область','Республика Марий Эл','Республика Мордовия',
                'Оренбургская область','Пензенская область','Пермский край','Самарская область','Саратовская область',
                'Республика Татарстан','Удмуртская Республика','Ульяновская область','Чувашская Республика'
            ) THEN 'Приволжский'
            WHEN region_name IN (
                'Свердловская область','Тюменская область','Челябинская область','Ханты-Мансийский автономный округ',
                'Ямало-Ненецкий автономный округ','Курганская область'
            ) THEN 'Уральский'
            WHEN region_name IN (
                'Новосибирская область','Иркутская область','Кемеровская область','Красноярский край','Омская область',
                'Томская область','Республика Алтай','Алтайский край','Республика Бурятия','Республика Тыва','Республика Хакасия',
                'Забайкальский край','Приморский край','Хабаровский край','Амурская область','Камчатский край','Магаданская область',
                'Сахалинская область','Еврейская автономная область','Чукотский автономный округ','Республика Саха (Якутия)'
            ) THEN 'Дальневосточный + Сибирский'
            WHEN region_name IN (
                'Гомельская область','Минская область','Брестская область','Витебская область','Гродненская область',
                'Могилевская область','Могилёвская область','г. Минск','Минск'
            ) THEN 'Беларусь'
            WHEN region_name IN (
                'Астана','город республиканского значения Астана','Алматы','Шымкент','Акмолинская область','Актюбинская область',
                'Алматинская область','Атырауская область','Восточно-Казахстанская область','Жамбылская область',
                'Западно-Казахстанская область','Карагандинская область','Костанайская область','Кызылординская область',
                'Мангистауская область','Павлодарская область','Северо-Казахстанская область','Туркестанская область',
                'область Жетысу','область Абай'
            ) THEN 'Казахстан'
            WHEN region_name IN (
                'Тбилиси','Аджария','Гурия','Имеретия','Кахетия','Мцхета-Мтианети','Рача-Лечхуми и Квемо-Сванети',
                'Самегрело-Верхняя Сванетия','Самцхе-Джавахети','Квемо-Картли','Шида-Картли'
            ) THEN 'Грузия'
            WHEN region_name IN (
                'Ереван','Арагацотн','Арагацотнская область','Арарат','Араратская область','Армавир','Гехаркуник',
                'Гехаркуникская область','Котайк','Котайкская область','Лори','Лорийская область','Ширак','Ширакская область',
                'Сюник','Сюникская область','Тавуш','Тавушская область','Вайоц-Дзор'
            ) THEN 'Армения'
            WHEN region_name IN (
                'Бишкек','город республиканского подчинения Бишкек','Ош','Баткенская область','Джалал-Абадская область',
                'Иссык-Кульская область','Нарынская область','Ошская область','Таласская область','Чуйская область'
            ) THEN 'Киргизия'
            WHEN region_name IN (
                'Душанбе','Горно-Бадахшанская автономная область','Согдийская область','Хатлонская область',
                'Районы республиканского подчинения'
            ) THEN 'Таджикистан'
            WHEN region_name IN (
                'Ташкент','Андижанская область','Бухарская область','Джизакская область','Кашкадарьинская область',
                'Навоийская область','Наманганская область','Самаркандская область','Сурхандарьинская область',
                'Сырдарьинская область','Ташкентская область','Ферганская область','Хорезмская область',
                'Республика Каракалпакстан'
            ) THEN 'Узбекистан'
            WHEN region_name = 'Стамбул' THEN 'Турция'
            ELSE 'Другие'
        END AS "Регион",
        COUNT(is_realization) AS "Количество заказов"
    FROM orders o
    WHERE date >= CURRENT_DATE - 14
    GROUP BY date, article_id, "Регион"
    ORDER BY article_id, date desc;
    '''
    
    data = get_df_from_db(query)
    clean_data = format_datetime(data, ["Дата"])

    return clean_data

def update_orders_by_regions(client, logger = logger):
    _regions_sh = client.open('Отгрузка ФБО').worksheet('Заказы_Регионы')
    _db_data = load_orders_by_regions()
    _gs_output = [_db_data.columns.tolist()] + _db_data.values.tolist()

    # _regions_sh.clear()
    _regions_sh.update(values = _gs_output, range_name = 'A2')
    _regions_sh.update(
        values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
        range_name='A1'
    )
    logger.info('Данные успешно добавлены на лист Заказы_Регионы')


if __name__ == "__main__":
    
    try:
        months = None
        client = init_client()
        gs_table = client.open(LOCAL_TABLE)

        orders_db = load_orders_data(months = months)

        datetime_cols = ['document_created_at', 'supply_date', 'update_document_datetime',
                 'created_at', 'expected_receipt_date', 'shipment_date', 'receipt_date']

        for col in datetime_cols:
            if col in orders_db.columns:
                orders_db[col] = orders_db[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
                orders_db[col] = pd.to_datetime(orders_db[col], errors='coerce')
                orders_db[col] = orders_db[col].dt.strftime('%d.%m.%Y')
                orders_db[col] = orders_db[col].fillna('')

        orders_renamed = orders_db.rename(columns=ORDERS_RENAME)

        orders_output = [orders_renamed.columns.tolist()] + orders_renamed.values.tolist()
        orders_sh = gs_table.worksheet('Заказы_поставщиков_1С')
        orders_sh.update(values = orders_output, range_name = 'A2')
        orders_sh.update(
            values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
        logger.info('Данные успешно добавлены на лист "Заказы_поставщиков_1С"')
    except Exception as e:
        logger.error(f'Ошибка при обновлении листа "Заказы_поставщиков_1С": {e}')

    try:
        supply_db = load_supply_data(months = months)

        datetime_cols = ['document_created_at', 'supply_date', 'update_document_datetime']

        for col in datetime_cols:
            if col in supply_db.columns:
                supply_db[col] = supply_db[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
                supply_db[col] = pd.to_datetime(supply_db[col], errors='coerce')
                supply_db[col] = supply_db[col].dt.strftime('%d.%m.%Y')
                supply_db[col] = supply_db[col].fillna('')

        supply_renamed = supply_db.rename(columns=SUPPLY_RENAME)
        supply_output = [supply_renamed.columns.tolist()] + supply_renamed.values.tolist()
        supply_sh = gs_table.worksheet('Приходы_1С')
        supply_sh.update(values = supply_output, range_name = 'A2')
        supply_sh.update(
            values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
        logger.info('Данные успешно добавлены на лист "Приходы_1С"')

    except Exception as e:
        logger.error(f'Ошибка при обновлении листа "Приходы_1С": {e}')
    
    try:
        wb_supplies = load_wb_supplies()
        wb_supplies['Статус'] = wb_supplies['Статус'].map({
            1: "Не запланировано",
            2: "Запланировано",
            3: "Отгрузка разрешена",
            4: "Идёт приёмка",
            5: "Принято",
            6: "Отгружено на воротах",
        })

        for col in ["Плановая дата поставки", "Фактическая дата поставки"]:
            if col in wb_supplies.columns:
                wb_supplies[col] = wb_supplies[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
                wb_supplies[col] = pd.to_datetime(wb_supplies[col], errors='coerce')
                wb_supplies[col] = wb_supplies[col].dt.strftime('%d.%m.%Y')
                wb_supplies[col] = wb_supplies[col].fillna('')

        wb_output = [wb_supplies.columns.tolist()] + wb_supplies.values.tolist()
        wb_supplies_sh = gs_table.worksheet('БД_поставки')
        wb_supplies_sh.update(values = wb_output, range_name = 'A2')
        wb_supplies_sh.update(
            values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
        logger.info('Данные успешно добавлены на лист "БД_поставки"')

        clean_extra_rows(wb_supplies_sh, wb_output, logger = logger)

    except Exception as e:
        logger.error(f'Failed to upload data to БД_поставки: {e}')