from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
#Импорт библиотек
from datetime import datetime, date, time


class DmProductsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: int
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class DmProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_products(self, dm_products_threshold: int, limit: int) -> List[DmProductsObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductsObj)) as cur:
            cur.execute(
                """
                    with product_data as (
                    select json_array_elements((object_value::json->>'menu')::json)->>'_id' as product_id,
	                       json_array_elements((object_value::json->>'menu')::json)->>'name' as product_name,
	                       json_array_elements((object_value::json->>'menu')::json)->>'price' as product_price,
	                       update_ts as active_from,
	                       '2099-12-31 00:00:00.000' as active_to,
	                       dds.dm_restaurants.id as restaurant_id
                    from stg.ordersystem_restaurants
                    left join dds.dm_restaurants using(id)
                    ),
                    all_product_date as (
                    select ROW_NUMBER() OVER (ORDER BY product_id) AS id, *
                    from product_data
                    )
                    select *
                    from all_product_date
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": dm_products_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmProductsDestRepository:

    def insert_dm_products(self, conn: Connection, dm_products: DmProductsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products (id, product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to,
                        restaurant_id = EXCLUDED.restaurant_id;
                """,
                {
                    "id": dm_products.id,
                    "product_id": dm_products.product_id,
                    "product_name": dm_products.product_name,
                    "product_price": dm_products.product_price,
                    "active_from": dm_products.active_from,
                    "active_to": dm_products.active_to,
                    "restaurant_id": dm_products.restaurant_id
                },
            )


class DmProductsLoader:
    WF_KEY = "example_products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 150  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductsOriginRepository(pg_origin)
        self.stg = DmProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_products in load_queue:
                self.stg.insert_dm_products(conn, dm_products)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
