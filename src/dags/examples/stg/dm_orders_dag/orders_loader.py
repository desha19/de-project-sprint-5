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


class DmOrdersObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: int


class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_orders(self, dm_orders_threshold: int, limit: int) -> List[DmOrdersObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrdersObj)) as cur:
            cur.execute(
                """
with orders_data as (
select id,
       object_id as order_key,
       object_value::json->>'final_status' as order_status,
       object_value::json->'restaurant'->>'id' as restaurant_id,
       --to_timestamp(object_value::json->>'update_ts', 'YYYY-MM-DD HH24:MI:SS') as update_ts,
       dds.dm_timestamps.id as timestamp_id,
       object_value::json->'user'->>'id' as user_id
from stg.ordersystem_orders
left join dds.dm_timestamps using(id)
),
order_data_2 as (
select orders_data.id,
       orders_data.order_key,
       orders_data.order_status,
       dds.dm_restaurants.id as restaurant_id,
       orders_data.timestamp_id,
       dds.dm_users.id as user_id
from orders_data
left join dds.dm_restaurants using(restaurant_id)
left join dds.dm_users using(user_id)
),
order_data_3 as (
select od2.id as id,
	   od2.user_id as user_id,
	   od2.restaurant_id as restaurant_id,
	   od2.timestamp_id as timestamp_id,
	   od2.order_key as order_key,
	   od2.order_status as order_status,
	   ac.id as courier_id
from order_data_2 od2
left join stg.api_deliveries ad on od2.order_key = ad.object_value::json->>'order_id'
left join stg.api_couriers ac on ac.couriers_id = ad.object_value::json->>'courier_id'
--where delivery_id is not null
--order by delivery_ts asc
)
select *
from order_data_3
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    and courier_id in (select courier_id from dds.dm_deliveries dd)  --Совершает проверку на наличие идентификатора
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": dm_orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmOrdersDestRepository:

    def insert_dm_orders(self, conn: Connection, dm_orders: DmOrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (id, order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id)
                    VALUES (%(id)s, %(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(courier_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id,
                        courier_id = EXCLUDED.courier_id;
                """,
                {
                    "id": dm_orders.id,
                    "order_key": dm_orders.order_key,
                    "order_status": dm_orders.order_status,
                    "restaurant_id": dm_orders.restaurant_id,
                    "timestamp_id": dm_orders.timestamp_id,
                    "user_id": dm_orders.user_id,
                    "courier_id": dm_orders.courier_id
                },
            )


class DmOrdersLoader:
    WF_KEY = "example_orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.stg = DmOrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_orders(self):
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
            load_queue = self.origin.list_dm_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_orders in load_queue:
                self.stg.insert_dm_orders(conn, dm_orders)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
