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


class DmDeliveriesObj(BaseModel):
    id: int
    delivery_id: str
    order_id: int
    courier_id: int
    delivery_address: str
    delivery_ts: datetime
    delivery_rate: float
    delivery_sum: float
    tip_sum: float


class DmDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_deliveries(self, dm_deliveries_threshold: int, limit: int) -> List[DmDeliveriesObj]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveriesObj)) as cur:
            cur.execute(
                """
                    with delivery_data as (
                    select ad.id as id,
	                       ad.delivery_id as delivery_id,
	                       --do2.id as order_id,
                           oo.id as order_id,
                           --dc.id as courier_id,
	                       ac.id as courier_id,
	                       ad.object_value::json->>'address' as delivery_address,
	                       ad.object_value::json->>'delivery_ts' as delivery_ts,
	                       ad.object_value::json->>'rate' as delivery_rate,
	                       ad.object_value::json->>'sum' as delivery_sum,
	                       ad.object_value::json->>'tip_sum' as tip_sum
                    from stg.api_deliveries ad 
                    --left join dds.dm_orders do2 on ad.object_value::json->>'order_id' = do2.order_key 
                    left join stg.ordersystem_orders oo on ad.object_value::json->>'order_id'= oo.object_id 
                    --left join dds.dm_couriers dc on ad.object_value::json->>'courier_id' = dc.courier_id 
                    left join stg.api_couriers ac on ad.object_value::json->>'courier_id' = ac.couriers_id 
                    )
                    select *
                    from delivery_data
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    and order_id in (select id from dds.dm_orders do2)  --для проверки (есть ли такой идентификатор order)
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": dm_deliveries_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmDeliveriesDestRepository:

    def insert_dm_deliveries(self, conn: Connection, dm_deliveries: DmDeliveriesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries (id, delivery_id, order_id, courier_id, delivery_address, delivery_ts, delivery_rate, delivery_sum, tip_sum)
                    VALUES (%(id)s, %(delivery_id)s, %(order_id)s, %(courier_id)s, %(delivery_address)s, %(delivery_ts)s, %(delivery_rate)s, %(delivery_sum)s, %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        delivery_address = EXCLUDED.delivery_address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        delivery_rate = EXCLUDED.delivery_rate,
                        delivery_sum = EXCLUDED.delivery_sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "id": dm_deliveries.id,
                    "delivery_id": dm_deliveries.delivery_id,
                    "order_id": dm_deliveries.order_id,
                    "courier_id": dm_deliveries.courier_id,
                    "delivery_address": dm_deliveries.delivery_address,
                    "delivery_ts": dm_deliveries.delivery_ts,
                    "delivery_rate": dm_deliveries.delivery_rate,
                    "delivery_sum": dm_deliveries.delivery_sum,
                    "tip_sum": dm_deliveries.tip_sum
                },
            )


class DmDeliveriesLoader:
    WF_KEY = "example_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveriesOriginRepository(pg_origin)
        self.stg = DmDeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_deliveries(self):
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
            load_queue = self.origin.list_dm_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_deliveries in load_queue:
                self.stg.insert_dm_deliveries(conn, dm_deliveries)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
