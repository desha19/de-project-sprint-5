from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctProductSalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class FctProductSalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct_product_sales(self, fct_product_sales_threshold: int, limit: int) -> List[FctProductSalesObj]:
        with self._db.client().cursor(row_factory=class_row(FctProductSalesObj)) as cur:
            cur.execute(
                """
with events as (
select json_array_elements((event_value::json->>'product_payments')::json)->>'product_id' as product_id,
	   event_value::json->>'order_id' as order_id,
	   (json_array_elements((event_value::json->>'product_payments')::json)->>'quantity')::numeric(19, 5) as "count",
	   (json_array_elements((event_value::json->>'product_payments')::json)->>'price')::numeric(19, 5) as price,
	   --(json_array_elements((event_value::json->>'product_payments')::json)->>'quantity')::numeric(19, 5) *
	   --(json_array_elements((event_value::json->>'product_payments')::json)->>'quantity')::numeric(19, 5) as total_sum,
	   (json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_payment')::numeric(19, 5) as bonus_payment,
	   (json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_grant')::numeric(19, 5) as bonus_grant
from stg.bonussystem_events
),
orders as (
select dds.dm_orders.id,
       order_key,
       dt.ts
from dds.dm_orders
left join dds.dm_timestamps dt on dds.dm_orders.timestamp_id = dt.id 
),
dm_products as (
select id,
       product_id,
       active_from
from dds.dm_products
),
product_sales as (
select row_number() over (order by o.id) - 1 as id, 
	   p.id as product_id, 
	   o.id as order_id, 
	   e."count", 
	   e.price, 
       e.price * e."count" as total_sum,
	   --e.total_sum, 
	   e.bonus_payment, 
	   e.bonus_grant,
	   ts
from events e
JOIN orders o on o.order_key = e.order_id
JOIN dm_products p on p.product_id = e.product_id
WHERE ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 3 AND (now() AT TIME ZONE 'utc')::date - 1  
)
select *
from product_sales ps
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": fct_product_sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class FctProductSalesDestRepository:

    def insert_fct_product_sales(self, conn: Connection, fct_product_sales: FctProductSalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(id)s, %(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "id": fct_product_sales.id,
                    "product_id": fct_product_sales.product_id,
                    "order_id": fct_product_sales.order_id,
                    "count": fct_product_sales.count,
                    "price": fct_product_sales.price,
                    "total_sum": fct_product_sales.total_sum,
                    "bonus_payment": fct_product_sales.bonus_payment,
                    "bonus_grant": fct_product_sales.bonus_grant
                },
            )


class FctProductSalesLoader:
    WF_KEY = "example_product_sales_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 2416  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctProductSalesOriginRepository(pg_origin)
        self.stg = FctProductSalesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct_product_sales(self):
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
            load_queue = self.origin.list_fct_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct_product_sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct_product_sales in load_queue:
                self.stg.insert_fct_product_sales(conn, fct_product_sales)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
