from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date


class DmCourierLedgerObj(BaseModel):
    id: int
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class DmCourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_courier_ledger(self, dm_courier_ledger_threshold: int, limit: int) -> List[DmCourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(DmCourierLedgerObj)) as cur:
            cur.execute(
                """
with events as (
select product_id,
	   order_id,
	   "count",
	    price
from dds.fct_product_sales_cur fpsc 
),
orders as (
select dds.dm_orders.id--,
       --order_key
from dds.dm_orders
),
dm_products as (
select id,
       product_id,
       active_from
from dds.dm_products
),
product_sales as (
select p.id as product_id, 
	   o.id as order_id, 
	   e."count", 
	   e.price, 
       e.price * e."count" as total_sum
from events e
join orders o on o.id = e.order_id
join dm_products p on p.id = e.product_id
),
courier_ledger_1 as (
select do1.courier_id as courier_id,
       dc.courier_name as courier_name,
       dt."year" as settlement_year,
       dt."month" as settlement_month,
       count(do1.id) as orders_count,
       sum(ps.total_sum) as orders_total_sum,
       avg(dd.delivery_rate) as rate_avg,
       sum(ps.total_sum) * 0.25 as order_processing_fee,
       case
        	when avg(dd.delivery_rate) < 4 then 
            	case
                	when sum(ps.total_sum) * 0.05 < 100 then 100
                	else sum(ps.total_sum) * 0.05
            	end
       		when avg(dd.delivery_rate) >= 4 and avg(dd.delivery_rate) < 4.5 then 
            	case
                	when sum(ps.total_sum) * 0.07 < 150 then 150
                	else sum(ps.total_sum) * 0.07
            	end
       		when avg(dd.delivery_rate) >= 4.5 and avg(dd.delivery_rate) < 4.9 then 
            	case
                	when sum(ps.total_sum) * 0.08 < 175 then 175
                	else sum(ps.total_sum) * 0.08
            	end
       		else
            	case
                	when sum(ps.total_sum) * 0.10 < 200 then 200
                	else sum(ps.total_sum) * 0.10
            	end
    	end as courier_order_sum
from dds.dm_orders do1
left join dds.dm_couriers dc on do1.courier_id = dc.id
left join dds.dm_timestamps dt on do1.timestamp_id = dt.id
left join dds.dm_deliveries dd on do1.id = dd.order_id
left join product_sales ps on do1.id = ps.order_id
where do1.order_status = 'CLOSED' and dt."month" = extract(month from (now()at time zone 'utc')::date) - 1 
group by do1.courier_id, 
    	 dc.courier_name, 
         dt."year",
         dt."month"
),
courier_ledger_2 as (
select row_number() over () as id,
       cl1.courier_id,
       courier_name,
       settlement_year,
       settlement_month,
       orders_count,
       orders_total_sum,
       rate_avg,
       order_processing_fee,
       courier_order_sum,
       sum(dd.tip_sum) as courier_tips_sum,
       courier_order_sum + sum(dd.tip_sum) * 0.95 as courier_reward_sum
from courier_ledger_1 cl1
join dds.dm_deliveries dd on cl1.courier_id = dd.courier_id
group by cl1.courier_id, 
		 courier_name, 
		 settlement_year, 
		 settlement_month, 
		 orders_count, 
		 orders_total_sum, 
		 rate_avg, 
		 order_processing_fee, 
		 courier_order_sum
)
select *
from courier_ledger_2
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": dm_courier_ledger_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmCourierLedgerDestRepository:

    def insert_dm_courier_ledger(self, conn: Connection, dm_courier_ledger: DmCourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                        id, 
                        courier_id, 
                        courier_name, 
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                        )
                    VALUES (%(id)s, 
                            %(courier_id)s, 
                            %(courier_name)s, 
                            %(settlement_year)s, 
                            %(settlement_month)s, 
                            %(orders_count)s, 
                            %(orders_total_sum)s, 
                            %(rate_avg)s, 
                            %(order_processing_fee)s, 
                            %(courier_order_sum)s,
                            %(courier_tips_sum)s,
                            %(courier_reward_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name,
                        settlement_year = EXCLUDED.settlement_year,
                        settlement_month = EXCLUDED.settlement_month,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "id": dm_courier_ledger.id,
                    "courier_id": dm_courier_ledger.courier_id,
                    "courier_name": dm_courier_ledger.courier_name,
                    "settlement_year": dm_courier_ledger.settlement_year,
                    "settlement_month": dm_courier_ledger.settlement_month,
                    "orders_count": dm_courier_ledger.orders_count,
                    "orders_total_sum": dm_courier_ledger.orders_total_sum,
                    "rate_avg": dm_courier_ledger.rate_avg,
                    "order_processing_fee": dm_courier_ledger.order_processing_fee,
                    "courier_order_sum": dm_courier_ledger.courier_order_sum,
                    "courier_tips_sum": dm_courier_ledger.courier_tips_sum,
                    "courier_reward_sum": dm_courier_ledger.courier_reward_sum
                },
            )


class DmCourierLedgerLoader:
    WF_KEY = "example_courier_ledger_origin_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmCourierLedgerOriginRepository(pg_origin)
        self.stg = DmCourierLedgerDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_courier_ledger(self):
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
            load_queue = self.origin.list_dm_courier_ledger(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_courier_ledger to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_courier_ledger in load_queue:
                self.stg.insert_dm_courier_ledger(conn, dm_courier_ledger)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
