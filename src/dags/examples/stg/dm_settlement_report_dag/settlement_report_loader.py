from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date


class DmSettlementReportObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class DmSettlementReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_settlement_report(self, dm_settlement_report_threshold: int, limit: int) -> List[DmSettlementReportObj]:
        with self._db.client().cursor(row_factory=class_row(DmSettlementReportObj)) as cur:
            cur.execute(
                """
with settlement_report_1 as (
select dr.restaurant_id as restaurant_id,
	   dr.restaurant_name as restaurant_name,
	   dt."date" as settlement_date,
	   count(distinct do2.id) as orders_count,
	   sum(fps.total_sum) as orders_total_sum,
	   sum(fps.bonus_payment) as orders_bonus_payment_sum,
	   sum(fps.bonus_grant) as orders_bonus_granted_sum
--from dds.dm_restaurants dr 
from dds.dm_orders do2 
--left join dds.dm_orders do2 on dr.id = do2.restaurant_id
left join dds.dm_restaurants dr on do2.restaurant_id = dr.id
left join dds.fct_product_sales fps on do2.id = fps.order_id 
left join dds.dm_timestamps dt on do2.timestamp_id = dt.id
where do2.order_status = 'CLOSED' 
	  and
      (dt."date" >= (now()at time zone 'utc')::date - 3
      and
      dt."date" <= (now()at time zone 'utc')::date - 1)
group by dr.restaurant_id,
 		 dr.restaurant_name,
 		 dt."date"
),
settlement_report_2 as (
select restaurant_id,
	   restaurant_name,
	   settlement_date,
	   orders_count,
	   orders_total_sum,
	   orders_bonus_payment_sum,
	   orders_bonus_granted_sum,
	   orders_total_sum * 0.25 as order_processing_fee
from settlement_report_1 sr1
),
settlement_report_3 as (
select row_number() over (order by settlement_date desc) - 1 as id,
	   restaurant_id,
	   restaurant_name,
	   settlement_date,
	   orders_count,
	   orders_total_sum,
	   orders_bonus_payment_sum,
	   orders_bonus_granted_sum,
	   order_processing_fee,
	   orders_total_sum - orders_bonus_payment_sum - order_processing_fee as restaurant_reward_sum
from settlement_report_2
order by settlement_date desc
)
select *
from settlement_report_3
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": dm_settlement_report_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmSettlementReportDestRepository:

    def insert_dm_settlement_report(self, conn: Connection, dm_settlement_report: DmSettlementReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(
                        id, 
                        restaurant_id, 
                        restaurant_name, 
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                        )
                    VALUES (%(id)s, 
                            %(restaurant_id)s, 
                            %(restaurant_name)s, 
                            %(settlement_date)s, 
                            %(orders_count)s, 
                            %(orders_total_sum)s, 
                            %(orders_bonus_payment_sum)s, 
                            %(orders_bonus_granted_sum)s, 
                            %(order_processing_fee)s, 
                            %(restaurant_reward_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        settlement_date = EXCLUDED.settlement_date,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "id": dm_settlement_report.id,
                    "restaurant_id": dm_settlement_report.restaurant_id,
                    "restaurant_name": dm_settlement_report.restaurant_name,
                    "settlement_date": dm_settlement_report.settlement_date,
                    "orders_count": dm_settlement_report.orders_count,
                    "orders_total_sum": dm_settlement_report.orders_total_sum,
                    "orders_bonus_payment_sum": dm_settlement_report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": dm_settlement_report.orders_bonus_granted_sum,
                    "order_processing_fee": dm_settlement_report.order_processing_fee,
                    "restaurant_reward_sum": dm_settlement_report.restaurant_reward_sum
                },
            )


class DmSettlementReportLoader:
    WF_KEY = "example_settlement_report_origin_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 15  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmSettlementReportOriginRepository(pg_origin)
        self.stg = DmSettlementReportDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_settlement_report(self):
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
            load_queue = self.origin.list_dm_settlement_report(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_settlement_report to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_settlement_report in load_queue:
                self.stg.insert_dm_settlement_report(conn, dm_settlement_report)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
