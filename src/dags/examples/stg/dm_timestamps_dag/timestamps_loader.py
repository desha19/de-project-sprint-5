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


class DmTimestampsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class DmTimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_timestamps(self, dm_timestamps_threshold: int, limit: int) -> List[DmTimestampsObj]:
        with self._db.client().cursor(row_factory=class_row(DmTimestampsObj)) as cur:
            cur.execute(
                """
                    with date_time as (
                    select id,
                           to_char(update_ts, 'YYYY-MM-DD HH24:MI:SS') as ts,
                           extract(year from update_ts) as "year",
                           extract(month from update_ts) as "month",
                           extract(day from update_ts) as "day",
                           update_ts::date as "date",
                           to_char(update_ts::time, 'HH24:MI:SS') as "time"
                    from stg.ordersystem_orders
                    order by id asc
                    )
                    select *
                    from date_time
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": dm_timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

####### ОСТАНОВИЛСЯ ТУТ #######
class DmTimestampsDestRepository:

    def insert_dm_timestamps(self, conn: Connection, dm_timestamps: DmTimestampsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps (id, ts, year, month, day, date, time)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "id": dm_timestamps.id,
                    "ts": dm_timestamps.ts,
                    "year": dm_timestamps.year,
                    "month": dm_timestamps.month,
                    "day": dm_timestamps.day,
                    "date": dm_timestamps.date,
                    "time": dm_timestamps.time
                },
            )


class DmTimestampsLoader:
    WF_KEY = "example_timestamps_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmTimestampsOriginRepository(pg_origin)
        self.stg = DmTimestampsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_timestamps(self):
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
            load_queue = self.origin.list_dm_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dm_timestamps in load_queue:
                self.stg.insert_dm_timestamps(conn, dm_timestamps)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
