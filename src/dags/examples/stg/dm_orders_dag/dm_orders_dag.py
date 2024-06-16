import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.dm_orders_dag.orders_loader import DmOrdersLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/50 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_dm_orders_dag():
    # Создаем подключение к базе dds.
    dwh_dds_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе stg.
    dwh_stg_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_orders_load")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmOrdersLoader(dwh_stg_pg_connect, dwh_dds_pg_connect, log)
        rest_loader.load_dm_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    dm_orders_dict = load_dm_orders()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_orders_dict  # type: ignore


stg_dds_dm_orders_dag = sprint5_example_dds_dm_orders_dag()
