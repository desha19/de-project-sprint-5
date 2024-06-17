import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.fct_product_sales_cur_dag.product_sales_cur_loader import FctProductSalesCurLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_fct_product_sales_cur_dag():
    # Создаем подключение к базе dds.
    dwh_dds_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе stg.
    dwh_stg_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="fct_product_sales_cur_load")
    def load_fct_product_sales_cur():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FctProductSalesCurLoader(dwh_stg_pg_connect, dwh_dds_pg_connect, log)
        rest_loader.load_fct_product_sales_cur()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fct_product_sales_dict = load_fct_product_sales_cur()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    fct_product_sales_dict  # type: ignore


stg_dds_fct_product_sales_dag = sprint5_example_dds_fct_product_sales_cur_dag()
