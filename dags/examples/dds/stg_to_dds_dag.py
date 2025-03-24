import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_users import UserLoader
from examples.dds.dm_restaurants import RestaurantLoader
from examples.dds.dm_timestamps import TimestampLoader
from examples.dds.dm_products import ProductLoader
from examples.dds.dm_orders import OrderLoader
from examples.dds.fct_product_sales import SaleLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_to_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    # origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="fct_product_sales_load")
    def load_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SaleLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_sales()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    sales_dict = load_product_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    # [users_dict, restaurants_dict, timestamps_dict] >> [products_dict] >> orders_dict  # type: ignore
    users_dict >> orders_dict >> sales_dict
    restaurants_dict >> [products_dict, orders_dict] >> sales_dict
    timestamps_dict >> orders_dict >> sales_dict


dds_dag = stg_to_dds_dag()
