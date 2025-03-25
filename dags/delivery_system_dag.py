import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder, MongoConnect
from stg.delivery_system.couriers import GetDeliverySystemData

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 3, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'delivery_system', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_delivery_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    get_delivery_data = GetDeliverySystemData(dwh_pg_connect)

    @task()
    def load_couriers():
        get_delivery_data.get_couriers(sort_field='name', limit=3)

    @task()
    def load_deliveries():
        get_delivery_data.get_deliveries()

    load_couriers_task = load_couriers()
    load_deliveries_task = load_deliveries()

    [load_couriers_task, load_deliveries_task]


stg_delivery_dag = stg_delivery_system_dag()


    # @task()
    # def load_restaurants():
    #     # Инициализируем класс, в котором реализована логика сохранения.
    #     pg_saver = PgSaver()
    #
    #     # Инициализируем подключение у MongoDB.
    #     mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    #
    #     # Инициализируем класс, реализующий чтение данных из источника.
    #     collection_reader = RestaurantReader(mongo_connect)
    #
    #     # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
    #     loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
    #
    #     # Запускаем копирование данных.
    #     loader.run_copy()
    #
    # @task()
    # def load_users():
    #     pg_saver = PgSaver()
    #     mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    #     collection_reader = UserReader(mongo_connect)
    #     loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
    #     loader.run_copy()
    #
    # @task()
    # def load_orders():
    #     pg_saver = PgSaver()
    #     mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    #     collection_reader = OrderReader(mongo_connect)
    #     loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)
    #     loader.run_copy()

    # restaurant_loader = load_restaurants()
    # user_loader = load_users()
    # order_loader = load_orders()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    # [restaurant_loader, user_loader, order_loader]  # type: ignore


# order_stg_dag = stg_delivery_system_dag()  # noqa
