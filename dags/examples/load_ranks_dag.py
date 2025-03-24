import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_users_dag.users_loader import UserLoader
from examples.stg.bonus_system_users_dag.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


def say_hello(log: logging.Logger) -> None:
    log.info("Hello Worlds!!")


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example', 'hello_world'],
    is_paused_upon_creation=False
)

def stg_bonus_system_ranks_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect)
        user_loader.load_users()

    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    # Инициализируем объявленные таски.
    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [ranks_dict, users_dict, events_dict]  # type: ignore


stg_bonus_system_ranks_dag = stg_bonus_system_ranks_dag()
