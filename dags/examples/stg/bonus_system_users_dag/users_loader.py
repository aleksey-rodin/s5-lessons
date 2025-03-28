from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    order_user_id: str


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_user_id
                    FROM users;
                """
            )
            objs = cur.fetchall()
        return objs


class UserDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_users(self, users: List[UserObj]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for user in users:
                    cur.execute(
                        """
                            INSERT INTO stg.bonussystem_users(id, order_user_id)
                            VALUES (%(id)s, %(order_user_id)s)
                            ON CONFLICT (id) DO NOTHING;
                        """,
                        {
                            "id": user.id,
                            "order_user_id": user.order_user_id
                        },
                    )
                conn.commit()


class UserLoader:
    WF_KEY = "ranks_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect) -> None:
        self.origin = UsersOriginRepository(pg_origin)
        self.stg = UserDestRepository(pg_dest)

    def load_users(self):
        load_queue = self.origin.list_users()
        self.stg.insert_users(load_queue)
