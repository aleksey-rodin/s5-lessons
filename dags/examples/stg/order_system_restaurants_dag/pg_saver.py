from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, id: str, collection_name:str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        if collection_name == 'restaurants':
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
        elif collection_name == 'users':
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
        elif collection_name == 'orders':
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
