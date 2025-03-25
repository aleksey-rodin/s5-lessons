import time
from datetime import datetime
import requests
import json
import pandas as pd
from typing import Optional, Union

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from lib import PgConnect

from stg.delivery_system.exceptions import GetCouriersDataException, GetDeliveriesDataException

# http_conn_id = HttpHook.get_connection('http_connection')
# api_key = http_conn_id.extra_dejson.get('api_key')
# base_url = http_conn_id.host

# postgres_conn_id = 'postgresql_de'

nickname = 'rodin-as'
cohort = '1'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

class GetDeliverySystemData:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
        self.http_conn_id = HttpHook.get_connection('http_connection')
        self.base_url = self.http_conn_id.host

    def get_couriers(self,
                     sort_field: Optional[str] = None,
                     sort_direction: Optional[str] = None,
                     limit: Optional[int] = None,
                     offset: Optional[int] = None
                     ):
        """Получает данные о курьерах из системы доставок."""

        url: str = (f'{self.base_url}/couriers?sort_field={sort_field}&sort_direction={sort_direction}' +
                    f'&limit={limit}&offset={offset}')
        print('get_couriers url:', url)

        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            response = json.loads(response.content)
            print(f'Response is {response}')
        except Exception as e:
            raise GetCouriersDataException(f'Error while receiving data: {e}')

    def get_deliveries(self,
                       restaurant_id: str = '',
                       date_from: Optional[datetime] = datetime(2000, 1, 1),
                       date_to: Optional[datetime] = datetime(2050, 1, 1),
                       sort_field: Optional[str] = None,
                       sort_direction: Optional[str] = None,
                       limit: Optional[int] = None,
                       offset: Optional[int] = None
                       ):
        """Получает данные о доставках из системы доставок."""
        date_from = datetime.strftime(date_from, '%Y-%m-%d+%H:%M:%S')
        date_to = datetime.strftime(date_to, '%Y-%m-%d+%H:%M:%S')

        url: str = (f'{self.base_url}/deliveries?restaurant_id={restaurant_id}&from={date_from}&to={date_to}' +
                    f'&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}')
        print('get_couriers url:', url)

        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            response = json.loads(response.content)
            print(f'Response is {response}')
        except Exception as e:
            raise GetDeliveriesDataException(f'Error while receiving data: {e}')
