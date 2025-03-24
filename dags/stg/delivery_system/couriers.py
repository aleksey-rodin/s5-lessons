import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from lib import PgConnect

http_conn_id = HttpHook.get_connection('http_connection')
# api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

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

    def get_couriers(self):
        """Получает данные о курьерах из системы доставок."""
        print('Making request get_report')

        response = requests.get(f'{base_url}/couriers?sort_field=&sort_direction=&limit=&offset=', headers=headers)
        response.raise_for_status()
        response = json.loads(response.content)
        print(f'Response is {response}')

    def get_deliveries(self):
        """Получает данные о доставках из системы доставок."""
        print('Making request get_report')

        response = requests.get(f'{base_url}/deliveries?restaurant_id=626a81cfefa404208fe9abae&from=2024-01-01+01:01:01&to=2025-03-24+02:02:02&sort_field=&sort_direction=&limit=&offset=', headers=headers)
        response.raise_for_status()
        response = json.loads(response.content)
        print(f'Response is {response}')
