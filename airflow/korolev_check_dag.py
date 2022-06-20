import datetime

from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import utils.korolev_functions as funcs


@dag(start_date=datetime.datetime(1, 1, 1), catchup=False, schedule_interval='@daily', tags=['korolev'])
def korolev_check_dag():
    url = 'https://pokeapi.co/api/v2/generation'

    check = PythonOperator(
        task_id='Check',
        python_callable=funcs.check_generations,
        op_args=[url]
    )
    check


dag = korolev_check_dag()
