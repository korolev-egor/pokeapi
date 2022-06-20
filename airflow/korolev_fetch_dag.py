import datetime

from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable

import utils.korolev_functions as funcs


@dag(start_date=datetime.datetime(1, 1, 1), schedule_interval=None, tags=['korolev'])
def korolev_fetch_dag():
    threads_number = 2
    sleep_duration = 0.2
    resources_limit_per_page = 2000
    poke_api_url = 'https://pokeapi.co/api/v2/'
    snowpipe_files = Variable.get('snowpipe_files')
    my_snowpipe = f'{snowpipe_files}Korolev/'
    unprocessed_files = Variable.get('unprocessed_files')
    my_unprocessed_folder = f'{unprocessed_files}Korolev/'
    
    kwargs = {
        'poke_api_url': poke_api_url,
        'sleep_duration': sleep_duration,
        'threads_number': threads_number,
        'processed_folder': my_snowpipe,
        'unprocessed_folder': my_unprocessed_folder,
        'resources_limit_per_page' : resources_limit_per_page,
    }

    fetch = PythonOperator(
        task_id='Fetch',
        python_callable=funcs.fetch_endpoints,
        op_kwargs=kwargs
    )
    process = PythonOperator(
        task_id='Process',
        python_callable=funcs.process_endpoints,
        op_kwargs=kwargs
    )
    cleanup = PythonOperator(
        task_id='Cleanup',
        python_callable=funcs.cleanup_unprocessed_folder,
        op_kwargs=kwargs
    )

    fetch >> process >> cleanup


dag = korolev_fetch_dag()
