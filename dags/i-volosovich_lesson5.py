"""
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from i_volosovich_plugins.i_volosovich_top_locations_operator import i_volosovich_push_top_locations_into_gp

DEFAULT_ARGS = {
    'owner': 'i-volosovich',
    'poke_interval': 600,
    'start_date': days_ago(2),
}

with DAG(
    dag_id = 'i-volosovich_lesson5',
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-volosovich']
) as dag:

    start_Dummy = DummyOperator(task_id = 'start_Dummy')

    delete_data_in_table = PostgresOperator(
        task_id='delete_data_in_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE "i-volosovich_ram_location";'
    )

    push_top_locations_into_gp = i_volosovich_push_top_locations_into_gp(
        task_id = 'push_top_locations_into_gp',
        n_rows = 3
    )

    end_Dummy = DummyOperator(task_id='end_Dummy')

    start_Dummy >> delete_data_in_table >> push_top_locations_into_gp >> end_Dummy
