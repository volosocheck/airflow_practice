"""
DAG забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds
кроме воскресенья, выводит регультат рыботы в логах
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from datetime import datetime
from pytz import timezone

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator



DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-volosovich',
    'poke_interval': 600
}

with DAG("i-volosovich_lesson4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-volosovich']
) as dag:

    start_dummy = DummyOperator(task_id="start_dummy")

    def is_not_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').isoweekday()
        return exec_day != 7


    is_not_sunday = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    not_sunday_dummy = DummyOperator(task_id='not_sunday_dummy')

    def take_heading_from_greenplum_func(execution_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        exec_day = datetime.strptime(execution_date, '%Y-%m-%d').isoweekday()
        sql = f"SELECT heading FROM articles WHERE id = '{exec_day}'"
        result = pg_hook.get_records(sql)
        headings = [str(row[0]) for row in result]
        logging.info(', '.join(headings))


    logging_heading_from_greenplum = PythonOperator(
        task_id='take_heading_from_greenplum',
        python_callable=take_heading_from_greenplum_func,
        op_kwargs={'execution_date': '{{ ds }}'}
    )


    start_dummy >> is_not_sunday >> not_sunday_dummy >> logging_heading_from_greenplum