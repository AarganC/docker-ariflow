from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime as dt
from datetime import timedelta

# Default DAG parameters
default_args = {'owner': 'airflow', 'depends_past': False, 'start_date': dt(2020, 3, 23),
                'retries': 0}

dag = DAG('task_example', default_args=default_args, schedule_interval='30 07 * * *')

run_script = BashOperator(task_id='show',
                         bash_command='aws configure list',
                         dag=dag)
