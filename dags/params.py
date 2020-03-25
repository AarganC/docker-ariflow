from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime as dt
from datetime import timedelta
from decimal import Decimal
import datetime
import time

# Default DAG parameters
default_args = {'owner': 'airflow', 'depends_past': False, 'start_date': dt(2020, 3, 23),
                'retries': 0}

dag = DAG('params_example', default_args=default_args, schedule_interval='30 07 * * *')

url_awscli = Variable.get("url_awscli")
directory_dest =Variable.get("directory_dest")

# Confirm path not exist in HDFS before write
cmd = """
mkdir -p {} \
curl "{}"  -o "/tmp/awscli.zip" \
unzip /tmp/awscli.zip -d {} \
sudo {}aws/install \
rm /tmp/awscli.zip \
aws emr create-default-roles
""".format(directory_dest, directory_dest, directory_dest, url_awscli)

delete_hdfs = SSHOperator(
    ssh_conn_id='adaltas_ssh',
    task_id='delete_hdfs',
    command=cmd,
    dag=dag)
