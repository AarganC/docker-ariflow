from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime as dt

# Default DAG parameters
default_args = {'owner': 'airflow', 'depends_past': False, 'start_date': dt(2020, 3, 23),
                'retries': 0}

dag = DAG('variable_example', default_args=default_args, schedule_interval='30 07 * * *')

url_awscli = Variable.get("url_awscli")
directory_dest =Variable.get("directory_dest")

# Install aws CLI in ssh
cmd = """
mkdir -p {} \
curl "{}"  -o "/tmp/awscli.zip" \
unzip /tmp/awscli.zip -d {} \
sudo {}aws/install \
rm /tmp/awscli.zip \
aws emr create-default-roles
""".format(directory_dest, url_awscli, directory_dest, directory_dest)

install_aws = SSHOperator(
    ssh_conn_id='adaltas_ssh',
    task_id='install_aws',
    command=cmd,
    dag=dag)
