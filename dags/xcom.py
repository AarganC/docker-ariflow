from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime as dt
from datetime import timedelta
import time
import json

# Default DAG parameters
default_args = {'owner': 'airflow', 'depends_past': False, 'start_date': dt(2020, 3, 23),
                'retries': 0}

dag = DAG('xcom_example', default_args=default_args, schedule_interval='30 07 * * *')

driver_cores = Variable.get("driver_cores")
driver_memory = Variable.get("driver_memory")
executor_memory = Variable.get("executor_memory")
executor_cores = Variable.get("executor_cores")
bucket_pyton = Variable.get("bucket_pyton")

start_emr = """
cluster_id=`aws emr create-cluster \
--release-label emr-5.14.0 \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large InstanceGroupType=CORE,InstanceCount=1,InstanceType=m4.large \
--use-default-roles \
--applications Name=Spark
--log-uri s3://aws-emr-airflow \
--ec2-attributes KeepJobFlowAliveWhenNoSteps=no
--auto-terminate`
echo $cluster_id
"""

def parse_emr_id(**kwargs):
    ti = kwargs['ti']
    json_str = ti.xcom_pull(key="return_value", task_ids="start_emr")
    cluster_id = json.loads(json_str)['ClusterId']
    ti.xcom_push(key="emr_cluster_id", value=cluster_id)


add_step='bc=' + str(bucket_pyton) + ' dc=' + str(driver_cores) + ' dm=' + \
str(driver_memory) + ' em=' + str(executor_memory) + ' ec=' + str(executor_cores) + '''
cluster_id="{{ ti.xcom_pull(key="emr_cluster_id", task_ids="clean_emr_id") }}"
echo $cluster_id
aws emr add-steps --cluster-id $cluster_id --steps Type=spark,Name=pyspark_job,\
Jar="command-runner.jar",\
Args=[\
--deploy-mode,client,\
s3://$bc/sparky.py\
],ActionOnFailure=TERMINATE_CLUSTER
'''
# .format(bucket_pyton, driver_cores, driver_memory, executor_memory, executor_cores)

start_emr = BashOperator(task_id='start_emr',
                         bash_command=start_emr,
                         provide_context=True,
                         xcom_push=True,
                         dag=dag)

clean_emr_id = PythonOperator(task_id='clean_emr_id',
                              python_callable=parse_emr_id,
                              provide_context=True,
                              dag=dag)

add_step = BashOperator(task_id='add_step',
                        bash_command=add_step,
                        provide_context=True,
                        dag=dag)

add_step.set_upstream(clean_emr_id)
clean_emr_id.set_upstream(start_emr)
