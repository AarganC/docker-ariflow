from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator
from datetime import datetime as dt
from datetime import timedelta


# Default DAG parameters
default_args = {'owner': 'airflow', 'depends_past': False, 'start_date': dt(2020, 3, 23),
                'retries': 0, 'sla': timedelta(hours=1)}

dag = DAG('params_example', default_args=default_args, schedule_interval='30 07 * * *')


cmd = """
# Set aws credential
aws configure set aws_access_key_id {{ params.secret_access_key }}
aws configure set aws_secret_access_key {{ params.secret_key }}
aws configure set region {{ params.region }}
aws configure set output_format {{ params.output_format }}

# Create bucketif not exist
if aws s3 ls "s3://{{ params.bucket_log }}" 2>&1 | grep -q 'NoSuchBucket'
then
    aws s3api create-bucket --bucket {{ params.bucket_log }} --region {{ params.region }}
fi
if aws s3 ls "s3://$bucket_pyton" 2>&1 | grep -q 'NoSuchBucket'
then
    aws s3api create-bucket --bucket {{ params.bucket_pyton }} --region {{ params.region }}
fi
if aws s3 ls "s3://{{ params.bucket_pyton }}" | grep -q 'sparky.py'
then
    aws s3 cp /usr/local/airflow/python/sparky.py s3://{{ params.bucket_pyton }}/
fi
"""
cmd = BashOperator(task_id='aws_configure',
                   bash_command=cmd,
                   params={"secret_access_key" : Variable.get("secret_access_key"), # AWS Access Key ID
                           "secret_key" : Variable.get("secret_key"), # AWS Secret Access Key
                           "region" : Variable.get("region"), # Default region name
                           "output_format" : Variable.get("output_format"), # Default output format
                           "bucket_log" : Variable.get("bucket_log"), # Bucket S3 for logs
                           "bucket_pyton" : Variable.get("bucket_pyton") # Bucket s3 for python
                           },
                   dag=dag)
