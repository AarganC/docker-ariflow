# Introducing Apache Airflow on AWS

This project provides you a container docker with Apache Airflow and AWS CLI.

The Airflow Dags of this project allow you to configure your container in order to be able to launch your jobs Spark on AWS EMR.

## Article
Found more information about this project in the articles :
- https://www.adaltas.com/en/2020/05/05/tutorial-apache-airflow-aws/
- https://medium.com/adaltas/introducing-apache-airflow-b504c3102fe1

## Run Container
Build container :
```bash
# clone the project
git clone https://github.com/AarganC/docker-ariflow
# go to the folder
cd docker-airflow/
# Create images
docker build --rm -t adaltas/docker-airflow .
# run Apacha Airflow in Local_Executor
docker-compose -f docker-compose-LocalExecutor.yml up -d
# visit localhost: 8080 in the browser for access to UI
```

## Dags Airflow
- **dags/aws_configure.py** : Configure the client AWS, create a bucket s3 and copy the file **python/sparky.py** in the bucket.
- **dags/show_aws_config.py** : Show the configuration of AWS CLI install on the container.
- **dags/variables.py** : Install AWS CLI on remote compute with `ssh`.
- **dags/xcom.py** : Run the job spark **python/sparky.py** on Cluster EMR.
