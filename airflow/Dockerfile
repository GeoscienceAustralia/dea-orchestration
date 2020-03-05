FROM puckel/docker-airflow:latest

USER root

RUN pip install --upgrade apache-airflow[kubernetes,gcp,password]

USER airflow
