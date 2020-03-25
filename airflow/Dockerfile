FROM puckel/docker-airflow:latest

USER root

RUN pip install --upgrade apache-airflow[kubernetes,sentry,aws,password,ssh]

USER airflow
