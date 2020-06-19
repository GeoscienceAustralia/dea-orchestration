FROM puckel/docker-airflow:latest

USER root

RUN pip install --upgrade apache-airflow[kubernetes,sentry,aws,password,ssh,google_auth,slack] airflow-exporter
RUN pip install shapely pyproj

USER airflow
