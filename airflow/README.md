
# Overview
Apache Airflow is a workflow tool.

# Todo
- Automate Docker image build and push

# Important links
- [Airflow Helm chart](https://github.com/helm/charts/blob/master/stable/airflow)
- [Airflow User Create](https://airflow.apache.org/docs/stable/cli.html#create_user) with correct DB connection set with `AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://<airflow_user>:<airflow_pw>@<airflow_hostname>:5432/<airflow_db> airflow create_user`
