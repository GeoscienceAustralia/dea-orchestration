CURRENT_VERSION=1.10.10

build: # Build the Docker image for Airflow
	docker pull puckel/docker-airflow:latest
	docker build --tag geoscienceaustralia/airflow .

push: build # Push the Docker image for Airflow
	docker push geoscienceaustralia/airflow

push-version:
	docker tag geoscienceaustralia/airflow:latest geoscienceaustralia/airflow:${CURRENT_VERSION}
	docker push geoscienceaustralia/airflow:${CURRENT_VERSION}

fernet-key:
	docker run --rm geoscienceaustralia/airflow \
		python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
