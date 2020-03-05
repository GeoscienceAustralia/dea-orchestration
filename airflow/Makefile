build: # Build the Docker image for Airflow
	docker build --tag geoscienceaustralia/airflow .

push: build # Push the Docker image for Airflow
	docker push geoscienceaustralia/airflow

fernet-key:
	docker run --rm geoscienceaustralia/airflow \
		python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"