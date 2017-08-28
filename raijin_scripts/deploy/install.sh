#!/bin/bash

mkdir -p deploy

module load python3/3.5.2
python3 -m venv trigger_env
source trigger_env/bin/activate
pip install --upgrade pip
pip install ansible
pip install boto3
deactivate
