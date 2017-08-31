#!/bin/bash

module load python3/3.5.2
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install jinja2
pip install boto3
deactivate
