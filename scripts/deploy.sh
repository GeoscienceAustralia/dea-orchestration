#!/usr/bin/env bash

set -eu

cd "lambda_functions/$1"

npm install -g serverless


echo
echo =======================
echo Attempting to deploy "$1"
echo =======================
echo

npm install
serverless deploy -v --stage prod
