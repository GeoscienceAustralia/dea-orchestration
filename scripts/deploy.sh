#!/usr/bin/env bash

npm install -g serverless

cd lambda_functions/execute_ssh_command_js/ && npm install

serverless config credentials --provider aws --key "${AWS_ACCESS_KEY_ID}" --secret "${AWS_SECRET_ACCESS_KEY}" --profile prodProfile


serverless deploy -v --stage prod


serverless invoke --log --stage prod --function git_pull_prod
