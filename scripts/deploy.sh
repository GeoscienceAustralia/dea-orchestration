#!/usr/bin/env bash

npm install -g serverless

cd lambda_functions/execute_ssh_command_js/ && npm install


serverless config credentials --provider aws --key ${aws_access_key_id} --secret ${aws_secret_access_key} --profile prodProfile

serverless deploy -v --stage prod


serverless invoke --log --stage prod --function git_pull_prod
