#!/usr/bin/env bash

set -eu

npm install -g serverless

serverless config credentials --provider aws --key "${AWS_ACCESS_KEY_ID}" \
--secret "${AWS_SECRET_ACCESS_KEY}" --profile prodProfile --overwrite

for d in lambda_functions/*; do
    pushd "${d}" || exit 1

    echo
    echo #######################
    echo Attempting to deploy "$d"
    echo #######################
    echo

    npm install
    serverless deploy -v --stage prod

    popd || exit 1
done

cd lambda_functions/execute_ssh_command_js/ || exit 1

serverless invoke --log --stage prod --function git_pull_prod
