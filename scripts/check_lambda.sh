#!/usr/bin/env bash

set -eu

cd "lambda_functions/$1"
_TMP="$(mktemp -d)"

# For now, lets be lazy and install requirements globally
if [[ -f requirements.txt ]]; then pip3 install -r requirements.txt; fi

# Install serverless requirements and run tests
npm install
npm test

# Attempt to package the lambda
echo "writing temporary serverless artifacts to ${_TMP}"
serverless package -s prod -p "${_TMP}"  # test prod setting
_RET=$?

# Cleanup test directory
rm -rf "${_TMP}"

if [[ ${_RET} -ne 0 ]]; then echo "serverless failed to generate a package" && exit 1; fi
