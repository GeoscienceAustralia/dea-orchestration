#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x
{
    readarray -t PY_FILES < <(find raijin_scripts lambda_functions ! -path '*node_modules*' ! -path '*.serverless*' -name '*.py')
} &> /dev/null

# Lint and formatting check of all python files in lambdas and raijin scripts
pycodestyle "${PY_FILES[@]}"
pylint -j 2 --reports no "${PY_FILES[@]}"

# Lint all YAML files
find . \( -iname '*.yaml' -o -iname '*.yml' \) ! -path '*node_modules*' -print0 | xargs -0 yamllint

# Run shellcheck on all raijin shell scripts
readarray -t SHELL_SCRIPTS < <(find scripts raijin_scripts -type f -exec file {} \; | grep "Bourne-Again shell" | cut -d: -f1)
shellcheck -e SC1071,SC1090,SC1091 "${SHELL_SCRIPTS[@]}"

# Run tests on raijin python scripts
find raijin_scripts -name '*.py' -print0 | xargs -0 pytest -r sx --doctest-ignore-import-errors --durations=5 "$@"

# Fix for problem with the moto python package, See https://stackoverflow.com/questions/38783140/importerror-no-module-named-google-compute-engine
export BOTO_CONFIG=/dev/null

# Check each Serverless AWS lambda function
for lambda_dir in "$PWD"/lambda_functions/*
do
    pushd "$lambda_dir"
    _TMP="$(mktemp -d)"

    # For now, lets be lazy and install requirements globally
    if [[ -f requirements.txt ]]; then pip install -r requirements.txt; fi

    # Install serverless requirements and run tests
    npm install && npm test


    # Attempt to package the lambda
    echo "writing temporary serverless artifacts to ${_TMP}"
    serverless package -s prod -p "${_TMP}"  # test prod setting
    _RET=$?

    # Cleanup test directory
    rm -rf "${_TMP}"

    if [[ ${_RET} -ne 0 ]]; then echo "serverless failed to generate a package" && exit 1; fi

    popd
done
