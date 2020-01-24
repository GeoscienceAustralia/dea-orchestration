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
pushd raijin_scripts
find . -name '*.py' \! -name s2_to_s3_rolling.py -print0 | xargs -0 pytest -r sx --doctest-ignore-import-errors --cov=. --durations=5 "$@"
popd

# Fix for problem with the moto python package, See https://stackoverflow.com/questions/38783140/importerror-no-module-named-google-compute-engine
export BOTO_CONFIG=/dev/null

find . -name .coverage -print0 | xargs -0 coverage combine
